//! AQL v0.13 query planner: parsed statements into executable plans.
//!
//! RECALL is the only read verb; the plan picks the access path from what is asked
//! (`spec/AQL_SPEC_v0.13.md` §1): FROM WORKING is the session's state (scan), FROM
//! TOOLS ranks tools (load), `WHERE key = v` alone is an exact lookup, `LIKE "text"`
//! on PROCEDURAL matches procedures, anything else a filtered or similarity read.

use adb_core::{
    Condition as CoreCondition, LogicalOp as CoreLogicalOp, MemoryType as CoreMemoryType,
    Modifiers as CoreModifiers, Operator as CoreOperator, OrderBy as CoreOrderBy,
    Predicate as CorePredicate, Scope as CoreScope, Value as CoreValue, Window as CoreWindow,
    AggregateFunc as CoreAggregateFunc, AggregateFuncType as CoreAggregateFuncType,
};
use aql_parser::ast::{
    AggregateFunc, AggregateFuncType, Condition, FieldAssignment, ForgetStmt, LinkStmt, LogicalOp, MemoryType, Modifiers,
    Operator, PipelineStmt, Predicate, RecallStmt, Statement, StoreStmt, SubscribeTarget, UnlinkStmt, UpdateStmt, Value,
    Window,
};

use crate::error::{PlanError, PlanResult};
use crate::plan::{
    ExecutionPlan, FanOutPlan, LinkData, NamespacePlan, Operation, PipelinePlan, PipelineStage, StepPlan, StoreData,
    SubscriptionPlan, SubscriptionTarget,
};

/// The query planner
pub struct Planner {
    /// Default scope for operations
    default_scope: CoreScope,
    /// Default namespace
    default_namespace: Option<String>,
}

impl Planner {
    /// Create a new planner with defaults
    pub fn new() -> Self {
        Self { default_scope: CoreScope::Private, default_namespace: None }
    }

    /// Create a planner with custom defaults
    pub fn with_defaults(scope: CoreScope, namespace: Option<String>) -> Self {
        Self { default_scope: scope, default_namespace: namespace }
    }

    /// Plan a statement
    pub fn plan(&self, stmt: &Statement) -> PlanResult<ExecutionPlan> {
        match stmt {
            // A script's USE is applied by aql_parser::parse_script; alone it plans nothing.
            Statement::Use(_) => Err(PlanError::NotImplemented("USE outside a script or session (the engine applies it)")),
            Statement::Pipeline(p) => self.plan_pipeline(p),
            Statement::Recall(r) => self.plan_recall(r),
            Statement::Store(s) => Ok(ExecutionPlan::Single(self.plan_store(s)?)),
            Statement::Update(u) => Ok(ExecutionPlan::Single(self.plan_update(u)?)),
            Statement::Forget(f) => self.plan_forget(f),
            Statement::Link(l) => Ok(ExecutionPlan::Single(self.plan_link(l)?)),
            Statement::Unlink(u) => Ok(ExecutionPlan::Single(self.plan_unlink(u)?)),
            Statement::Branch(n) => Ok(ExecutionPlan::Namespace(NamespacePlan::Branch {
                from: n.from.clone(),
                into: n.into.clone(),
                as_of: n.as_of.as_ref().map(convert_time_point),
            })),
            Statement::Fork(n) => Ok(ExecutionPlan::Namespace(NamespacePlan::Fork {
                from: n.from.clone(),
                into: n.into.clone(),
                as_of: n.as_of.as_ref().map(convert_time_point),
            })),
            Statement::Merge(m) => Ok(ExecutionPlan::Namespace(NamespacePlan::Merge {
                from: m.from.clone(),
                into: m.into.clone(),
                prefer: m.prefer.map(|p| matches!(p, aql_parser::ast::Prefer::Source)),
            })),
            Statement::Subscribe(s) => match &s.target {
                SubscribeTarget::Topic(t) => Ok(ExecutionPlan::Subscription(SubscriptionPlan::Subscribe {
                    target: SubscriptionTarget::Topic(crate::plan::namespaced_topic(s.namespace.as_deref(), t)),
                    kinds: Vec::new(),
                    filter: s.filter.as_ref().map(|c| convert_conditions(c)).transpose()?,
                    from: None,
                })),
                // A live query is registered by the engine (its continuous queries).
                SubscribeTarget::Query(_) => Err(PlanError::NotImplemented("SUBSCRIBE RECALL (the engine registers live queries)")),
            },
            Statement::Unsubscribe(u) => {
                Ok(ExecutionPlan::Subscription(SubscriptionPlan::Unsubscribe { subscription: u.subscription.clone(), namespace: u.namespace.clone() }))
            }
            Statement::Publish(p) => Ok(ExecutionPlan::Subscription(SubscriptionPlan::Publish {
                topic: crate::plan::namespaced_topic(p.modifiers.namespace.as_deref(), &p.topic),
                payload: assignments_to_json(&p.payload),
                ttl: p.modifiers.ttl,
            })),
        }
    }

    /// Plan a pipeline: RECALL stages, a write last.
    fn plan_pipeline(&self, stmt: &PipelineStmt) -> PlanResult<ExecutionPlan> {
        if stmt.stages.is_empty() {
            return Err(PlanError::EmptyPipeline);
        }
        let mut stages = Vec::new();
        for stage in &stmt.stages {
            match self.plan(stage)? {
                ExecutionPlan::Single(step) => stages.push(PipelineStage::Step(step)),
                ExecutionPlan::Pipeline(p) => stages.extend(p.stages),
                ExecutionPlan::FanOut(_) => {
                    return Err(PlanError::UnsupportedOperation { op: "RECALL FROM ALL in PIPELINE".to_string(), memory_type: "ALL".to_string() })
                }
                _ => return Err(PlanError::UnsupportedOperation { op: "this statement in PIPELINE".to_string(), memory_type: "-".to_string() }),
            }
        }
        Ok(ExecutionPlan::Pipeline(PipelinePlan { name: stmt.name.clone(), timeout: stmt.timeout, stages }))
    }

    /// RECALL: the access path from what is asked.
    fn plan_recall(&self, stmt: &RecallStmt) -> PlanResult<ExecutionPlan> {
        if !stmt.modifiers.namespaces.is_empty() {
            return Err(PlanError::NotImplemented("NAMESPACE (…) — a read across namespaces runs on the engine"));
        }
        if stmt.modifiers.with_history.as_ref().is_some_and(|h| h.between.is_some()) {
            return Err(PlanError::NotImplemented("WITH HISTORY BETWEEN"));
        }
        let modifiers = convert_modifiers(&stmt.modifiers)?;
        let predicate = convert_predicate(&stmt.predicate, stmt.memory_type, stmt.modifiers.confidence)?;
        if stmt.memory_type == MemoryType::All {
            return Ok(ExecutionPlan::FanOut(FanOutPlan { operation: Operation::Recall, predicate, modifiers, data: None }));
        }
        let memory = convert_memory_type(stmt.memory_type)?;
        let history = modifiers.as_of.is_some() || modifiers.with_history;
        let key = match &predicate {
            CorePredicate::Where { conditions } if modifiers.aggregate.is_none() => exact_key(conditions),
            _ => None,
        };
        if let Some((field, value)) = key.filter(|_| !(stmt.memory_type == MemoryType::Tools && !history)) {
            return Ok(ExecutionPlan::Single(StepPlan::lookup(memory, CorePredicate::Key { field, value }, modifiers)));
        }
        Ok(ExecutionPlan::Single(match (stmt.memory_type, &predicate) {
            // The session's state, newest first, over an optional window.
            (MemoryType::Working, CorePredicate::All) if !history => StepPlan::scan(modifiers),
            // Tools ranked for the task.
            (MemoryType::Tools, CorePredicate::Where { .. } | CorePredicate::All) if !history && modifiers.aggregate.is_none() => {
                StepPlan::load(predicate, modifiers)
            }
            (_, CorePredicate::Pattern { .. }) => StepPlan::lookup(memory, predicate, modifiers),
            _ => StepPlan::recall(memory, predicate, modifiers),
        }))
    }

    fn plan_store(&self, stmt: &StoreStmt) -> PlanResult<StepPlan> {
        let memory_type = convert_memory_type(stmt.memory_type)?;
        let modifiers = convert_modifiers(&stmt.modifiers)?;
        let key = stmt
            .payload
            .iter()
            .find(|f| f.field == "key" || f.field == "id")
            .map(|f| value_to_string(&f.value))
            .unwrap_or_else(|| ulid::Ulid::new().to_string()); // D3 S1
        let store_data = StoreData {
            key,
            payload: assignments_to_json(&stmt.payload),
            scope: modifiers.scope.unwrap_or(self.default_scope),
            namespace: modifiers.namespace.clone().or_else(|| self.default_namespace.clone()),
            in_scope: modifiers.in_scope.clone(),
            ttl: modifiers.ttl,
            fact: None,
        };
        Ok(StepPlan::store(memory_type, store_data, modifiers))
    }

    fn plan_update(&self, stmt: &UpdateStmt) -> PlanResult<StepPlan> {
        let memory_type = convert_memory_type(stmt.memory_type)?;
        let predicate = CorePredicate::Where { conditions: convert_conditions(&stmt.conditions)? };
        let modifiers = convert_modifiers(&stmt.modifiers)?;
        Ok(StepPlan::update(memory_type, predicate, assignments_to_json(&stmt.payload), modifiers))
    }

    fn plan_forget(&self, stmt: &ForgetStmt) -> PlanResult<ExecutionPlan> {
        let predicate = match stmt.conditions.is_empty() {
            true => CorePredicate::All,
            false => CorePredicate::Where { conditions: convert_conditions(&stmt.conditions)? },
        };
        let modifiers = convert_modifiers(&stmt.modifiers)?;
        if stmt.memory_type == MemoryType::All {
            return Ok(ExecutionPlan::FanOut(FanOutPlan { operation: Operation::Forget, predicate, modifiers, data: None }));
        }
        let memory_type = convert_memory_type(stmt.memory_type)?;
        Ok(ExecutionPlan::Single(StepPlan::forget(memory_type, predicate, modifiers)))
    }

    /// The two ends of a LINK / UNLINK.
    fn link_ends(&self, from: MemoryType, from_conditions: &[Condition], to: MemoryType, to_conditions: &[Condition], verb: &str) -> PlanResult<LinkData> {
        let from_type = convert_memory_type(from).map_err(|_| PlanError::InvalidMemoryType(format!("ALL cannot be used as {verb} source memory type")))?;
        let to_type = convert_memory_type(to).map_err(|_| PlanError::InvalidMemoryType(format!("ALL cannot be used as {verb} target memory type")))?;
        Ok(LinkData {
            from_type,
            from_predicate: CorePredicate::Where { conditions: convert_conditions(from_conditions)? },
            to_type,
            to_predicate: CorePredicate::Where { conditions: convert_conditions(to_conditions)? },
            link_type: String::new(),
            weight: 1.0,
            type_filter: None,
            set_weight: None,
            set_type: None,
        })
    }

    /// LINK: create, or set the weight of the link that exists (a repeated LINK
    /// without WEIGHT keeps the weight).
    fn plan_link(&self, stmt: &LinkStmt) -> PlanResult<StepPlan> {
        let mut d = self.link_ends(stmt.from_type, &stmt.from_conditions, stmt.to_type, &stmt.to_conditions, "LINK")?;
        d.link_type = stmt.link_type.clone();
        d.weight = stmt.weight.map(|w| w as f32).unwrap_or(1.0);
        d.set_weight = stmt.weight.map(|w| w as f32);
        let mut step = StepPlan::link(d);
        step.modifiers.namespace = stmt.namespace.clone();
        Ok(step)
    }

    fn plan_unlink(&self, stmt: &UnlinkStmt) -> PlanResult<StepPlan> {
        let mut d = self.link_ends(stmt.from_type, &stmt.from_conditions, stmt.to_type, &stmt.to_conditions, "UNLINK")?;
        d.type_filter = stmt.link_type.clone();
        let mut step = StepPlan::link_edit(d, true);
        step.modifiers.namespace = stmt.namespace.clone();
        Ok(step)
    }
}

/// `key = v` or `id = v` as the only condition: the field and value of an exact lookup.
fn exact_key(conditions: &[CoreCondition]) -> Option<(String, CoreValue)> {
    match conditions {
        [CoreCondition::Simple { field, operator: CoreOperator::Eq, value, .. }] if matches!(field.as_str(), "key" | "id") => {
            Some((field.clone(), value.clone()))
        }
        _ => None,
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Conversion functions
// ============================================================================

fn convert_memory_type(mt: MemoryType) -> PlanResult<CoreMemoryType> {
    match mt {
        MemoryType::Working => Ok(CoreMemoryType::Working),
        MemoryType::Tools => Ok(CoreMemoryType::Tools),
        MemoryType::Procedural => Ok(CoreMemoryType::Procedural),
        MemoryType::Semantic => Ok(CoreMemoryType::Semantic),
        MemoryType::Episodic => Ok(CoreMemoryType::Episodic),
        MemoryType::All => Err(PlanError::InvalidMemoryType(
            "ALL cannot be used as target memory type".to_string(),
        )),
    }
}

/// A RECALL's predicate. `LIKE "text"` on PROCEDURAL is the token-overlap match
/// (what PATTERN was), its CONFIDENCE the threshold; any other LIKE is similarity.
fn convert_predicate(pred: &Predicate, memory: MemoryType, confidence: Option<f32>) -> PlanResult<CorePredicate> {
    match pred {
        Predicate::Where { conditions } => Ok(CorePredicate::Where { conditions: convert_conditions(conditions)? }),
        Predicate::Like { variable, text: Some(text) } if memory == MemoryType::Procedural => {
            Ok(CorePredicate::Pattern { pattern_var: variable.clone(), threshold: confidence, text: Some(text.clone()) })
        }
        Predicate::Like { variable, .. } => Ok(CorePredicate::Like { embedding_var: variable.clone() }),
        Predicate::All => Ok(CorePredicate::All),
    }
}

/// `field BETWEEN a AND b` is `(field >= a AND field <= b)`.
fn convert_conditions(conds: &[Condition]) -> PlanResult<Vec<CoreCondition>> {
    conds.iter().map(convert_condition).collect()
}

fn convert_condition(cond: &Condition) -> PlanResult<CoreCondition> {
    match cond {
        Condition::Simple { field, operator: Operator::Between, value, logical_op } => {
            let Value::Array(ends) = value else { return Err(PlanError::MissingField("BETWEEN's two ends".into())) };
            let [low, high] = ends.as_slice() else { return Err(PlanError::MissingField("BETWEEN's two ends".into())) };
            let side = |op: CoreOperator, v: &Value, join: Option<CoreLogicalOp>| -> PlanResult<CoreCondition> {
                Ok(CoreCondition::Simple { field: field.clone(), operator: op, value: convert_value(v)?, logical_op: join })
            };
            Ok(CoreCondition::Group {
                conditions: vec![side(CoreOperator::Gte, low, None)?, side(CoreOperator::Lte, high, Some(CoreLogicalOp::And))?],
                logical_op: logical_op.map(convert_logical_op),
            })
        }
        Condition::Simple { field, operator, value, logical_op } => {
            Ok(CoreCondition::Simple {
                field: field.clone(),
                operator: convert_operator(*operator),
                value: convert_value(value)?,
                logical_op: logical_op.map(convert_logical_op),
            })
        }
        Condition::Group { conditions, logical_op } => {
            let converted: PlanResult<Vec<_>> = conditions.iter().map(convert_condition).collect();
            Ok(CoreCondition::Group {
                conditions: converted?,
                logical_op: logical_op.map(convert_logical_op),
            })
        }
    }
}

fn convert_logical_op(op: LogicalOp) -> CoreLogicalOp {
    match op {
        LogicalOp::And => CoreLogicalOp::And,
        LogicalOp::Or => CoreLogicalOp::Or,
    }
}

fn convert_operator(op: Operator) -> CoreOperator {
    match op {
        Operator::Eq => CoreOperator::Eq,
        Operator::Ne => CoreOperator::Ne,
        Operator::Gt => CoreOperator::Gt,
        Operator::Gte => CoreOperator::Gte,
        Operator::Lt => CoreOperator::Lt,
        Operator::Lte => CoreOperator::Lte,
        Operator::Contains => CoreOperator::Contains,
        Operator::StartsWith => CoreOperator::StartsWith,
        Operator::EndsWith => CoreOperator::EndsWith,
        Operator::In => CoreOperator::In,
        // Planned as two comparisons (convert_condition).
        Operator::Between => CoreOperator::Gte,
    }
}

fn convert_value(val: &Value) -> PlanResult<CoreValue> {
    match val {
        Value::Null => Ok(CoreValue::Null),
        Value::Bool(b) => Ok(CoreValue::Bool(*b)),
        Value::Int(i) => Ok(CoreValue::Int(*i)),
        Value::Float(f) => Ok(CoreValue::Float(*f)),
        Value::String(s) => Ok(CoreValue::String(s.clone())),
        // Pass variables through - they'll be bound at execution time in pipelines
        Value::Variable(v) => Ok(CoreValue::Variable(v.clone())),
        Value::Array(arr) => {
            let converted: PlanResult<Vec<_>> = arr.iter().map(convert_value).collect();
            Ok(CoreValue::Array(converted?))
        }
    }
}

fn convert_modifiers(mods: &Modifiers) -> PlanResult<CoreModifiers> {
    Ok(CoreModifiers {
        limit: mods.limit,
        order_by: mods.order_by.as_ref().map(|o| CoreOrderBy { field: o.field.clone(), ascending: o.ascending }),
        return_fields: mods.return_fields.clone(),
        timeout: None,
        min_confidence: mods.confidence,
        scope: None,
        namespace: mods.namespace.clone(),
        branch: None,
        in_scope: mods.in_scope.clone(),
        like_query: None,
        ttl: mods.ttl,
        aggregate: mods.aggregate.as_ref().map(|aggs| aggs.iter().map(convert_aggregate).collect()),
        group_by: mods.group_by.clone(),
        having: mods.having.as_ref().map(|c| convert_conditions(c)).transpose()?,
        window: mods.window.as_ref().map(convert_window),
        with_links: mods.with_links.as_ref().map(link_select),
        follow_links: mods.follow_links.as_ref().map(|f| adb_core::FollowLinks { select: link_select(&f.select), depth: f.depth }),
        as_of: mods.as_of.as_ref().map(convert_time_point),
        valid_at: None,
        with_history: mods.with_history.is_some(),
    })
}






fn convert_aggregate(agg: &AggregateFunc) -> CoreAggregateFunc {
    CoreAggregateFunc {
        func: convert_aggregate_type(agg.func),
        field: agg.field.clone(),
        alias: agg.alias.clone(),
    }
}

fn convert_aggregate_type(func: AggregateFuncType) -> CoreAggregateFuncType {
    match func {
        AggregateFuncType::Count => CoreAggregateFuncType::Count,
        AggregateFuncType::Sum => CoreAggregateFuncType::Sum,
        AggregateFuncType::Avg => CoreAggregateFuncType::Avg,
        AggregateFuncType::Min => CoreAggregateFuncType::Min,
        AggregateFuncType::Max => CoreAggregateFuncType::Max,
    }
}


fn convert_time_point(t: &aql_parser::ast::TimePoint) -> adb_core::TimePoint {
    use aql_parser::ast::TimePoint as T;
    match t {
        T::At { time } => adb_core::TimePoint::At { time: time.clone() },
        T::Variable { name } => adb_core::TimePoint::Variable { name: name.clone() },
    }
}

/// v0.13 links are read both ways; WEIGHT is a minimum.
fn link_select(s: &aql_parser::ast::LinkSelect) -> adb_core::LinkSelect {
    adb_core::LinkSelect { types: s.types.clone(), direction: adb_core::LinkDirection::Both, min_weight: s.min_weight.map(|w| w as f32) }
}

fn convert_window(window: &Window) -> CoreWindow {
    match window {
        Window::LastN { count } => CoreWindow::LastN { count: *count },
        Window::LastDuration { duration } => CoreWindow::LastDuration { duration: *duration },
    }
}

fn value_to_string(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Variable(v) => format!("${}", v),
        Value::Array(_) => "array".to_string(),
    }
}

fn assignments_to_json(assignments: &[FieldAssignment]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for assign in assignments {
        map.insert(assign.field.clone(), assign.value.to_json());
    }
    serde_json::Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aql_parser::parse;

    fn single(q: &str) -> StepPlan {
        match Planner::new().plan(&parse(q).unwrap()).unwrap() {
            ExecutionPlan::Single(s) => s,
            other => panic!("{q}: {other:?}"),
        }
    }

    #[test]
    fn recall_picks_the_access_path() {
        assert_eq!(single("RECALL FROM WORKING WINDOW LAST 10").operation, Operation::Scan);
        assert_eq!(single(r#"RECALL FROM TOOLS WHERE task = "bidding" LIMIT 3"#).operation, Operation::Load);
        let s = single(r#"RECALL FROM SEMANTIC WHERE key = "person:ada""#);
        assert_eq!(s.operation, Operation::Lookup);
        assert!(matches!(s.predicate, CorePredicate::Key { ref field, .. } if field == "key"));
        assert_eq!(single(r#"RECALL FROM SEMANTIC WHERE key = "a" AND n > 1"#).operation, Operation::Recall);
        let s = single(r#"RECALL FROM PROCEDURAL LIKE "pod OOMKilled restart" CONFIDENCE 0.6"#);
        assert!(matches!(s.predicate, CorePredicate::Pattern { threshold: Some(t), .. } if (t - 0.6).abs() < 1e-6));
        assert!(matches!(single("RECALL FROM SEMANTIC LIKE $q").predicate, CorePredicate::Like { .. }));
        assert_eq!(single(r#"RECALL FROM EPISODIC WHERE pod = "p" LIMIT 5"#).operation, Operation::Recall);
    }

    #[test]
    fn between_is_two_comparisons() {
        let s = single(r#"RECALL FROM EPISODIC WHERE n BETWEEN 1 AND 5"#);
        let CorePredicate::Where { conditions } = s.predicate else { panic!() };
        let CoreCondition::Group { conditions: g, .. } = &conditions[0] else { panic!("{conditions:?}") };
        assert_eq!(g.len(), 2);
    }

    #[test]
    fn writes_links_and_namespaces() {
        assert_eq!(single(r#"STORE INTO WORKING (key = "task-1", status = "pending")"#).operation, Operation::Store);
        assert_eq!(single(r#"FORGET FROM WORKING WHERE status = "done""#).operation, Operation::Forget);
        let l = single(r#"LINK FROM SEMANTIC WHERE key = "a" INTO SEMANTIC WHERE key = "b" TYPE "t" NAMESPACE "crm""#);
        assert_eq!((l.operation, l.modifiers.namespace.as_deref()), (Operation::Link, Some("crm")));
        assert_eq!(single(r#"UNLINK FROM SEMANTIC WHERE key = "a" INTO SEMANTIC WHERE key = "b""#).operation, Operation::Unlink);
        assert!(matches!(Planner::new().plan(&parse(r#"BRANCH FROM "a" INTO "b""#).unwrap()).unwrap(), ExecutionPlan::Namespace(NamespacePlan::Branch { .. })));
        assert!(matches!(
            Planner::new().plan(&parse(r#"MERGE FROM "b" INTO "a" PREFER SOURCE"#).unwrap()).unwrap(),
            ExecutionPlan::Namespace(NamespacePlan::Merge { prefer: Some(true), .. })
        ));
    }

    #[test]
    fn pipeline() {
        let p = Planner::new().plan(&parse(r#"PIPELINE p TIMEOUT 50ms RECALL FROM WORKING | STORE INTO EPISODIC (a = 1)"#).unwrap()).unwrap();
        assert!(matches!(p, ExecutionPlan::Pipeline(ref pp) if pp.stages.len() == 2));
    }
}
