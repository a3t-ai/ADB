//! AQL Query Planner
//!
//! Converts parsed AST statements into executable plans.

use adb_core::{
    Condition as CoreCondition, LogicalOp as CoreLogicalOp, MemoryType as CoreMemoryType,
    Modifiers as CoreModifiers, Operator as CoreOperator, OrderBy as CoreOrderBy,
    Predicate as CorePredicate, Scope as CoreScope, Value as CoreValue, Window as CoreWindow,
    AggregateFunc as CoreAggregateFunc, AggregateFuncType as CoreAggregateFuncType,
};
use aql_parser::ast::{
    AggregateFunc, AggregateFuncType, Condition, FieldAssignment, ForgetStmt, LinkStmt, LoadStmt,
    LogicalOp, LookupStmt, MemoryType, Modifiers, Operator, PipelineStmt, Predicate, RecallStmt,
    ReflectSource, ReflectStmt, ScanStmt, Scope, Statement, StoreStmt, UpdateStmt, Value, Window,
};

use crate::error::{PlanError, PlanResult};
use crate::plan::{
    ExecutionPlan, FanOutPlan, LinkData, LinkOptions, Operation, PipelinePlan, PipelineStage,
    ReflectPlan, ReflectSourcePlan, StepPlan, StoreData,
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
        Self {
            default_scope: CoreScope::Private,
            default_namespace: None,
        }
    }

    /// Create a planner with custom defaults
    pub fn with_defaults(scope: CoreScope, namespace: Option<String>) -> Self {
        Self {
            default_scope: scope,
            default_namespace: namespace,
        }
    }

    /// Plan a statement
    pub fn plan(&self, stmt: &Statement) -> PlanResult<ExecutionPlan> {
        match stmt {
            Statement::Pipeline(p) => self.plan_pipeline(p),
            Statement::Reflect(r) => self.plan_reflect(r),
            Statement::Scan(s) => Ok(ExecutionPlan::Single(self.plan_scan(s)?)),
            Statement::Recall(r) => self.plan_recall(r),
            Statement::Lookup(l) => Ok(ExecutionPlan::Single(self.plan_lookup(l)?)),
            Statement::Load(l) => Ok(ExecutionPlan::Single(self.plan_load(l)?)),
            Statement::Store(s) => Ok(ExecutionPlan::Single(self.plan_store(s)?)),
            Statement::Update(u) => Ok(ExecutionPlan::Single(self.plan_update(u)?)),
            Statement::Forget(f) => self.plan_forget(f),
            Statement::Link(l) => Ok(ExecutionPlan::Single(self.plan_link(l)?)),
            // AQL v0.7 — change or remove existing links
            Statement::UpdateLink(u) => {
                let mut d = self.link_ends(u.from_type, &u.from_conditions, u.to_type, &u.to_conditions, "UPDATE LINK")?;
                d.type_filter = u.link_type.clone();
                d.set_weight = u.weight;
                d.set_type = u.new_type.clone();
                Ok(ExecutionPlan::Single(StepPlan::link_edit(d, false)))
            }
            Statement::Unlink(u) => {
                let mut d = self.link_ends(u.from_type, &u.from_conditions, u.to_type, &u.to_conditions, "UNLINK")?;
                d.type_filter = u.link_type.clone();
                Ok(ExecutionPlan::Single(StepPlan::link_edit(d, true)))
            }
            // AQL v0.6 — subscriptions and continuous queries (ARCHITECTURE §11.7)
            Statement::Subscribe(s) => Ok(ExecutionPlan::Subscription(SubscriptionPlan::Subscribe {
                target: match &s.target {
                    aql_parser::ast::SubscribeTarget::Scope(v) => SubscriptionTarget::Scope(v.clone()),
                    aql_parser::ast::SubscribeTarget::Topic(v) => SubscriptionTarget::Topic(v.clone()),
                },
                kinds: s.kinds.iter().map(|k| k.to_lowercase()).collect(),
                filter: s.filter.as_ref().map(|c| c.iter().map(convert_condition).collect::<PlanResult<Vec<_>>>()).transpose()?,
                from: match s.start {
                    aql_parser::ast::StartPosition::Now => None,
                    aql_parser::ast::StartPosition::Offset(n) => Some(n),
                },
            })),
            Statement::Unsubscribe(u) => {
                Ok(ExecutionPlan::Subscription(SubscriptionPlan::Unsubscribe { subscription: u.subscription.clone() }))
            }
            Statement::Ack(a) => Ok(ExecutionPlan::Subscription(SubscriptionPlan::Ack {
                subscription: a.subscription.clone(),
                offset: a.offset,
            })),
            Statement::Publish(p) => Ok(ExecutionPlan::Subscription(SubscriptionPlan::Publish {
                topic: p.topic.clone(),
                payload: assignments_to_json(&p.payload),
                ttl: p.modifiers.ttl,
            })),
            Statement::Watch(_) | Statement::Unwatch(_) => {
                Err(PlanError::NotImplemented("Continuous queries (WATCH / UNWATCH)"))
            }
        }
    }

    /// Plan a pipeline
    fn plan_pipeline(&self, stmt: &PipelineStmt) -> PlanResult<ExecutionPlan> {
        if stmt.stages.is_empty() {
            return Err(PlanError::EmptyPipeline);
        }

        let mut stages = Vec::new();
        for stage in &stmt.stages {
            match self.plan(stage)? {
                ExecutionPlan::Single(step) => stages.push(PipelineStage::Step(step)),
                ExecutionPlan::Pipeline(p) => stages.extend(p.stages),
                ExecutionPlan::Reflect(r) => {
                    // Support REFLECT as a pipeline stage
                    stages.push(PipelineStage::Reflect(r));
                }
                ExecutionPlan::Subscription(_) => {
                    return Err(PlanError::UnsupportedOperation {
                        op: "SUBSCRIBE / UNSUBSCRIBE / ACK / PUBLISH in PIPELINE".to_string(),
                        memory_type: "-".to_string(),
                    });
                }
                ExecutionPlan::FanOut(_) => {
                    // FanOut in pipeline requires special handling
                    return Err(PlanError::UnsupportedOperation {
                        op: "FROM ALL in PIPELINE".to_string(),
                        memory_type: "ALL".to_string(),
                    });
                }
            }
        }

        Ok(ExecutionPlan::Pipeline(PipelinePlan {
            name: stmt.name.clone(),
            timeout: stmt.timeout,
            stages,
        }))
    }

    /// Plan a reflect statement
    fn plan_reflect(&self, stmt: &ReflectStmt) -> PlanResult<ExecutionPlan> {
        let mut sources = Vec::new();

        for source in &stmt.sources {
            // Handle FROM ALL by expanding to all memory types
            if source.memory_type == MemoryType::All {
                let predicate = source
                    .predicate
                    .as_ref()
                    .map(|p| convert_predicate(p))
                    .transpose()?
                    .unwrap_or(CorePredicate::All);

                for mem_type in CoreMemoryType::all() {
                    sources.push(ReflectSourcePlan {
                        memory_type: *mem_type,
                        predicate: predicate.clone(),
                        modifiers: CoreModifiers::default(),
                    });
                }
            } else {
                sources.push(self.plan_reflect_source(source)?);
            }
        }

        let link_options = if stmt.with_links.is_some() || stmt.follow_links.is_some() {
            Some(LinkOptions {
                with_links: stmt.with_links.is_some(),
                // FOLLOW's selection when there is one, else WITH's.
                select: match (&stmt.follow_links, &stmt.with_links) {
                    (Some(f), _) => convert_follow_links(f).select,
                    (None, Some(w)) => convert_with_links(w),
                    (None, None) => adb_core::LinkSelect::default(),
                },
                follow: stmt.follow_links.is_some(),
                depth: stmt.follow_links.as_ref().and_then(|f| f.depth).unwrap_or(1),
            })
        } else {
            None
        };

        let then_step = match &stmt.then_clause {
            Some(then_stmt) => match self.plan(then_stmt)? {
                ExecutionPlan::Single(step) => Some(Box::new(step)),
                _ => None,
            },
            None => None,
        };

        Ok(ExecutionPlan::Reflect(ReflectPlan {
            sources,
            link_options,
            then_step,
        }))
    }

    fn plan_reflect_source(&self, source: &ReflectSource) -> PlanResult<ReflectSourcePlan> {
        Ok(ReflectSourcePlan {
            memory_type: convert_memory_type(source.memory_type)?,
            predicate: source
                .predicate
                .as_ref()
                .map(|p| convert_predicate(p))
                .transpose()?
                .unwrap_or(CorePredicate::All),
            modifiers: CoreModifiers::default(),
        })
    }

    /// Plan a scan statement
    fn plan_scan(&self, stmt: &ScanStmt) -> PlanResult<StepPlan> {
        let mut modifiers = convert_modifiers(&stmt.modifiers)?;
        modifiers.window = stmt.window.as_ref().map(convert_window).transpose()?;

        Ok(StepPlan::scan(modifiers))
    }

    /// Plan a recall statement
    fn plan_recall(&self, stmt: &RecallStmt) -> PlanResult<ExecutionPlan> {
        // Handle FROM ALL with fan-out
        if stmt.memory_type == MemoryType::All {
            let predicate = convert_predicate(&stmt.predicate)?;
            let modifiers = convert_modifiers(&stmt.modifiers)?;
            return Ok(ExecutionPlan::FanOut(FanOutPlan {
                operation: Operation::Recall,
                predicate,
                modifiers,
                data: None,
            }));
        }

        let memory_type = convert_memory_type(stmt.memory_type)?;
        let predicate = convert_predicate(&stmt.predicate)?;
        let modifiers = convert_modifiers(&stmt.modifiers)?;

        Ok(ExecutionPlan::Single(StepPlan::recall(memory_type, predicate, modifiers)))
    }

    /// Plan a lookup statement
    /// LOOKUP is only valid on PROCEDURAL, TOOLS, and SEMANTIC (§5 of spec)
    fn plan_lookup(&self, stmt: &LookupStmt) -> PlanResult<StepPlan> {
        // Validate memory type - LOOKUP not allowed on WORKING or EPISODIC
        match stmt.memory_type {
            MemoryType::Working => {
                return Err(PlanError::InvalidMemoryType(
                    "LOOKUP is not valid on WORKING memory. Use SCAN or RECALL instead.".to_string()
                ));
            }
            MemoryType::Episodic => {
                return Err(PlanError::InvalidMemoryType(
                    "LOOKUP is not valid on EPISODIC memory. Use RECALL instead.".to_string()
                ));
            }
            _ => {}
        }

        let memory_type = convert_memory_type(stmt.memory_type)?;
        let predicate = convert_predicate(&stmt.predicate)?;
        let modifiers = convert_modifiers(&stmt.modifiers)?;

        Ok(StepPlan::lookup(memory_type, predicate, modifiers))
    }

    /// Plan a load statement
    fn plan_load(&self, stmt: &LoadStmt) -> PlanResult<StepPlan> {
        let predicate = convert_predicate(&stmt.predicate)?;
        let modifiers = convert_modifiers(&stmt.modifiers)?;

        Ok(StepPlan::load(predicate, modifiers))
    }

    /// Plan a store statement
    fn plan_store(&self, stmt: &StoreStmt) -> PlanResult<StepPlan> {
        let memory_type = convert_memory_type(stmt.memory_type)?;
        let modifiers = convert_modifiers(&stmt.modifiers)?;

        // Extract key from payload
        let key = stmt
            .payload
            .iter()
            .find(|f| f.field == "key" || f.field == "id")
            .map(|f| value_to_string(&f.value))
            .unwrap_or_else(|| ulid::Ulid::new().to_string()); // D3 S1

        // Convert payload to JSON
        let payload = assignments_to_json(&stmt.payload);

        let store_data = StoreData {
            key,
            payload,
            scope: modifiers.scope.unwrap_or(self.default_scope),
            namespace: modifiers.namespace.clone().or_else(|| self.default_namespace.clone()),
            in_scope: modifiers.in_scope.clone(),
            ttl: modifiers.ttl,
            fact: stmt.fact.map(|f| crate::plan::FactPlan { multi: f.multi }),
        };

        Ok(StepPlan::store(memory_type, store_data, modifiers))
    }

    /// Plan an update statement
    fn plan_update(&self, stmt: &UpdateStmt) -> PlanResult<StepPlan> {
        let memory_type = convert_memory_type(stmt.memory_type)?;
        let predicate = CorePredicate::Where {
            conditions: stmt
                .conditions
                .iter()
                .map(convert_condition)
                .collect::<PlanResult<Vec<_>>>()?,
        };
        let modifiers = convert_modifiers(&stmt.modifiers)?;
        let payload = assignments_to_json(&stmt.payload);

        Ok(StepPlan::update(memory_type, predicate, payload, modifiers))
    }

    /// Plan a forget statement
    fn plan_forget(&self, stmt: &ForgetStmt) -> PlanResult<ExecutionPlan> {
        let predicate = CorePredicate::Where {
            conditions: stmt
                .conditions
                .iter()
                .map(convert_condition)
                .collect::<PlanResult<Vec<_>>>()?,
        };
        let modifiers = convert_modifiers(&stmt.modifiers)?;

        // Handle FROM ALL with fan-out
        if stmt.memory_type == MemoryType::All {
            return Ok(ExecutionPlan::FanOut(FanOutPlan {
                operation: Operation::Forget,
                predicate,
                modifiers,
                data: None,
            }));
        }

        let memory_type = convert_memory_type(stmt.memory_type)?;
        Ok(ExecutionPlan::Single(StepPlan::forget(memory_type, predicate, modifiers)))
    }

    /// Plan a link statement
    /// The ends UPDATE LINK and UNLINK address, as `LinkData` (no type or weight yet).
    fn link_ends(
        &self,
        from: aql_parser::ast::MemoryType,
        from_conditions: &[aql_parser::ast::Condition],
        to: aql_parser::ast::MemoryType,
        to_conditions: &[aql_parser::ast::Condition],
        verb: &str,
    ) -> PlanResult<LinkData> {
        let from_type = convert_memory_type(from)
            .map_err(|_| PlanError::InvalidMemoryType(format!("ALL cannot be used as {verb} source memory type")))?;
        let to_type = convert_memory_type(to)
            .map_err(|_| PlanError::InvalidMemoryType(format!("ALL cannot be used as {verb} target memory type")))?;
        let conditions = |c: &[aql_parser::ast::Condition]| -> PlanResult<CorePredicate> {
            Ok(CorePredicate::Where { conditions: c.iter().map(convert_condition).collect::<PlanResult<Vec<_>>>()? })
        };
        Ok(LinkData {
            from_type,
            from_predicate: conditions(from_conditions)?,
            to_type,
            to_predicate: conditions(to_conditions)?,
            link_type: String::new(),
            weight: 1.0,
            type_filter: None,
            set_weight: None,
            set_type: None,
        })
    }

    fn plan_link(&self, stmt: &LinkStmt) -> PlanResult<StepPlan> {
        let from_type = convert_memory_type(stmt.from_type).map_err(|_| {
            PlanError::InvalidMemoryType("ALL cannot be used as LINK source memory type".to_string())
        })?;
        let to_type = convert_memory_type(stmt.to_type).map_err(|_| {
            PlanError::InvalidMemoryType("ALL cannot be used as LINK target memory type".to_string())
        })?;

        let from_predicate = CorePredicate::Where {
            conditions: stmt
                .from_conditions
                .iter()
                .map(convert_condition)
                .collect::<PlanResult<Vec<_>>>()?,
        };

        let to_predicate = CorePredicate::Where {
            conditions: stmt
                .to_conditions
                .iter()
                .map(convert_condition)
                .collect::<PlanResult<Vec<_>>>()?,
        };

        let link_data = LinkData {
            from_type,
            from_predicate,
            to_type,
            to_predicate,
            link_type: stmt.link_type.clone(),
            weight: stmt.weight.unwrap_or(1.0),
            type_filter: None,
            // As written: a repeated LINK without WEIGHT keeps the link's weight.
            set_weight: stmt.weight,
            set_type: None,
        };

        Ok(StepPlan::link(link_data))
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

fn convert_predicate(pred: &Predicate) -> PlanResult<CorePredicate> {
    match pred {
        Predicate::Where { conditions } => Ok(CorePredicate::Where {
            conditions: conditions
                .iter()
                .map(convert_condition)
                .collect::<PlanResult<Vec<_>>>()?,
        }),
        Predicate::Key { field, value } => Ok(CorePredicate::Key {
            field: field.clone(),
            value: convert_value(value)?,
        }),
        Predicate::Like { variable } => Ok(CorePredicate::Like {
            embedding_var: variable.clone(),
        }),
        Predicate::Pattern { variable, threshold, text } => Ok(CorePredicate::Pattern {
            pattern_var: variable.clone(),
            threshold: *threshold,
            text: text.clone(),
        }),
        Predicate::All => Ok(CorePredicate::All),
    }
}

fn convert_condition(cond: &Condition) -> PlanResult<CoreCondition> {
    match cond {
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
        order_by: mods.order_by.as_ref().map(|o| CoreOrderBy {
            field: o.field.clone(),
            ascending: o.ascending,
        }),
        return_fields: mods.return_fields.clone(),
        timeout: mods.timeout,
        min_confidence: mods.min_confidence,
        scope: mods.scope.map(convert_scope),
        namespace: mods.namespace.clone(),
        in_scope: mods.in_scope.clone(),
        like_query: None,
        ttl: mods.ttl,
        aggregate: mods.aggregate.as_ref().map(|aggs| {
            aggs.iter().map(convert_aggregate).collect()
        }),
        group_by: mods.group_by.clone(),
        having: mods.having.as_ref().map(|conds| {
            conds.iter().filter_map(|c| convert_condition(c).ok()).collect()
        }),
        window: mods.window.as_ref().and_then(|w| convert_window(w).ok()),
        with_links: mods.with_links.as_ref().map(convert_with_links),
        follow_links: mods.follow_links.as_ref().map(convert_follow_links),
        as_of: mods.as_of.as_ref().map(convert_time_point),
        valid_at: mods.valid_at.as_ref().map(convert_time_point),
        with_history: mods.with_history,
    })
}

fn convert_time_point(t: &aql_parser::ast::TimePoint) -> adb_core::TimePoint {
    use aql_parser::ast::TimePoint as T;
    match t {
        T::At { time } => adb_core::TimePoint::At { time: time.clone() },
        T::Variable { name } => adb_core::TimePoint::Variable { name: name.clone() },
        T::Ago { duration } => adb_core::TimePoint::Ago { duration: *duration },
    }
}

fn link_select(
    link_types: Vec<String>,
    direction: aql_parser::ast::LinkDirection,
    min_weight: Option<f64>,
) -> adb_core::LinkSelect {
    adb_core::LinkSelect {
        types: link_types,
        direction: match direction {
            aql_parser::ast::LinkDirection::Outbound => adb_core::LinkDirection::Outbound,
            aql_parser::ast::LinkDirection::Inbound => adb_core::LinkDirection::Inbound,
            aql_parser::ast::LinkDirection::Both => adb_core::LinkDirection::Both,
        },
        min_weight: min_weight.map(|w| w as f32),
    }
}

fn convert_with_links(wl: &aql_parser::ast::WithLinks) -> adb_core::WithLinks {
    use aql_parser::ast::WithLinks as W;
    match wl {
        W::All { direction, min_weight } => link_select(Vec::new(), *direction, *min_weight),
        W::Type { link_type, direction, min_weight } => link_select(vec![link_type.clone()], *direction, *min_weight),
        W::Types { link_types, direction, min_weight } => link_select(link_types.clone(), *direction, *min_weight),
    }
}

fn convert_follow_links(fl: &aql_parser::ast::FollowLinks) -> adb_core::FollowLinks {
    let types = match &fl.link_type {
        Some(t) => vec![t.clone()],
        None => fl.link_types.clone(),
    };
    adb_core::FollowLinks {
        select: link_select(types, fl.direction, fl.min_weight),
        depth: fl.depth,
    }
}

fn convert_scope(scope: Scope) -> CoreScope {
    match scope {
        Scope::Private => CoreScope::Private,
        Scope::Shared => CoreScope::Shared,
        Scope::Cluster => CoreScope::Cluster,
    }
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

fn convert_window(window: &Window) -> PlanResult<CoreWindow> {
    match window {
        Window::LastN { count } => Ok(CoreWindow::LastN { count: *count }),
        Window::LastDuration { duration } => Ok(CoreWindow::LastDuration { duration: *duration }),
        Window::TopBy { count, field } => Ok(CoreWindow::TopBy {
            count: *count,
            field: field.clone(),
        }),
        Window::Since { condition } => Ok(CoreWindow::Since {
            condition: convert_condition(condition)?,
        }),
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

    #[test]
    fn test_plan_scan() {
        let stmt = parse("SCAN FROM WORKING LIMIT 10").unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Single(step) => {
                assert_eq!(step.operation, Operation::Scan);
                assert_eq!(step.memory_type, CoreMemoryType::Working);
                assert_eq!(step.modifiers.limit, Some(10));
            }
            _ => panic!("Expected single step plan"),
        }
    }

    #[test]
    fn test_plan_recall() {
        let stmt = parse(r#"RECALL FROM EPISODIC WHERE pod = "payments" LIMIT 5"#).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Single(step) => {
                assert_eq!(step.operation, Operation::Recall);
                assert_eq!(step.memory_type, CoreMemoryType::Episodic);
                assert_eq!(step.modifiers.limit, Some(5));
            }
            _ => panic!("Expected single step plan"),
        }
    }

    #[test]
    fn test_plan_lookup() {
        let planner = Planner::new();
        // AQL v0.5 §19: LOOKUP is not valid on WORKING memory
        let working = parse(r#"LOOKUP FROM WORKING KEY id = "task-1""#).unwrap();
        assert!(planner.plan(&working).is_err());

        let stmt = parse(r#"LOOKUP FROM PROCEDURAL KEY id = "oom-fix""#).unwrap();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Single(step) => {
                assert_eq!(step.operation, Operation::Lookup);
                assert_eq!(step.memory_type, CoreMemoryType::Procedural);
            }
            _ => panic!("Expected single step plan"),
        }
    }

    #[test]
    fn test_plan_load() {
        let stmt = parse(r#"LOAD FROM TOOLS WHERE category = "file" LIMIT 5"#).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Single(step) => {
                assert_eq!(step.operation, Operation::Load);
                assert_eq!(step.memory_type, CoreMemoryType::Tools);
            }
            _ => panic!("Expected single step plan"),
        }
    }

    #[test]
    fn test_plan_store() {
        let stmt = parse(r#"STORE INTO WORKING (key = "task-1", status = "pending")"#).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Single(step) => {
                assert_eq!(step.operation, Operation::Store);
                assert_eq!(step.memory_type, CoreMemoryType::Working);
                assert!(step.data.is_some());
            }
            _ => panic!("Expected single step plan"),
        }
    }

    #[test]
    fn test_plan_forget() {
        let stmt = parse(r#"FORGET FROM WORKING WHERE status = "completed""#).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Single(step) => {
                assert_eq!(step.operation, Operation::Forget);
                assert_eq!(step.memory_type, CoreMemoryType::Working);
            }
            _ => panic!("Expected single step plan"),
        }
    }

    #[test]
    fn test_plan_pipeline() {
        let stmt = parse(
            r#"PIPELINE error_handler TIMEOUT 30s RECALL FROM EPISODIC WHERE type = "error" LIMIT 10 | RECALL FROM PROCEDURAL PATTERN $error LIMIT 3"#,
        )
        .unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&stmt).unwrap();

        match plan {
            ExecutionPlan::Pipeline(p) => {
                assert_eq!(p.name, "error_handler");
                assert_eq!(p.stages.len(), 2);
            }
            _ => panic!("Expected pipeline plan"),
        }
    }
}
