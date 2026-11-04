//! Query Executor
//!
//! Executes planned queries against ADB backends.

use std::sync::Arc;
use std::time::Instant;

use std::collections::{HashMap, HashSet};

use adb_core::{Condition, MemoryType, Predicate, Scope, Value};
use aql_parser::parse;
use aql_planner::{
    ExecutionPlan, FanOutPlan, Operation, PipelinePlan, PipelineStage, Planner, ReflectPlan, StepPlan, SubscriptionPlan,
    SubscriptionTarget,
};

use crate::error::{ExecutorError, ExecutorResult};
use crate::result::{QueryResult, ResultSet, SourceResult};
use crate::reads::Reads;
use crate::writes::Writes;

/// Deepest `FOLLOW LINKS … DEPTH` (P1_GRAPH D1).
pub const MAX_DEPTH: u32 = 5;
/// Most records one traversal returns.
pub const MAX_REACHED: usize = 1000;

/// The query executor
pub struct Executor {
    /// Where writes go: through the log (stage C)
    writes: Arc<dyn Writes>,
    /// Where reads come from: the Lance table, through the query engine (W7)
    reads: Arc<dyn Reads>,
    /// The planner
    planner: Planner,
    /// Whether `PIPELINE … TIMEOUT` is a hard budget (spec §12). Off by default
    /// (`ADB_ENFORCE_TIMEOUT=1` turns it on) until reads are fast enough for
    /// tens-of-milliseconds budgets (user, 28 Oct).
    enforce_timeout: bool,
}

impl Executor {
    /// An executor over its write and read paths (log + Lance).
    pub fn with_io(writes: Arc<dyn Writes>, reads: Arc<dyn Reads>) -> Self {
        Self {
            writes,
            reads,
            planner: Planner::new(),
            enforce_timeout: std::env::var("ADB_ENFORCE_TIMEOUT").is_ok_and(|v| v == "1" || v == "true"),
        }
    }

    /// Turn the pipeline `TIMEOUT` budget on or off (overrides `ADB_ENFORCE_TIMEOUT`).
    pub fn with_enforced_timeout(mut self, on: bool) -> Self {
        self.enforce_timeout = on;
        self
    }

    /// The write path (tests seed through it).
    pub fn writes(&self) -> &Arc<dyn Writes> {
        &self.writes
    }

    /// The read path.
    pub fn reads(&self) -> &Arc<dyn Reads> {
        &self.reads
    }

    /// Execute an AQL query string
    pub async fn execute(&self, query: &str) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        // Parse
        let stmt = parse(query).map_err(|e| ExecutorError::Parse(e.to_string()))?;

        // Plan
        let plan = self.planner.plan(&stmt)?;

        // Execute
        let result = self.execute_plan(&plan).await?;

        // Update timing
        let mut result = result;
        result.metadata.execution_time_ms = start.elapsed().as_millis() as u64;

        Ok(result)
    }

    /// Execute an AQL query string with request-level variables (`{name}` / `$name`),
    /// as sent by `POST /query` with `"variables": {...}`.
    pub async fn execute_with_vars(
        &self,
        query: &str,
        variables: &HashMap<String, serde_json::Value>,
    ) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        let stmt = parse(query).map_err(|e| ExecutorError::Parse(e.to_string()))?;
        let plan = self.planner.plan(&stmt)?;
        let bindings: HashMap<String, Value> =
            variables.iter().map(|(k, v)| (k.clone(), Value::from(v))).collect();
        let mut result = self.execute_plan_with(&plan, bindings).await?;
        result.metadata.execution_time_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Execute a parsed statement with request-level variables (a `WATCH`'s query,
    /// MVP W6).
    pub async fn execute_statement(
        &self,
        stmt: &aql_parser::ast::Statement,
        variables: &HashMap<String, serde_json::Value>,
    ) -> ExecutorResult<QueryResult> {
        let plan = self.planner.plan(stmt)?;
        let bindings: HashMap<String, Value> = variables.iter().map(|(k, v)| (k.clone(), Value::from(v))).collect();
        self.execute_plan_with(&plan, bindings).await
    }

    /// Execute a pre-planned query
    pub async fn execute_plan(&self, plan: &ExecutionPlan) -> ExecutorResult<QueryResult> {
        self.execute_plan_with(plan, HashMap::new()).await
    }

    /// Execute a pre-planned query with initial variable bindings
    async fn execute_plan_with(
        &self,
        plan: &ExecutionPlan,
        bindings: HashMap<String, Value>,
    ) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        let result = match plan {
            ExecutionPlan::Single(step) => {
                self.execute_step(&self.bind_variables_in_step(step, &bindings)).await?
            }
            ExecutionPlan::Pipeline(pipeline) => self.execute_pipeline(pipeline, bindings).await?,
            ExecutionPlan::Reflect(reflect) => self.execute_reflect(reflect).await?,
            ExecutionPlan::FanOut(fanout) => self.execute_fanout(fanout).await?,
            ExecutionPlan::Subscription(sub) => self.execute_subscription(sub).await?,
        };

        let mut result = result;
        result.metadata.execution_time_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Execute a single step
    async fn execute_step(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        // Record ids in a named scope carry it (E-L12).
        let scoped;
        // And a record in a namespace, the namespace (NS, schema v5).
        let (ns, sc) = (step.modifiers.namespace.as_deref(), step.modifiers.in_scope.as_deref().unwrap_or(adb_store::DEFAULT_SCOPE));
        let step = match ns.is_some() || sc != adb_store::DEFAULT_SCOPE {
            true => {
                scoped = StepPlan { predicate: crate::reads::scope_ids(&step.predicate, ns, sc), ..step.clone() };
                &scoped
            }
            false => step,
        };

        let result = match step.operation {
            Operation::Scan => self.execute_scan(step).await?,
            Operation::Recall => self.execute_recall(step).await?,
            Operation::Lookup => self.execute_lookup(step).await?,
            Operation::Load => self.execute_load(step).await?,
            Operation::Store => self.execute_store(step).await?,
            Operation::Update => self.execute_update(step).await?,
            Operation::Forget => self.execute_forget(step).await?,
            Operation::Link => self.execute_link(step).await?,
            Operation::UpdateLink => self.execute_link_edit(step, false).await?,
            Operation::Unlink => self.execute_link_edit(step, true).await?,
            Operation::Merge => {
                let start = Instant::now();
                let data = step.data.as_ref().ok_or_else(|| ExecutorError::MissingData("merge data".to_string()))?;
                let key = |k: &str| data.get(k).and_then(|v| v.as_str()).unwrap_or_default().to_string();
                if let Some(ns) = &step.modifiers.namespace {
                    return Err(ExecutorError::InvalidOperation(format!("MERGE in NAMESPACE {ns:?} isn't supported yet (NS-L1): MERGE works on records without a namespace")));
                }
                let n = self.writes.merge(&key("from"), &key("into")).await?;
                QueryResult::count(n, start.elapsed().as_millis() as u64)
            }
        };

        let mut result = result;
        result.metadata.execution_time_ms = start.elapsed().as_millis() as u64;
        Ok(result)
    }

    /// Execute a pipeline
    async fn execute_pipeline(
        &self,
        pipeline: &PipelinePlan,
        mut var_bindings: HashMap<String, Value>,
    ) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        let mut step_results = Vec::new();

        for (i, stage) in pipeline.stages.iter().enumerate() {
            // Bind variables in the stage's predicate from previous results
            let bound_stage = match stage {
                PipelineStage::Step(step) => {
                    let bound_step = self.bind_variables_in_step(step, &var_bindings);
                    PipelineStage::Step(bound_step)
                }
                PipelineStage::Reflect(reflect) => PipelineStage::Reflect(reflect.clone()),
            };

            let run = async {
                match &bound_stage {
                    PipelineStage::Step(step) => self.execute_step(step).await,
                    PipelineStage::Reflect(reflect) => self.execute_reflect(reflect).await,
                }
            };
            // A hard budget: the stage gets what is left of the pipeline's TIMEOUT;
            // when it runs out, the stages done so far are the (partial) answer.
            let result = match pipeline.timeout.filter(|_| self.enforce_timeout) {
                Some(budget) => match budget.checked_sub(start.elapsed()).map(|left| tokio::time::timeout(left, run)) {
                    Some(t) => match t.await {
                        Ok(r) => r,
                        Err(_) => return Ok(Self::timed_out(step_results, start)),
                    },
                    None => return Ok(Self::timed_out(step_results, start)),
                },
                None => run.await,
            };

            match result {
                Ok(r) => {
                    // Extract variable bindings from this stage's results
                    self.extract_bindings(&r, &mut var_bindings);
                    step_results.push(r);
                }
                Err(e) => {
                    return Err(ExecutorError::PipelineError {
                        step: i,
                        message: e.to_string(),
                    });
                }
            }
        }

        Ok(QueryResult::pipeline(
            step_results,
            start.elapsed().as_millis() as u64,
        ))
    }

    /// A pipeline cut short by its `TIMEOUT`: the stages that finished, flagged.
    fn timed_out(steps: Vec<QueryResult>, start: Instant) -> QueryResult {
        let mut r = QueryResult::pipeline(steps, start.elapsed().as_millis() as u64);
        r.metadata.timed_out = Some(true);
        r
    }

    /// Bind variables in a step's predicate from bindings
    fn bind_variables_in_step(&self, step: &StepPlan, bindings: &HashMap<String, Value>) -> StepPlan {
        let mut bound_step = step.clone();
        bound_step.predicate = self.bind_variables_in_predicate(&step.predicate, bindings);
        // `LIKE $q` (MVP W4): the reads rank against `$q`'s value (text or a vector).
        if let Predicate::Like { embedding_var } = &step.predicate {
            let name = embedding_var.trim_start_matches('$');
            if let Some(v) = bindings.get(name).or_else(|| bindings.get(embedding_var)) {
                bound_step.modifiers.like_query = Some(v.to_json());
            }
        }
        // `PATTERN $p` (W9): matched against `$p`'s value, not its name.
        if let Predicate::Pattern { pattern_var, threshold, text: None } = &step.predicate {
            let name = pattern_var.trim_start_matches('$');
            if let Some(v) = bindings.get(name).or_else(|| bindings.get(pattern_var)) {
                let text = match v.to_json() {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                bound_step.predicate = Predicate::Pattern { pattern_var: pattern_var.clone(), threshold: *threshold, text: Some(text) };
            }
        }
        // `AS OF $t` / `VALID AT $t` (AQL v0.10): the bound value is the time.
        for t in [&mut bound_step.modifiers.as_of, &mut bound_step.modifiers.valid_at].into_iter().flatten() {
            if let adb_core::TimePoint::Variable { name } = t {
                if let Some(serde_json::Value::String(time)) = bindings.get(name.as_str()).map(|v| v.to_json()) {
                    *t = adb_core::TimePoint::At { time };
                }
            }
        }
        bound_step
    }

    /// Bind variables in a predicate
    fn bind_variables_in_predicate(&self, predicate: &Predicate, bindings: &HashMap<String, Value>) -> Predicate {
        match predicate {
            Predicate::Where { conditions } => {
                let bound_conditions: Vec<Condition> = conditions
                    .iter()
                    .map(|c| self.bind_variables_in_condition(c, bindings))
                    .collect();
                Predicate::Where { conditions: bound_conditions }
            }
            Predicate::Key { field, value } => Predicate::Key {
                field: field.clone(),
                value: self.bind_value(value, bindings),
            },
            _ => predicate.clone(),
        }
    }

    /// Bind variables in a condition (handles both Simple and Group)
    fn bind_variables_in_condition(&self, condition: &Condition, bindings: &HashMap<String, Value>) -> Condition {
        match condition {
            Condition::Simple { field, operator, value, logical_op } => {
                Condition::Simple {
                    field: field.clone(),
                    operator: *operator,
                    value: self.bind_value(value, bindings),
                    logical_op: *logical_op,
                }
            }
            Condition::Group { conditions, logical_op } => {
                Condition::Group {
                    conditions: conditions.iter()
                        .map(|c| self.bind_variables_in_condition(c, bindings))
                        .collect(),
                    logical_op: *logical_op,
                }
            }
        }
    }

    /// Bind a value if it's a variable
    fn bind_value(&self, value: &Value, bindings: &HashMap<String, Value>) -> Value {
        match value {
            Value::Variable(var_name) => {
                bindings.get(var_name).cloned().unwrap_or_else(|| value.clone())
            }
            _ => value.clone(),
        }
    }

    /// Extract variable bindings from query results
    fn extract_bindings(&self, result: &QueryResult, bindings: &mut HashMap<String, Value>) {
        match &result.data {
            ResultSet::Records { records } => {
                // Extract from the first record (for pipeline use)
                if let Some(record) = records.first() {
                    if let Some(obj) = record.data.as_object() {
                        for (key, val) in obj {
                            let value: Value = json_to_value(val);
                            bindings.insert(key.clone(), value);
                        }
                    }
                }
            }
            ResultSet::Reflect { sources, .. } => {
                // Extract from first source's first record
                if let Some(source) = sources.first() {
                    if let Some(record) = source.records.first() {
                        if let Some(obj) = record.data.as_object() {
                            for (key, val) in obj {
                                let value: Value = json_to_value(val);
                                bindings.insert(key.clone(), value);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Execute a fan-out query across ALL memory types
    async fn execute_fanout(&self, fanout: &FanOutPlan) -> ExecutorResult<QueryResult> {
        // Record ids in a named scope carry it (E-L12).
        let scoped;
        // And a record in a namespace, the namespace (NS, schema v5).
        let (ns, sc) = (fanout.modifiers.namespace.as_deref(), fanout.modifiers.in_scope.as_deref().unwrap_or(adb_store::DEFAULT_SCOPE));
        let fanout = match ns.is_some() || sc != adb_store::DEFAULT_SCOPE {
            true => {
                scoped = FanOutPlan { predicate: crate::reads::scope_ids(&fanout.predicate, ns, sc), ..fanout.clone() };
                &scoped
            }
            false => fanout,
        };
        let start = Instant::now();

        match fanout.operation {
            Operation::Recall => {
                // Fan-out RECALL across all memory types; one merged record list (each
                // record keeps its memory_type), as AQL's FROM ALL and ClawDB return it.
                let mut all_records = Vec::new();
                for mem_type in MemoryType::all() {
                    let records = self
                        .reads
                        .recall_with_modifiers(*mem_type, &fanout.predicate, &fanout.modifiers)
                        .await?;
                    all_records.extend(records);
                }

                if let Some(limit) = fanout.modifiers.limit {
                    all_records.truncate(limit);
                }

                Ok(QueryResult::records(all_records, start.elapsed().as_millis() as u64))
            }
            Operation::Forget => {
                // Fan-out FORGET across all memory types
                let mut total_count = 0u64;

                let in_scope = fanout.modifiers.in_scope.as_deref();
                for mem_type in MemoryType::all().iter().filter(|m| in_scope.is_none() || crate::reads::lance_owned(**m)) {
                    let count = self.writes.forget(*mem_type, &fanout.predicate, in_scope).await?;
                    total_count += count;
                }

                Ok(QueryResult::count(total_count, start.elapsed().as_millis() as u64))
            }
            Operation::Update => {
                // Fan-out UPDATE across all memory types
                let mut total_count = 0u64;

                if let Some(data) = &fanout.data {
                    let in_scope = fanout.modifiers.in_scope.as_deref();
                    for mem_type in MemoryType::all().iter().filter(|m| in_scope.is_none() || crate::reads::lance_owned(**m)) {
                        let count = self
                            .writes
                            .update(*mem_type, &fanout.predicate, data.clone(), in_scope)
                            .await?;
                        total_count += count;
                    }
                }

                Ok(QueryResult::count(total_count, start.elapsed().as_millis() as u64))
            }
            _ => Err(ExecutorError::UnsupportedOperation(format!(
                "{} FROM ALL",
                fanout.operation
            ))),
        }
    }

    /// Execute a reflect query
    async fn execute_reflect(&self, reflect: &ReflectPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        let mut source_results = Vec::new();
        let mut all_links: Vec<adb_core::Link> = Vec::new();

        // Execute each source
        for source in &reflect.sources {
            let records = self
                .reads
                .recall_with_modifiers(source.memory_type, &source.predicate, &source.modifiers)
                .await?;

            source_results.push(SourceResult {
                memory_type: format!("{:?}", source.memory_type),
                records,
            });
        }

        // Handle link options if set
        if let Some(link_opts) = &reflect.link_options {
            // FOLLOW LINKS: the reached records, by memory type, instead of the sources.
            if link_opts.follow {
                let sources: Vec<adb_core::MemoryRecord> = source_results.iter().flat_map(|s| s.records.iter().cloned()).collect();
                let (reached, walked) = self.traverse(&sources, &link_opts.select, link_opts.depth).await?;
                all_links.extend(walked);
                if !reached.is_empty() {
                    let mut by_type: Vec<SourceResult> = Vec::new();
                    for r in reached {
                        let memory_type = format!("{:?}", r.memory_type);
                        match by_type.iter_mut().find(|s| s.memory_type == memory_type) {
                            Some(s) => s.records.push(r),
                            None => by_type.push(SourceResult { memory_type, records: vec![r] }),
                        }
                    }
                    source_results = by_type;
                }
            } else if link_opts.with_links {
                // WITH LINKS - include link metadata with source records
                let sources: Vec<adb_core::MemoryRecord> = source_results.iter().flat_map(|s| s.records.iter().cloned()).collect();
                let by_record = self.links_of(&sources, &link_opts.select).await?;
                for r in &sources {
                    all_links.extend(by_record.get(&(r.memory_type, r.id.clone())).cloned().unwrap_or_default());
                }
            }
        }

        // Execute then clause if present
        if let Some(then_step) = &reflect.then_step {
            let _then_result = self.execute_step(then_step).await?;
        }

        Ok(QueryResult {
            success: true,
            data: ResultSet::Reflect {
                sources: source_results,
                links: all_links,
            },
            metadata: crate::result::ResultMetadata {
                execution_time_ms: start.elapsed().as_millis() as u64,
                ..Default::default()
            },
        })
    }

    // =========================================================================
    // Individual operation implementations
    // =========================================================================

    /// The links `select` picks touching each of `records`, by (memory type, id): one
    /// batched read per memory type (W7, P4).
    async fn links_of(
        &self,
        records: &[adb_core::MemoryRecord],
        select: &adb_core::LinkSelect,
    ) -> ExecutorResult<HashMap<(MemoryType, String), Vec<adb_core::Link>>> {
        let mut ids: HashMap<MemoryType, Vec<String>> = HashMap::new();
        for r in records {
            ids.entry(r.memory_type).or_default().push(r.id.clone());
        }
        let mut out = HashMap::new();
        for (memory, ids) in ids {
            for (id, links) in self.reads.links_touching(memory, &ids, select).await? {
                out.insert((memory, id), links);
            }
        }
        Ok(out)
    }

    /// `FOLLOW LINKS … DEPTH n` (P1_GRAPH D1): every current record reached within
    /// `depth` hops of `records`, breadth-first, each once at its shortest hop, by hop
    /// then path weight (the product of link weights along the path), each marked with
    /// `_hop` and `_path_weight`. Cycle-safe; depth capped at [`MAX_DEPTH`], results at
    /// [`MAX_REACHED`]. Also the links walked, in order. One read for the links and one
    /// per target memory type, per hop.
    async fn traverse(
        &self,
        records: &[adb_core::MemoryRecord],
        select: &adb_core::LinkSelect,
        depth: u32,
    ) -> ExecutorResult<(Vec<adb_core::MemoryRecord>, Vec<adb_core::Link>)> {
        let mut seen: HashSet<(MemoryType, String)> = records.iter().map(|r| (r.memory_type, r.id.clone())).collect();
        let mut frontier: Vec<(adb_core::MemoryRecord, f64)> = records.iter().map(|r| (r.clone(), 1.0)).collect();
        let mut reached = Vec::new();
        let mut walked = Vec::new();
        for hop in 1..=depth.clamp(1, MAX_DEPTH) {
            let sources: Vec<adb_core::MemoryRecord> = frontier.iter().map(|(r, _)| r.clone()).collect();
            let mut by_record = self.links_of(&sources, select).await?;
            // Each new target once, at its best path weight this hop, in link order.
            let mut order: Vec<(MemoryType, String)> = Vec::new();
            let mut best: HashMap<(MemoryType, String), f64> = HashMap::new();
            for (r, w) in &frontier {
                for l in by_record.remove(&(r.memory_type, r.id.clone())).unwrap_or_default() {
                    // The far end: the target of an outbound link, the source of an inbound one.
                    let to = if l.from_type == r.memory_type && l.from_id == r.id && select.direction.outbound() {
                        (l.to_type, l.to_id.clone())
                    } else {
                        (l.from_type, l.from_id.clone())
                    };
                    if !seen.contains(&to) {
                        let pw = w * f64::from(l.weight);
                        match best.get_mut(&to) {
                            Some(b) => *b = b.max(pw),
                            None => {
                                best.insert(to.clone(), pw);
                                order.push(to);
                            }
                        }
                    }
                    walked.push(l);
                }
            }
            let mut wanted: HashMap<MemoryType, Vec<String>> = HashMap::new();
            for (memory, id) in &order {
                wanted.entry(*memory).or_default().push(id.clone());
            }
            let mut found: HashMap<(MemoryType, String), adb_core::MemoryRecord> = HashMap::new();
            for (memory, ids) in wanted {
                for (id, r) in self.reads.records_by_id(memory, &ids).await? {
                    found.insert((memory, id), r);
                }
            }
            let mut level: Vec<(adb_core::MemoryRecord, f64)> = order
                .into_iter()
                .filter_map(|k| {
                    seen.insert(k.clone());
                    let w = best[&k];
                    found.remove(&k).map(|r| (r, w))
                })
                .collect();
            // Stable: equal weights keep link order.
            level.sort_by(|a, b| b.1.total_cmp(&a.1));
            level.truncate(MAX_REACHED.saturating_sub(reached.len()));
            for (r, w) in &level {
                let mut r = r.clone();
                if let Some(obj) = r.data.as_object_mut() {
                    obj.insert("_hop".into(), serde_json::json!(hop));
                    obj.insert("_path_weight".into(), serde_json::json!(w));
                }
                reached.push(r);
            }
            if level.is_empty() || reached.len() >= MAX_REACHED {
                break;
            }
            frontier = level;
        }
        Ok((reached, walked))
    }

    /// `FOLLOW LINKS` from a RECALL / LOOKUP step: the reached records, projected by
    /// its `RETURN` fields.
    async fn follow(&self, records: &[adb_core::MemoryRecord], step: &StepPlan) -> ExecutorResult<Option<Vec<adb_core::MemoryRecord>>> {
        let Some(follow_links) = &step.modifiers.follow_links else {
            return Ok(None);
        };
        let (reached, _) = self.traverse(records, &follow_links.select, follow_links.depth.unwrap_or(1)).await?;
        Ok(Some(match &step.modifiers.return_fields {
            Some(fields) => reached
                .into_iter()
                .map(|mut r| {
                    r.data = r.project_fields(fields);
                    r
                })
                .collect(),
            None => reached,
        }))
    }

    async fn execute_scan(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        let records = self.reads.scan(&step.modifiers).await?;

        Ok(QueryResult::records(
            records,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn execute_recall(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        // For aggregation, we need records WITHOUT field projection applied
        // So we recall with predicate and filters, but apply RETURN later
        let has_aggregate = step.modifiers.aggregate.is_some();

        let records = if has_aggregate {
            // Create modifiers without return_fields to preserve all fields for aggregation
            let mut mods_for_recall = step.modifiers.clone();
            mods_for_recall.return_fields = None;
            // With GROUP BY, ORDER BY and LIMIT apply to the groups, not the records.
            if mods_for_recall.group_by.is_some() {
                mods_for_recall.order_by = None;
                mods_for_recall.limit = None;
            }
            self.reads
                .recall_with_modifiers(step.memory_type, &step.predicate, &mods_for_recall)
                .await?
        } else {
            self.reads
                .recall_with_modifiers(step.memory_type, &step.predicate, &step.modifiers)
                .await?
        };

        // Handle aggregation if present - works for all memory types
        if let (Some(fields), Some(_)) = (&step.modifiers.group_by, &step.modifiers.aggregate) {
            let rows = crate::query::group_aggregate(&records, fields, &step.modifiers);
            return Ok(QueryResult::aggregation(serde_json::Value::Array(rows), start.elapsed().as_millis() as u64));
        }
        if let Some(agg_funcs) = &step.modifiers.aggregate {
            let agg_result = crate::query::aggregate(&records, agg_funcs);

            // Apply HAVING filter if present
            if let Some(having_conditions) = &step.modifiers.having {
                // Check if aggregation results match HAVING conditions
                let matches_having = having_conditions.iter().all(|cond| {
                    cond.matches(&agg_result)
                });

                // If HAVING fails, return the aggregation with a flag or empty
                // Based on SQL semantics, HAVING filters groups - if condition fails,
                // the entire group is excluded, returning no data
                if !matches_having {
                    return Ok(QueryResult::aggregation(
                        serde_json::json!(null),
                        start.elapsed().as_millis() as u64,
                    ));
                }
            }

            return Ok(QueryResult::aggregation(
                agg_result,
                start.elapsed().as_millis() as u64,
            ));
        }

        // FOLLOW LINKS returns the reached records, not the recalled ones.
        if let Some(reached) = self.follow(&records, step).await? {
            return Ok(QueryResult::records(reached, start.elapsed().as_millis() as u64));
        }

        // Handle WITH LINKS - add links array to each record
        if let Some(with_links) = &step.modifiers.with_links {
            let mut records_with_links = Vec::new();

            let mut by_record = self.links_of(&records, with_links).await?;
            for mut record in records {
                let links = by_record.remove(&(record.memory_type, record.id.clone())).unwrap_or_default();

                // Add links array to record data
                if let Some(obj) = record.data.as_object_mut() {
                    let links_json: Vec<serde_json::Value> = links.iter().map(|l| link_json(l, with_links)).collect();
                    obj.insert("links".to_string(), serde_json::Value::Array(links_json));
                }

                records_with_links.push(record);
            }

            return Ok(QueryResult::records(
                records_with_links,
                start.elapsed().as_millis() as u64,
            ));
        }

        Ok(QueryResult::records(
            records,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn execute_lookup(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        // A merged entity's key finds the entity it merged into (GR-12).
        let resolved;
        let step = match (&step.memory_type, &step.predicate) {
            (MemoryType::Semantic, Predicate::Key { field, value: adb_core::Value::String(k) }) if matches!(field.as_str(), "id" | "key") => {
                match self.writes.alias_of(k) {
                    Some(to) => {
                        resolved = StepPlan { predicate: Predicate::Key { field: field.clone(), value: adb_core::Value::String(to) }, ..step.clone() };
                        &resolved
                    }
                    None => step,
                }
            }
            _ => step,
        };

        let records = self
            .reads
            .lookup_with_modifiers(step.memory_type, &step.predicate, &step.modifiers)
            .await?;

        // FOLLOW LINKS returns the reached records, as RECALL does (L6 / GR-7).
        if let Some(reached) = self.follow(&records, step).await? {
            return Ok(QueryResult::records(reached, start.elapsed().as_millis() as u64));
        }

        // Handle WITH LINKS - add links array to each record
        if let Some(with_links) = &step.modifiers.with_links {
            let mut records_with_links = Vec::new();

            let mut by_record = self.links_of(&records, with_links).await?;
            for mut record in records {
                let links = by_record.remove(&(record.memory_type, record.id.clone())).unwrap_or_default();

                if let Some(obj) = record.data.as_object_mut() {
                    let links_json: Vec<serde_json::Value> = links.iter().map(|l| link_json(l, with_links)).collect();
                    obj.insert("links".to_string(), serde_json::Value::Array(links_json));
                }

                records_with_links.push(record);
            }

            return Ok(QueryResult::records(
                records_with_links,
                start.elapsed().as_millis() as u64,
            ));
        }

        Ok(QueryResult::records(
            records,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn execute_load(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        let limit = step.modifiers.limit.unwrap_or(10);
        let records = self.reads.load(&step.predicate, limit).await?;

        Ok(QueryResult::records(
            records,
            start.elapsed().as_millis() as u64,
        ))
    }

    /// SUBSCRIBE / UNSUBSCRIBE / ACK / PUBLISH (stage E, E_SUBSCRIPTIONS.md): each is a
    /// committed write; deliveries are read through the gateway, not here.
    async fn execute_subscription(&self, plan: &SubscriptionPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        let value = match plan {
            SubscriptionPlan::Subscribe { target, kinds, filter, from } => {
                let target = match target {
                    SubscriptionTarget::Scope(s) => adb_gateway::Target::Scope(s.clone()),
                    SubscriptionTarget::Topic(t) => adb_gateway::Target::Topic(t.clone()),
                };
                let kinds = kinds
                    .iter()
                    .map(|k| serde_json::from_value(serde_json::Value::String(k.clone())))
                    .collect::<Result<Vec<adb_core::log::Kind>, _>>()
                    .map_err(|e| ExecutorError::InvalidOperation(format!("KINDS: {e}")))?;
                let spec = adb_gateway::SubSpec { target, kinds, filter: filter.clone() };
                let (id, start_offset) = self.writes.subscribe(spec, *from).await?;
                serde_json::json!({ "subscription": id, "start_offset": start_offset })
            }
            SubscriptionPlan::Unsubscribe { subscription } => {
                let existed = self.writes.unsubscribe(subscription).await?;
                serde_json::json!({ "subscription": subscription, "unsubscribed": existed })
            }
            SubscriptionPlan::Ack { subscription, offset } => {
                let cursor = self.writes.ack(subscription, *offset).await?;
                serde_json::json!({ "subscription": subscription, "cursor": cursor })
            }
            SubscriptionPlan::Publish { topic, payload, ttl } => {
                let (id, offset) = self.writes.publish(topic, payload.clone(), *ttl).await?;
                serde_json::json!({ "id": id, "topic": topic, "offset": offset })
            }
        };
        Ok(QueryResult::subscription(value, start.elapsed().as_millis() as u64))
    }

    async fn execute_store(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        let data = step
            .data
            .as_ref()
            .ok_or_else(|| ExecutorError::MissingData("store data".to_string()))?;

        // Extract store data
        let key = data
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExecutorError::MissingData("key".to_string()))?;

        let payload = data
            .get("payload")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        let scope = data
            .get("scope")
            .and_then(|v| v.as_str())
            .and_then(|s| match s {
                "Private" => Some(Scope::Private),
                "Shared" => Some(Scope::Shared),
                "Cluster" => Some(Scope::Cluster),
                _ => None,
            })
            .unwrap_or(Scope::Private);

        let namespace = data.get("namespace").and_then(|v| v.as_str());
        let in_scope = data.get("in_scope").and_then(|v| v.as_str());

        let ttl = data
            .get("ttl_ms")
            .and_then(|v| v.as_u64())
            .map(std::time::Duration::from_millis);

        let record = match data.get("fact") {
            // STORE … FACT [MULTI] (P1_GRAPH D2): the writes give it its key.
            Some(fact) if !fact.is_null() => {
                let multi = fact.get("multi").and_then(|v| v.as_bool()).unwrap_or(false);
                self.writes.store_fact(payload, multi, scope, namespace, ttl, in_scope).await?
            }
            _ => self.writes.store(step.memory_type, key, payload, scope, namespace, ttl, in_scope).await?,
        };

        Ok(QueryResult::stored(
            record,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn execute_update(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        let data = step
            .data
            .as_ref()
            .ok_or_else(|| ExecutorError::MissingData("update data".to_string()))?
            .clone();

        let count = self
            .writes
            .update(step.memory_type, &step.predicate, data, step.modifiers.in_scope.as_deref())
            .await?;

        Ok(QueryResult::count(count, start.elapsed().as_millis() as u64))
    }

    async fn execute_forget(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        let count = self.writes.forget(step.memory_type, &step.predicate, step.modifiers.in_scope.as_deref()).await?;

        Ok(QueryResult::count(count, start.elapsed().as_millis() as u64))
    }

    async fn execute_link(&self, step: &StepPlan) -> ExecutorResult<QueryResult> {
        let start = Instant::now();

        // Extract link data from step.data
        let data = step
            .data
            .as_ref()
            .ok_or_else(|| ExecutorError::MissingData("link data".to_string()))?;

        // Get link metadata
        let link_type = data
            .get("link_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExecutorError::MissingData("link_type".to_string()))?;

        // `None` when the LINK has no WEIGHT: 1.0 for a new link, unchanged for an
        // existing one (AQL v0.7 §3).
        let weight = data.get("set_weight").and_then(|v| v.as_f64()).map(|w| w as f32);

        // Parse target memory type
        let to_type_str = data
            .get("to_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExecutorError::MissingData("to_type".to_string()))?;

        let to_type = match to_type_str {
            "Working" => MemoryType::Working,
            "Tools" => MemoryType::Tools,
            "Procedural" => MemoryType::Procedural,
            "Semantic" => MemoryType::Semantic,
            "Episodic" => MemoryType::Episodic,
            _ => return Err(ExecutorError::InvalidOperation(format!("Invalid target memory type: {}", to_type_str))),
        };

        // Build target predicate from to_conditions
        let to_predicate = self.build_predicate_from_conditions(data.get("to_conditions"))?;

        // Look up source records using step.memory_type and step.predicate
        let source_records = self
            .reads
            .lookup(step.memory_type, &step.predicate)
            .await?;

        if source_records.is_empty() {
            return Err(ExecutorError::LinkValidation(
                format!("No source records found in {:?} matching predicate", step.memory_type)
            ));
        }

        // Look up target records
        let target_records = self
            .reads
            .lookup(to_type, &to_predicate)
            .await?;

        if target_records.is_empty() {
            return Err(ExecutorError::LinkValidation(
                format!("No target records found in {:?} matching predicate", to_type)
            ));
        }

        // Create links between all matching source and target records
        let mut link_count = 0u64;
        for source in &source_records {
            for target in &target_records {
                self.writes
                    .link(step.memory_type, &source.id, to_type, &target.id, link_type, weight)
                    .await?;
                link_count += 1;
            }
        }

        Ok(QueryResult::count(link_count, start.elapsed().as_millis() as u64))
    }

    /// AQL v0.7 UPDATE LINK (`unlink` false) / UNLINK: the links between the
    /// matched records, of `type_filter` if given. No matching records → 0, as
    /// UPDATE and FORGET with no match.
    async fn execute_link_edit(&self, step: &StepPlan, unlink: bool) -> ExecutorResult<QueryResult> {
        let start = Instant::now();
        let data = step.data.as_ref().ok_or_else(|| ExecutorError::MissingData("link data".to_string()))?;
        let to_type = match data.get("to_type").and_then(|v| v.as_str()) {
            Some("Working") => MemoryType::Working,
            Some("Tools") => MemoryType::Tools,
            Some("Procedural") => MemoryType::Procedural,
            Some("Semantic") => MemoryType::Semantic,
            Some("Episodic") => MemoryType::Episodic,
            other => return Err(ExecutorError::InvalidOperation(format!("Invalid target memory type: {other:?}"))),
        };
        let to_predicate = self.build_predicate_from_conditions(data.get("to_conditions"))?;
        let type_filter = data.get("type_filter").and_then(|v| v.as_str());
        let ids = |records: Vec<adb_core::MemoryRecord>| records.into_iter().map(|r| r.id).collect::<Vec<_>>();
        let from_ids = ids(self.reads.lookup(step.memory_type, &step.predicate).await?);
        let to_ids = ids(self.reads.lookup(to_type, &to_predicate).await?);
        let n = if from_ids.is_empty() || to_ids.is_empty() {
            0
        } else if unlink {
            self.writes.unlink(step.memory_type, &from_ids, to_type, &to_ids, type_filter).await?
        } else {
            let weight = data.get("set_weight").and_then(|v| v.as_f64()).map(|w| w as f32);
            let new_type = data.get("set_type").and_then(|v| v.as_str());
            self.writes.update_links(step.memory_type, &from_ids, to_type, &to_ids, type_filter, weight, new_type).await?
        };
        Ok(QueryResult::count(n, start.elapsed().as_millis() as u64))
    }

    /// Build a predicate from serialized conditions array
    fn build_predicate_from_conditions(&self, conditions_value: Option<&serde_json::Value>) -> ExecutorResult<Predicate> {
        let conditions_arr = conditions_value
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExecutorError::MissingData("to_conditions".to_string()))?;

        if conditions_arr.is_empty() {
            return Ok(Predicate::All);
        }

        let mut conditions = Vec::new();
        for cond_val in conditions_arr {
            let field = cond_val
                .get("field")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ExecutorError::MissingData("condition field".to_string()))?
                .to_string();

            let operator_str = cond_val
                .get("operator")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ExecutorError::MissingData("condition operator".to_string()))?;

            let operator = match operator_str {
                "Eq" => adb_core::Operator::Eq,
                "Ne" => adb_core::Operator::Ne,
                "Gt" => adb_core::Operator::Gt,
                "Gte" => adb_core::Operator::Gte,
                "Lt" => adb_core::Operator::Lt,
                "Lte" => adb_core::Operator::Lte,
                "Contains" => adb_core::Operator::Contains,
                "StartsWith" => adb_core::Operator::StartsWith,
                "EndsWith" => adb_core::Operator::EndsWith,
                "In" => adb_core::Operator::In,
                _ => return Err(ExecutorError::InvalidOperation(format!("Unknown operator: {}", operator_str))),
            };

            let value = cond_val
                .get("value")
                .map(|v| json_to_value(v))
                .unwrap_or(Value::Null);

            conditions.push(Condition::Simple {
                field,
                operator,
                value,
                logical_op: None,
            });
        }

        Ok(Predicate::Where { conditions })
    }
}

/// Convert a JSON value to a Value type
fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(_) => {
            // Convert object to string representation
            Value::String(json.to_string())
        }
    }
}

/// A link in a record's `links` (`WITH LINKS`). Inbound or both: with its source too.
fn link_json(l: &adb_core::Link, select: &adb_core::LinkSelect) -> serde_json::Value {
    let mut j = serde_json::json!({
        "link_type": l.link_type,
        "to_type": serde_json::to_value(l.to_type).unwrap_or_default(),
        "to_id": l.to_id,
        "weight": l.weight
    });
    if select.direction.inbound() {
        j["from_type"] = serde_json::to_value(l.from_type).unwrap_or_default();
        j["from_id"] = serde_json::json!(l.from_id);
    }
    j
}
