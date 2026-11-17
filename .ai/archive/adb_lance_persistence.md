# ADB LanceDB Persistence Architecture

> **Superseded by [`ARCHITECTURE.md`](ARCHITECTURE.md) v1.0 (5 Oct 2026).** Kept for history: this is the v0.2 (Lance persistence draft). Its query-engine design carries over; storage, persistence and deployment are replaced (see ARCHITECTURE.md §2).


> **Version:** 0.2.0
> **Date:** April 2026
> **Status:** Design Draft

---

## 1. Overview

ADB requires durable persistence to enable:
- **Session Recovery**: Agents resume from exact memory state after restart
- **Cross-VM Migration**: Agents relocate to any runtime with full memory intact
- **Scalable Storage**: Handle large semantic/episodic datasets beyond RAM limits

**LanceDB** is chosen for persistence because:
- Native vector search (semantic memory)
- Columnar format (efficient time-range queries for episodic)
- Zero-copy reads (fast session loading)
- Embedded or serverless deployment options

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ADB Instance                                    │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         Cache Layer (In-Memory)                        │  │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │  │
│  │  │ Working │ │  Tools  │ │Procedur.│ │Semantic │ │Episodic │        │  │
│  │  │ Cache   │ │ Cache   │ │ Cache   │ │ Cache   │ │ Cache   │        │  │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘        │  │
│  └───────┼──────────┼──────────┼──────────┼──────────┼─────────────────┘  │
│          │          │          │          │          │                     │
│  ┌───────┴──────────┴──────────┴──────────┴──────────┴─────────────────┐  │
│  │                      Persistence Manager                              │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │  │
│  │  │ Cache Policy │  │ Write Buffer │  │ Read Strategy│               │  │
│  │  │   Engine     │  │    Queue     │  │   Resolver   │               │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                    │                                         │
└────────────────────────────────────┼─────────────────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │        LanceDB Storage          │
                    │  ┌──────────────────────────┐   │
                    │  │ working_{agent_id}       │   │
                    │  │ tools_{namespace}        │   │
                    │  │ procedural_{namespace}   │   │
                    │  │ semantic_{namespace}     │   │
                    │  │ episodic_{namespace}     │   │
                    │  │ agent_config_{agent_id}  │   │
                    │  │ links_{namespace}        │   │
                    │  └──────────────────────────┘   │
                    │                                  │
                    │  Storage: Local FS / S3 / GCS   │
                    └─────────────────────────────────┘
```

---

## 3. Memory Type Persistence Strategies

### 3.1 Strategy Matrix

| Memory Type | Default Read | Default Write | Load on Start | Typical Size |
|-------------|--------------|---------------|---------------|--------------|
| Working     | Local        | Write-through | Full          | Small (<1MB) |
| Tools       | Read-through | Write-through | Full          | Small (<10MB)|
| Procedural  | Read-through | Write-through | Full          | Medium       |
| Semantic    | Read-through | Write-behind  | Topic subset  | Large        |
| Episodic    | Read-through | Write-behind  | Time window   | Large        |

### 3.2 Read Strategies

```rust
#[derive(Debug, Clone, Copy)]
pub enum ReadStrategy {
    /// Only read from in-memory cache, never hit storage
    /// Use: Hot working memory during active session
    LocalOnly,

    /// Check cache first, fall back to LanceDB on miss
    /// Use: Procedural/Tools that may be shared across agents
    ReadThrough,

    /// Always read from LanceDB, cache result
    /// Use: When consistency with other agents is critical
    ReadFromStorage,

    /// Read from LanceDB, don't cache (streaming large results)
    /// Use: Large episodic queries that exceed cache budget
    StreamFromStorage,
}
```

### 3.3 Write Strategies

```rust
#[derive(Debug, Clone, Copy)]
pub enum WriteStrategy {
    /// Only write to cache, never persist (volatile)
    /// Use: Temporary scratch data
    LocalOnly,

    /// Write to cache and immediately to LanceDB (sync)
    /// Use: Critical data that must survive crashes
    WriteThrough,

    /// Write to cache, async flush to LanceDB
    /// Use: High-throughput episodic/semantic writes
    WriteBehind {
        flush_interval_ms: u64,
        max_buffer_size: usize,
    },

    /// Write to LanceDB only, invalidate cache
    /// Use: Bulk imports, migrations
    WriteAround,
}
```

---

## 4. Session Loading Strategy

### 4.1 Agent Startup Sequence

```
┌─────────────────────────────────────────────────────────────────┐
│                    Agent Startup Sequence                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Load Agent Config                                           │
│     └── agent_config_{agent_id} → AgentConfiguration            │
│                                                                  │
│  2. Load Working Memory (FULL)                                  │
│     └── SELECT * FROM working_{agent_id}                        │
│     └── Restore complete session state                          │
│                                                                  │
│  3. Load Procedural Memory (FULL)                               │
│     └── SELECT * FROM procedural_{namespace}                    │
│     └── All patterns, runbooks, learned procedures              │
│                                                                  │
│  4. Load Tools Registry (FULL)                                  │
│     └── SELECT * FROM tools_{namespace}                         │
│     └── All available tools with rankings                       │
│                                                                  │
│  5. Load Episodic Window (CONFIGURED)                           │
│     └── SELECT * FROM episodic_{namespace}                      │
│         WHERE created_at > {now - window_duration}              │
│         OR id IN (SELECT id FROM episodic_pinned)               │
│         LIMIT {max_episodes}                                     │
│                                                                  │
│  6. Load Semantic Topics (CONFIGURED)                           │
│     └── SELECT * FROM semantic_{namespace}                      │
│         WHERE topic IN {subscribed_topics}                      │
│         OR similarity(embedding, agent_context) > threshold     │
│                                                                  │
│  7. Load Links (for loaded records)                             │
│     └── SELECT * FROM links_{namespace}                         │
│         WHERE from_id IN {loaded_ids} OR to_id IN {loaded_ids}  │
│                                                                  │
│  8. Agent Ready                                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Window Configuration

```rust
/// Configuration for what to load at session start
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLoadConfig {
    /// Working memory: always load full
    pub working: WorkingLoadConfig,

    /// Procedural: load full or filtered
    pub procedural: ProceduralLoadConfig,

    /// Tools: load full or by category
    pub tools: ToolsLoadConfig,

    /// Episodic: load by time window and/or count
    pub episodic: EpisodicLoadConfig,

    /// Semantic: load by topics or similarity
    pub semantic: SemanticLoadConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodicLoadConfig {
    /// Load episodes from last N duration
    pub time_window: Option<Duration>,

    /// Maximum number of episodes to load
    pub max_count: Option<usize>,

    /// Always load pinned/starred episodes
    pub include_pinned: bool,

    /// Filter by specific event types
    pub event_types: Option<Vec<String>>,

    /// Minimum importance score
    pub min_importance: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLoadConfig {
    /// Explicitly subscribed topics
    pub subscribed_topics: Vec<String>,

    /// Load semantically similar to agent's current context
    pub context_similarity_threshold: Option<f32>,

    /// Maximum concepts to load
    pub max_concepts: Option<usize>,

    /// Required confidence level
    pub min_confidence: Option<f32>,
}
```

---

## 5. Cache Policy Configuration

### 5.1 Per-Memory-Type Configuration

```rust
/// Full cache configuration for an ADB instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Global defaults
    pub defaults: CachePolicyDefaults,

    /// Per-memory-type overrides
    pub working: Option<MemoryCachePolicy>,
    pub tools: Option<MemoryCachePolicy>,
    pub procedural: Option<MemoryCachePolicy>,
    pub semantic: Option<MemoryCachePolicy>,
    pub episodic: Option<MemoryCachePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCachePolicy {
    /// Read strategy
    pub read: ReadStrategy,

    /// Write strategy
    pub write: WriteStrategy,

    /// Maximum cache size (records or bytes)
    pub max_cache_size: CacheLimit,

    /// Cache TTL (for read-through entries)
    pub ttl: Option<Duration>,

    /// Eviction policy when cache is full
    pub eviction: EvictionPolicy,

    /// Pre-warm cache on startup
    pub warm_on_start: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheLimit {
    Records(usize),
    Bytes(usize),
    Unlimited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionPolicy {
    LRU,           // Least Recently Used
    LFU,           // Least Frequently Used
    FIFO,          // First In First Out
    TTL,           // Oldest by creation time
    Importance,    // Lowest importance score first
}
```

### 5.2 Example Configurations

```yaml
# High-consistency agent (financial, compliance)
cache:
  working:
    read: local_only
    write: write_through
  procedural:
    read: read_through
    write: write_through
  episodic:
    read: read_through
    write: write_through  # Every action persisted immediately

# High-throughput agent (data processing, monitoring)
cache:
  working:
    read: local_only
    write: write_behind
    flush_interval_ms: 5000
    max_buffer_size: 1000
  episodic:
    read: stream_from_storage  # Don't cache large history
    write: write_behind
    flush_interval_ms: 10000
    max_buffer_size: 10000

# Collaborative agent (shared knowledge base)
cache:
  semantic:
    read: read_from_storage  # Always get latest shared facts
    write: write_through
  procedural:
    read: read_from_storage  # Always get latest procedures
    write: write_through
```

---

## 6. Agent Configuration Persistence

### 6.1 Agent Configuration Schema

```rust
/// Complete agent configuration for persistence and recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfiguration {
    /// Unique agent identifier
    pub agent_id: String,

    /// Agent version (for migration compatibility)
    pub version: String,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last active timestamp
    pub last_active_at: DateTime<Utc>,

    /// Namespace for memory isolation
    pub namespace: String,

    /// Agent's behavioral configuration
    pub behavior: AgentBehavior,

    /// Memory loading configuration
    pub session_load: SessionLoadConfig,

    /// Cache policies
    pub cache_config: CacheConfig,

    /// Subscribed semantic topics
    pub semantic_subscriptions: Vec<String>,

    /// Custom agent metadata
    pub metadata: serde_json::Value,

    /// Recovery checkpoint
    pub checkpoint: Option<AgentCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentBehavior {
    /// Default scope for new memories
    pub default_scope: Scope,

    /// Default TTL for working memory
    pub working_ttl: Option<Duration>,

    /// Default TTL for episodic memory
    pub episodic_ttl: Option<Duration>,

    /// Auto-link creation enabled
    pub auto_linking: bool,

    /// Minimum confidence for procedural execution
    pub min_procedural_confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCheckpoint {
    /// Checkpoint timestamp
    pub timestamp: DateTime<Utc>,

    /// Working memory snapshot ID
    pub working_snapshot_id: String,

    /// Last processed episodic ID
    pub last_episodic_id: Option<String>,

    /// Active procedure states
    pub active_procedures: Vec<ProcedureState>,

    /// Pending write buffer (for recovery)
    pub pending_writes: Vec<PendingWrite>,
}
```

### 6.2 Recovery Process

```
┌─────────────────────────────────────────────────────────────────┐
│                    Agent Recovery Process                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Load AgentConfiguration from LanceDB                        │
│     └── agent_config_{agent_id}                                 │
│                                                                  │
│  2. Check for Checkpoint                                        │
│     ├── If checkpoint exists:                                   │
│     │   └── Restore from checkpoint snapshot                    │
│     │   └── Replay pending_writes to LanceDB                    │
│     │   └── Resume active_procedures                            │
│     └── If no checkpoint:                                       │
│         └── Full session load (Section 4.1)                     │
│                                                                  │
│  3. Verify Memory Integrity                                     │
│     └── Check record counts match expected                      │
│     └── Verify link consistency                                 │
│     └── Validate semantic embeddings                            │
│                                                                  │
│  4. Apply Pending Operations                                    │
│     └── Process any write-behind buffer entries                 │
│     └── Re-execute interrupted procedures                       │
│                                                                  │
│  5. Update last_active_at                                       │
│     └── Mark agent as active in agent_config                    │
│                                                                  │
│  6. Agent Ready (identical state to pre-crash)                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. LanceDB Schema Design

### 7.1 Table Schemas

```sql
-- Working Memory Table
CREATE TABLE working_{agent_id} (
    id              VARCHAR PRIMARY KEY,
    data            JSON,
    created_at      TIMESTAMP,
    accessed_at     TIMESTAMP,
    scope           VARCHAR,
    ttl_ms          BIGINT,
    version         BIGINT
);

-- Tools Registry Table
CREATE TABLE tools_{namespace} (
    id              VARCHAR PRIMARY KEY,
    tool_id         VARCHAR UNIQUE,
    name            VARCHAR,
    description     TEXT,
    schema          JSON,
    category        VARCHAR,
    ranking         FLOAT,
    call_count      BIGINT,
    last_called     TIMESTAMP,
    relevance       JSON,  -- task_type -> score mapping
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- Procedural Memory Table
CREATE TABLE procedural_{namespace} (
    id              VARCHAR PRIMARY KEY,
    pattern_id      VARCHAR UNIQUE,
    pattern         TEXT,
    steps           JSON,
    severity        VARCHAR,
    confidence      FLOAT,
    source          VARCHAR,
    success_count   BIGINT,
    failure_count   BIGINT,
    variables       JSON,  -- tunable parameters
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- Graph edges stored in links table
);

-- Semantic Memory Table (with vector embeddings)
CREATE TABLE semantic_{namespace} (
    id              VARCHAR PRIMARY KEY,
    concept         VARCHAR,
    knowledge       TEXT,
    embedding       VECTOR(768),  -- sentence-transformers dimension
    topic           VARCHAR,
    confidence      FLOAT,
    source          VARCHAR,
    created_at      TIMESTAMP,
    accessed_at     TIMESTAMP,
    version         BIGINT
);

-- Episodic Memory Table
CREATE TABLE episodic_{namespace} (
    id              VARCHAR PRIMARY KEY,
    data            JSON,
    event_type      VARCHAR,
    importance      FLOAT,
    pinned          BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP,
    accessed_at     TIMESTAMP,
    scope           VARCHAR,
    ttl_ms          BIGINT,
    version         BIGINT
);

-- Links Table (Ontology Edges)
CREATE TABLE links_{namespace} (
    id              VARCHAR PRIMARY KEY,
    from_type       VARCHAR,  -- WORKING, TOOLS, PROCEDURAL, SEMANTIC, EPISODIC
    from_id         VARCHAR,
    to_type         VARCHAR,
    to_id           VARCHAR,
    link_type       VARCHAR,  -- "applied_to", "triggers", "requires", etc.
    weight          FLOAT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,

    INDEX idx_from (from_type, from_id),
    INDEX idx_to (to_type, to_id),
    INDEX idx_type (link_type)
);

-- Agent Configuration Table
CREATE TABLE agent_config (
    agent_id        VARCHAR PRIMARY KEY,
    version         VARCHAR,
    namespace       VARCHAR,
    config          JSON,  -- Full AgentConfiguration
    checkpoint      JSON,  -- Optional AgentCheckpoint
    created_at      TIMESTAMP,
    last_active_at  TIMESTAMP,

    INDEX idx_namespace (namespace),
    INDEX idx_active (last_active_at)
);
```

### 7.2 Indexing Strategy

```rust
/// Index configuration for LanceDB tables
pub struct IndexConfig {
    /// Semantic memory: IVF-PQ index for vector search
    pub semantic_vector_index: VectorIndexConfig,

    /// Episodic: B-tree on created_at for time-range queries
    pub episodic_time_index: bool,

    /// Links: Composite index on (from_type, from_id) and (to_type, to_id)
    pub links_composite_index: bool,
}

pub struct VectorIndexConfig {
    /// Index type: IVF_PQ, IVF_FLAT, HNSW
    pub index_type: VectorIndexType,

    /// Number of partitions (IVF)
    pub num_partitions: usize,

    /// Number of sub-vectors (PQ)
    pub num_sub_vectors: usize,

    /// Rebuild threshold (% of new data)
    pub rebuild_threshold: f32,
}
```

---

## 8. Write-Behind Buffer

### 8.1 Buffer Implementation

```rust
/// Write-behind buffer for async persistence
pub struct WriteBehindBuffer {
    /// Buffered writes per memory type
    buffers: HashMap<MemoryType, VecDeque<BufferedWrite>>,

    /// Maximum buffer size per type
    max_sizes: HashMap<MemoryType, usize>,

    /// Flush interval per type
    flush_intervals: HashMap<MemoryType, Duration>,

    /// Last flush time per type
    last_flush: HashMap<MemoryType, Instant>,

    /// Persistence handle
    lance_client: Arc<LanceClient>,

    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct BufferedWrite {
    pub id: String,
    pub operation: WriteOperation,
    pub memory_type: MemoryType,
    pub data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum WriteOperation {
    Insert,
    Update,
    Delete,
}

impl WriteBehindBuffer {
    /// Add write to buffer
    pub async fn buffer(&self, write: BufferedWrite) -> AdbResult<()> {
        let buffer = self.buffers.get_mut(&write.memory_type)
            .ok_or(AdbError::InvalidMemoryType)?;

        buffer.push_back(write.clone());

        // Check if flush needed
        if buffer.len() >= self.max_sizes[&write.memory_type] {
            self.flush(write.memory_type).await?;
        }

        Ok(())
    }

    /// Flush buffer to LanceDB
    pub async fn flush(&self, memory_type: MemoryType) -> AdbResult<usize> {
        let buffer = self.buffers.get_mut(&memory_type)
            .ok_or(AdbError::InvalidMemoryType)?;

        let writes: Vec<_> = buffer.drain(..).collect();
        let count = writes.len();

        if count == 0 {
            return Ok(0);
        }

        // Batch write to LanceDB
        self.lance_client.batch_write(memory_type, writes).await?;

        self.last_flush.insert(memory_type, Instant::now());

        Ok(count)
    }

    /// Background flusher task
    pub async fn run_flusher(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        while !self.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;

            for (memory_type, last) in &self.last_flush {
                let flush_interval = self.flush_intervals[memory_type];
                if last.elapsed() >= flush_interval {
                    if let Err(e) = self.flush(*memory_type).await {
                        tracing::error!("Flush error for {:?}: {}", memory_type, e);
                    }
                }
            }
        }

        // Final flush on shutdown
        for memory_type in self.buffers.keys() {
            let _ = self.flush(*memory_type).await;
        }
    }
}
```

### 8.2 Crash Recovery for Write-Behind

```rust
/// Recovery of write-behind buffer after crash
impl WriteBehindBuffer {
    /// Persist buffer state for crash recovery
    pub async fn checkpoint(&self) -> AdbResult<()> {
        let checkpoint = BufferCheckpoint {
            timestamp: Utc::now(),
            buffers: self.buffers.iter()
                .map(|(k, v)| (*k, v.iter().cloned().collect()))
                .collect(),
        };

        self.lance_client.save_checkpoint(&checkpoint).await
    }

    /// Recover buffer state after crash
    pub async fn recover(&mut self) -> AdbResult<usize> {
        let checkpoint = self.lance_client.load_checkpoint().await?;

        let mut recovered = 0;
        for (memory_type, writes) in checkpoint.buffers {
            for write in writes {
                self.buffers.get_mut(&memory_type)
                    .map(|b| b.push_back(write));
                recovered += 1;
            }
        }

        // Immediately flush recovered writes
        for memory_type in self.buffers.keys().cloned().collect::<Vec<_>>() {
            self.flush(memory_type).await?;
        }

        Ok(recovered)
    }
}
```

---

## 9. Implementation Phases

### Phase 1: Core Persistence (Week 1-2)
- [ ] LanceDB client wrapper
- [ ] Schema creation and migration
- [ ] Basic CRUD operations for all memory types
- [ ] Write-through implementation

### Phase 2: Caching Layer (Week 3-4)
- [ ] Cache policy engine
- [ ] Read-through implementation
- [ ] Write-behind buffer with flush
- [ ] Cache eviction policies

### Phase 3: Session Management (Week 5-6)
- [ ] Agent configuration persistence
- [ ] Session loading with windows
- [ ] Checkpoint/recovery mechanism
- [ ] Cross-VM migration testing

### Phase 4: Optimization (Week 7-8)
- [ ] Vector index tuning for semantic
- [ ] Time-range index for episodic
- [ ] Batch operation optimization
- [ ] Performance benchmarking

---

## 10. API Extensions

### 10.1 New AQL Modifiers

```sql
-- Explicit persistence control
STORE INTO EPISODIC (...) PERSIST IMMEDIATE   -- Force write-through
STORE INTO EPISODIC (...) PERSIST DEFERRED    -- Force write-behind
STORE INTO WORKING (...) PERSIST NONE         -- Volatile only

-- Session loading hints
RECALL FROM EPISODIC WHERE ... WINDOW LAST 7d LIMIT 1000
RECALL FROM SEMANTIC WHERE topic IN ("billing", "payments") PRELOAD

-- Recovery operations
CHECKPOINT AGENT "agent-123"
RECOVER AGENT "agent-123" FROM CHECKPOINT "chk-456"
MIGRATE AGENT "agent-123" TO NAMESPACE "new-ns"
```

### 10.2 Configuration API

```rust
/// Runtime cache policy updates
impl Adb {
    /// Update cache policy for a memory type
    pub async fn set_cache_policy(
        &self,
        memory_type: MemoryType,
        policy: MemoryCachePolicy,
    ) -> AdbResult<()>;

    /// Force flush all write-behind buffers
    pub async fn flush_all(&self) -> AdbResult<FlushStats>;

    /// Create agent checkpoint
    pub async fn checkpoint_agent(
        &self,
        agent_id: &str,
    ) -> AdbResult<AgentCheckpoint>;

    /// Recover agent from checkpoint
    pub async fn recover_agent(
        &self,
        agent_id: &str,
        checkpoint_id: Option<&str>,
    ) -> AdbResult<AgentConfiguration>;

    /// Migrate agent to new namespace/instance
    pub async fn migrate_agent(
        &self,
        agent_id: &str,
        target_namespace: &str,
        target_lance_uri: Option<&str>,
    ) -> AdbResult<MigrationResult>;
}
```

---

## 11. Deployment Configurations

### 11.1 Embedded (Single Agent)

```yaml
lance:
  uri: "./data/lance"
  type: embedded

cache:
  working:
    read: local_only
    write: write_through
  episodic:
    read: read_through
    write: write_behind
    flush_interval_ms: 5000
```

### 11.2 Serverless (Multi-Agent, Cloud)

```yaml
lance:
  uri: "s3://bucket/adb-data"
  type: serverless
  region: us-east-1

cache:
  # All agents share semantic/procedural
  semantic:
    read: read_from_storage
    write: write_through
  procedural:
    read: read_from_storage
    write: write_through
  # Working/Episodic per-agent
  working:
    read: local_only
    write: write_through
  episodic:
    read: read_through
    write: write_behind
    flush_interval_ms: 10000
```

### 11.3 Hybrid (On-Prem + Cloud Backup)

```yaml
lance:
  primary:
    uri: "./data/lance"
    type: embedded
  replica:
    uri: "s3://bucket/adb-backup"
    type: serverless
    sync_interval_ms: 60000

cache:
  defaults:
    write: write_through  # All writes sync'd
```

---

## 12. Observability

### 12.1 Metrics

```rust
/// Persistence metrics
pub struct PersistenceMetrics {
    // Write metrics
    pub writes_total: Counter,
    pub writes_buffered: Counter,
    pub writes_flushed: Counter,
    pub write_latency_ms: Histogram,

    // Read metrics
    pub reads_total: Counter,
    pub cache_hits: Counter,
    pub cache_misses: Counter,
    pub read_latency_ms: Histogram,

    // Buffer metrics
    pub buffer_size: Gauge,
    pub buffer_flushes: Counter,
    pub buffer_flush_latency_ms: Histogram,

    // Recovery metrics
    pub checkpoints_created: Counter,
    pub recoveries_completed: Counter,
    pub recovery_latency_ms: Histogram,
}
```

### 12.2 Health Checks

```rust
/// LanceDB health check
pub struct PersistenceHealth {
    pub lance_connected: bool,
    pub lance_latency_ms: u64,
    pub buffer_healthy: bool,
    pub buffer_size: usize,
    pub last_flush: DateTime<Utc>,
    pub last_checkpoint: Option<DateTime<Utc>>,
}
```

---

## 13. Security Considerations

### 13.1 Data at Rest
- LanceDB supports encryption via storage layer (S3 SSE, GCS CMEK)
- Option for application-level encryption of sensitive fields

### 13.2 Access Control
- Namespace isolation ensures agents only access their data
- Agent ID validation on all persistence operations
- Audit logging for all write operations

### 13.3 Data Retention
- Configurable TTL enforcement at LanceDB level
- Automatic cleanup of expired records
- GDPR-compliant deletion support

---

## 14. References

- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)
- [AQL Specification v0.5](./AQL_v0.5_spec.md)
- [ADB Architecture](./ARCHITECTURE.md)
