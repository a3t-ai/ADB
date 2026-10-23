# ClawDB & WASM Playground - Requirements and Execution Plan

> **Version:** 1.0
> **Date:** April 2026
> **Status:** Planning

---

## 1. Project Overview

Three deployment targets from the same codebase:

| Version | Description | Storage | Compile Target | Use Case |
|---------|-------------|---------|----------------|----------|
| **(1) ClawDB** | Single-node ADB with direct LanceDB | LanceDB (embedded) | Native (x86/ARM) | Local dev, Claude Desktop |
| **(2) WASM Playground** | Pure AQL parser + in-memory demo | HashMap (no persistence) | wasm32-unknown-unknown | GitHub README demo |
| **(3) Full ADB** | Distributed with caching layers | LanceDB + Cache | Native | Production workloads |

**This session focuses on (1) and (2).**

---

## 2. Architecture Diagram

```
                    ┌─────────────────────────────────────┐
                    │          aql-parser crate           │
                    │   (pure Rust, no_std compatible)    │
                    │  Grammar, AST, Parser, Validation   │
                    └─────────────────┬───────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
              ▼                       ▼                       ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│   WASM Playground   │  │      ClawDB         │  │     Full ADB        │
│                     │  │                     │  │                     │
│ Feature: "wasm"     │  │ Feature: "lance"    │  │ Feature: "full"     │
│                     │  │                     │  │                     │
│ - In-memory HashMap │  │ - Direct LanceDB    │  │ - Cache + LanceDB   │
│ - No persistence    │  │ - 5 memory types    │  │ - Stop/Move/Resume  │
│ - Parse + Execute   │  │ - Single binary     │  │ - Multi-agent       │
│ - Browser/Node.js   │  │ - MCP server        │  │ - Distributed       │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

---

## 3. Requirements

### 3.1 ClawDB Requirements (Version 1)

**Functional:**
- [ ] Parse and execute all AQL v0.5 statements
- [ ] Support all 5 memory types (Working, Tools, Procedural, Semantic, Episodic)
- [ ] Direct LanceDB writes (no caching layer)
- [ ] MCP server for Claude Desktop integration
- [ ] HTTP API for testing/debugging
- [ ] CLI with REPL

**Non-Functional:**
- [ ] Single static binary
- [ ] < 50MB binary size
- [ ] Startup time < 500ms
- [ ] Query latency (see table below)

| Memory Type | Lookup P99 | Store P99 |
|-------------|------------|-----------|
| Working     | < 5ms      | < 5ms     |
| Tools       | < 10ms     | < 10ms    |
| Procedural  | < 20ms     | < 20ms    |
| Semantic    | < 50ms     | < 50ms    |
| Episodic    | < 100ms    | < 100ms   |

**Storage:**
- [ ] All data in single `.lance` directory
- [ ] Each memory type = separate LanceDB table
- [ ] Links stored in dedicated table
- [ ] Metadata index for fast lookups

### 3.2 WASM Playground Requirements (Version 2)

**Functional:**
- [ ] Parse any AQL v0.5 query
- [ ] Display AST as JSON
- [ ] Execute queries against in-memory store
- [ ] Support STORE, SELECT, FORGET operations
- [ ] Display query results

**Non-Functional:**
- [ ] Pure WASM (no WASI, no filesystem)
- [ ] < 2MB WASM bundle size
- [ ] Works in all modern browsers
- [ ] No external dependencies at runtime
- [ ] Can be embedded in GitHub README via iframe

**Exclusions (WASM):**
- No LanceDB (requires filesystem)
- No DataFusion (too large, requires WASI)
- No usearch (native bindings)
- No embedding generation (use pre-seeded data for Semantic)
- No persistence (state lost on refresh)

**Memory Type Support:**

| Memory | STORE | QUERY | Notes |
|--------|-------|-------|-------|
| Working | ✅ | ✅ | HashMap + TTL |
| Episodic | ✅ | ✅ | HashMap + timestamp |
| Procedural | ✅ | ✅ | HashMap + pattern match |
| Tools | ✅ | ✅ | HashMap + ranking |
| Semantic | ❌ | ✅ | Pre-seeded 15-20 concepts with embeddings |

**Semantic Pre-seeded Data:**
- ~20 tech concepts (kubernetes, docker, rust, python, etc.)
- Pre-computed 384-dim embeddings (MiniLM)
- Users can RECALL LIKE "query" but cannot STORE
- Honest: "Semantic is read-only in playground"

---

## 4. Current Type Duplication Analysis

**Problem:** Types are duplicated between `aql-parser` and `adb-core`:

| Type | aql-parser/src/ast.rs | adb-core/src/predicate.rs |
|------|----------------------|---------------------------|
| `Condition` | AST enum (Simple/Group) | Runtime with `matches()` |
| `Operator` | Parse-time enum | Runtime with `compare()` |
| `Value` | AST value types | Runtime with `to_json()` |
| `Predicate` | AST variants | Runtime with helpers |
| `LogicalOp` | Parsing | Evaluation |
| `Modifiers` | Parse structure | Runtime with builders |
| `Window` | AST variants | Runtime variants |

**Solution:** Keep clean separation:
- **aql-parser** → Pure parsing (AST only, WASM-compatible)
- **clawdb** → Evaluation logic (converts AST to runtime, executes)

The `aql-parser` is already self-contained! The `adb-core` dependency in its Cargo.toml is **unused**.

---

## 5. Crate Structure

### 5.1 New Structure (Simplified)

**Key Insight:** `aql-parser` is already self-contained! No need for separate `aql-types`.

```
ADB/
├── Cargo.toml                    # Workspace root
├── .ai/
│   ├── ARCHITECTURE.md           # Current doc
│   └── CLAWDB_REQUIREMENTS.md    # This document
│
├── crates/
│   ├── aql-parser/               # PURE: WASM-compatible parser
│   │   ├── Cargo.toml            # deps: pest, serde, serde_json ONLY
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── ast.rs            # AST types (already complete!)
│   │   │   ├── parser.rs         # Parse AQL → AST
│   │   │   └── error.rs          # Parse errors
│   │   └── grammar/
│   │       └── aql.pest          # PEG grammar
│   │
│   ├── clawdb/                   # ClawDB: Single crate (everything else)
│   │   ├── Cargo.toml            # deps: lancedb, arrow, aql-parser
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── db.rs             # ClawDB struct
│   │       ├── config.rs         # Configuration
│   │       ├── types.rs          # MemoryRecord, runtime types
│   │       ├── predicate.rs      # Condition evaluation (from adb-core)
│   │       ├── executor.rs       # Query execution
│   │       ├── planner.rs        # AST → Plan (converts parser types)
│   │       └── backends/
│   │           ├── mod.rs        # Backend trait
│   │           ├── lance.rs      # LanceDB storage layer
│   │           ├── working.rs    # Working memory table
│   │           ├── tools.rs      # Tools memory table
│   │           ├── procedural.rs # Procedural memory table
│   │           ├── semantic.rs   # Semantic memory table
│   │           ├── episodic.rs   # Episodic memory table
│   │           └── links.rs      # Links table
│   │
│   ├── clawdb-wasm/              # WASM Playground (no LanceDB)
│   │   ├── Cargo.toml            # deps: wasm-bindgen, aql-parser ONLY
│   │   └── src/
│   │       ├── lib.rs            # WASM bindings
│   │       ├── executor.rs       # Simple executor (no planning)
│   │       └── memory.rs         # In-memory HashMap store
│   │
│   └── clawdb-mcp/               # MCP Server
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
│
├── clawdb-cli/                   # CLI binary
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
│
├── playground/                   # Web playground
│   ├── index.html
│   ├── style.css
│   └── app.js
│
└── tests/
    └── suites/                   # Shared test suites (YAML)
        ├── select_basic.yaml
        ├── select_where.yaml
        ├── store_basic.yaml
        └── ...
```

### 5.2 Feature Flags

```toml
# Cargo.toml (workspace)

[workspace.features]
default = ["lance"]
lance = ["lancedb", "arrow", "datafusion"]
wasm = []
full = ["lance", "cache"]
cache = []
```

### 5.3 Crate Dependencies

```
                    ┌────────────────────────────────┐
                    │         aql-parser             │
                    │  (pest, serde, serde_json)     │
                    │  WASM-compatible, ~500KB       │
                    └───────────────┬────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
┌───────▼────────┐          ┌───────▼────────┐          ┌───────▼────────┐
│  clawdb-wasm   │          │     clawdb     │          │   Full ADB     │
│ (wasm-bindgen) │          │   (lancedb)    │          │  (future)      │
│   ~2MB WASM    │          │   ~20MB bin    │          │  (+ cache)     │
└────────────────┘          └───────┬────────┘          └────────────────┘
                                    │
                            ┌───────▼────────┐
                            │  clawdb-mcp    │
                            │  clawdb-cli    │
                            └────────────────┘
```

---

## 5. Execution Plan

### Phase 1: Pure Parser Extraction (Day 1)

**Goal:** Make `aql-parser` compile to WASM with zero external dependencies.

**Tasks:**
1. [ ] Audit current `aql-parser` dependencies
2. [ ] Remove any dependencies on `adb-core`, `adb-backends`
3. [ ] Extract types to new `aql-types` crate
4. [ ] Add `#![no_std]` support with `alloc` feature
5. [ ] Verify: `cargo build --target wasm32-unknown-unknown -p aql-parser`

**Acceptance Criteria:**
- `aql-parser` compiles to WASM
- All existing parser tests pass
- No dependencies on LanceDB, DataFusion, etc.

### Phase 2: WASM Playground (Day 2)

**Goal:** Working AQL playground in the browser.

**Tasks:**
1. [ ] Create `clawdb-wasm` crate with wasm-bindgen
2. [ ] Implement in-memory HashMap backend
3. [ ] Expose parse() function to JavaScript
4. [ ] Expose execute() function to JavaScript
5. [ ] Create basic HTML/JS playground
6. [ ] Test in browser

**API Design:**
```rust
// clawdb-wasm/src/lib.rs

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmClawDB {
    // In-memory stores (HashMap per memory type)
}

#[wasm_bindgen]
impl WasmClawDB {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self;

    /// Parse AQL and return AST as JSON
    #[wasm_bindgen]
    pub fn parse(&self, query: &str) -> Result<String, JsError>;

    /// Execute AQL and return results as JSON
    #[wasm_bindgen]
    pub fn execute(&mut self, query: &str) -> Result<String, JsError>;

    /// Get all records from a memory type
    #[wasm_bindgen]
    pub fn dump(&self, memory_type: &str) -> Result<String, JsError>;

    /// Clear all data
    #[wasm_bindgen]
    pub fn clear(&mut self);
}
```

**Playground Features:**
- Query input textarea
- "Parse" button → shows AST JSON
- "Execute" button → shows results
- Memory browser (shows all records)
- Example queries dropdown

### Phase 3: ClawDB Core (Day 3-4)

**Goal:** ClawDB with all 5 memory types using LanceDB.

**Tasks:**
1. [ ] Create `clawdb` crate structure
2. [ ] Implement LanceDB connection manager
3. [ ] Implement Working memory backend (LanceDB table)
4. [ ] Implement Tools memory backend
5. [ ] Implement Procedural memory backend
6. [ ] Implement Episodic memory backend
7. [ ] Implement Semantic memory backend (without embeddings initially)
8. [ ] Implement Links table
9. [ ] Create ClawDB struct with query execution

**LanceDB Schema Per Memory Type:**

```rust
// Working Memory Table
struct WorkingRecord {
    id: String,
    data: String,           // JSON
    namespace: String,
    scope: String,
    created_at: i64,        // Unix timestamp ms
    accessed_at: i64,
    ttl_ms: Option<i64>,
    version: i64,
}

// Episodic Memory Table
struct EpisodicRecord {
    id: String,
    data: String,           // JSON
    namespace: String,
    scope: String,
    created_at: i64,
    accessed_at: i64,
    version: i64,
    // Indexed fields (extracted from data for fast filtering)
    data_campaign: Option<String>,
    data_won: Option<bool>,
    data_cpm_paid: Option<f64>,
}

// Links Table
struct LinkRecord {
    id: String,
    from_type: String,
    from_id: String,
    to_type: String,
    to_id: String,
    link_type: String,
    weight: f32,
    created_at: i64,
    updated_at: i64,
}
```

### Phase 4: ClawDB MCP & CLI (Day 5)

**Goal:** Working MCP server and CLI.

**Tasks:**
1. [ ] Create `clawdb-mcp` crate
2. [ ] Implement MCP tools (aql_query, store_memory, etc.)
3. [ ] Create `clawdb-cli` with REPL
4. [ ] Add HTTP server endpoint for testing
5. [ ] Test with Claude Desktop

**MCP Tools:**
```json
{
  "tools": [
    {
      "name": "aql_query",
      "description": "Execute an AQL query",
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": { "type": "string" }
        },
        "required": ["query"]
      }
    },
    {
      "name": "store_memory",
      "description": "Store a memory record",
      "inputSchema": {
        "type": "object",
        "properties": {
          "memory_type": { "type": "string", "enum": ["WORKING", "TOOLS", "PROCEDURAL", "SEMANTIC", "EPISODIC"] },
          "data": { "type": "object" }
        },
        "required": ["memory_type", "data"]
      }
    }
  ]
}
```

### Phase 5: Testing & Documentation (Day 6)

**Goal:** Comprehensive tests and docs.

**Tasks:**
1. [ ] Port existing AQL test suites to YAML format
2. [ ] Create WASM-specific tests (browser)
3. [ ] Create ClawDB integration tests
4. [ ] Write README for each crate
5. [ ] Create GitHub Pages for playground
6. [ ] Performance benchmarks

---

## 6. Migration from Current ADB

### Files to Keep (Refactor)

| Current File | New Location | Changes |
|--------------|--------------|---------|
| `aql-parser/src/grammar.rs` | `aql-parser/src/grammar.rs` | No change |
| `aql-parser/src/ast.rs` | `aql-types/src/` | Split into types crate |
| `aql-parser/src/parser.rs` | `aql-parser/src/parser.rs` | Remove backend deps |
| `adb-core/src/predicate.rs` | `aql-types/src/condition.rs` | Rename, simplify |
| `adb-backends/src/working.rs` | `clawdb/src/backends/working.rs` | LanceDB impl |
| `adb-backends/src/tools.rs` | `clawdb/src/backends/tools.rs` | LanceDB impl |
| `adb-executor/src/executor.rs` | `clawdb/src/executor.rs` | Simplify |

### Files to Remove/Replace

| Current File | Reason |
|--------------|--------|
| `adb-backends/src/backend.rs` | Replace with LanceDB trait |
| `adb-backends/src/semantic.rs` | Replace with LanceDB (no usearch) |
| `adb-backends/src/episodic.rs` | Replace with LanceDB (no DataFusion) |
| `adb-server/` | Replace with `clawdb/` |

### Dependency Changes

**Remove for ClawDB:**
- `datafusion` (350+ crates)
- `usearch` (native bindings)
- `dashmap` (use LanceDB)
- `petgraph` (use LanceDB relationships)

**Add for ClawDB:**
- `lancedb` (single dep for all storage)
- `arrow` (for schema)

**WASM Playground deps:**
- `wasm-bindgen`
- `serde_json`
- `aql-parser`
- `aql-types`

---

## 7. Comparison: ClawDB vs Current ADB

| Aspect | Current ADB | ClawDB |
|--------|-------------|--------|
| **Crates** | 9 | 4 |
| **Dependencies** | 400+ | ~50 |
| **Binary size** | ~100MB | ~20MB |
| **Storage** | In-memory | LanceDB |
| **Persistence** | No | Yes |
| **Compile time** | ~3 min | ~30 sec |
| **WASM support** | No | Yes (parser) |
| **Complexity** | High | Low |

---

## 8. Risk Assessment

| Risk | Mitigation |
|------|------------|
| LanceDB performance | Benchmark early, fallback to hybrid |
| WASM bundle size | Tree shaking, feature flags |
| Parser compatibility | Shared test suites |
| Missing features | Document what's excluded |

---

## 9. Success Criteria

### WASM Playground
- [ ] Compiles to < 2MB WASM
- [ ] Parses all AQL v0.5 examples
- [ ] Works in Chrome, Firefox, Safari
- [ ] Embedded in GitHub README

### ClawDB
- [ ] Single binary < 50MB
- [ ] All AQL v0.5 operations work
- [ ] LanceDB persistence verified
- [ ] MCP works with Claude Desktop
- [ ] Query latency targets met

---

## 10. Timeline

| Day | Deliverable |
|-----|-------------|
| 1 | Pure aql-parser, aql-types crates |
| 2 | WASM playground working |
| 3-4 | ClawDB with LanceDB backends |
| 5 | MCP server, CLI |
| 6 | Tests, docs, release |

---

## 11. Next Steps

1. **Immediate:** Audit `aql-parser` dependencies
2. **Today:** Extract `aql-types` crate
3. **Today:** Verify WASM compilation
4. **Tomorrow:** Build WASM playground
5. **Day 3:** Start ClawDB LanceDB implementation

---

*Document created: 2026-04-05*
*Last updated: 2026-04-05*
