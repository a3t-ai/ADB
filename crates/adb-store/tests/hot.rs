//! W10: the hot cache against the table. Random puts, supersedes, patches,
//! forgets and expiries over three memory types and three scopes, under tight
//! rules; after every 5th record, for every memory type and scope:
//! - every current row the cache returns is the table's current row, as stored;
//! - every current row of the table newer than the bucket's floor is in the
//!   cache (all of them when the bucket is complete);
//! - a lineage the table has is never reported missing.
//! Also across a compaction, a rebuild from the table and a reopen.

use std::collections::HashMap;
use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_store::{row_id, CacheConfig, HotCache, Keep, PartitionStore, Rule, StoreConfig, StoredRow};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;
const SCOPES: [&str; 3] = ["s1", "s2", "s3"];
const MEMORIES: [Memory; 3] = [Memory::Working, Memory::Episodic, Memory::Semantic];

struct Rng(u64);

impl Rng {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

fn rules() -> CacheConfig {
    let r = |memory, scope: &str, keep| Rule { memory, scope: scope.into(), keep };
    CacheConfig {
        max_bytes: 64 << 10,
        rules: vec![
            r(Memory::Working, "*", Keep::Bytes(3_000)),
            r(Memory::Episodic, "s1", Keep::Last(3)),
            r(Memory::Episodic, "s2", Keep::All),
            r(Memory::Episodic, "*", Keep::None),
            r(Memory::Semantic, "*", Keep::Last(5)),
        ],
        vectors: true,
    }
}

fn kind(m: Memory) -> Kind {
    match m {
        Memory::Episodic => Kind::Episode,
        Memory::Semantic => Kind::Fact,
        _ => Kind::Entity,
    }
}

fn new_row(m: Memory, id: &str, lineage: &str, version: u32, scope: &str, n: u64, ttl: Option<i64>) -> NewRow {
    NewRow {
        id: RecordId(id.into()),
        lineage_id: RecordId(lineage.into()),
        version,
        data: RowData {
            tenant_id: "default".into(),
            scope: scope.into(),
            visibility: Visibility::Private,
            namespace: None,
            memory: m,
            kind: kind(m),
            key: Some(lineage.into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            text: Some(format!("text {n}")),
            value: json!({ "key": lineage, "n": n, "pad": "x".repeat((n % 300) as usize) }),
            sealed: None,
            embedding: Some(vec![half::f16::from_f32(0.25); 4]),
            ttl_at: ttl.map(CommitTs),
            provenance: Provenance::default(),
        },
    }
}

/// A live lineage in the model: its memory type, scope, current row id, version.
struct Live {
    memory: Memory,
    scope: String,
    id: String,
    version: u32,
}

fn now() -> CommitTs {
    CommitTs::from_datetime(adb_core::time::now())
}

fn same(a: &StoredRow, b: &StoredRow) -> bool {
    a.id == b.id && a.version == b.version && a.data.value == b.data.value && a.valid_from == b.valid_from && a.commit_ts == b.commit_ts && a.data.scope == b.data.scope
}

async fn check(store: &PartitionStore, step: usize) {
    let hot = store.hot().expect("a cache");
    let now = now();
    for m in MEMORIES {
        let table: Vec<StoredRow> = store
            .current(Some(&format!("memory = '{}' AND kind IN ('entity', 'fact', 'episode', 'tool')", m.tag())), now)
            .await
            .unwrap();
        for scope in SCOPES.iter().map(|s| Some(*s)).chain([None]) {
            let want: HashMap<&str, &StoredRow> = table.iter().filter(|r| scope.is_none_or(|s| r.data.scope == s)).map(|r| (r.id.as_str(), r)).collect();
            let hit = hot.rows(m, scope).expect("tracked");
            let got: HashMap<&str, &StoredRow> = hit.rows.iter().map(|c| &*c.row).filter(|r| r.is_current(now)).map(|r| (r.id.as_str(), r)).collect();
            for (id, r) in &got {
                let t = want.get(id).unwrap_or_else(|| panic!("step {step}: {m:?} {scope:?}: cached {id} is not current in the table"));
                assert!(same(r, t), "step {step}: {m:?} {scope:?}: cached {id} differs from the table\n{r:?}\n{t:?}");
            }
            for (id, t) in &want {
                if hit.floor.is_none_or(|f| t.commit_ts > f) {
                    assert!(got.contains_key(id), "step {step}: {m:?} {scope:?}: {id} (ts {}) missing, floor {:?}", t.commit_ts.0, hit.floor);
                }
            }
        }
        for t in &table {
            match hot.lineage(m, &t.lineage_id) {
                Some(Some(c)) => assert!(same(&c.row, t), "step {step}: lineage {} cached as another row", t.lineage_id),
                Some(None) => panic!("step {step}: lineage {} reported missing", t.lineage_id),
                None => {}
            }
        }
    }
}

#[tokio::test]
async fn cache_follows_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 7, wal_flush: Duration::from_millis(20), embedding_model: None };
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    store.set_cache(Some(HotCache::new(rules(), false))).await.unwrap();
    let mut rng = Rng(7);
    let mut live: HashMap<String, Live> = HashMap::new();
    let mut next = 0u64;
    let mut offset = 0u64;
    for step in 0..400 {
        offset += 1;
        next += 1;
        let ts = T0 + offset as i64 * 1_000;
        let keys: Vec<String> = live.keys().cloned().collect();
        let pick = |rng: &mut Rng| keys[rng.next(keys.len() as u64) as usize].clone();
        let effect = match rng.next(10) {
            0..=3 | _ if keys.is_empty() => {
                let m = MEMORIES[rng.next(3) as usize];
                let scope = SCOPES[rng.next(3) as usize];
                let key = format!("k{next}");
                // Some rows carry a TTL that has passed already (not current).
                let ttl = (rng.next(8) == 0).then_some(ts + 500);
                live.insert(key.clone(), Live { memory: m, scope: scope.into(), id: key.clone(), version: 1 });
                let row = new_row(m, &key, &key, 1, scope, next, ttl);
                match m {
                    Memory::Episodic => Effect::PutEpisode(row),
                    Memory::Semantic => Effect::PutFact(row),
                    _ => Effect::PutEntity(row),
                }
            }
            4 | 5 => {
                let key = pick(&mut rng);
                let l = live.get_mut(&key).unwrap();
                l.version += 1;
                let id = format!("{key}.v{}", l.version);
                let old = std::mem::replace(&mut l.id, id.clone());
                Effect::Supersede { old: RecordId(old), new: new_row(l.memory, &id, &key, l.version, &l.scope, next, None) }
            }
            6 | 7 => {
                let key = pick(&mut rng);
                let l = &live[&key];
                let mut fields = serde_json::Map::new();
                fields.insert("note".into(), json!(format!("patched {next}")));
                Effect::Patch { memory: l.memory, id: RecordId(l.id.clone()), fields, version: l.version + 1 }
            }
            8 => {
                let key = pick(&mut rng);
                let l = live.remove(&key).unwrap();
                Effect::Forget { memory: l.memory, lineage_id: RecordId(key) }
            }
            _ => {
                let key = pick(&mut rng);
                let l = live.remove(&key).unwrap();
                Effect::Expire { memory: l.memory, lineage_id: RecordId(key) }
            }
        };
        let record = LogRecord { partition: PartitionId(0), offset: LogOffset(offset), epoch: Epoch(1), commit_ts: CommitTs(ts), txn_id: None, effects: vec![effect] };
        store.apply(&record).await.unwrap();
        // Every step was checked once (400 steps, 16 min in debug); every 5th keeps
        // the gate quick.
        if step % 5 == 0 {
            check(&store, step).await;
        }
        if step == 150 {
            store.compact(Duration::from_secs(90 * 86_400), now()).await.unwrap();
            check(&store, step).await;
        }
        if step == 250 {
            store.hot().unwrap().sweep();
            check(&store, step).await;
            // A rebuild from the table holds what the followed cache held.
            let before = store.hot().unwrap().stats();
            store.set_cache(Some(HotCache::new(rules(), false))).await.unwrap();
            check(&store, step).await;
            let after = store.hot().unwrap().stats();
            assert!(after.rows <= before.rows + 1, "rebuild: {} rows, followed: {}", after.rows, before.rows);
        }
    }
    let st = store.hot().unwrap().stats();
    assert!(st.buckets.iter().any(|b| !b.complete), "the rules should have evicted something");
    assert!(st.buckets.iter().any(|b| b.complete && b.rows > 0), "s2 episodes are kept whole");
    assert!(st.bytes <= 64 << 10);
    // Reopen: the cache starts from the table again.
    store.close().await.unwrap();
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    store.set_cache(Some(HotCache::new(rules(), false))).await.unwrap();
    check(&store, 400).await;
    let _ = row_id(Memory::Working, "x");
}
