//! S3 option B step 8: `adb serve-http` as a 3-node cluster — three processes of the
//! real binary, local object storage. It starts from empty; any node answers; killing
//! the owner of a namespace's partition loses none of its data; all three stopped and
//! their disks wiped (control state included), the cluster starts again from object
//! storage alone with every record.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use adb_cluster::map::PartitionMap;
use serde_json::{json, Value};

const TOKEN: &str = "cluster-test-token";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Cluster {
    ports: Vec<(u16, u16, u16)>,
    cold: String,
    dirs: Vec<tempfile::TempDir>,
    procs: Vec<Option<Child>>,
}

impl Cluster {
    fn new(cold: String) -> Self {
        Self { ports: (0..3).map(|_| (free_port(), free_port(), free_port())).collect(), cold, dirs: (0..3).map(|_| tempfile::tempdir().unwrap()).collect(), procs: vec![None, None, None] }
    }

    fn http(&self, i: usize) -> String {
        format!("http://127.0.0.1:{}", self.ports[i].0)
    }

    fn start(&mut self, i: usize) {
        let voters: Vec<String> = self.ports.iter().enumerate().map(|(k, (_, _, c))| format!("{}=http://127.0.0.1:{c}", k + 1)).collect();
        let (h, f, c) = self.ports[i];
        let log = std::fs::File::create(self.dirs[i].path().with_extension("log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_adb"))
            .args(["--data-dir", &self.dirs[i].path().display().to_string(), "serve-http", "--host", "127.0.0.1", "--port", &h.to_string(), "--flight-port", &f.to_string()])
            .env("ADB_NODE_ID", (i + 1).to_string())
            .env("ADB_CONTROL_PEERS", voters.join(","))
            .env("ADB_CONTROL_PORT", c.to_string())
            .env("ADB_ADVERTISE", self.http(i))
            .env("ADB_ADVERTISE_FLIGHT", format!("http://127.0.0.1:{f}"))
            .env("ADB_COLD_URI", &self.cold)
            .env("ADB_PARTITIONS", "8")
            .env("ADB_AUTH_TOKEN", TOKEN)
            .env("ADB_EMBED", "off")
            .env("ADB_LEASE_MS", "2000")
            .env("ADB_HEARTBEAT_MS", "300")
            .env("ADB_WAL_SEGMENT_MS", "300")
            .env("RUST_LOG", "info,lance=warn")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        self.procs[i] = Some(child);
    }

    fn kill(&mut self, i: usize) {
        if let Some(mut c) = self.procs[i].take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn wipe(&mut self, i: usize) {
        self.dirs[i] = tempfile::tempdir().unwrap();
    }

    fn logs(&self) -> String {
        (0..3).map(|i| format!("--- node {}:\n{}", i + 1, std::fs::read_to_string(self.dirs[i].path().with_extension("log")).unwrap_or_default().lines().rev().take(30).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"))).collect::<Vec<_>>().join("\n")
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..3 {
            self.kill(i);
        }
    }
}

async fn query(http: &reqwest::Client, node: &str, aql: &str) -> Result<Value, String> {
    let r = http.post(format!("{node}/query")).bearer_auth(TOKEN).json(&json!({ "query": aql })).timeout(Duration::from_secs(20)).send().await.map_err(|e| e.to_string())?;
    let v: Value = r.json().await.map_err(|e| e.to_string())?;
    if v["success"] == true {
        Ok(v)
    } else {
        Err(v.to_string())
    }
}

fn ns_of(v: &Value) -> Vec<i64> {
    let recs = v.pointer("/data/data/records").and_then(|r| r.as_array()).cloned().unwrap_or_default();
    let mut n: Vec<i64> = recs.iter().filter_map(|r| r["data"]["n"].as_i64()).collect();
    n.sort();
    n
}

async fn wait_ready(http: &reqwest::Client, c: &Cluster, nodes: &[usize], within: Duration) {
    let t = Instant::now();
    'next: for &i in nodes {
        loop {
            if let Ok(r) = http.get(format!("{}/ready", c.http(i))).timeout(Duration::from_secs(2)).send().await {
                if r.status().is_success() {
                    continue 'next;
                }
            }
            assert!(t.elapsed() < within, "node {} not ready in {within:?}\n{}", i + 1, c.logs());
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Until `f` holds for the answer of `aql` through `node` (or panic after `within`).
async fn until(http: &reqwest::Client, c: &Cluster, node: usize, aql: &str, within: Duration, f: impl Fn(&Value) -> bool) -> Value {
    let t = Instant::now();
    loop {
        if let Ok(v) = query(http, &c.http(node), aql).await {
            if f(&v) {
                return v;
            }
        }
        assert!(t.elapsed() < within, "{aql} through node {} not as expected in {within:?}\n{}", node + 1, c.logs());
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_three_node_cluster_from_empty_and_from_object_storage() {
    let cold = tempfile::tempdir().unwrap();
    let mut c = Cluster::new(format!("file://{}", cold.path().display()));
    let http = reqwest::Client::new();
    for i in 0..3 {
        c.start(i);
    }
    wait_ready(&http, &c, &[0, 1, 2], Duration::from_secs(60)).await;

    // From empty: writes through node 1, read through nodes 2 and 3.
    let spaces = ["a3t-shared--bot", "a3t-shared--ops", "demo--y"];
    for (k, ns) in spaces.iter().enumerate() {
        for j in 0..10 {
            query(&http, &c.http(0), &format!(r#"STORE INTO SEMANTIC (key = "k{j}", n = {}) NAMESPACE "{ns}""#, k * 100 + j)).await.unwrap();
        }
    }
    let all = "RECALL FROM SEMANTIC WHERE n >= 0 LIMIT 1000";
    for node in [1, 2] {
        let v = until(&http, &c, node, all, Duration::from_secs(20), |v| ns_of(v).len() == 30).await;
        assert_eq!(ns_of(&v).len(), 30);
    }

    // Kill the owner of a3t-shared--bot's partition: its data stays readable.
    let map: PartitionMap = http.get(format!("http://127.0.0.1:{}/control/map", c.ports[0].2)).send().await.unwrap().json().await.unwrap();
    let p = adb_cluster::routing::partition_of(&map, "a3t-shared--bot");
    let owner = map.assignments[&p].owner as usize - 1;
    c.kill(owner);
    let other = (owner + 1) % 3;
    let bot = r#"RECALL FROM SEMANTIC WHERE n >= 0 NAMESPACE "a3t-shared--bot" LIMIT 100"#;
    let v = until(&http, &c, other, bot, Duration::from_secs(60), |v| ns_of(v).len() == 10).await;
    assert_eq!(ns_of(&v), (0..10).collect::<Vec<_>>(), "no write lost when the owner died");
    query(&http, &c.http(other), r#"STORE INTO SEMANTIC (key = "after", n = 50) NAMESPACE "a3t-shared--bot""#).await.unwrap();

    // Everything shipped, then all three stopped and their disks wiped.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..3 {
        c.kill(i);
        c.wipe(i);
    }
    // From object storage alone: every record is back.
    for i in 0..3 {
        c.start(i);
    }
    wait_ready(&http, &c, &[0, 1, 2], Duration::from_secs(90)).await;
    let v = until(&http, &c, 2, all, Duration::from_secs(60), |v| ns_of(v).len() == 31).await;
    assert!(ns_of(&v).contains(&50), "including the write after the failover");
}
