//! S8 (vector index at partition scale) and S9 (f16 embeddings in the log).
//!
//!   vecbench <dir> <rows> <dim> <indexes>     indexes: comma list of flat,pq,rq,hnsw
//!
//! Data: unit vectors around 256 clusters x 32 sub-clusters (real embeddings are clustered;
//! uniform random vectors make ANN recall look worse than it is). 100 held-out
//! queries; ground truth = exact cosine top-10 computed here, in f32.
//! S9: exact top-10 over the same vectors rounded to f16 and back, against f32 truth.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{Dataset, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance::index::DatasetIndexExt;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::IndexType;
use lance_linalg::distance::MetricType;

const K: usize = 10;
const QUERIES: usize = 100;
const CLUSTERS: usize = 256;
const SUBS: usize = 32;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// Approximately normal(0,1): sum of 4 uniforms, rescaled.
    fn gauss(&mut self) -> f32 {
        let s: f32 = (0..4).map(|_| (self.next() >> 40) as f32 / (1u64 << 24) as f32).sum();
        (s - 2.0) * 1.732
    }
}

fn normalize(v: &mut [f32]) {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
    v.iter_mut().for_each(|x| *x /= n);
}

/// `n` unit vectors with two-level cluster structure, flattened: one of `centres`
/// (CLUSTERS × SUBS sub-centres), plus noise. All offsets are scaled by 1/sqrt(dim) so
/// their norms don't grow with the dimension — otherwise high-dim noise swamps the
/// clusters and the data degenerates to uniform random (no meaningful neighbours).
fn data(n: usize, dim: usize, seed: u64, centres: &[Vec<f32>]) -> Vec<f32> {
    let mut r = Rng(seed | 1);
    let s = 0.35 / (dim as f32).sqrt();
    let mut out = Vec::with_capacity(n * dim);
    for _ in 0..n {
        let c = &centres[(r.next() % centres.len() as u64) as usize];
        let mut v: Vec<f32> = c.iter().map(|x| x + s * r.gauss()).collect();
        normalize(&mut v);
        out.extend(v);
    }
    out
}

/// Exact top-K by dot product (vectors are unit length, so = cosine), multithreaded.
fn exact(base: &[f32], dim: usize, queries: &[f32]) -> Vec<Vec<u64>> {
    let n = base.len() / dim;
    let nq = queries.len() / dim;
    let threads = std::thread::available_parallelism().map(|x| x.get()).unwrap_or(8);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    let mut res = Vec::new();
                    for q in (t..nq).step_by(threads) {
                        let qv = &queries[q * dim..(q + 1) * dim];
                        let mut best: Vec<(f32, u64)> = Vec::with_capacity(K + 1);
                        for i in 0..n {
                            let v = &base[i * dim..(i + 1) * dim];
                            let d: f32 = qv.iter().zip(v).map(|(a, b)| a * b).sum();
                            if best.len() < K || d > best[best.len() - 1].0 {
                                let pos = best.partition_point(|x| x.0 >= d);
                                best.insert(pos, (d, i as u64));
                                best.truncate(K);
                            }
                        }
                        res.push((q, best.into_iter().map(|x| x.1).collect::<Vec<_>>()));
                    }
                    res
                })
            })
            .collect();
        let mut all = vec![Vec::new(); nq];
        for h in handles {
            for (q, ids) in h.join().unwrap() {
                all[q] = ids;
            }
        }
        all
    })
}

fn recall(truth: &[Vec<u64>], got: &[Vec<u64>]) -> f64 {
    let hit: usize = truth
        .iter()
        .zip(got)
        .map(|(t, g)| {
            let t: HashSet<_> = t.iter().collect();
            g.iter().filter(|x| t.contains(x)).count()
        })
        .sum();
    hit as f64 / (truth.len() * K) as f64
}

fn schema(dim: i32) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        ),
    ]))
}

async fn search(
    ds: &Dataset,
    queries: &[f32],
    dim: usize,
    nprobes: Option<usize>,
    refine: Option<u32>,
    ef: Option<usize>,
    use_index: bool,
) -> (Vec<Vec<u64>>, Duration, Duration) {
    let mut got = Vec::new();
    let mut lat = Vec::new();
    for q in queries.chunks(dim) {
        let qa = Float32Array::from(q.to_vec());
        let t = Instant::now();
        let mut s = ds.scan();
        s.nearest("embedding", &qa, K).unwrap().use_index(use_index).project(&["id"]).unwrap();
        if let Some(n) = nprobes {
            s.nprobes(n);
        }
        if let Some(r) = refine {
            s.refine(r);
        }
        if let Some(e) = ef {
            s.ef(e);
        }
        let b = s.try_into_batch().await.unwrap();
        lat.push(t.elapsed());
        let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
        got.push(ids.values().to_vec());
    }
    lat.sort();
    (got, lat[lat.len() / 2], lat[lat.len() * 99 / 100])
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (dir, n, dim) = (&a[1], a[2].parse::<usize>().unwrap(), a[3].parse::<usize>().unwrap());
    let which: Vec<&str> = a[4].split(',').collect();
    let mut r = Rng(42);
    // CLUSTERS unit centres, each with SUBS sub-centres at distance ~0.6.
    let sub = 0.6 / (dim as f32).sqrt();
    let mut centres: Vec<Vec<f32>> = Vec::with_capacity(CLUSTERS * SUBS);
    for _ in 0..CLUSTERS {
        let mut c: Vec<f32> = (0..dim).map(|_| r.gauss()).collect();
        normalize(&mut c);
        for _ in 0..SUBS {
            centres.push(c.iter().map(|x| x + sub * r.gauss()).collect());
        }
    }
    let t = Instant::now();
    let base = data(n, dim, 7, &centres);
    let queries = data(QUERIES, dim, 99, &centres);
    let truth = exact(&base, dim, &queries);
    println!("== {n} x {dim}: data + exact ground truth in {:.1?}", t.elapsed());

    // S9: exact search over f16-rounded vectors.
    let r16: Vec<f32> = base.iter().map(|x| half::f16::from_f32(*x).to_f32()).collect();
    let q16: Vec<f32> = queries.iter().map(|x| half::f16::from_f32(*x).to_f32()).collect();
    println!("  S9 exact recall@10 with f16-rounded vectors+queries: {:.4}", recall(&truth, &exact(&r16, dim, &q16)));
    drop(r16);

    let _ = std::fs::remove_dir_all(dir);
    let chunk = 20_000;
    let batches: Vec<_> = (0..n.div_ceil(chunk))
        .map(|c| {
            let lo = c * chunk;
            let hi = (lo + chunk).min(n);
            let emb = FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
                Arc::new(Float32Array::from(base[lo * dim..hi * dim].to_vec())),
                None,
            )
            .unwrap();
            Ok(RecordBatch::try_new(
                schema(dim as i32),
                vec![Arc::new(UInt64Array::from((lo as u64..hi as u64).collect::<Vec<_>>())), Arc::new(emb)],
            )
            .unwrap())
        })
        .collect();
    let mut ds = Dataset::write(RecordBatchIterator::new(batches, schema(dim as i32)), dir, Some(WriteParams::default()))
        .await
        .unwrap();
    drop(base);

    let parts = ((n as f64).sqrt() as usize).clamp(16, 1024);
    for w in which {
        if w == "flat" {
            let (got, p50, p99) = search(&ds, &queries, dim, None, None, None, false).await;
            println!("  flat (brute force, local disk): recall {:.3} p50 {p50:.2?} p99 {p99:.2?}", recall(&truth, &got));
            continue;
        }
        let (params, label) = match w {
            "pq" => (VectorIndexParams::ivf_pq(parts, 8, dim / 16, MetricType::Cosine, 50), format!("IVF_PQ ({parts} parts, {} subvec)", dim / 16)),
            "rq" => (VectorIndexParams::ivf_rq(parts, 1, MetricType::Cosine), format!("IVF_RQ ({parts} parts, 1 bit)")),
            "hnsw" => {
                let hp = (parts / 4).max(4);
                (
                    VectorIndexParams::with_ivf_hnsw_sq_params(
                        MetricType::Cosine,
                        IvfBuildParams::new(hp),
                        HnswBuildParams::default(),
                        SQBuildParams::default(),
                    ),
                    format!("IVF_HNSW_SQ ({hp} parts)"),
                )
            }
            x => panic!("unknown index {x}"),
        };
        let t = Instant::now();
        ds.create_index(&["embedding"], IndexType::Vector, Some("vec_idx".into()), &params, true)
            .await
            .unwrap();
        println!("  {label}: build {:.1?}", t.elapsed());
        let sweeps: Vec<(Option<usize>, Option<u32>, Option<usize>)> = match w {
            "hnsw" => vec![(Some(4), None, Some(64)), (Some(8), None, Some(128)), (Some(16), None, Some(256))],
            _ => vec![(Some(10), None, None), (Some(20), None, None), (Some(20), Some(5), None), (Some(50), Some(10), None)],
        };
        for (np, rf, ef) in sweeps {
            let (got, p50, p99) = search(&ds, &queries, dim, np, rf, ef, true).await;
            println!(
                "    nprobes={:<3} refine={:<4} ef={:<4} recall@10 {:.3}  p50 {p50:>9.2?}  p99 {p99:>9.2?}",
                np.unwrap_or(0),
                rf.map(|x| x.to_string()).unwrap_or("-".into()),
                ef.map(|x| x.to_string()).unwrap_or("-".into()),
                recall(&truth, &got)
            );
        }
    }
}
