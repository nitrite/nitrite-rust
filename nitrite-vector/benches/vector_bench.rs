//! Performance benchmarks for the vector index.
//!
//! Run with `cargo bench -p nitrite_vector` (release). Covers the distance
//! kernels and build/query throughput for both the in-memory HNSW backend and
//! the disk-resident DiskANN backend.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;
use nitrite_vector::diskann::DiskAnnIndex;
use nitrite_vector::distance::Metric;
use nitrite_vector::hnsw::Hnsw;
use nitrite_vector::{DiskAnnConfig, Precision};

/// Deterministic pseudo-random vectors (xorshift). `id_base` offsets ids into
/// the NitriteId snowflake range (>= 10^18) that DiskANN's search requires.
fn gen(n: usize, dim: usize, seed: u64, id_base: u64) -> Vec<(u64, Vec<f32>)> {
    let mut s = seed;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        (s >> 40) as f32 / (1u64 << 24) as f32 - 0.5
    };
    (0..n)
        .map(|i| (id_base + i as u64, (0..dim).map(|_| next()).collect()))
        .collect()
}

fn temp_db() -> (tempfile::TempDir, Nitrite) {
    let dir = tempfile::tempdir().unwrap();
    let db = Nitrite::builder()
        .load_module(
            FjallModule::with_config()
                .db_path(dir.path().to_str().unwrap())
                .low_memory_preset()
                .build(),
        )
        .open_or_create(None, None)
        .unwrap();
    (dir, db)
}

fn bench_distance(c: &mut Criterion) {
    let dim = 128;
    let a = gen(1, dim, 1, 0)[0].1.clone();
    let b = gen(1, dim, 2, 0)[0].1.clone();
    let mut group = c.benchmark_group("distance_128d");
    for (name, metric) in [
        ("cosine", Metric::Cosine),
        ("euclidean", Metric::Euclidean),
        ("dot", Metric::Dot),
    ] {
        group.bench_function(name, |bencher| {
            bencher.iter(|| metric.distance(black_box(&a), black_box(&b)))
        });
    }
    group.finish();
}

fn bench_hnsw(c: &mut Criterion) {
    // 384-dim ~ a real sentence-embedding size (e.g. all-MiniLM), where the
    // distance kernel dominates and SIMD matters.
    let dim = 384;
    let n = 2000;
    let vectors = gen(n, dim, 42, 0);

    // Query throughput on a pre-built index.
    let mut graph = Hnsw::new(dim, Metric::Cosine, 16, 200, 64);
    for (id, v) in &vectors {
        graph.insert(*id, v.clone()).unwrap();
    }
    let query = gen(1, dim, 7, 0)[0].1.clone();
    c.bench_function("hnsw_query_2k", |b| {
        b.iter(|| black_box(graph.search(black_box(&query), 10, None)))
    });

    // Build throughput (1k vectors per iteration).
    let build_set = gen(1000, dim, 99, 0);
    c.benchmark_group("hnsw_build_1k")
        .sample_size(10)
        .bench_function("build", |b| {
            b.iter_batched(
                || build_set.clone(),
                |set| {
                    let mut g = Hnsw::new(dim, Metric::Cosine, 16, 200, 64);
                    for (id, v) in set {
                        g.insert(id, v).unwrap();
                    }
                    black_box(g.len())
                },
                BatchSize::SmallInput,
            )
        });
}

fn diskann_cfg(pq: usize, pq_threshold: usize) -> DiskAnnConfig {
    DiskAnnConfig {
        degree: 48,
        build_beam: 100,
        search_beam: 120,
        alpha: 1.2,
        pq_subvectors: pq,
        pq_train_threshold: pq_threshold,
        cache_bytes: 64 * 1024 * 1024,
        consolidate_threshold: 0,
    }
}

fn bench_diskann(c: &mut Criterion) {
    let dim = 384;
    let id_base = 1_000_000_000_000_000_000;

    // Query throughput on a pre-built (PQ-trained) index.
    let (_dir, db) = temp_db();
    let index = DiskAnnIndex::open(&db.config(), "q", dim, Metric::Cosine, Precision::F32, &diskann_cfg(16, 500))
        .unwrap()
        .0;
    for (id, v) in gen(2000, dim, 42, id_base) {
        index.insert(id, v).unwrap();
    }
    // PQ training is asynchronous; wait so the query bench measures the
    // PQ-guided path.
    while !index.pq_trained() {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let query = gen(1, dim, 7, 0)[0].1.clone();
    c.bench_function("diskann_query_2k", |b| {
        b.iter(|| black_box(index.search(black_box(&query), 10, Some(120)).unwrap()))
    });

    // Concurrency: run 128 queries, single-threaded vs across 8 worker threads
    // (each thread runs 16 queries so thread-spawn cost is amortized). Compare
    // the two to see how far shared-read query throughput scales — near 8× if
    // reads don't contend.
    let queries: Vec<Vec<f32>> = (0..128).map(|s| gen(1, dim, 100 + s, 0)[0].1.clone()).collect();
    c.bench_function("diskann_query_128_seq", |b| {
        b.iter(|| {
            for q in &queries {
                black_box(index.search(q, 10, Some(120)).unwrap());
            }
        })
    });
    c.bench_function("diskann_query_128_par8", |b| {
        b.iter(|| {
            std::thread::scope(|scope| {
                for chunk in queries.chunks(16) {
                    let idx = &index;
                    scope.spawn(move || {
                        for q in chunk {
                            black_box(idx.search(q, 10, Some(120)).unwrap());
                        }
                    });
                }
            })
        })
    });

    // Build throughput (1k vectors per iteration, graph only — PQ disabled).
    let build_set = gen(1000, dim, 99, id_base);
    c.benchmark_group("diskann_build_1k")
        .sample_size(10)
        .bench_function("build", |b| {
            b.iter_batched(
                || {
                    let (dir, db) = temp_db();
                    let index = DiskAnnIndex::open(
                        &db.config(),
                        "b",
                        dim,
                        Metric::Cosine,
                        Precision::F32,
                        &diskann_cfg(0, usize::MAX),
                    )
                    .unwrap()
                    .0;
                    (dir, db, index, build_set.clone())
                },
                |(_dir, _db, index, set)| {
                    for (id, v) in set {
                        index.insert(id, v).unwrap();
                    }
                    black_box(index.len())
                },
                BatchSize::SmallInput,
            )
        });
}

criterion_group!(benches, bench_distance, bench_hnsw, bench_diskann);
criterion_main!(benches);
