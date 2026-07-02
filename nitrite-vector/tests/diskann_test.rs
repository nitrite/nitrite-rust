//! Disk-resident DiskANN backend (memory-mapped flat store): recall, disk
//! residency, persistence, delete, that every knob drives logic, and parity
//! through the collection/RAG API.
//!
//! Counts are kept modest so the suite is quick in debug builds; the index is
//! ~30x faster in release.

mod common;

use std::collections::HashSet;

use common::{doc_with_vector, names, open_plain_db, temp_db, temp_plain_db};
use nitrite::nitrite::Nitrite;
use nitrite_vector::diskann::DiskAnnIndex;
use nitrite_vector::{
    vector_field, vector_index_options, DiskAnnConfig, IndexBackend, Metric, Precision,
    VectorIndexConfig,
};

// ---- helpers -------------------------------------------------------------

fn gen(n: usize, dim: usize, seed: u64) -> Vec<(u64, Vec<f32>)> {
    let mut s = seed;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        (s >> 40) as f32 / (1u64 << 24) as f32 - 0.5
    };
    // Snowflake-range ids (NitriteId requires id >= 10^18).
    (0..n)
        .map(|i| (i as u64 + 1_000_000_000_000_000_000, (0..dim).map(|_| next()).collect()))
        .collect()
}

fn brute_force(vectors: &[(u64, Vec<f32>)], q: &[f32], k: usize, metric: Metric) -> Vec<u64> {
    let pq = metric.prepare(q.to_vec());
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .map(|(id, v)| (metric.distance(&pq, &metric.prepare(v.clone())), *id))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn open_index(
    db: &Nitrite,
    base: &str,
    dim: usize,
    metric: Metric,
    precision: Precision,
    cfg: DiskAnnConfig,
) -> DiskAnnIndex {
    DiskAnnIndex::open(&db.config(), base, dim, metric, precision, &cfg)
        .expect("open diskann")
        .0
}

/// PQ training runs in the background off the insert path; wait for it so
/// assertions about trained state are deterministic.
fn wait_for_pq(index: &DiskAnnIndex) {
    let mut waited = 0;
    while !index.pq_trained() && waited < 60_000 {
        std::thread::sleep(std::time::Duration::from_millis(20));
        waited += 20;
    }
}

/// The on-disk file name is a sanitized+hashed form of the base name; find the
/// single `.dann` data file in the db directory.
fn find_dann_file(dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().map(|e| e == "dann").unwrap_or(false))
        .expect("no .dann data file found")
}

// ---- direct DiskAnnIndex tests ------------------------------------------

#[test]
fn recall_is_high_vs_brute_force_with_pq() {
    let (_dir, db) = temp_plain_db();
    let dim = 32;
    let n = 1200;
    let metric = Metric::Euclidean;
    let vectors = gen(n, dim, 0xABCDEF);

    let cfg = DiskAnnConfig {
        degree: 48,
        build_beam: 100,
        search_beam: 130,
        alpha: 1.2,
        pq_subvectors: 8,
        pq_train_threshold: 400, // ensure PQ actually trains
        cache_bytes: 32 * 1024 * 1024,
        consolidate_threshold: 1000,
    };
    let index = open_index(&db, "recall", dim, metric, Precision::F32, cfg);
    for (id, v) in &vectors {
        index.insert(*id, v.clone()).unwrap();
    }
    wait_for_pq(&index);
    assert!(index.pq_trained(), "PQ should be trained past the threshold");

    let k = 10;
    let mut hits = 0usize;
    let mut total = 0usize;
    for (_, q) in gen(40, dim, 0x999).iter() {
        let truth: HashSet<u64> = brute_force(&vectors, q, k, metric).into_iter().collect();
        for (id, _) in index.search(q, k, Some(150)).unwrap() {
            if truth.contains(&id.id_value()) {
                hits += 1;
            }
        }
        total += k;
    }
    let recall = hits as f64 / total as f64;
    assert!(recall >= 0.88, "recall {recall} below 0.88");
}

#[test]
fn vectors_are_disk_resident_and_queries_are_correct() {
    let (dir, db) = temp_plain_db();
    let dim = 32;
    let n = 1500;
    let cfg = DiskAnnConfig { pq_train_threshold: 400, pq_subvectors: 8, ..Default::default() };
    let index = open_index(&db, "resident", dim, Metric::Cosine, Precision::F32, cfg);
    let vectors = gen(n, dim, 0x1111);
    for (id, v) in &vectors {
        index.insert(*id, v.clone()).unwrap();
    }
    index.flush().unwrap();

    // The full vectors live in the memory-mapped file on disk, not the heap.
    let data_file = find_dann_file(dir.path());
    let file_len = std::fs::metadata(&data_file).unwrap().len() as usize;
    assert!(
        file_len >= n * dim * 4,
        "data file {file_len} too small to hold {n} x {dim} f32 vectors on disk"
    );

    // Queries still resolve correctly.
    let (id0, q0) = &vectors[7];
    let got = index.search(q0, 1, Some(120)).unwrap();
    assert_eq!(got[0].0.id_value(), *id0);
}

#[test]
fn survives_close_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let dim = 16;
    let vectors = gen(800, dim, 0x2222);
    let cfg = DiskAnnConfig { pq_train_threshold: 300, pq_subvectors: 8, ..Default::default() };

    let query = vectors[42].1.clone();
    let expected;
    {
        let db = open_plain_db(&path);
        let index = open_index(&db, "persist", dim, Metric::Euclidean, Precision::F16, cfg);
        for (id, v) in &vectors {
            index.insert(*id, v.clone()).unwrap();
        }
        wait_for_pq(&index); // deterministic: query with the trained PQ both times
        expected = index.search(&query, 5, Some(120)).unwrap()
            .into_iter().map(|(id, _)| id.id_value()).collect::<Vec<_>>();
        index.flush().unwrap();
        db.close().unwrap();
    }
    {
        let db = open_plain_db(&path);
        let index = open_index(&db, "persist", dim, Metric::Euclidean, Precision::F16, cfg);
        assert_eq!(index.len(), 800);
        let got: Vec<u64> = index.search(&query, 5, Some(120)).unwrap()
            .into_iter().map(|(id, _)| id.id_value()).collect();
        assert_eq!(got, expected, "results must be identical after reopen");
        db.close().unwrap();
    }
}

#[test]
fn delete_removes_from_results_and_keeps_graph_searchable() {
    let (_dir, db) = temp_plain_db();
    let dim = 16;
    let vectors = gen(500, dim, 0x3333);
    let cfg = DiskAnnConfig { pq_train_threshold: 200, pq_subvectors: 8, ..Default::default() };
    let index = open_index(&db, "del", dim, Metric::Euclidean, Precision::F32, cfg);
    for (id, v) in &vectors {
        index.insert(*id, v.clone()).unwrap();
    }

    let (target, query) = (vectors[10].0, vectors[10].1.clone());
    assert_eq!(index.search(&query, 1, Some(100)).unwrap()[0].0.id_value(), target);

    index.remove(target).unwrap();
    let after = index.search(&query, 5, Some(120)).unwrap();
    assert!(after.iter().all(|(id, _)| id.id_value() != target));
    assert!(!after.is_empty());
    assert_eq!(index.len(), 499);
}

#[test]
fn precision_drives_on_disk_vector_size() {
    // I8 vectors must produce a materially smaller on-disk data file than F32.
    fn data_file_len(precision: Precision) -> u64 {
        let (dir, db) = temp_plain_db();
        let dim = 128;
        // Small degree so the vector (not adjacency) dominates the slot stride.
        let cfg = DiskAnnConfig { degree: 8, pq_subvectors: 0, ..Default::default() };
        let index = open_index(&db, "prec", dim, Metric::Cosine, precision, cfg);
        for (id, v) in gen(300, dim, 0x4444) {
            index.insert(id, v).unwrap();
        }
        index.flush().unwrap();
        std::fs::metadata(find_dann_file(dir.path())).unwrap().len()
    }
    let f32_len = data_file_len(Precision::F32);
    let i8_len = data_file_len(Precision::I8);
    assert!(i8_len < f32_len, "I8 file ({i8_len}) not smaller than F32 ({f32_len})");
}

#[test]
fn degree_caps_out_degree() {
    let (_dir, db) = temp_plain_db();
    let dim = 16;
    let cfg = DiskAnnConfig { degree: 12, pq_subvectors: 0, ..Default::default() };
    let index = open_index(&db, "deg", dim, Metric::Euclidean, Precision::F32, cfg);
    for (id, v) in gen(400, dim, 0x5555) {
        index.insert(id, v).unwrap();
    }
    assert!(index.max_out_degree() <= 12, "out-degree exceeded configured R");
}

#[test]
fn consolidate_repairs_and_reclaims_after_deletes() {
    let (_dir, db) = temp_plain_db();
    let dim = 16;
    let vectors = gen(600, dim, 0x6666);
    // Disable auto-consolidation so we drive it explicitly; exact (no PQ).
    let cfg = DiskAnnConfig {
        degree: 32,
        build_beam: 64,
        pq_subvectors: 0,
        consolidate_threshold: 0,
        ..Default::default()
    };
    let index = open_index(&db, "cons", dim, Metric::Euclidean, Precision::F32, cfg);
    for (id, v) in &vectors {
        index.insert(*id, v.clone()).unwrap();
    }

    // Delete 100 nodes.
    let deleted: Vec<u64> = vectors.iter().take(100).map(|(id, _)| *id).collect();
    for id in &deleted {
        index.remove(*id).unwrap();
    }
    assert_eq!(index.pending_len(), 100);
    assert_eq!(index.len(), 500);

    index.consolidate().unwrap();
    assert_eq!(index.pending_len(), 0, "consolidation must reclaim pending slots");

    // Surviving nodes are still findable; deleted ones are gone.
    let deleted_set: HashSet<u64> = deleted.into_iter().collect();
    let mut correct = 0;
    let mut total = 0;
    for (id, v) in vectors.iter().skip(100).take(60) {
        let got = index.search(v, 1, Some(80)).unwrap();
        assert!(got.iter().all(|(g, _)| !deleted_set.contains(&g.id_value())));
        if got[0].0.id_value() == *id {
            correct += 1;
        }
        total += 1;
    }
    assert!(correct as f64 / total as f64 >= 0.9, "recall after consolidation dropped");
}

#[test]
fn background_consolidation_triggers_past_threshold() {
    let (_dir, db) = temp_plain_db();
    let dim = 16;
    let vectors = gen(500, dim, 0x7777);
    let cfg = DiskAnnConfig {
        degree: 32,
        build_beam: 64,
        pq_subvectors: 0,
        consolidate_threshold: 40, // low, so deletes trigger a background pass
        ..Default::default()
    };
    let index = open_index(&db, "bg", dim, Metric::Euclidean, Precision::F32, cfg);
    for (id, v) in &vectors {
        index.insert(*id, v.clone()).unwrap();
    }

    for (id, _) in vectors.iter().take(60) {
        index.remove(*id).unwrap();
    }

    // A background consolidation should have been spawned; wait for it to drain.
    let mut waited = 0;
    while index.pending_len() > 0 && waited < 5000 {
        std::thread::sleep(std::time::Duration::from_millis(20));
        waited += 20;
    }
    assert_eq!(index.pending_len(), 0, "background consolidation did not run");
    assert_eq!(index.len(), 440);
}

// ---- parity through the collection / RAG API ----------------------------

#[test]
fn diskann_backend_works_through_collection_api() {
    let dim = 8;
    let config = VectorIndexConfig::new(dim, Metric::Cosine)
        .backend(IndexBackend::DiskAnn)
        .pq_subvectors(4)
        .pq_train_threshold(100);
    let (_dir, db) = temp_db(config);
    let c = db.collection("docs").unwrap();
    c.create_index(vec!["embedding"], &vector_index_options()).unwrap();

    c.insert(doc_with_vector("a", &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])).unwrap();
    c.insert(doc_with_vector("b", &[0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])).unwrap();
    c.insert(doc_with_vector("z", &[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])).unwrap();

    let filter = vector_field("embedding")
        .nearest(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 2)
        .build();
    let got = names(&c, filter);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], "a");
    assert!(got.contains(&"b".to_string()));
}
