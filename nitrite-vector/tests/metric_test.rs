//! Each metric must rank the same corpus differently and consistently with a
//! brute-force computation.

mod common;

use std::collections::HashSet;

use common::{doc_with_vector, names, temp_db};
use nitrite_vector::{vector_field, vector_index_options, Metric, VectorIndexConfig};

/// Inserts a small crafted 2D corpus and returns the nearest name to `q`.
fn nearest_name(metric: Metric, points: &[(&str, [f32; 2])], q: [f32; 2]) -> String {
    let (_dir, db) = temp_db(VectorIndexConfig::new(2, metric));
    let collection = db.collection("docs").unwrap();
    collection
        .create_index(vec!["embedding"], &vector_index_options())
        .unwrap();
    for (name, v) in points {
        collection.insert(doc_with_vector(name, v)).unwrap();
    }
    let filter = vector_field("embedding").nearest(q.to_vec(), 1).build();
    names(&collection, filter).into_iter().next().unwrap()
}

#[test]
fn metrics_rank_the_same_corpus_differently() {
    // X is close in direction and in space; Y has a much larger dot product.
    let points = [("x", [0.6, 0.05]), ("y", [2.0, 1.5])];
    let q = [1.0, 0.0];

    assert_eq!(nearest_name(Metric::Cosine, &points, q), "x");
    assert_eq!(nearest_name(Metric::Euclidean, &points, q), "x");
    assert_eq!(nearest_name(Metric::Dot, &points, q), "y");
}

// Deterministic pseudo-random vectors (xorshift) so ground truth is stable.
fn gen(n: usize, dim: usize) -> Vec<(String, Vec<f32>)> {
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 40) as f32 / (1u64 << 24) as f32 - 0.5
    };
    (0..n)
        .map(|i| (format!("v{i}"), (0..dim).map(|_| next()).collect()))
        .collect()
}

fn brute_force_top(metric: Metric, corpus: &[(String, Vec<f32>)], q: &[f32], k: usize) -> HashSet<String> {
    let pq = metric.prepare(q.to_vec());
    let mut scored: Vec<(f32, String)> = corpus
        .iter()
        .map(|(name, v)| (metric.distance(&pq, &metric.prepare(v.clone())), name.clone()))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0));
    scored.into_iter().take(k).map(|(_, n)| n).collect()
}

#[test]
fn each_metric_agrees_with_brute_force_top1() {
    let dim = 8;
    let corpus = gen(40, dim);

    for metric in [Metric::Cosine, Metric::Euclidean, Metric::Dot] {
        let (_dir, db) = temp_db(VectorIndexConfig::new(dim, metric));
        let collection = db.collection("docs").unwrap();
        collection
            .create_index(vec!["embedding"], &vector_index_options())
            .unwrap();
        for (name, v) in &corpus {
            let mut d = nitrite::collection::Document::new();
            d.put("name", name.clone()).unwrap();
            d.put("embedding", nitrite_vector::vector_to_value(v)).unwrap();
            collection.insert(d).unwrap();
        }

        // Use a handful of queries drawn from the corpus.
        for (_, q) in corpus.iter().take(8) {
            let filter = vector_field("embedding").nearest(q.clone(), 1).ef(64).build();
            let got = names(&collection, filter).into_iter().next().unwrap();
            let truth = brute_force_top(metric, &corpus, q, 3);
            assert!(
                truth.contains(&got),
                "{metric:?}: top-1 {got} not in brute-force top-3 {truth:?}"
            );
        }
    }
}
