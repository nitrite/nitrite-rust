//! RAG store: add/search/filter/min_score/delete over text + embedding + metadata.

mod common;

use common::temp_db;
use nitrite::doc;
use nitrite::filter::field;
use nitrite::nitrite::Nitrite;
use nitrite_vector::{Metric, RagStore, VectorIndexConfig};

// Keep the `Nitrite` handle alive: dropping it closes the underlying store.
fn store() -> (tempfile::TempDir, Nitrite, RagStore) {
    let (dir, db) = temp_db(VectorIndexConfig::new(3, Metric::Cosine));
    let store = RagStore::create(&db, "kb", Metric::Cosine).unwrap();
    (dir, db, store)
}

#[test]
fn search_returns_scored_hits_in_order() {
    let (_dir, _db, store) = store();
    store.add("apple", vec![1.0, 0.0, 0.0], doc! {"source": "fruit"}).unwrap();
    store.add("banana", vec![0.9, 0.1, 0.0], doc! {"source": "fruit"}).unwrap();
    store.add("car", vec![0.0, 1.0, 0.0], doc! {"source": "vehicle"}).unwrap();

    let hits = store.search(vec![1.0, 0.0, 0.0], 2).run().unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].text, "apple");
    assert_eq!(hits[1].text, "banana");
    // Scores are descending and the exact match is ~1.0 cosine similarity.
    assert!(hits[0].score >= hits[1].score);
    assert!((hits[0].score - 1.0).abs() < 1e-4);
}

#[test]
fn search_respects_metadata_filter() {
    let (_dir, _db, store) = store();
    store.add("apple", vec![1.0, 0.0, 0.0], doc! {"source": "fruit"}).unwrap();
    store.add("almost", vec![0.98, 0.02, 0.0], doc! {"source": "vehicle"}).unwrap();
    store.add("banana", vec![0.9, 0.1, 0.0], doc! {"source": "fruit"}).unwrap();

    // Nearest is "apple"/"almost", but only fruit is allowed through.
    let hits = store
        .search(vec![1.0, 0.0, 0.0], 2)
        .filter(field("source").eq("fruit"))
        .run()
        .unwrap();
    assert!(hits.iter().all(|h| {
        matches!(h.document.get("source"), Ok(nitrite::common::Value::String(s)) if s == "fruit")
    }));
    assert_eq!(hits[0].text, "apple");
}

#[test]
fn selective_filter_finds_matches_outside_the_oversample_window() {
    let (_dir, _db, store) = store();
    // 40 chunks that are all closer to the query than the one we want, so the
    // fixed `k * oversample` window (5 * 4 = 20) contains none of the matches.
    for i in 0..40 {
        let jitter = i as f32 * 0.001;
        store
            .add(format!("other-{i}"), vec![1.0 - jitter, jitter, 0.0], doc! {"entry": "other"})
            .unwrap();
    }
    store.add("wanted", vec![0.0, 0.0, 1.0], doc! {"entry": "target"}).unwrap();

    let hits = store
        .search(vec![1.0, 0.0, 0.0], 5)
        .filter(field("entry").eq("target"))
        .run()
        .unwrap();
    assert_eq!(hits.len(), 1, "selective filter must reach past the initial window");
    assert_eq!(hits[0].text, "wanted");
}

#[test]
fn filter_matching_nothing_returns_empty() {
    let (_dir, _db, store) = store();
    for i in 0..20 {
        store.add(format!("d{i}"), vec![1.0, i as f32 * 0.01, 0.0], doc! {"entry": "a"}).unwrap();
    }

    let hits = store
        .search(vec![1.0, 0.0, 0.0], 5)
        .filter(field("entry").eq("nope"))
        .run()
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn filter_returns_all_matches_when_fewer_than_k() {
    let (_dir, _db, store) = store();
    for i in 0..30 {
        store.add(format!("o{i}"), vec![1.0, i as f32 * 0.001, 0.0], doc! {"entry": "other"}).unwrap();
    }
    store.add("t1", vec![0.0, 0.0, 1.0], doc! {"entry": "target"}).unwrap();
    store.add("t2", vec![0.0, 0.1, 1.0], doc! {"entry": "target"}).unwrap();

    let hits = store
        .search(vec![1.0, 0.0, 0.0], 10)
        .filter(field("entry").eq("target"))
        .run()
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(hits[0].score >= hits[1].score);
}

#[test]
fn min_score_filters_out_dissimilar_hits() {
    let (_dir, _db, store) = store();
    store.add("close", vec![1.0, 0.0, 0.0], doc! {}).unwrap();
    store.add("orthogonal", vec![0.0, 1.0, 0.0], doc! {}).unwrap();

    // Cosine similarity of orthogonal vectors is 0, well below 0.5.
    let hits = store
        .search(vec![1.0, 0.0, 0.0], 5)
        .min_score(0.5)
        .run()
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].text, "close");
}

#[test]
fn delete_removes_a_hit() {
    let (_dir, _db, store) = store();
    let apple = store.add("apple", vec![1.0, 0.0, 0.0], doc! {}).unwrap();
    store.add("banana", vec![0.9, 0.1, 0.0], doc! {}).unwrap();

    assert_eq!(store.len().unwrap(), 2);
    assert!(store.delete(&apple).unwrap());
    assert_eq!(store.len().unwrap(), 1);
    assert!(store.get(&apple).unwrap().is_none());

    let hits = store.search(vec![1.0, 0.0, 0.0], 5).run().unwrap();
    assert!(hits.iter().all(|h| h.text != "apple"));
    assert_eq!(hits[0].text, "banana");
}
