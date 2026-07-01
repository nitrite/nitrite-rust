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
