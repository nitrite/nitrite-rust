//! End-to-end tests of the vector index through the collection API.

mod common;

use common::{doc_with_vector, names, temp_db};
use nitrite_vector::{vector_field, vector_index_options, Metric, VectorIndexConfig};

fn config() -> VectorIndexConfig {
    VectorIndexConfig::new(3, Metric::Euclidean)
}

#[test]
fn nearest_returns_documents_in_distance_order() {
    let (_dir, db) = temp_db(config());
    let collection = db.collection("docs").unwrap();
    collection
        .create_index(vec!["embedding"], &vector_index_options())
        .unwrap();

    collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();
    collection.insert(doc_with_vector("b", &[0.0, 1.0, 0.0])).unwrap();
    collection.insert(doc_with_vector("c", &[0.0, 0.0, 1.0])).unwrap();
    collection.insert(doc_with_vector("d", &[0.9, 0.1, 0.0])).unwrap();

    // Query nearest to "a": expect a, then d (closest by L2), before b/c.
    let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 2).build();
    let got = names(&collection, filter);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], "a");
    assert_eq!(got[1], "d");
}

#[test]
fn update_reindexes_the_vector() {
    let (_dir, db) = temp_db(config());
    let collection = db.collection("docs").unwrap();
    collection
        .create_index(vec!["embedding"], &vector_index_options())
        .unwrap();

    let res = collection.insert(doc_with_vector("a", &[0.0, 0.0, 0.0])).unwrap();
    collection.insert(doc_with_vector("b", &[10.0, 10.0, 10.0])).unwrap();
    let id = res.affected_nitrite_ids()[0];

    // Move "a" far from the origin; the nearest to the origin becomes "b".
    let moved = doc_with_vector("a", &[100.0, 100.0, 100.0]);
    collection.update_by_id(&id, &moved, false).unwrap();

    let filter = vector_field("embedding").nearest(vec![0.1, 0.1, 0.1], 1).build();
    let got = names(&collection, filter);
    assert_eq!(got, vec!["b"]);
}

#[test]
fn remove_drops_document_from_results() {
    let (_dir, db) = temp_db(config());
    let collection = db.collection("docs").unwrap();
    collection
        .create_index(vec!["embedding"], &vector_index_options())
        .unwrap();

    collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();
    collection.insert(doc_with_vector("b", &[0.9, 0.1, 0.0])).unwrap();
    collection.insert(doc_with_vector("c", &[0.0, 1.0, 0.0])).unwrap();

    // Remove the exact match, then querying near it must not return "a".
    collection
        .remove(nitrite::filter::field("name").eq("a"), true)
        .unwrap();

    let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 3).build();
    let got = names(&collection, filter);
    assert!(!got.contains(&"a".to_string()));
    assert_eq!(got[0], "b");
}
