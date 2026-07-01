//! The HNSW index must survive a database close/reopen (durability).

mod common;

use common::{doc_with_vector, names, open_db};
use nitrite_vector::{vector_field, vector_index_options, Metric, VectorIndexConfig};

#[test]
fn index_survives_close_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let config = VectorIndexConfig::new(3, Metric::Cosine);

    // Phase 1: build the index, then close the database.
    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        collection
            .create_index(vec!["embedding"], &vector_index_options())
            .unwrap();
        collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("b", &[0.0, 1.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("c", &[0.0, 0.0, 1.0])).unwrap();
        collection.insert(doc_with_vector("d", &[0.9, 0.1, 0.0])).unwrap();
        db.close().unwrap();
    }

    // Phase 2: reopen at the same path and query — the graph must be intact.
    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 2).build();
        let got = names(&collection, filter);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], "a");
        assert_eq!(got[1], "d");
        db.close().unwrap();
    }
}
