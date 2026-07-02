//! The HNSW index must survive a database close/reopen (durability), honor the
//! configured stored-vector precision, and heal itself from corrupt or stale
//! index storage by rebuilding from the collection.

mod common;

use common::{doc_with_vector, names, open_db, open_plain_db};
use nitrite::common::Value;
use nitrite_vector::{
    vector_field, vector_index_options, IndexBackend, Metric, Precision, VectorIndexConfig,
};

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

#[test]
fn hnsw_honors_stored_vector_precision_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    // F16 stored vectors: search must still resolve correctly after a reopen
    // forces vectors to round-trip through the half-precision codec.
    let config = VectorIndexConfig::new(3, Metric::Cosine).precision(Precision::F16);

    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        collection.create_index(vec!["embedding"], &vector_index_options()).unwrap();
        collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("b", &[0.0, 1.0, 0.0])).unwrap();
        db.close().unwrap();
    }
    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 1).build();
        assert_eq!(names(&collection, filter), vec!["a"]);
        db.close().unwrap();
    }
}

#[test]
fn corrupt_hnsw_header_triggers_automatic_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let config = VectorIndexConfig::new(3, Metric::Cosine);

    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        collection.create_index(vec!["embedding"], &vector_index_options()).unwrap();
        collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("b", &[0.0, 1.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("c", &[0.9, 0.1, 0.0])).unwrap();
        db.close().unwrap();
    }
    // Corrupt the index header through a plain (no vector module) session.
    {
        let db = open_plain_db(&path);
        let store = db.config().nitrite_store().unwrap();
        let map = store.open_map("docs_embedding_vector_idx").unwrap();
        map.put(
            Value::String("__hnsw_meta__".to_string()),
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        )
        .unwrap();
        db.close().unwrap();
    }
    // Reopen with the module: the damaged index must be rebuilt and correct.
    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 2).build();
        let got = names(&collection, filter);
        assert_eq!(got, vec!["a", "c"], "rebuilt index must rank correctly");
        db.close().unwrap();
    }
}

#[test]
fn lost_diskann_sidecar_triggers_automatic_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let config = VectorIndexConfig::new(3, Metric::Cosine)
        .backend(IndexBackend::DiskAnn)
        .pq_subvectors(0);

    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        collection.create_index(vec!["embedding"], &vector_index_options()).unwrap();
        collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("b", &[0.0, 1.0, 0.0])).unwrap();
        collection.insert(doc_with_vector("c", &[0.9, 0.1, 0.0])).unwrap();
        db.close().unwrap();
    }
    // Simulate a crash artifact: delete the sidecar, keep the data file.
    let meta = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.to_string_lossy().ends_with(".dann.meta"))
        .expect("sidecar exists");
    std::fs::remove_file(meta).unwrap();

    {
        let db = open_db(&path, config);
        let collection = db.collection("docs").unwrap();
        let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 2).build();
        let got = names(&collection, filter);
        assert_eq!(got, vec!["a", "c"], "rebuilt DiskANN index must rank correctly");
        db.close().unwrap();
    }
}

#[test]
fn per_index_configs_allow_mixed_dimensions() {
    use nitrite::nitrite::Nitrite;
    use nitrite_fjall_adapter::FjallModule;
    use nitrite_vector::VectorModule;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = Nitrite::builder()
        .load_module(FjallModule::with_config().db_path(&path).low_memory_preset().build())
        .load_module(
            VectorModule::builder(3, Metric::Cosine)
                .index_config("wide", "embedding", VectorIndexConfig::new(5, Metric::Euclidean))
                .build(),
        )
        .open_or_create(None, None)
        .unwrap();

    let narrow = db.collection("docs").unwrap();
    narrow.create_index(vec!["embedding"], &vector_index_options()).unwrap();
    narrow.insert(doc_with_vector("n", &[1.0, 0.0, 0.0])).unwrap();

    let wide = db.collection("wide").unwrap();
    wide.create_index(vec!["embedding"], &vector_index_options()).unwrap();
    wide.insert(doc_with_vector("w", &[1.0, 0.0, 0.0, 0.0, 0.0])).unwrap();

    let got = names(&narrow, vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 1).build());
    assert_eq!(got, vec!["n"]);
    let got = names(&wide, vector_field("embedding").nearest(vec![1.0, 0.0, 0.0, 0.0, 0.0], 1).build());
    assert_eq!(got, vec!["w"]);
    db.close().unwrap();
}

#[test]
fn knn_query_without_index_is_an_error() {
    let (_dir, db) = common::temp_db(VectorIndexConfig::new(3, Metric::Cosine));
    let collection = db.collection("noindex").unwrap();
    collection.insert(doc_with_vector("a", &[1.0, 0.0, 0.0])).unwrap();

    let filter = vector_field("embedding").nearest(vec![1.0, 0.0, 0.0], 1).build();
    let result = collection.find(filter).and_then(|c| c.map(|r| r.map(|_| ())).collect::<Result<Vec<_>, _>>());
    assert!(result.is_err(), "kNN without a vector index must error, not scan");
}
