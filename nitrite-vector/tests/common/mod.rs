//! Shared helpers for vector integration tests.
#![allow(dead_code)] // each test binary uses only a subset of these helpers

use nitrite::collection::{Document, NitriteCollection};
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;
use nitrite_vector::{vector_to_value, VectorIndexConfig, VectorModule};

/// Opens (or reopens) a Fjall-backed Nitrite database at `path` with a vector
/// module configured by `config`.
pub fn open_db(path: &str, config: VectorIndexConfig) -> Nitrite {
    let storage_module = FjallModule::with_config()
        .db_path(path)
        .low_memory_preset()
        .build();

    Nitrite::builder()
        .load_module(storage_module)
        .load_module(VectorModule::new(config))
        .open_or_create(None, None)
        .expect("failed to open vector-enabled Nitrite database")
}

/// A fresh temporary database directory (kept alive by the returned guard).
pub fn temp_db(config: VectorIndexConfig) -> (tempfile::TempDir, Nitrite) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_str().expect("utf8 path").to_string();
    let db = open_db(&path, config);
    (dir, db)
}

/// Opens a Fjall-backed database at `path` with **no** vector module, for tests
/// that drive a `DiskAnnIndex` directly against the store.
pub fn open_plain_db(path: &str) -> Nitrite {
    let storage_module = FjallModule::with_config()
        .db_path(path)
        .low_memory_preset()
        .build();
    Nitrite::builder()
        .load_module(storage_module)
        .open_or_create(None, None)
        .expect("failed to open Nitrite database")
}

/// A fresh plain (no vector module) temporary database.
pub fn temp_plain_db() -> (tempfile::TempDir, Nitrite) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_str().expect("utf8 path").to_string();
    let db = open_plain_db(&path);
    (dir, db)
}

/// Builds a document with a `name` label and an embedding vector.
pub fn doc_with_vector(name: &str, vector: &[f32]) -> Document {
    let mut d = Document::new();
    d.put("name", name.to_string()).unwrap();
    d.put("embedding", vector_to_value(vector)).unwrap();
    d
}

/// Collects the `name` field of every document a cursor yields, in order.
pub fn names(collection: &NitriteCollection, filter: nitrite::filter::Filter) -> Vec<String> {
    let cursor = collection.find(filter).expect("find");
    cursor
        .map(|r| {
            let doc = r.expect("doc");
            match doc.get("name").expect("name") {
                nitrite::common::Value::String(s) => s,
                _ => String::new(),
            }
        })
        .collect()
}
