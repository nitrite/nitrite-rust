//! Store factory functions for benchmarks

use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;
use nitrite_spatial::SpatialModule;
use nitrite_tantivy_fts::TantivyFtsModule;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Result type for store operations
pub type StoreResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Counter for unique database paths within a run
static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Get the test-data directory path at the project root
fn get_test_data_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("test-data")
}

/// Create a unique database path within the test-data directory
fn create_unique_db_path() -> PathBuf {
    let test_data_dir = get_test_data_dir();
    std::fs::create_dir_all(&test_data_dir).ok();

    let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let unique_id = Uuid::new_v4();
    test_data_dir.join(format!("bench_{}_{}", counter, unique_id))
}

/// Context holding a database and its path for cleanup
pub struct BenchContext {
    db: Nitrite,
    db_path: Option<PathBuf>,
}

impl BenchContext {
    pub fn db(&self) -> &Nitrite {
        &self.db
    }
}

impl Drop for BenchContext {
    fn drop(&mut self) {
        // Close the database first
        let _ = self.db.close();

        // Then clean up the database directory
        if let Some(ref path) = self.db_path {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

/// Clean up all benchmark data in the test-data directory
pub fn cleanup_all_bench_data() {
    let test_data_dir = get_test_data_dir();
    if test_data_dir.exists() {
        // Remove all contents but keep the directory
        if let Ok(entries) = std::fs::read_dir(&test_data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let _ = std::fs::remove_dir_all(&path);
                } else {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
}

/// Create an in-memory Nitrite database
pub fn create_inmemory_db() -> StoreResult<BenchContext> {
    let db = Nitrite::builder().open_or_create(None, None)?;
    Ok(BenchContext { db, db_path: None })
}

/// Create an in-memory Nitrite database with spatial support
pub fn create_inmemory_spatial_db() -> StoreResult<BenchContext> {
    let db = Nitrite::builder()
        .load_module(SpatialModule)
        .open_or_create(None, None)?;

    Ok(BenchContext { db, db_path: None })
}

/// Create an in-memory Nitrite database with FTS support
pub fn create_inmemory_fts_db() -> StoreResult<BenchContext> {
    let db = Nitrite::builder()
        .load_module(TantivyFtsModule::default())
        .open_or_create(None, None)?;

    Ok(BenchContext { db, db_path: None })
}

/// Create a Fjall-backed Nitrite database
pub fn create_fjall_db() -> StoreResult<BenchContext> {
    let db_path = create_unique_db_path();

    let fjall_module = FjallModule::with_config()
        .db_path(db_path.to_str().unwrap())
        .build();

    let db = Nitrite::builder()
        .load_module(fjall_module)
        .open_or_create(None, None)?;

    Ok(BenchContext {
        db,
        db_path: Some(db_path),
    })
}

/// Create a Fjall-backed Nitrite database with spatial support
pub fn create_fjall_spatial_db() -> StoreResult<BenchContext> {
    let db_path = create_unique_db_path();

    let fjall_module = FjallModule::with_config()
        .db_path(db_path.to_str().unwrap())
        .build();

    let db = Nitrite::builder()
        .load_module(fjall_module)
        .load_module(SpatialModule)
        .open_or_create(None, None)?;

    Ok(BenchContext {
        db,
        db_path: Some(db_path),
    })
}

/// Create a Fjall-backed Nitrite database with FTS support
pub fn create_fjall_fts_db() -> StoreResult<BenchContext> {
    let db_path = create_unique_db_path();

    let fjall_module = FjallModule::with_config()
        .db_path(db_path.to_str().unwrap())
        .build();

    let db = Nitrite::builder()
        .load_module(fjall_module)
        .load_module(TantivyFtsModule::default())
        .open_or_create(None, None)?;

    Ok(BenchContext {
        db,
        db_path: Some(db_path),
    })
}
