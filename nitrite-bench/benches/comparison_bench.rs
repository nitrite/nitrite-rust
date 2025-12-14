//! Database comparison benchmarks - compare Nitrite with SQLite, Redb, Sled
//!
//! This benchmark requires the `comparison` feature to be enabled:
//! ```bash
//! cargo bench -p nitrite_bench --features comparison -- comparison
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite::filter::all;
use nitrite_bench::stores::create_fjall_db;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use uuid::Uuid;

/// Counter for unique database paths
static COMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Get test-data directory for comparison databases
fn get_comparison_db_path(name: &str) -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let test_data_dir = PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("test-data");
    std::fs::create_dir_all(&test_data_dir).ok();

    let counter = COMP_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let unique_id = Uuid::new_v4();
    test_data_dir.join(format!("{}_{}_{}", name, counter, unique_id))
}

/// Clean up a database path
fn cleanup_db_path(path: &PathBuf) {
    if path.exists() {
        let _ = std::fs::remove_dir_all(path);
    }
    // Also try removing as file for SQLite
    let _ = std::fs::remove_file(path);
}

/// Simple document for comparison tests
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchDoc {
    id: u64,
    name: String,
    email: String,
    age: i32,
    active: bool,
}

impl BenchDoc {
    fn new(id: u64) -> Self {
        Self {
            id,
            name: format!("User {}", id),
            email: format!("user{}@example.com", id),
            age: (id % 80 + 18) as i32,
            active: id % 2 == 0,
        }
    }
}

// ============================================================================
// Nitrite benchmarks
// ============================================================================

fn nitrite_insert(doc_count: usize) -> std::time::Duration {
    let ctx = create_fjall_db().unwrap();
    let collection = ctx.db().collection("bench").unwrap();

    let start = Instant::now();
    for i in 0..doc_count as u64 {
        let name = format!("User {}", i);
        let email = format!("user{}@example.com", i);
        let age = (i % 80 + 18) as i64;
        let active = i % 2 == 0;
        let doc = nitrite::doc! {
            id: i,
            name: name,
            email: email,
            age: age,
            active: active
        };
        collection.insert(doc).unwrap();
    }
    let elapsed = start.elapsed();

    // Explicitly drop to close DB and release file locks
    drop(collection);
    drop(ctx);
    // Small delay to allow OS to release file locks
    std::thread::sleep(std::time::Duration::from_millis(50));

    elapsed
}

fn nitrite_read(doc_count: usize) -> std::time::Duration {
    let ctx = create_fjall_db().unwrap();
    let collection = ctx.db().collection("bench").unwrap();

    // Insert first
    for i in 0..doc_count as u64 {
        let name = format!("User {}", i);
        let email = format!("user{}@example.com", i);
        let age = (i % 80 + 18) as i64;
        let active = i % 2 == 0;
        let doc = nitrite::doc! {
            id: i,
            name: name,
            email: email,
            age: age,
            active: active
        };
        collection.insert(doc).unwrap();
    }

    let start = Instant::now();
    let cursor = collection.find(all()).unwrap();
    let count = cursor.count();
    let elapsed = start.elapsed();
    black_box(count);

    // Explicitly drop to close DB and release file locks
    drop(collection);
    drop(ctx);
    // Small delay to allow OS to release file locks
    std::thread::sleep(std::time::Duration::from_millis(50));

    elapsed
}

// ============================================================================
// SQLite benchmarks
// ============================================================================

#[cfg(feature = "comparison")]
fn sqlite_insert(doc_count: usize) -> std::time::Duration {
    use rusqlite::Connection;

    let db_path = get_comparison_db_path("sqlite");
    let conn = Connection::open(&db_path).unwrap();

    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)",
        [],
    )
    .unwrap();

    let start = Instant::now();
    for i in 0..doc_count as u64 {
        conn.execute(
            "INSERT INTO users (id, name, email, age, active) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                i,
                format!("User {}", i),
                format!("user{}@example.com", i),
                (i % 80 + 18) as i32,
                if i % 2 == 0 { 1 } else { 0 }
            ],
        )
        .unwrap();
    }
    let elapsed = start.elapsed();
    drop(conn);
    cleanup_db_path(&db_path);
    elapsed
}

#[cfg(feature = "comparison")]
fn sqlite_read(doc_count: usize) -> std::time::Duration {
    use rusqlite::Connection;

    let db_path = get_comparison_db_path("sqlite");
    let conn = Connection::open(&db_path).unwrap();

    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)",
        [],
    )
    .unwrap();

    for i in 0..doc_count as u64 {
        conn.execute(
            "INSERT INTO users (id, name, email, age, active) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                i,
                format!("User {}", i),
                format!("user{}@example.com", i),
                (i % 80 + 18) as i32,
                if i % 2 == 0 { 1 } else { 0 }
            ],
        )
        .unwrap();
    }

    let start = Instant::now();
    let mut stmt = conn.prepare("SELECT * FROM users").unwrap();
    let count = stmt.query_map([], |_| Ok(())).unwrap().count();
    let elapsed = start.elapsed();
    black_box(count);
    drop(stmt);
    drop(conn);
    cleanup_db_path(&db_path);
    elapsed
}

// ============================================================================
// Redb benchmarks
// ============================================================================

#[cfg(feature = "comparison")]
fn redb_insert(doc_count: usize) -> std::time::Duration {
    use redb::{Database, TableDefinition};

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("users");

    let db_path = get_comparison_db_path("redb");
    let db = Database::create(&db_path).unwrap();

    let start = Instant::now();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..doc_count as u64 {
            let doc = BenchDoc::new(i);
            let serialized = serde_json::to_vec(&doc).unwrap();
            table.insert(i, serialized.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();
    let elapsed = start.elapsed();
    drop(db);
    cleanup_db_path(&db_path);
    elapsed
}

#[cfg(feature = "comparison")]
fn redb_read(doc_count: usize) -> std::time::Duration {
    use redb::{Database, ReadableTable, TableDefinition};

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("users");

    let db_path = get_comparison_db_path("redb");
    let db = Database::create(&db_path).unwrap();

    // Insert first
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..doc_count as u64 {
            let doc = BenchDoc::new(i);
            let serialized = serde_json::to_vec(&doc).unwrap();
            table.insert(i, serialized.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let start = Instant::now();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    let count = table.iter().unwrap().count();
    let elapsed = start.elapsed();
    black_box(count);
    drop(read_txn);
    drop(db);
    cleanup_db_path(&db_path);
    elapsed
}

// ============================================================================
// Sled benchmarks
// ============================================================================

#[cfg(feature = "comparison")]
fn sled_insert(doc_count: usize) -> std::time::Duration {
    let db_path = get_comparison_db_path("sled");
    let db = sled::open(&db_path).unwrap();

    let start = Instant::now();
    for i in 0..doc_count as u64 {
        let doc = BenchDoc::new(i);
        let serialized = serde_json::to_vec(&doc).unwrap();
        db.insert(i.to_be_bytes(), serialized).unwrap();
    }
    db.flush().unwrap();
    let elapsed = start.elapsed();
    drop(db);
    cleanup_db_path(&db_path);
    elapsed
}

#[cfg(feature = "comparison")]
fn sled_read(doc_count: usize) -> std::time::Duration {
    let db_path = get_comparison_db_path("sled");
    let db = sled::open(&db_path).unwrap();

    // Insert first
    for i in 0..doc_count as u64 {
        let doc = BenchDoc::new(i);
        let serialized = serde_json::to_vec(&doc).unwrap();
        db.insert(i.to_be_bytes(), serialized).unwrap();
    }
    db.flush().unwrap();

    let start = Instant::now();
    let count = db.iter().count();
    let elapsed = start.elapsed();
    black_box(count);
    drop(db);
    cleanup_db_path(&db_path);
    elapsed
}

// ============================================================================
// Criterion benchmarks
// ============================================================================

fn bench_comparison_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("Comparison/Insert");
    group.sample_size(10);

    for &doc_count in [100, 1_000, 5_000].iter() {
        group.bench_with_input(
            BenchmarkId::new("nitrite", doc_count),
            &doc_count,
            |b, &doc_count| {
                b.iter(|| nitrite_insert(doc_count));
            },
        );

        #[cfg(feature = "comparison")]
        {
            group.bench_with_input(
                BenchmarkId::new("sqlite", doc_count),
                &doc_count,
                |b, &doc_count| {
                    b.iter(|| sqlite_insert(doc_count));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("redb", doc_count),
                &doc_count,
                |b, &doc_count| {
                    b.iter(|| redb_insert(doc_count));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("sled", doc_count),
                &doc_count,
                |b, &doc_count| {
                    b.iter(|| sled_insert(doc_count));
                },
            );
        }
    }

    group.finish();
}

fn bench_comparison_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Comparison/Read");
    group.sample_size(10);

    for &doc_count in [100, 1_000, 5_000].iter() {
        group.bench_with_input(
            BenchmarkId::new("nitrite", doc_count),
            &doc_count,
            |b, &doc_count| {
                b.iter(|| nitrite_read(doc_count));
            },
        );

        #[cfg(feature = "comparison")]
        {
            group.bench_with_input(
                BenchmarkId::new("sqlite", doc_count),
                &doc_count,
                |b, &doc_count| {
                    b.iter(|| sqlite_read(doc_count));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("redb", doc_count),
                &doc_count,
                |b, &doc_count| {
                    b.iter(|| redb_read(doc_count));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("sled", doc_count),
                &doc_count,
                |b, &doc_count| {
                    b.iter(|| sled_read(doc_count));
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_comparison_insert, bench_comparison_read);
criterion_main!(benches);
