# Nitrite Rust — Agent SKILL File

This file provides a complete, accurate reference for AI coding agents (Codex, Claude Code, Gemini, Windsurf, etc.) working in the `nitrite-rust` workspace.

---

## Project Overview

**Nitrite** is an embedded, serverless NoSQL document database written in Rust. It stores documents in typed collections, supports ACID transactions, pluggable storage backends, spatial indexing, and full-text search.

- **Version**: `0.2.0`
- **Edition**: Rust 2021
- **License**: Apache-2.0
- **MSRV**: Rust 1.70+
- **Repository**: https://github.com/nitrite/nitrite-rust

---

## Workspace Layout

```
nitrite-rust/
├── nitrite/                  # Core database engine (published crate)
├── nitrite-derive/           # Procedural macros: Convertible, NitriteEntity
├── nitrite-fjall-adapter/    # Persistent storage via Fjall LSM-tree (published)
├── nitrite-spatial/          # R-tree spatial indexing (published)
├── nitrite-tantivy-fts/      # Full-text search via Tantivy (published)
├── nitrite-int-test/         # Integration tests (publish = false)
├── nitrite-bench/            # Criterion benchmarks (publish = false)
└── Cargo.toml                # Workspace root, resolver = "2"
```

Workspace-shared dependency: `parking_lot = "0.12.3"`.

---

## Build & Test Commands

```bash
# Build everything
cargo build --workspace

# Run all unit + doc tests
cargo test --workspace --verbose

# Run tests with custom separator feature flag
cargo test --features custom_separator -- custom_separator_test

# Integration tests: Fjall (persistent) backend (default)
cargo test -p nitrite_int_test

# Integration tests: in-memory backend
cargo test -p nitrite_int_test --features memory --no-default-features

# Run benchmarks
cargo bench -p nitrite_bench

# Run comparison benchmarks (vs SQLite/Redb/Sled)
cargo bench -p nitrite_bench --features comparison
```

CI runs on `ubuntu-latest`, `macos-latest`, and `windows-latest` via `.github/workflows/rust.yml`.

---

## Crate-by-Crate Reference

### `nitrite` — Core Engine

**Crate name**: `nitrite`
**Entry point**: `nitrite::nitrite::Nitrite` (PIMPL pattern over `Arc<NitriteInner>`)

#### Key Public Modules

| Module | Key Exports |
|--------|-------------|
| `nitrite::nitrite` | `Nitrite` — database handle |
| `nitrite::nitrite_builder` | `NitriteBuilder` — fluent builder |
| `nitrite::nitrite_config` | `NitriteConfig` — config access |
| `nitrite::collection` | `Document`, `NitriteCollection`, `NitriteId`, `FindOptions`, `UpdateOptions`, `CollectionEvents`, `CollectionEventInfo`, `WriteResult` |
| `nitrite::filter` | `field()`, `all()`, `by_id()`, `and()`, `or()`, `not()` — filter API |
| `nitrite::index` | `IndexOptions`, `IndexDescriptor`, `unique_index()`, `non_unique_index()`, `full_text_index()`, `NitriteIndexer` |
| `nitrite::repository` | `ObjectRepository`, `NitriteEntity`, `RepositoryCursor` |
| `nitrite::transaction` | `Session`, `NitriteTransaction`, `TransactionContext`, `TransactionStore`, `TransactionalMap` |
| `nitrite::store` | `NitriteStore`, `NitriteMapProvider`, `NitriteStoreProvider`, `InMemoryStoreModule`, `StoreEventListener` |
| `nitrite::migration` | `Migration`, `MigrationStep`, `MigrationArguments`, `MigrationManager` |
| `nitrite::common` | `Value`, `Convertible`, `NitriteModule`, `NitritePlugin`, `NitritePluginProvider`, `PluginRegistrar`, `PersistentCollection`, `EventAware`, `AttributeAware`, `SortOrder`, `SortableFields`, `DocumentCursor`, `Processor`, constants |
| `nitrite::errors` | `NitriteError`, `NitriteResult<T>`, `ErrorKind` |
| `nitrite::metadata` | `NitriteMetadata` |

#### Global Statics (in `lib.rs`)

- `FIELD_SEPARATOR: LazyLock<Atomic<String>>` — default `"."`, configurable once via builder
- `ID_GENERATOR: LazyLock<SnowflakeIdGenerator>` — Snowflake-based unique IDs
- `SCHEDULER: LazyLock<Scheduler>` — background task scheduler

#### Features

- `default = ["serde"]` — enables serde support on `Value` and `Document`
- `custom_separator` — unlocks tests for non-default field separator
- `serde` — optional, enables serde derive impls for `Value`/`Document`

#### Key Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| `im` | 15.1.0 | Persistent immutable `OrdMap` for `Document` |
| `parking_lot` | 0.12.3 | Fast mutexes/rwlocks |
| `dashmap` | 6.1.0 | Concurrent hash maps |
| `crossbeam-skiplist` | 0.1.3 | Ordered concurrent structures |
| `argon2` | 0.5.3 | Password hashing for auth |
| `aes-gcm` | 0.10.3 | AES-GCM encryption |
| `icu_collator` | 2.0.0 | Unicode-aware collation for sorting |
| `chrono` | 0.4.39 | Date/time support |
| `rand` | =0.8.5 | **Pinned** — CryptoRng trait compat issue |
| `basu` | 0.1.5 | Event bus system |
| `regex` | 1.11.1 | Regex filter support |

---

### `nitrite_derive` — Procedural Macros

**Crate name**: `nitrite_derive`
**Type**: `proc-macro` crate

Two derive macros:

```rust
#[derive(Convertible)]           // structs + enums
#[derive(NitriteEntity)]         // structs only
```

#### `Convertible`

- Works on **structs with named fields** and **enums**
- Unions are not supported
- Field attribute: `#[converter(serialize = "fn_name", deserialize = "fn_name")]`
- Generates `to_value()` / `from_value()` implementations

#### `NitriteEntity`

- Works on **structs with named fields only**
- Must also derive `Convertible`
- Struct-level `#[entity(...)]` attribute:

```rust
#[entity(
    name = "collection_name",                        // optional: override collection name
    id(field = "id"),                                // required: specify ID field name
    id(field = "id", embedded_fields = "a, b"),      // optional: composite embedded ID
    index(type = "unique", fields = "email"),         // optional: define indexes
    index(type = "non-unique", fields = "author, year")
)]
```

- **ID field types**: any type implementing `Convertible`, including `NitriteId`, `i64`, `String`, `Option<NitriteId>`, etc.
- **Index types**: `"unique"`, `"non-unique"`, `"full-text"`, `"spatial"`

---

### `nitrite_fjall_adapter` — Persistent Storage

**Crate name**: `nitrite_fjall_adapter`

```rust
use nitrite_fjall_adapter::FjallModule;

let db = Nitrite::builder()
    .load_module(FjallModule::with_config()
        .db_path("/path/to/db")
        .build())
    .open_or_create(None, None)?;
```

Internally uses `fjall 2.6.3` (LSM-tree) with `bincode 2.0.1` for binary serialization.

#### Public API

- `FjallModule` — implements `NitriteModule`
- `FjallModuleBuilder` — fluent builder via `FjallModule::with_config()`
- `FjallConfig` — thread-safe atomic config (PIMPL with `Arc`)

#### Builder Methods

| Method | Default | Description |
|--------|---------|-------------|
| `.db_path(path)` | `""` | Database directory path (**required**) |
| `.block_cache_capacity(bytes)` | 64 MB | Block cache size |
| `.blob_cache_capacity(bytes)` | 32 MB | Blob cache size |
| `.max_write_buffer_size(bytes)` | 128 MB | Write buffer size |
| `.max_journaling_size(bytes)` | 512 MB | Transaction journal limit |
| `.max_memtable_size(bytes)` | 32 MB | Memtable size |
| `.flush_workers(n)` | CPU cores | Flush worker threads |
| `.compaction_workers(n)` | CPU/2 | Compaction worker threads |
| `.bloom_filter_bits(n)` | 10 | Bloom filter bits per key (0 = disabled) |
| `.compression_type(ct)` | `Lz4` | Compression (`None`, `Lz4`, `Miniz`, `Lz4Hc`) |
| `.compaction_strategy(s)` | default | Compaction strategy |
| `.fsync_frequency(ms)` | 0 | Fsync interval in ms (0 = disabled) |
| `.manual_journal_persist(bool)` | `false` | Manual journal persistence |
| `.kv_separated(bool)` | `false` | KV separation for large values |
| `.block_size(bytes)` | 4 KB | Block size |
| `.space_amp_factor(f)` | 1.5 | Space amplification factor |
| `.staleness_threshold(f)` | 0.8 | Staleness threshold |

#### Configuration Presets

```rust
FjallModule::with_config()
    .production_preset()       // Balanced for production (256 MB cache, bloom, fsync 100ms)
    .db_path("/path")
    .build();

FjallModule::with_config()
    .high_throughput_preset()  // Batch imports (512 MB cache, manual journal, KV sep)
    .db_path("/path")
    .build();

FjallModule::with_config()
    .low_memory_preset()       // Embedded/dev (16 MB cache, 1 worker each)
    .db_path("/path")
    .build();
```

Presets can be overridden by subsequent builder calls.

---

### `nitrite_spatial` — Spatial Indexing

**Crate name**: `nitrite_spatial`

Disk-based R-tree (`DiskRTree`) with LRU page cache and Hilbert-curve ordering.

```rust
use nitrite_spatial::{SpatialModule, spatial_index, spatial_field, Geometry};

let db = Nitrite::builder()
    .load_module(SpatialModule)
    .open_or_create(None, None)?;

let col = db.collection("places")?;
col.create_index(vec!["location"], &spatial_index())?;

// Spatial filters
col.find(spatial_field("location").within(Geometry::envelope(-74.0, 40.7, -73.9, 40.9)))?;
col.find(spatial_field("location").intersects(Geometry::point(0.0, 0.0)))?;
col.find(spatial_field("location").near(Geometry::point(0.0, 0.0), 1000.0))?;
```

#### Public Types

- **Geometry**: `Point`, `GeoPoint`, `Coordinate`, `Geometry`, `LineString`, `PolygonWithHoles`, `MultiGeometry`
- **Filters**: `IntersectsFilter`, `WithinFilter`, `NearFilter`, `GeoNearFilter`
- **R-Tree**: `DiskRTree`, `BoundingBox`, `NitriteRTree`, `RTreeStats`, `SpatialError`, `SpatialResult`
- **Module**: `SpatialModule`, `SpatialIndexer`
- **Fluent API**: `spatial_field(name)` → `SpatialFluentFilter`
- **Index type constant**: `SPATIAL_INDEX = "spatial"`
- **Helper functions**: `create_geodesic_circle()`, `meters_to_degrees()`, `parse_geojson()`, `parse_wkt()`

#### Key Dependencies

- `rstar 0.12` — R-tree core
- `memmap2 0.9` — memory-mapped file I/O
- `bincode 2.0` — binary serialization

---

### `nitrite_tantivy_fts` — Full-Text Search

**Crate name**: `nitrite_tantivy_fts`

Backed by `tantivy 0.25.0` with BM25 scoring, phrase queries, fuzzy and prefix search.

```rust
use nitrite_tantivy_fts::{TantivyFtsModule, fts_index, fts_field};

// Default config
let db = Nitrite::builder()
    .load_module(TantivyFtsModule::default())
    .open_or_create(None, None)?;

// Custom config
let db = Nitrite::builder()
    .load_module(TantivyFtsModule::with_config()
        .index_writer_heap_size(100 * 1024 * 1024)
        .num_threads(4)
        .search_result_limit(5000)
        .build())
    .open_or_create(None, None)?;

let col = db.collection("articles")?;
col.create_index(vec!["content"], &fts_index())?;
let results = col.find(fts_field("content").matches("rust database"))?;
```

#### FTS Config Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `index_writer_heap_size` | 50 MB | Memory for index writer |
| `num_threads` | 0 (auto) | Indexing threads |
| `search_result_limit` | 10,000 | Max results per search |

#### Public Types

- `TantivyFtsModule`, `TantivyFtsModuleBuilder`
- `FtsConfig`
- `FtsFilter`, `PhraseFilter`, `TextSearchFilter`
- `FtsFluentFilter`, `fts_field(name)`
- `FtsIndexer`
- `FTS_INDEX = "full-text"`

---

### `nitrite_int_test` — Integration Tests

Located in `nitrite-int-test/tests/`. Features: `default = ["fjall"]`, or `--features memory --no-default-features` for in-memory.

Test directories:
- `collection/` — CRUD, find, index, events (18 files)
- `repository/` — repository CRUD and operations (10 files)
- `transaction/` — transaction tests (3 files)
- `migration/` — schema migration tests (2 files)
- `spatial/` — spatial index tests (3 files)
- `fts/` — full-text search tests (2 files)
- `event/` — event listener tests (2 files)
- Root tests: `convertible_test.rs`, `custom_filter_test.rs`, `document_metadata_test.rs`, `multi_threaded_test.rs`, `nitrite_builder_test.rs`, `nitrite_entity_derive_test.rs`, `store_test.rs`, `stream_test.rs`

Test utilities in `nitrite-int-test/src/test_util.rs`:
- `run_test(before, test, after)` — test harness with retry (3 attempts)
- `TestContext` — holds `path` + `Nitrite` handle
- `create_test_context()` — Fjall or in-memory DB creation
- `create_spatial_test_context()` — DB with `SpatialModule`
- `create_fts_test_context()` — DB with `TantivyFtsModule`
- `cleanup(ctx)` — close DB + remove temp directory
- `create_test_docs()` — standard 3-document test dataset
- `NitriteDateTime` — test datetime wrapper

### `nitrite_bench` — Benchmarks

Criterion-based benchmarks in `nitrite-bench/benches/`:
- `crud_bench` — insert/find/delete throughput
- `index_bench` — index creation and query
- `spatial_bench` — spatial query performance
- `fts_bench` — full-text search performance
- `concurrency_bench` — concurrent read/write
- `transaction_bench` — transaction overhead
- `comparison_bench` — vs SQLite, Redb, Sled (requires `--features comparison`)

---

## Core Abstractions

### `Document`

Immutable persistent ordered map (`im::OrdMap<String, Value>`), O(1) clone via structural sharing.

```rust
use nitrite::collection::Document;
use nitrite::doc;

let mut doc = Document::new();
doc.put("name", "Alice")?;            // accepts any Into<Value>
doc.put("address.city", "NYC")?;      // nested path via field separator
let name = doc.get("name")?;          // returns &Value

// Or use the macro
let doc = doc! { "name": "Alice", "age": 30i64 };

// Macro with nested documents
let doc = doc! {
    "name": "Alice",
    "age": 30i64,
    "active": true,
    "address": { "city": "NYC", "zip": "10001" },
    "tags": ["rust", "database"],
};
```

Reserved fields (managed internally, do not set manually):
- `_id` — unique `NitriteId`
- `_revision` — revision counter
- `_source` — source tag
- `_modified` — last modification timestamp

### `Value`

Enum representing all storable types:

```
Null, Bool, I8, I16, I32, I64, I128, U8, U16, U32, U64, U128,
ISize, USize, F32, F64, Char, String, Document, Array, Map, NitriteId, Bytes, Unknown
```

Use `From` implementations or `.into()` for conversion. `Bytes` is not indexable.

Macros: `val!(42i32)` — shorthand for `Value::from(42i32)`.

### `NitriteId`

Snowflake-based unique ID generated at insertion. Used as `_id` in documents. Can also be used as entity ID field type in repositories.

### `Convertible` Trait

```rust
pub trait Convertible {
    type Output;
    fn to_value(&self) -> NitriteResult<Value>;
    fn from_value(value: &Value) -> NitriteResult<Self::Output>;
}
```

Implemented for: all primitive types, `String`, `Option<T>`, `Vec<T>`, `HashMap`, `BTreeMap`, `Document`, `NitriteId`, `chrono::DateTime`, etc.

Use `#[derive(Convertible)]` for custom types.

### `NitriteModule` Trait

```rust
pub trait NitriteModule: Send + Sync {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>>;
    fn load(&self, registrar: &PluginRegistrar) -> NitriteResult<()>;
}
```

Implement this to create custom modules. Load via `NitriteBuilder::load_module(module)`.

### `PersistentCollection` Trait

Shared interface for both `NitriteCollection` and `ObjectRepository`:

```rust
pub trait PersistentCollection: EventAware + AttributeAware + Send + Sync {
    fn create_index(&self, field_names: Vec<&str>, index_options: &IndexOptions) -> NitriteResult<()>;
    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()>;
    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>>;
    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool>;
    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool>;
    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()>;
    fn drop_all_indexes(&self) -> NitriteResult<()>;
    fn clear(&self) -> NitriteResult<()>;
    fn dispose(&self) -> NitriteResult<()>;
    fn is_dropped(&self) -> NitriteResult<bool>;
    fn is_open(&self) -> NitriteResult<bool>;
    fn size(&self) -> NitriteResult<u64>;
    fn close(&self) -> NitriteResult<()>;
    fn store(&self) -> NitriteResult<NitriteStore>;
    fn add_processor(&self, processor: Processor) -> NitriteResult<()>;
}
```

---

## API Patterns

### Opening a Database

```rust
use nitrite::nitrite::Nitrite;

// In-memory (default)
let db = Nitrite::builder()
    .open_or_create(None, None)?;

// In-memory with auth
let db = Nitrite::builder()
    .open_or_create(Some("username"), Some("password"))?;

// Persistent with Fjall
use nitrite_fjall_adapter::FjallModule;
let db = Nitrite::builder()
    .load_module(FjallModule::with_config().db_path("./mydb").build())
    .open_or_create(None, None)?;

// Custom field separator and schema version
let db = Nitrite::builder()
    .field_separator("|")
    .schema_version(2)
    .add_migration(my_migration)
    .open_or_create(None, None)?;

db.close()?;  // explicit close; also auto-closes on last Arc drop
```

### Nitrite Instance Methods

```rust
// Collections
db.collection("users")?;                          // get or create
db.has_collection("users")?;                       // check existence
db.list_collection_names()?;                       // HashSet<String>
db.destroy_collection("users")?;                   // drop collection

// Repositories
db.repository::<User>()?;                          // get or create
db.keyed_repository::<User>("prod")?;              // keyed variant
db.has_repository::<User>()?;                      // check existence
db.has_keyed_repository::<User>("prod")?;          // check keyed
db.list_repositories()?;                           // HashSet<String>
db.list_keyed_repositories()?;                     // HashMap<String, HashSet<String>>
db.destroy_repository::<User>()?;                  // drop repository
db.destroy_keyed_repository::<User>("prod")?;      // drop keyed

// Database operations
db.commit()?;                                      // flush to storage
db.compact()?;                                     // reclaim space
db.is_closed()?;
db.has_unsaved_changes()?;
db.database_metadata()?;                           // NitriteMetadata
db.config();                                       // NitriteConfig
db.store();                                        // NitriteStore
```

### Collections

```rust
let col = db.collection("users")?;

// Insert
let result = col.insert(doc! { "name": "Bob", "age": 25i64 })?;  // WriteResult
let results = col.insert_many(vec![doc1, doc2])?;

// Find
let cursor = col.find(field("age").gt(18))?;   // returns DocumentCursor
for doc in cursor { /* ... */ }

let cursor = col.find_with_options(
    field("age").gt(18),
    &FindOptions::new().sort_by("name", SortOrder::Ascending).limit(10)
)?;

// Update
col.update(field("name").eq("Bob"), &doc! { "age": 26i64 })?;
col.update_by_id(&id, &updated_doc, false)?;
col.update_one(&doc, false)?;  // by document's _id

// Remove
col.remove(field("age").lt(0), false)?;  // false = remove all matching
col.remove_one(&doc)?;                   // by document's _id

// Get by ID
let doc = col.get_by_id(&id)?;   // returns Option<Document>

// Indexes
col.create_index(vec!["email"], &unique_index())?;
col.create_index(vec!["name", "age"], &non_unique_index())?;
col.drop_index(vec!["email"])?;
col.has_index(vec!["email"])?;
col.rebuild_index(vec!["email"])?;
col.list_indexes()?;
col.drop_all_indexes()?;

// Collection metadata
let count = col.size()?;
col.clear()?;              // remove all documents
col.is_dropped()?;
col.is_open()?;
```

### FindOptions

```rust
use nitrite::collection::{FindOptions, order_by, skip_by, limit_to, distinct};
use nitrite::common::SortOrder;

// Builder pattern
let opts = FindOptions::new()
    .sort_by("name", SortOrder::Ascending)
    .sort_by("age", SortOrder::Descending)
    .skip(10)
    .limit(20);

// Convenience constructors
let opts = order_by("name", SortOrder::Ascending);
let opts = skip_by(5);
let opts = limit_to(100);
let opts = distinct();

// With collation
let opts = FindOptions::new()
    .collator_options(collator_options)
    .collator_preferences(collator_preferences);
```

### Collection Events

```rust
use nitrite::collection::{CollectionEvents, CollectionEventInfo, CollectionEventListener};

col.subscribe(CollectionEventListener::new(|event: CollectionEventInfo| {
    match event.event_type() {
        CollectionEvents::Insert => println!("Inserted"),
        CollectionEvents::Update => println!("Updated"),
        CollectionEvents::Remove => println!("Removed"),
        CollectionEvents::IndexStart => println!("Indexing started"),
        CollectionEvents::IndexEnd => println!("Indexing ended"),
    }
    Ok(())
}))?;
```

### Repositories (Type-Safe)

```rust
use nitrite_derive::{Convertible, NitriteEntity};

#[derive(Default, Convertible, NitriteEntity)]
#[entity(id(field = "id"), index(type = "unique", fields = "email"))]
pub struct User {
    id: i64,
    name: String,
    email: String,
}

let repo: ObjectRepository<User> = db.repository()?;

// Keyed repository (multiple instances of same type)
let repo_prod: ObjectRepository<User> = db.keyed_repository("prod")?;

repo.insert(User { id: 1, name: "Alice".into(), email: "a@b.com".into() })?;
repo.insert_all(vec![user1, user2])?;

let cursor = repo.find(field("name").eq("Alice"))?;   // RepositoryCursor<User>
let user: Option<User> = repo.get_by_id(&1)?;

repo.update(field("name").eq("Alice"), &updated_user)?;
repo.remove(field("id").eq(1))?;
repo.remove_by_id(&1)?;
```

### Filters

```rust
use nitrite::filter::{field, all, by_id, and, or, not};

// Comparison
field("age").eq(30)
field("age").ne(0)
field("age").gt(18)
field("age").gte(18)
field("age").lt(65)
field("age").lte(65)

// Logical chaining (fluent)
field("age").gt(18).and(field("active").eq(true))
field("role").eq("admin").or(field("role").eq("mod"))

// Standalone logical
and(vec![field("a").eq(1), field("b").eq(2)])
or(vec![field("x").lt(0), field("x").gt(100)])
not(field("deleted").eq(true))

// Pattern
field("name").regex("^Alice")
field("bio").text("keyword")      // uses built-in text index

// Array
field("tags").in_(vec!["rust", "database"])
field("tags").nin(vec!["spam"])
field("items").elem_match(field("price").gt(100))

// Special
all()           // match all documents
by_id(&id)      // match by NitriteId
```

### Transactions

```rust
// Using with_session (recommended — auto-manages session lifecycle)
db.with_session(|session| {
    let tx = session.begin_transaction()?;

    let tx_col = tx.collection("users")?;
    tx_col.insert(doc! { "name": "Charlie" })?;

    let tx_repo: TransactionalRepository<User> = tx.repository()?;
    tx_repo.insert(user)?;

    tx.commit()?;   // or tx.rollback()?
    Ok(())
})?;
```

Transaction semantics:
- **Atomicity**: All operations succeed or fail together
- **Isolation**: Copy-on-write; uncommitted changes invisible outside transaction
- **Consistency**: Database state remains consistent on error
- **Durability**: Committed changes are persisted

### Migrations

```rust
use nitrite::migration::{Migration, MigrationStep, MigrationArguments};

struct MyMigration;

impl MigrationStep for MyMigration {
    fn migrate(&self, args: &mut MigrationArguments) -> nitrite::errors::NitriteResult<()> {
        // args provides transactional access:
        // args.collection("name") -> TransactionalCollection
        // args.repository::<T>() -> TransactionalRepository<T>
        Ok(())
    }
}

let migration = Migration::create(1, 2)
    .add_instruction(Box::new(MyMigration))
    .finalize();

let db = Nitrite::builder()
    .schema_version(2)
    .add_migration(migration)
    .open_or_create(None, None)?;
```

Migrations are atomic — either all steps succeed or the database is rolled back.

---

## Error Handling

All fallible operations return `NitriteResult<T>` = `Result<T, NitriteError>`.

```rust
use nitrite::errors::{NitriteError, ErrorKind, NitriteResult};

fn example() -> NitriteResult<()> {
    Err(NitriteError::new("message", ErrorKind::IndexNotFound))
}

// Error with cause chain
let cause = NitriteError::new("IO failed", ErrorKind::IOError);
let err = NitriteError::new_with_cause("Build failed", ErrorKind::IndexBuildFailed, cause);

// Accessors
err.message()    // &str
err.kind()       // &ErrorKind
err.cause()      // Option<&Box<NitriteError>>
```

### `ErrorKind` Variants

**Filter**: `FilterError`
**Indexing**: `IndexingError`, `IndexNotFound`, `IndexAlreadyExists`, `IndexBuildFailed`, `IndexCorrupted`, `IndexTypeMismatch`, `IndexingInProgress`
**Identity**: `InvalidId`, `NotIdentifiable`, `NotFound`
**Operation**: `InvalidOperation`
**IO/Storage**: `IOError`, `DiskFull`, `FileNotFound`, `PermissionDenied`, `FileCorrupted`, `FileAccessError`
**Encoding**: `EncodingError`, `ObjectMappingError`
**Security**: `SecurityError`
**Constraints**: `UniqueConstraintViolation`
**Validation**: `ValidationError`, `InvalidDataType`, `InvalidFieldName`, `MissingRequiredField`
**Collection/Repo**: `CollectionNotFound`, `RepositoryNotFound`
**Events**: `EventError`
**Plugin**: `PluginError`, `PluginLoadFailed`
**Backend**: `BackendError`, `StoreNotInitialized`, `StoreAlreadyClosed`
**Migration**: `MigrationError`
**Extension**: `Extension(String)` — for external crate errors (e.g., `"spatial"`, `"FullText"`)
**Internal**: `InternalError`

### Automatic `From` Conversions

`NitriteError` implements `From` for: `std::io::Error`, `FromUtf8Error`, `fmt::Error`, `ParseIntError`, `ParseFloatError`, `String`, `&str`. All support the `?` operator in functions returning `NitriteResult`.

---

## Architecture & Design Patterns

### PIMPL Pattern

`Nitrite`, `NitriteConfig`, `NitriteCollection`, `ObjectRepository`, `FjallConfig`, `FtsConfig` all use PIMPL: a thin public struct holds `Arc<...Inner>`. Cloning is cheap — all clones share the same state. This ensures thread safety and API stability.

### Plugin System

All extensions (storage backends, indexers, FTS, spatial) are `NitriteModule` implementations loaded at database-open time. Each module registers `NitritePlugin` objects via `PluginRegistrar`. Plugins are initialized on DB open and closed on DB shutdown.

### Storage Abstraction

`NitriteStoreProvider` trait abstracts the storage engine. Provides `NitriteMapProvider` (key-value maps). Default is `InMemoryStoreModule` (auto-configured when no store module is loaded). Swapped to `FjallModule` for persistence.

### Concurrency

- Documents use `im::OrdMap` (persistent, lock-free structural sharing)
- Global state protected by `parking_lot` mutexes/rwlocks
- `dashmap` for concurrent hash maps (index maps, collection catalog)
- `crossbeam-skiplist` for ordered concurrent structures
- Config values use `std::sync::atomic` types for lock-free access

### Index Architecture

Three built-in indexers:
- `unique_indexer` — enforces uniqueness, O(1) lookup
- `non_unique_indexer` — allows duplicates, multi-valued
- `text_indexer` — built-in substring/text index

External indexers (via modules):
- `SpatialIndexer` from `nitrite-spatial`
- `FtsIndexer` from `nitrite-tantivy-fts`

Index type string constants: `"unique"`, `"non-unique"`, `"full-text"`, `"spatial"`.

---

## Reserved Names & Fields

**Reserved document fields** (do not use as user-defined keys):
```
_id, _revision, _source, _modified
```

**Reserved store/collection names** (do not use as collection/repository names):
```
$nitrite_meta_map, $nitrite_catalog, $nitrite_users, $nitrite_store_info,
$nitrite_index, $nitrite_index_meta, |, :, +
```

---

## Constants Reference (`nitrite::common`)

| Constant | Value | Purpose |
|----------|-------|---------|
| `DOC_ID` | `"_id"` | Document ID field |
| `DOC_REVISION` | `"_revision"` | Revision field |
| `DOC_MODIFIED` | `"_modified"` | Modified timestamp field |
| `DOC_SOURCE` | `"_source"` | Source tag field |
| `TYPE_NAME` | `"_type"` | Type name field |
| `UNIQUE_INDEX` | `"unique"` | Unique index type |
| `NON_UNIQUE_INDEX` | `"non-unique"` | Non-unique index type |
| `FULL_TEXT_INDEX` | `"full-text"` | Built-in text index type |
| `INITIAL_SCHEMA_VERSION` | `1` | Default schema version |
| `KEY_OBJ_SEPARATOR` | `"+"` | Key-object separator |
| `NAME_SEPARATOR` | `"\|"` | Name separator |
| `INTERNAL_NAME_SEPARATOR` | `"\|"` | Internal name separator |
| `OBJECT_STORE_NAME_SEPARATOR` | `":"` | Object store name separator |
| `NITRITE_VERSION` | `env!("CARGO_PKG_VERSION")` | Crate version at compile time |
| `INDEX_PREFIX` | `"$nitrite_index"` | Index store prefix |
| `INDEX_META_PREFIX` | `"$nitrite_index_meta"` | Index meta store prefix |
| `META_MAP_NAME` | `"$nitrite_meta_map"` | Metadata map name |
| `COLLECTION_CATALOG` | `"$nitrite_catalog"` | Collection catalog name |
| `USER_MAP` | `"$nitrite_users"` | User credentials map |
| `STORE_INFO` | `"$nitrite_store_info"` | Store info map |

---

## Macros

```rust
// doc! — create Document literals
let doc = doc! {
    "name": "Alice",
    "age": 30i64,
    "active": true,
    "address": { "city": "NYC" },
    "tags": ["rust", "db"],
};

// doc! — empty document
let empty = doc! {};

// val! — create Value literals
let v = val!(42i32);
```

The `doc!` macro is defined in `nitrite::collection::document` and re-exported via `#[macro_export]` (use `nitrite::doc`). The `val!` macro is defined in `nitrite::common::value`.

---

## Testing Conventions

- Unit tests live alongside source in `#[cfg(test)]` modules
- Integration tests in `nitrite-int-test/tests/`
- Test utilities in `nitrite-int-test/src/test_util.rs`
- Uses `ctor` crate for test setup/teardown (e.g., `colog::init()`)
- Uses `awaitility` for async-style wait assertions in multi-threaded tests
- Uses `fake` crate for generating test data
- Log initialization via `colog`
- Integration tests use `#[cfg(feature = "fjall")]` / `#[cfg(feature = "memory")]` guards
- Test harness (`run_test`) retries up to 3 times with exponential backoff
- Cleanup uses robust retry logic for file removal (handles OS lock delays)

---

## Common Pitfalls & Notes

1. **Field separator**: Default is `"."`. Set once via `NitriteBuilder::field_separator()`. Nested field access (e.g., `"address.city"`) uses this separator in all filter and document operations. Cannot be changed after database creation.

2. **`Convertible` strictness**: `from_value` is strict — `Value::I8` will not deserialize as `i16`. Derive `Convertible` carefully for numeric types.

3. **`NitriteEntity` requires `Convertible`**: Always derive both together. `NitriteEntity` is structs-only; use `Convertible` alone for enums.

4. **`Default` required for entities**: Structs using `#[derive(NitriteEntity)]` typically need `#[derive(Default)]`.

5. **Bytes are not indexable**: `Value::Bytes` cannot be used in index or filter operations.

6. **Transaction isolation**: Transactions use copy-on-write. Uncommitted changes are not visible outside the transaction.

7. **Collection names**: Avoid names starting with `$` — those are reserved for internal Nitrite use. Names cannot be empty, contain spaces, or match reserved names.

8. **`rand` version pin**: `rand = "=0.8.5"` is pinned due to a `CryptoRng` trait compatibility issue. Do not upgrade without verifying.

9. **`serde` feature**: Enabled by default on the `nitrite` crate. Disabling it removes serde impls from `Value` and `Document`.

10. **Bench comparison feature**: `cargo bench -p nitrite_bench --features comparison` requires `rusqlite`, `redb`, and `sled` — these are heavy dependencies, gated behind the feature.

11. **`doc!` macro import**: Import from `nitrite::doc` (re-exported via `#[macro_export]`).

12. **Auth credentials**: Must provide both username and password, or neither. Providing only one returns `SecurityError`.

13. **Module loading order**: Load storage modules (e.g., `FjallModule`) before calling `open_or_create()`. The builder calls `auto_configure()` internally which sets up `InMemoryStoreModule` if no store is loaded.

14. **Thread safety**: All core types (`Nitrite`, `NitriteCollection`, `ObjectRepository`) are `Send + Sync` and safe to share across threads via `Clone` (which is cheap due to `Arc`).

15. **Fjall test preset**: For integration tests, use `.low_memory_preset()` to minimize thread count and prevent resource exhaustion when running many tests in parallel.

16. **Spatial index in memory mode**: When using spatial indexes with in-memory backend, R-tree files are created as temp files. Test cleanup should handle these via `cleanup_rtree_temp_files()`.
