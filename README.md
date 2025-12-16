# Nitrite

[![Rust](https://github.com/nitrite/nitrite-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/nitrite/nitrite-rust/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/nitrite/nitrite-rust/graph/badge.svg?token=cAta7fAFpR)](https://codecov.io/gh/nitrite/nitrite-rust)

**Nitrite** is an embedded NoSQL document database for Rust. It stores data as documents in collections and supports indexing, querying, and full ACID transactions.

## Features

- **Embedded Database** - In-process, no separate server required
- **Document Store** - Store flexible JSON-like documents
- **Collections & Repositories** - Organize data by collection or type-safe repository
- **Rich Query API** - Filter documents with chainable operators
- **Indexing** - Unique and non-unique indexes for fast queries
- **Transactions** - Full ACID transaction support
- **Pluggable Storage** - In-memory or persistent storage (Fjall)
- **Spatial Indexing** - R-tree based geospatial queries
- **Full-Text Search** - Tantivy-powered text search

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
nitrite = "0.1"
nitrite_derive = "0.1"
```

### Basic Usage with Collections

```rust
use nitrite::nitrite::Nitrite;
use nitrite::filter::field;
use nitrite::doc;

// Open an in-memory database
let db = Nitrite::builder()
    .open_or_create(None, None)
    .expect("Failed to open database");

// Get a collection
let collection = db.collection("users").unwrap();

// Insert documents using the doc! macro
collection.insert(doc!{
    "name": "John Doe",
    "age": 30,
    "active": true
}).unwrap();

// Query with filters
let cursor = collection.find(field("name").eq("John Doe")).unwrap();
for doc in cursor {
    println!("{:?}", doc);
}

// Close the database
db.close().unwrap();
```

### Type-Safe Repository

```rust
use nitrite::nitrite::Nitrite;
use nitrite::repository::ObjectRepository;
use nitrite::filter::field;
use nitrite_derive::{Convertible, NitriteEntity};

#[derive(Default, Convertible, NitriteEntity)]
#[entity(id(field = "id"))]
pub struct User {
    id: i64,
    name: String,
    email: String,
}

let db = Nitrite::builder()
    .open_or_create(None, None)
    .unwrap();

// Get a typed repository
let repo: ObjectRepository<User> = db.repository().unwrap();

// Insert
repo.insert(User {
    id: 1,
    name: "Alice".to_string(),
    email: "alice@example.com".to_string(),
}).unwrap();

// Query
let user = repo.get_by_id(&1).unwrap();
let users = repo.find(field("name").eq("Alice")).unwrap();
```

## Filter Operators

```rust
use nitrite::filter::{field, all, and, or, not};

// Equality
field("name").eq("John")

// Comparison
field("age").gt(18)     // greater than
field("age").gte(21)    // greater than or equal
field("age").lt(65)     // less than
field("age").lte(60)    // less than or equal

// Text search
field("address").text("street")

// Logical operators
and(vec![field("active").eq(true), field("age").gte(18)])
or(vec![field("role").eq("admin"), field("role").eq("moderator")])
not(field("deleted").eq(true))

// All documents
all()
```

## Indexing

```rust
use nitrite::index::{unique_index, non_unique_index};

let collection = db.collection("users").unwrap();

// Create indexes
collection.create_index(vec!["email"], &unique_index()).unwrap();
collection.create_index(vec!["department"], &non_unique_index()).unwrap();

// Check if index exists
let has_index = collection.has_index(vec!["email"]).unwrap();

// Drop index
collection.drop_index(vec!["email"]).unwrap();
```

## Crate Ecosystem

| Crate | Description |
|-------|-------------|
| [`nitrite`](nitrite/) | Core database engine with collections, filters, and transactions |
| [`nitrite-derive`](nitrite-derive/) | Procedural macros for `Convertible` and `NitriteEntity` |
| [`nitrite-fjall-adapter`](nitrite-fjall-adapter/) | Persistent storage using Fjall LSM-tree |
| [`nitrite-spatial`](nitrite-spatial/) | Spatial indexing with R-tree (geospatial queries) |
| [`nitrite-tantivy-fts`](nitrite-tantivy-fts/) | Full-text search using Tantivy |

### Persistent Storage (Fjall)

```rust
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;

let storage = FjallModule::with_config()
    .db_path("/path/to/database")
    .build();

let db = Nitrite::builder()
    .load_module(storage)
    .open_or_create(None, None)
    .unwrap();
```

### Spatial Indexing

```rust
use nitrite::nitrite::Nitrite;
use nitrite_spatial::{SpatialModule, spatial_index, spatial_field, Geometry};

let db = Nitrite::builder()
    .load_module(SpatialModule)
    .open_or_create(None, None)
    .unwrap();

let collection = db.collection("locations").unwrap();
collection.create_index(vec!["location"], &spatial_index()).unwrap();

// Query within bounding box
let bbox = Geometry::envelope(-74.0, 40.7, -73.9, 40.9);
let cursor = collection.find(spatial_field("location").within(bbox)).unwrap();
```

### Full-Text Search

```rust
use nitrite::nitrite::Nitrite;
use nitrite_tantivy_fts::{TantivyFtsModule, fts_index, fts_field};

let db = Nitrite::builder()
    .load_module(TantivyFtsModule::default())
    .open_or_create(None, None)
    .unwrap();

let collection = db.collection("articles").unwrap();
collection.create_index(vec!["content"], &fts_index()).unwrap();

// Search for terms
let cursor = collection.find(fts_field("content").matches("rust database")).unwrap();
```

## Entity Attributes

Define entities with automatic ID management and indexes:

```rust
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite::collection::NitriteId;

#[derive(Default, Convertible, NitriteEntity)]
#[entity(
    name = "books",
    id(field = "id"),
    index(type = "unique", fields = "isbn"),
    index(type = "non-unique", fields = "author, year")
)]
pub struct Book {
    id: NitriteId,
    title: String,
    author: String,
    isbn: String,
    year: i32,
}
```

## Building from Source

```bash
git clone https://github.com/nitrite/nitrite-rust.git
cd nitrite-rust
cargo build --release
cargo test --workspace
```

## License

Apache License 2.0
