# Nitrite

Nitrite is an embedded NoSQL document database for Rust.

## Features

- **Embedded Database** - In-process database with no network overhead
- **Document Store** - Store JSON-like documents with flexible schemas
- **Collections** - Organize documents into named collections
- **Indexing** - Unique and non-unique indexes for fast queries
- **Filters** - Query documents using field-based filters
- **Encryption** - AES-GCM encryption for sensitive data
- **Pluggable Storage** - In-memory or persistent storage via modules

## Quick Start

```rust
use nitrite::nitrite::Nitrite;
use nitrite::filter::field;
use nitrite::doc;

// Create an in-memory database
let db = Nitrite::builder()
    .open_or_create(None, None)
    .expect("Failed to create database");

// Get a collection
let collection = db.collection("users").unwrap();

// Insert a document
collection.insert(doc!{"name": "John", "age": 30}).unwrap();

// Query documents
let cursor = collection.find(field("name").eq("John")).unwrap();
for doc in cursor {
    println!("{:?}", doc);
}

// Close the database
db.close().unwrap();
```

## Indexing

```rust
use nitrite::index::{unique_index, non_unique_index};

// Create a unique index
collection.create_index(vec!["email"], &unique_index()).unwrap();

// Create a non-unique index
collection.create_index(vec!["department"], &non_unique_index()).unwrap();
```

## Filters

```rust
use nitrite::filter::field;

// Equality
field("name").eq("John")

// Comparison
field("age").gt(18)
field("age").gte(21)
field("age").lt(65)
field("age").lte(60)

// Logical operators
field("active").eq(true).and(field("age").gte(18))
field("role").eq("admin").or(field("role").eq("moderator"))
```

## Storage Modules

Nitrite supports pluggable storage backends:

- **In-memory** - Built-in `InMemoryStoreModule` for testing and ephemeral data
- **Fjall** - Persistent storage via `nitrite-fjall-adapter` crate
- **Spatial** - Geospatial indexing via `nitrite-spatial` crate
- **Full-Text Search** - Tantivy-based FTS via `nitrite-tantivy-fts` crate

## License

Apache License 2.0
