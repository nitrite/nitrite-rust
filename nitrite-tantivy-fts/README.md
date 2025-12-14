# Nitrite Tantivy FTS

Full-text search module for Nitrite using [Tantivy](https://github.com/quickwit-oss/tantivy).

## Features

- **Full-Text Indexing** - Tokenized text search with Tantivy
- **Term Matching** - Search for individual terms in text fields
- **Index Management** - Create and drop FTS indexes

## Usage

### Loading the Module

```rust
use nitrite::nitrite::Nitrite;
use nitrite_tantivy_fts::TantivyFtsModule;

let db = Nitrite::builder()
    .load_module(TantivyFtsModule::default())
    .open_or_create(None, None)
    .expect("Failed to create database");
```

### Creating an FTS Index

```rust
use nitrite_tantivy_fts::fts_index;

let collection = db.collection("articles").unwrap();
collection.create_index(vec!["content"], &fts_index()).unwrap();
```

### Inserting Documents

```rust
use nitrite::doc;

let doc = doc! {
    title: "Hello World",
    content: "A quick brown fox jumps over the lazy dog"
};
collection.insert(doc).unwrap();
```

### Searching

```rust
use nitrite_tantivy_fts::fts_field;

// Search for a term in the indexed field
let filter = fts_field("content").matches("fox");
let cursor = collection.find(filter).unwrap();
```

### Dropping an FTS Index

```rust
collection.drop_index(vec!["content"]).unwrap();
```

### Checking Index Existence

```rust
let has_index = collection.has_index(vec!["content"]).unwrap();
```

## Integration with Fjall

For persistent FTS with Fjall storage:

```rust
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;
use nitrite_tantivy_fts::TantivyFtsModule;

let storage = FjallModule::with_config()
    .db_path("/path/to/database")
    .build();

let db = Nitrite::builder()
    .load_module(storage)
    .load_module(TantivyFtsModule::default())
    .open_or_create(None, None)
    .expect("Failed to create database");
```

## License

Apache License 2.0
