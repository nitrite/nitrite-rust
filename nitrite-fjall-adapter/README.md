# Nitrite Fjall Adapter

Persistent storage adapter for Nitrite using [Fjall](https://github.com/fjall-rs/fjall), an LSM-based key-value store.

## Features

- **Persistent Storage** - Data survives process restarts
- **LSM-tree Architecture** - Optimized for write-heavy workloads
- **Configurable Presets** - Pre-configured settings for common use cases
- **Custom Configuration** - Fine-grained control over storage behavior

## Usage

```rust
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;

// Create with default configuration
let storage = FjallModule::with_config()
    .db_path("/path/to/database")
    .build();

let db = Nitrite::builder()
    .load_module(storage)
    .open_or_create(None, None)
    .expect("Failed to create database");
```

## Configuration Presets

### Production Preset

Balanced settings for production workloads:

```rust
let storage = FjallModule::with_config()
    .production_preset()
    .db_path("/path/to/database")
    .build();
```

### High Throughput Preset

Optimized for batch imports and write-heavy workloads:

```rust
let storage = FjallModule::with_config()
    .high_throughput_preset()
    .db_path("/path/to/database")
    .build();
```

### Low Memory Preset

Reduced memory footprint for constrained environments:

```rust
let storage = FjallModule::with_config()
    .low_memory_preset()
    .db_path("/path/to/database")
    .build();
```

## Custom Configuration

Override specific settings after applying a preset:

```rust
let storage = FjallModule::with_config()
    .production_preset()
    .block_cache_capacity(128 * 1024 * 1024)  // 128MB cache
    .fsync_frequency(50)                       // More frequent fsyncs
    .db_path("/path/to/database")
    .build();
```

## Persistence

```rust
// Write data
let collection = db.collection("items").unwrap();
collection.insert(doc!{"key": "value"}).unwrap();

// Commit changes to disk
db.commit().unwrap();

// Close database
db.close().unwrap();
```

## License

Apache License 2.0
