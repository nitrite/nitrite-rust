# Nitrite Integration Tests

Integration test suite for the Nitrite database and its modules.

## Purpose

This crate contains comprehensive integration tests covering:

- Core Nitrite functionality (collections, documents, filters)
- Index creation and queries (unique, non-unique)
- Repository and entity operations
- Transactions (commit, rollback)
- Multi-threaded operations
- Spatial indexing and queries
- Full-text search indexing and queries
- Fjall persistent storage

## Running Tests

```bash
# Run all integration tests
cargo test -p nitrite-int-test

# Run specific test file
cargo test -p nitrite-int-test --test store_test

# Run spatial tests
cargo test -p nitrite-int-test --test spatial

# Run FTS tests
cargo test -p nitrite-int-test --test fts
```

## Test Structure

```
tests/
├── collection/         # Collection operation tests
├── event/              # Event handling tests
├── fts/                # Full-text search tests
├── migration/          # Migration tests
├── repository/         # Repository pattern tests
├── spatial/            # Spatial indexing tests
├── transaction/        # Transaction tests
├── store_test.rs       # Storage backend tests
├── multi_threaded_test.rs  # Concurrency tests
└── ...
```

## Test Utilities

The `src/test_util.rs` module provides helper functions:

- `create_test_context()` - Creates a test database with in-memory storage
- `create_fjall_test_context()` - Creates a test database with Fjall storage
- `create_spatial_test_context()` - Creates a database with spatial module
- `create_fts_test_context()` - Creates a database with FTS module
- `cleanup(ctx)` - Cleans up test resources
- `run_test(setup, test, teardown)` - Runs tests with proper setup/teardown

## License

Apache License 2.0
