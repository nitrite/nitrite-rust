# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-06-05

### Fixed

- **`nitrite-fjall-adapter`** — transactional reads inside an active Fjall atomic scope no
  longer rely on Fjall 2.11.2's KV-separated `WriteTransaction` read helpers. The adapter now
  reconstructs read-your-writes semantics from committed partition reads plus a scoped
  transaction overlay, so commits that touch unique indexes succeed on `kv_separated(true)`
  stores instead of failing with `MaybeInlineValue` decode errors.
- **`nitrite-int-test`** — added a KV-separated unique-index transaction regression that
  exercises both `Durability::Periodic` and `Durability::OnCommit`, validates immediate
  post-commit reads, and verifies the data/index pair still agrees after reopen.

### Changed

- **Release** — bumped the workspace crates to `0.3.1` for publication.

## [0.3.0] - 2026-06-05

This release makes cross-partition writes crash-atomic, adds configurable durability, and
substantially speeds up indexed range queries, counts, full-text indexing, and large-result
iteration. It contains breaking changes (see below), hence the `0.2.x` → `0.3.0` bump.

### ⚠️ Breaking Changes

- **`nitrite`** — `NitriteError::cause()` now returns `Option<&NitriteError>` instead of
  `Option<&Box<NitriteError>>`. Most call sites are unaffected thanks to deref coercion; code
  that named the `&Box<…>` type explicitly must drop the `Box`.
- **`nitrite-fjall-adapter`** — the default storage durability is now `Durability::Periodic`
  (commits are buffered to the OS and fsynced by a background timer within ~1s) rather than an
  fsync on every commit. A process crash never loses an acknowledged write; a power loss can
  lose up to ~1s of acknowledged writes by default. Opt back into per-commit fsync with
  `FjallModule::with_config().durability(Durability::OnCommit)`.
- **`nitrite-tantivy-fts`** — full-text writes/deletes are now batched and committed on the next
  search (or on `close()`) instead of once per document. Searches still observe their own writes;
  an unclean crash loses the *uncommitted* FTS batch (the index is derived and rebuildable).
- **`nitrite`** — a `find()` cursor is now *streaming*: `reset()` re-runs the query instead of
  replaying a cached snapshot. Observable results are unchanged for a stable collection; a
  cursor reset after the underlying data has changed now reflects the current data.

### Added

- **Configurable durability** (`nitrite-fjall-adapter`): a `Durability { OnCommit, Periodic }`
  enum with `FjallModuleBuilder::durability(..)` and `FjallConfig::durability()`; a bounded
  background fsync interval (default 1000 ms) so `Periodic` has a bounded power-loss window.
- **Atomic cross-partition transactions** (`nitrite`, `nitrite-fjall-adapter`): a logical write
  (a transaction commit, or a single insert/update/remove and all of its index updates) now
  lands in **one** `fjall::WriteTransaction` via a scoped thread-local bridge, so data and index
  partitions commit — and recover — together.
- **`Durability`-aware regression suite**: crash/reopen consistency tests, exact-result range
  tests (single-field, compound-terminal, `between`), a `contains_key` normalization test, and a
  streaming-cursor reset/replay test; plus criterion benchmarks for range, count, and FTS.

### Changed / Performance

- **Crash-atomic close/reopen**: `close()` drains (persist + bounded wait on compactions) so a
  subsequent open observes a fully consistent state (no index entry without its data row).
- **Indexed range queries are now actually index-accelerated.** Multi-bound ranges
  (`x >= a AND x <= b`, `between`, and the range on a compound index's terminal field) drive a
  single **bounded index scan** (`ceiling(a)..floor(b)`) instead of a one-sided scan plus a
  post-fetch filter — narrow ranges no longer fetch nearly the whole collection (~40% faster vs
  full scan at 10k rows, growing with size).
- **`count()` / `size()` short-circuit**: index-covered queries answer from the index id-set
  length, and `find(all())` from the map size — without fetching any document (~26× faster at
  ~800 matches, ~121× at ~8000 matches).
- **Full-text indexing** (`nitrite-tantivy-fts`): batched commits + a single reused reader make
  bulk indexing ~**87×** faster (100-document insert: 14.85 s → 0.17 s).
- **Streaming cursors**: forward-only iteration retains O(1) documents instead of the entire
  result set (joins and raw/`vec` cursors still cache for cheap replay).
- **Fewer write-path clones**: `NitriteStore::with_atomic` relaxed from `Fn` to `FnOnce` so
  inserts/updates/removes move their input into the atomic scope instead of cloning it (single
  insert ~10% faster; no transient full-batch document clone).

### Fixed

- **Corrupted/foreign on-disk data no longer panics.** The Fjall read path deserializes through
  a fallible `decode_value`, surfacing damaged or format-incompatible bytes as a recoverable
  `NitriteError` instead of crashing the process; the write path likewise handles serialization
  errors without panicking.
- **`contains_key` numeric-key consistency**: `contains_key` now applies the same numeric-type
  normalization as `get`/`put`/`remove`, so a key stored as one numeric type is found regardless
  of the numeric type queried.
- **`NitriteError::cause()`** returns `&NitriteError` (removed the redundant `Box`).
- Workspace-wide clippy/lint cleanup (correctness lints in tests, deprecated `criterion::black_box`,
  redundant clones, and more).

## [0.2.0] - 2026-02-13

### Fixed

- **Spatial Indexing** (`nitrite-spatial`)
  - Standardized index type names to lowercase for consistency across the codebase

### Changed

- **Dependencies**
  - Bumped `lru` from 0.16.2 to 0.16.3
  - Bumped `oneshot` from 0.1.11 to 0.1.13

## [0.1.0] - 2024-12-17

### Added

- **Core Database** (`nitrite`)
  - Document-oriented embedded database for Rust
  - ACID transactions with optimistic locking
  - Flexible indexing: unique, non-unique, and compound indexes
  - Rich query API with fluent filter builders
  - Document and object repository patterns
  - Schema migration support

- **Derive Macros** (`nitrite-derive`)
  - `#[derive(NitriteEntity)]` for automatic entity mapping
  - `#[derive(Convertible)]` for document serialization
  - Attribute macros for ID fields and indexes

- **Storage Backend** (`nitrite-fjall-adapter`)
  - Fjall LSM-tree based persistent storage
  - Bincode serialization for efficient binary storage
  - High-performance disk-backed storage

- **Full-Text Search** (`nitrite-tantivy-fts`)
  - Tantivy-powered FTS integration
  - Phrase search, fuzzy matching, wildcards
  - Configurable tokenizers and analyzers

- **Spatial Indexing** (`nitrite-spatial`)
  - R-tree based spatial index implementation
  - Disk-persistent R-tree with crash recovery
  - Range and nearest-neighbor queries

- **Testing & Benchmarks**
  - Comprehensive integration test suite
  - Performance benchmarks comparing with SQLite and Redb

### Notes

- Initial release
- Minimum supported Rust version: 1.70+
