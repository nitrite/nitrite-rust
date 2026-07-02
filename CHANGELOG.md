# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.2] - 2026-07-02

### Fixed

- **`nitrite` — range filters mishandled null values.** `Value`'s mixed-type ordering falls
  back to string comparison, which made `Null` compare as the string `"null"`. As a result:
  - collection-scan `gt`/`gte` (and, depending on the store's key order, other range filters)
    matched documents whose field is null or missing;
  - indexed `lt`/`lte` returned an **empty** result as soon as the indexed field was null in
    any document, because the forward scan seeded from the null index key and terminated
    immediately.
  Range filters now explicitly treat null/missing values as never lesser or greater than the
  search term: the document-scan path rejects null field values, and every index range scan
  (all four comparison modes, both scan directions, plus the bounded `ceiling(lower) ..
  floor(upper)` scan) skips the null index key. Mirrors nitrite-java issue
  [#1262](https://github.com/nitrite/nitrite-java/issues/1262).
- **`nitrite-int-test`** — added regression tests for range scans over indexed, unique-indexed
  and non-indexed fields containing nulls (`index_null_key_scan_test.rs`).

## [0.4.1] - 2026-06-19

### Fixed

- **`nitrite-fjall-adapter` — space amplification / disk usage blowup.** During write bursts, Fjall's preallocated 32 MiB journal files could accumulate on disk and fail to be reclaimed because low-traffic partitions never rotated their memtables, pinning the keyspace-wide journal. Compaction (`FjallStore::compact`) now sequentially flushes the active memtable of every open partition, unpinning and reclaiming the sealed-journal backlog.
- **`nitrite-int-test`** — added a regression test for multi-partition journal reclamation (`disk_usage_repro_test.rs`).

## [0.4.0] - 2026-06-06

This release makes the index engine production-ready for high-volume, ordered workloads such as
an email client's initial sync (the motivating use case). It eliminates an O(n²) index build,
fixes long-standing key-ordering bugs that made integer/float range and descending-sorted index
queries return wrong results on the persistent store, and corrects `Value`'s numeric ordering.
It changes the on-disk index/key format, hence the `0.3.x` → `0.4.0` bump.

### ⚠️ Breaking Changes

- **On-disk format (indexes + keys).** Two storage-format changes mean databases created with
  `0.3.x` must be rebuilt (indexes are derived data; re-create the database or drop and
  re-create indexes):
  - **Non-unique simple and compound indexes** now use a flat composite-key layout — one
    `(field-values…, id)` row per entry — instead of a single `value → [ids]` array (or nested
    map) per key.
  - **`nitrite-fjall-adapter`** now serializes **keys** with an order-preserving codec instead
    of `bincode`, so the store's byte order matches `Value` ordering.

### Fixed

- **`nitrite` — O(n²) non-unique index build.** A non-unique index stored every matching
  `NitriteId` in one ever-growing array per indexed value, so each insert on a low-cardinality
  field (e.g. `account_id`, `folder_id`) did an O(k) read-modify-write + re-sort of that array —
  O(n²) total and O(n) per-insert serialized memory. Non-unique simple **and** compound indexes
  now store one composite `(value…, id)` row per entry: inserts and removals are O(1) point
  operations, equality is a prefix range scan, and per-insert memory is flat. Read behavior is
  unchanged (verified for parity against the old layout).
- **`nitrite-fjall-adapter` — wrong results for ordered index queries.** Keys were serialized
  with little-endian `bincode` and the LSM store orders by raw bytes, so integer/float range
  scans and sorted index walks were wrong across byte boundaries (e.g. `I32(255)` sorted after
  `I32(256)`; `seq BETWEEN 100 AND 199` could return a single row). Keys now use an
  order-preserving codec, so range, `between`, and sorted index scans are exact — including
  negative and large integers (nanosecond timestamps order exactly, beyond `f64` precision).
- **`nitrite` — `Value` numeric ordering.** `Value::cmp` compared integers via `as u128`, which
  wrapped negative integers to huge positives (sorting them *after* positives) and collapsed
  integers beyond `2^53` to "equal". It now compares signed (`i128`) with an exact tie-break, so
  negative and very large integers order correctly and consistently across the in-memory and
  persistent stores. Added `Value::as_signed_integer`.
- **`nitrite` — descending sort over an index-covered query.** An `order_by` whose field matched
  the queried index relied on the index scan emitting rows already in order, but the scanner
  deduplicates by `NitriteId` and discarded that order — so descending (and some ascending)
  sorts silently returned index/id order. `order_by` now always applies an explicit field sort
  to the filtered result, so ascending/descending sorts are correct regardless of index
  coverage.

### Performance

- Micro-benchmark (per-message insert into a collection with a unique `id` index
  plus non-unique `account_id`/`folder_id` indexes, release build, `Durability::Periodic`):

  | messages | 0.3.x | 0.4.0 |
  | --- | --- | --- |
  | 2,000 | 2.26 s (885 msg/s) | 0.11 s (~17,800 msg/s) |
  | 10,000 | 38.0 s (263 msg/s) | 0.24 s (~41,600 msg/s) |
  | 50,000 | ~16 min (extrapolated) | 1.12 s (~44,600 msg/s) |

  Throughput is now flat-to-rising with collection size (O(1) per insert) instead of collapsing
  (O(n²)).

### Added

- **`nitrite-int-test`** — regression tests for the composite-key layout (equality / `in` /
  `not in` / range / removal parity with the array layout, exact integer range plus ascending
  and descending sorted scans, compound prefix and full-tuple equality, and low-cardinality
  bulk-load scaling), and an ignored `index_write_bench` throughput benchmark.

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
