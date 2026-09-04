# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Ported from the corresponding fixes in `nitrite-java`, where each was found.

### Fixed

- **A unique index no longer rejects a document over a key that document already holds.**
  `add_nitrite_ids` treated *any* existing id under the key as a violation, so it counted the
  writer's own id against it. That bites a unique index over an array field with a repeated
  element — `["a", "b", "a"]` visits `a` twice, and the second visit collided with the entry the
  first had just written — and any path that reaches a key the document already owns, such as an
  index rebuild or a replayed write. Another document under the key is still a violation.
  (nitrite/nitrite-java#1295)

### Changed

- **An update that leaves an indexed value unchanged no longer rewrites the index.**
  `update_index_entry` treated an index as affected whenever the update document carried the
  indexed field, and then removed and rewrote the entry. An update that writes the whole document
  back — the common upsert shape — carries every indexed field with its old value, so every index
  was rebuilt on every update for nothing. The old and new values are now compared, and the index
  is left alone when they match. A dirty index is still rebuilt, since that has to happen on the
  first write regardless. (nitrite/nitrite-java#1297)

## [1.0.0] - 2026-09-01

**Why 1.0.0 and not 0.11.0.** The storage engine underneath the adapter changed major version, and
with it the on-disk format: a database written by any earlier Nitrite for Rust cannot be opened by
this one. A format break is the loudest thing a release can do to someone already running the
library, and it should not arrive behind a version number that reads like a routine step. The API
is also settled enough to say so — this release removes the last knob that existed only to serve
Fjall 2's garbage collector, and nothing else is waiting to be pulled out.

### Changed

- **`nitrite-fjall-adapter` now builds on Fjall 3** (`3.1.10`, up from `2.6.3`). Fjall 3 renamed
  its whole vocabulary — a *partition* is now a *keyspace* and a *keyspace* is now a *database* —
  reshaped per-partition settings into per-level policies, made iteration lazy behind a `Guard`,
  and replaced explicit blob garbage collection with reclamation folded into compaction.

  **This changes the on-disk format.** A database written by `0.10.x` or earlier cannot be opened
  by `1.0.0`; the break is inside the storage engine, below any layer Nitrite could migrate.
  Recreate the database from your source of truth, or export before upgrading and import after.

- **`FjallModuleBuilder::compaction_strategy(...)` takes `nitrite_fjall_adapter::Strategy`**
  (`Leveled`, the default, or `Fifo`) instead of `fjall::compaction::Strategy`. Fjall 3 replaced
  that enum with a boxed trait object and dropped size-tiered compaction, so `SizeTiered` is gone.
  Keeping the knob a plain `Copy` value also keeps it storable in the adapter's atomic config.

- **`staleness_threshold(...)` moved from per-call to per-keyspace.** It is still the ratio that
  drives blob reclamation, but Fjall 3 takes it when the keyspace is created rather than as an
  argument to a garbage-collection call. It still only has an effect under `kv_separated(true)`.

- **`compact()` and `NitriteMap::collect_garbage()` now run a major compaction.** Fjall 2 needed an
  explicit `gc_scan` → `gc_with_space_amp_target` → `gc_with_staleness_threshold` sequence; Fjall 3
  reclaims blob space during ordinary compaction, and a major compaction is what forces that pass.

- **`flush_workers(...)` and `compaction_workers(...)` feed one shared pool.** Fjall 3 has a single
  worker pool instead of two, so the adapter sizes it at the larger of the two counts. Both setters
  are still accepted, so existing configuration keeps working.

- **`max_journaling_size(...)` has a 64 MiB floor** (Fjall 2's was 24 MiB); Fjall panics below it.
  The default is 512 MiB, so this only matters if you set it explicitly and set it small.

- **`tantivy` 0.25.0 → 0.26.1.** `TopDocs` is no longer a `Collector` on its own — it is a top-K
  *spec* that becomes one when a ranking is chosen — so the FTS search now asks for
  `TopDocs::with_limit(n).order_by_score()`. That is the relevance ranking `TopDocs` used to imply,
  so results are unchanged. 0.26 also brings lazy scorers, `DocSet::cost()`-ordered intersections,
  and faster unions.

- **`cargo_toml` 0.22 → 1.0**, **`rand` 0.8.5 → 0.8.8**, **`thiserror` 2.0.17 → 2.0.20**,
  **`tempfile` 3.23 → 3.27** (the open Dependabot updates). `cargo_toml` 1.0 parses versions into
  `semver::VersionReq`, so `store_version()` renders the requirement back without the caret semver
  adds — it still reads `Fjall/3.1.10`.

### Performance

Measured on the criterion suite (`nitrite-bench`), 0.10.0 as the baseline, two independent runs
per side on the same machine. The in-memory store is untouched by this release and acts as a
control: its median reported change is −0.8%, which is where the noise floor sits. The
`Spatial/*/inmemory` family disagreed between runs by up to 196 percentage points and is excluded
as unmeasurable in this environment.

| fjall-backed workload | median change |
|---|---|
| CRUD writes (insert single + batch) | **−94%** |
| Indexed and non-indexed search | **−77% to −95%** |
| Spatial index build and queries | **−80% to −93%** |
| Full-text index build and search | **−14% to −59%** |
| Concurrent insert (2/4/8 threads) | **−42% to −54%** |
| **All 34 fjall benchmarks** | **−80%** |

The mechanism is visible in the journal. `disk_usage_repro_test` writes 10k messages across five
partitions and measures the on-disk footprint:

| | 0.10.0 (Fjall 2) | 1.0.0 (Fjall 3) |
|---|---|---|
| journal peak, during the bulk write | 1664 MiB | **128 MiB** |
| journal after `compact()` | 32 MiB | 64 MiB |
| data | 8.7 MiB | 12.1 MiB |
| total after `compact()` | 40.7 MiB | 76.1 MiB |

Fjall 2 wrote 1.6 GiB of journal for a workload holding 8 MiB of data — pinned, preallocated
32 MiB segments that no partition had flushed past. Fjall 3 writes 128 MiB for the same work, and
that missing I/O is most of the speedup above. The settled footprint goes the other way: 40.7 MiB
becomes 76.1 MiB, because Fjall 3 retains two journal segments where Fjall 2 kept one and its
tables are slightly larger. Both are far under the 250 MiB-per-10k-messages gate.

**Creating a keyspace costs roughly twice as much**, and that is the one real regression. Paired
measurement of the first write to a new collection, which is where the keyspace is created:

| | 0.10.0 | 1.0.0 |
|---|---|---|
| create a keyspace (SSD) | 40 ms | 97 ms |
| create a keyspace (external volume) | 91 ms | 148 ms |
| steady-state transaction (5 inserts + commit) | 94–124 µs | 86–113 µs |

This is a one-time cost per collection and per index, not a per-operation one — a database with
40 keyspaces pays a few seconds more on the open that first creates them, and nothing afterwards.
It is also what makes `Transaction/*` in the criterion suite read +151%: that benchmark creates a
fresh database *and* first touches its collection inside the timed region, so every iteration pays
one keyspace creation. Transaction throughput itself is unchanged to slightly better.

### Added

- **The adapter now runs the periodic-fsync timer itself.** Fjall 3 dropped `Config::fsync_ms` and
  the background thread behind it. Left alone, that would have silently downgraded the default
  `Durability::Periodic` from "durable within `fsync_frequency`" to "durable only on a clean
  close" — a power-loss window that grows without bound. `FjallStore` now owns the timer, joins it
  in `close()` before releasing the keyspace, and honours `fsync_frequency(0)` as "no timer" exactly
  as before. `Durability::OnCommit` is unaffected: every commit already fsyncs.

### Fixed

- **A map handle no longer keeps its database open after `close()`.** A Fjall 3 keyspace handle
  carries a clone of the database it belongs to, and Fjall 3 holds an exclusive lock file over the
  database directory. Since Nitrite's core holds a `NitriteMap` for every collection and index,
  those handles outlived `store.close()` and a reopen of the same path failed with `Locked`.
  `FjallMap` now releases its handle in `close()`/`dispose()`. Fjall 2 had no lock file, so the
  same over-long handle lifetime was invisible there.

### Note

- **`space_amp_factor(...)` was removed** from `FjallModuleBuilder` and `FjallConfig`. It existed
  only to feed Fjall 2's `gc_with_space_amp_target`, and Fjall 3 has no space-amplification target
  to aim at — there was nothing left for the setting to do.

- **`disk_usage_repro_test` measures the journal where Fjall 3 puts it.** Fjall 2 kept journals in a
  `journals/` directory; Fjall 3 writes `*.jnl` files in the database root, so the old helper read
  zero. The test's precondition also changed: it used to require the PERF-014 bloat to reproduce
  (pinned journals > 250 MiB), which Fjall 3 no longer produces — the same 10k-message workload now
  peaks at 128 MiB of journal and settles at 64 MiB after compaction, against a 250 MiB gate. It
  now asserts that journals accumulated at all and that compaction reclaims them, which is what the
  regression gate needs to stay meaningful.

## [0.10.0] - 2026-08-31

### Changed

- **Paging a collection no longer costs a seek per skipped row.** `skip` was applied by pulling
  and discarding, and each discarded row was a key lookup *and* a `get` — the fetch and decode of
  a document the caller had asked to pass over. The offset is now taken at the source, and reached
  by stepping fjall's key iterator, which reads keys without the values behind them.

  Measured over 20k rows of ~1KB, 400 to a page, against a full scan that is drained rather than
  counted:

  | | paged walk over the whole collection | vs one full scan |
  |---|---|---|
  | 0.9.0 | 1.089s | 18.6x |
  | 0.10.0 | **0.093s** | **1.6x** |

  The push-down is declined where anything between the source and the page drops or reorders rows
  — a scanned filter, a blocking sort, an `or` plan — and those keep the pipeline skip, which is
  correct and merely pays for what it passes over.

### Added

- `NitriteMapProvider::skip_keys_from_start`, defaulted to `None` so every backend outside this
  repository keeps working unchanged. A store that can iterate keys without reading values should
  implement it. The fjall implementation declines while a transaction is in scope, because the
  keys visible there include the scope's uncommitted inserts and exclude its tombstones, and a
  count over the committed partition alone would land on the wrong row.
- `EntryIteratorProvider::skip_entries`, same shape, for providers that iterate entries.

### Note

- The paging figures published in [#24](https://github.com/nitrite/nitrite-rust/pull/24) — "68.8x
  to 40.4x" — were measured against `find(all()).count()`, and an unfiltered `count()` is answered
  from the map size without reading a single document. The absolute times were right; the
  denominator was not. Read as 18.6x to 9.8x, with this release taking it to 1.6x.

## [0.9.0] - 2026-08-30

### Note

- `nitrite-bridge` is published from this release onward, at the same version as
  every other crate here rather than from 0.1.0 — `nitrite_bridge` on pub.dev and
  `nitrite-bridge` on Maven Central are both versioned with their core, and a
  reader should not have to work out which `nitrite` a given bridge pairs with.
  It could not be published before `dbinspect-bridge` reached crates.io, which is
  also why it was outside the Cargo workspace until now.

### Added

- **`nitrite-bridge` — inspect a running Nitrite database from a desktop client.**
  The Nitrite adapter for the `dbinspect` wire protocol: collections and repositories as browsable
  stores, schema inferred by sampling 50 documents and flagged as a sample, the filter DSL, and
  watch over `subscribe`. The engine-neutral core it plugs into is the `dbinspect-bridge` crate and
  has no database in its dependency tree at all.

  **Row editing is behind `allow_write`, and whole-store `snapshot` behind `allow_snapshot`** —
  both `false` unless the embedding application asks, and absent from the reported capabilities
  while they are. A row is addressed by `_id`, in the rendering a page carried or as the bare
  number; an update is partial; `changes: 0` means the row was not there. `_id` inside an update's
  `values` is refused, because Nitrite merges an update document and it would rewrite the identity
  of the row it just matched.

  **Everything is behind a non-default `bridge` feature, and that is the release guard.** A build
  that does not name the feature compiles no server, no protocol strings and no adapter into the
  binary. Depend on it from `[dev-dependencies]` and it cannot reach a release build.

  The protocol conformance suite passes against it unmodified over **both stores** — 124 checks,
  122 passed, 0 failed, 2 skipped, in memory and again over fjall — and the adapter's own tests run
  over both as well.

  The package is deliberately **excluded from the workspace `members`**: `dbinspect-bridge` is not
  on crates.io yet, so a workspace build of a clean checkout would fail on it. Build it with
  `cargo test --manifest-path nitrite-bridge/Cargo.toml --features bridge`.

  It requires nitrite 0.9.0. Originally 0.7.0, for `exists`: the adapter advertised every v1 operator except that
  one for as long as no Nitrite implementation had a filter that tested whether a field was
  present — not this one, not `nitrite-java`, not `nitrite-flutter`. 0.6.0 added it here and the
  other two added theirs, so `filter_ops` now carries the whole v1 set.

  One thing found while writing it, which is not a bug:

  - **A repository cannot be opened by name**, so the developer hands in the ones they want
    inspected. `list_repositories()` answers with entity names, but there is no runtime registry to
    turn one back into a type, and `CollectionFactory::create_collection` refuses a name the
    repository registry owns. Keyed repositories are handed in the same way and browse correctly.

- **The bridge can open a transaction** (`docs/PROTOCOL.md` §3.1). A transaction is a second
  adapter over the same database rather than a mode on the first, so one connection's uncommitted
  documents can never reach another connection's reads. Nitrite's transaction lives above the
  storage engine, so this works identically in memory and over fjall — nothing in the adapter
  re-implements either half.

  `capabilities.transactions` follows `allow_write`: that flag is the permission, this reports what
  the engine can undo. The transactional twin reports it `false`, because Nitrite does not nest one.
  `list_stores` counts through the transaction, since a total that left out the rows the person just
  staged is not read-your-own-writes.

- **`Nitrite::create_session`** — a session whose lifetime the caller owns. `with_session` closes
  the session for you and so cannot serve a caller that begins a transaction on one request and
  commits it on another, which is what a server does. The same call `createSession` has always been
  in the Java and Dart APIs. The caller must `Session::close` it; everything still open is rolled
  back when they do.

- **`NitriteTransaction::view_of`** — a transactional view over a collection the caller already
  holds. `collection(name)` opens the primary by name and `Nitrite::collection` refuses a name a
  repository owns, so a caller holding an `ObjectRepository`'s document collection — and no type to
  reach `repository()` with — had no way in. Keyed by the collection's own name, so asking twice
  returns what `collection(name)` would.

### Fixed

- **A `NitriteId` goes over the wire as the number underneath it**, not `Display`'s `[1755…]NO₂`.
  That is a debug rendering, and sending it made the same sample database read one way through the
  Rust bridge and another through the Java and Dart ones, for the same document — the drift a frozen
  protocol exists to prevent. Ids too wide for an `i64` degrade to their decimal string, exactly as
  a `u64` cell does. Parsing still accepts either rendering, so a client that echoes back what it
  was given can still address the row.

## [0.8.0] - 2026-08-22

### Changed

- **A sorted, limited `find` no longer fetches the whole collection when the sort field is indexed.**
  `find_with_options(all(), order_by("created_at", Descending).limit(20))` asked for 20 rows and
  cost what draining every stored document costs. `SortedStream` collects and fully deserializes
  the entire result set in its constructor, before `Skip`/`Take` get to drop 99% of it - and the
  cost is the decode, not the comparison, so it scales with document *size* as well as count. An
  index on the sort field bought nothing (~3%): the index was only ever used to *filter*, never to
  order, and page 50 cost exactly what page 1 cost because the work finished before `skip` applied.

  When the query has no filter, one sort field, a limit, and a simple unique or non-unique index on
  exactly that field, the sort keys are now read from that index - which already stores them - and
  only the documents actually returned are fetched. Measured over 1000 rows each carrying a
  150-element array, a `limit(20)` page went from 384 ms to 15 ms, and stopped growing with the
  collection.

  The index is used only when it holds exactly one entry per stored document. A multi-valued field
  is indexed once per element and a non-comparable value is not indexed at all, so both are detected
  (by a duplicate-id check and an entry-count check) and fall back to the blocking sort. Ordering,
  including where nulls sort and how ties break, is identical either way: the same comparator runs
  over keys taken from the index instead of from the documents.

  New on `FindPlan`: `sort_index_descriptor()`, the planner's hint that a sort may be answerable
  from an index. Additive; no existing API changed.

- **`size()` on a fjall-backed map no longer decodes every stored value.** Counting is a full scan
  either way, but it went through `PartitionHandle::len`, which walks key *and* value - so counting
  a collection of fat documents read and decoded every document in it. It now walks keys only.
  This is what made the index-ordered sort above worth having on the fjall adapter: with the old
  count, checking that the index covers the collection cost more than the blocking sort it replaced.

- `SortedStream::new` no longer holds the result set twice. It cloned the collected buffer into a
  second `Vec` purely to strip errors, keeping both alive until the constructor returned, so peak
  footprint was 2n documents - the cause of the instability above ~2000 fat rows. It now truncates
  the buffer it already owns.

## [0.7.0] - 2026-08-07

### Removed

- **BREAKING**: Removed the `distinct` find option - the `distinct()` free function, `FindOptions::distinct()`, and `FindPlan::distinct()`. It had no effect on the result set. A find never returns the same document twice, and the only place the flag was ever read was the `or` sub-plan union, which now deduplicates unconditionally (see below) because an `or` is a set union by definition. The flag's sole remaining effect was to paper over the duplicate defect fixed in this release. Callers passing `distinct()` can drop it without any change in results.

### Fixed

- `find` no longer returns a document more than once from an `or` filter when the document satisfies more than one branch. Two separate defects both produced duplicates:
  - When any branch of the `or` was not index-backed, the planner intended to discard the per-branch sub-plans and run the whole `or` as a single full scan, but only dropped a borrowed handle to them - the sub-plans stayed on the `FindPlan` and were executed *in addition to* the full scan. A document matching two branches came out twice, so `field("x").eq(1).or(field("y").eq(2))` over a two-document collection reported three rows. Sub-plans are now attached only once every branch is known to be index-backed (or resolvable by `_id`); otherwise the `or` runs purely as a full scan.
  - When every branch *was* index-backed, the union of the per-branch scans was only deduplicated if the caller passed the `distinct()` find option, which defaulted to off. An `or` is a set union by definition, so the union is now always deduplicated by `NitriteId`.

## [0.6.0] - 2026-08-07

### Added

- **An `exists` filter.** `field("name").exists()` matches the documents which have the field,
  irrespective of its value; `field("name").exists().not()` matches those which do not.

  A field explicitly set to `Value::Null` is present and matches. This is the case no existing
  filter could express: `eq(Value::Null)` and `ne(Value::Null)` cannot tell a missing field apart
  from one holding null, so "has this document been given a value for this field at all" was not
  answerable.

  The filter reports `has_field() == false` and so is never elected for an index scan. A missing
  field and a field holding null are stored under the same null key in an index, so an index scan
  could not tell them apart and would disagree with a full scan. `get_field_name()` still returns
  the name.

  Embedded fields are addressed by their dotted path (`field("address.city").exists()`), the same
  way `Document::contains_field` resolves them.

## [0.5.0] - 2026-08-02

### Changed

- **Log levels reclassified so an embedding application is no longer flooded by default.**
  Nitrite logged 253 statements at `error!`, and 204 of them sat immediately before the matching
  `Err(...)` return — every rejected filter, every failed type conversion, every "collection name
  cannot be empty" validation was reported twice: once through the returned `NitriteError`, which
  the caller handles, and once screamed into the host application's log, which the caller cannot
  suppress without silencing Nitrite entirely. On a normal workload the second copy is pure noise,
  and it arrives at the one level every application leaves enabled.

  Levels now follow a single rule — *log at a level proportional to what the caller cannot already
  see*:

  - `debug` — the error is returned to the caller as `Err`. The `NitriteError` carries the same
    message and kind, so the caller decides the severity. 245 statements moved here.
  - `warn` — an anomaly Nitrite absorbed and the caller never learns about: a skipped
    corrupt catalog/index entry, a corrupt index (whose key detail exists only in the log line, as
    the propagated error is a shared static), best-effort background compaction/GC/flush failures,
    an invalid regex that silently degrades a filter to "matches nothing", a scheduled task
    dropped because its duration could not be converted.
  - `error` — a swallowed failure that may have left state inconsistent: the four rollback paths
    in batch insert/update, and close/drop failures for the index manager, plugin manager, Fjall
    map/store, and keyspace drain. Ten statements remain at this level, down from 253.

  A default `info`-level application therefore sees only failures it can act on. Full detail is
  still one target filter away and unchanged in content:
  `RUST_LOG=warn,nitrite=debug,nitrite_fjall_adapter=debug`.

  Two other level changes fall out of the same rule: `Snowflake` no longer announces its node id
  at `info!` on every generator construction (now `debug!`), and `nitrite_vector`'s "index was
  stale or damaged; rebuilt from collection" moved from `info!` to `warn!`, matching the
  already-`warn!` sibling messages in the HNSW and DiskANN loaders that report the same class of
  damage.

  No message text, error type, `ErrorKind`, or control flow changed — this is purely which
  statements reach a default logger.

## [0.4.4] - 2026-08-01

### Fixed

- **`nitrite_vector` — a selective metadata filter could silently return zero hits.**
  `RagStore::search(...).filter(...)` fetched a fixed `k * oversample` (default 4) nearest
  neighbours from the ANN index and applied the metadata filter *afterwards*. That works when
  the filter keeps a large fraction of the index, but fails completely when it is selective —
  the common RAG shape, "search only this document's chunks": every one of the `k * oversample`
  candidates can fail the filter, and the search returns nothing while matching documents sit
  further out. Measured on a real index: 41 records, one chunk each, `k = 5`, filtered to a
  single record, query semantically closer to the other 40 — **0 hits**. The failure scales
  with index size and is invisible in small-index testing. `oversample` was never a fix, only a
  knob: the multiple needed is `index_size / matching_docs`, which the caller cannot know in
  advance.

  The traversal now widens and re-queries until `k` matching hits are found or the index is
  exhausted, turning a silently wrong answer into a correct one; `oversample` becomes the
  *starting* window rather than a hard ceiling. A filter matching very few documents therefore
  costs a full index scan — for the single-document case an exact scan over that document's
  chunks (`collection().find(meta_filter)`) remains both faster and more accurate than any ANN
  path. Widening deliberately does **not** apply to `min_score`: a score cutoff is monotone in
  the ANN ranking, so the hits it drops are the tail and fetching further out cannot recover
  any.

## [0.4.3] - 2026-07-20

### Fixed

- **Array-field membership is now index-independent.** `field.eq(x)` and `field.in([..])` on an
  array field are matched by element containment on a full/collection scan, mirroring the index
  path (arrays are indexed element-wise). Previously the full-scan path did whole-value equality,
  so results depended on whether an index existed or was chosen by the planner: when both an
  array field and a range field were indexed, the planner would claim the range index for a
  `between` and relegate the array-eq to a full scan, which then silently matched nothing.

## [0.4.2] - 2026-07-02

### Added

- **`nitrite_vector`** — new approximate-nearest-neighbour (ANN) vector index and RAG store
  extension crate, with two backends (in-memory HNSW and disk-resident DiskANN) selectable per
  database or per index, cosine/Euclidean/dot metrics, and `F32`/`F16`/`I8` stored-vector
  precision.
- **`nitrite_vector` — automatic index rebuild.** Both backends detect stale/corrupt index
  storage on open (torn writes, missing/corrupt sidecar, crash before checkpoint, format
  change) and transparently re-index from the collection's documents.
- **`nitrite_vector` — per-index configuration.** `VectorModule::builder(...).index_config(
  collection, field, config)` lets collections with different embedding dimensions, metrics,
  precisions, or backends coexist in one database.
- **`nitrite_vector` — DiskANN crash-consistency machinery**: data-file dirty bit + generation
  stamping, checksummed sidecar replaced atomically (tmp + rename), structural validation of
  everything read from disk, and a periodic sidecar checkpoint (every ~8k mutations) in
  addition to flush-on-close.
- **`nitrite_vector`** — HNSW deletes now reconnect the orphaned neighborhood (diversity-pruned),
  so sustained insert/delete churn no longer fragments the graph.

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
- **`nitrite_vector` — torn HNSW persists could panic searches.** Each mutation now persists as
  a single atomic `put_all` batch (records + header), traversal skips dangling neighbor
  references instead of panicking, and loading sanitizes the graph (bad records dropped,
  dangling links pruned, entry point re-elected).
- **`nitrite_vector` — DiskANN writer races.** Inserts, removes, delete-consolidation, and the
  PQ encode step now serialize on a per-index write gate (background passes take it per chunk),
  eliminating lost adjacency updates and stale-edge reattachment; only slots pending at the
  start of a consolidation sweep are reclaimed.
- **`nitrite_vector`** — PQ training runs in the background (single-flight) instead of stalling
  the insert that crosses the threshold; queries fall back to exact distances for nodes not yet
  encoded, so they can never become invisible mid-training.
- **`nitrite_vector`** — `DiskAnnIndex::flush` no longer permanently disables background
  consolidation; it is now a safe, repeatable checkpoint.
- **`nitrite_vector`** — the HNSW backend now honors the configured stored-vector `Precision`
  (F32/F16/I8) as documented; adjacency-only changes no longer rewrite full vectors, cutting
  per-insert write amplification by roughly an order of magnitude; persistence happens outside
  the graph lock so searches are never blocked on storage I/O.
- **`nitrite_vector`** — a racing double-open of the same index could create two live instances
  over the same storage (double-checked registry lock); DiskANN file names are sanitized +
  digest-suffixed so hostile collection/field names cannot escape the database directory;
  `min_score` now over-fetches (4×k) before applying the cutoff so it no longer starves `k`.

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
