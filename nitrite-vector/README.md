# Nitrite Vector

Approximate-nearest-neighbour (ANN) vector index and RAG store for the
[Nitrite](https://github.com/nitrite/nitrite-rust) embedded database.

It plugs into Nitrite as a standard index extension (like `nitrite-spatial` /
`nitrite-tantivy-fts`): load a module, create a vector index on a field, and
query it through the normal collection API — or use the higher-level `RagStore`.

- **Two backends**, chosen per database or per index:
  - **HNSW** (default) — in-memory Malkov–Yashunin graph, persisted to Nitrite's
    own KV store (`NitriteMap`): every mutation is written through as **one
    atomic batch**, loading tolerates torn state, and a damaged index is
    **rebuilt automatically** from the collection. Fastest when the index fits
    in RAM.
  - **DiskANN** — disk-resident Vamana graph + full vectors in a
    **memory-mapped flat file**, with product-quantized (PQ) codes resident in
    RAM for traversal and **exact re-ranking** from the on-disk vectors.
    Resident memory is bounded by the OS page cache (hot pages stay, cold pages
    are reclaimed under pressure — the index cannot OOM from its vector data), so
    it serves **indexes larger than RAM**, e.g. on mobile. Requires a
    persistent `db_path` (refuses in-memory databases).
- **Metrics**: cosine, Euclidean (L2), and dot product.
- **Configurable precision**: store vectors as `F32`, `F16`, or `I8` (scalar
  quantized) to trade size for exactness.
- **Deletes + background consolidation** (DiskANN): deletes are correct
  immediately; a FreshDiskANN-style pass later repairs the graph and reclaims
  space, off the caller's thread.
- **Tuned for throughput**: portable SIMD, fast integer hashing, allocation-free
  hot path, and lock-free concurrent queries (see [Performance](#performance)).
- **Bring-your-own embeddings** — this crate stores and searches vectors; it does
  not generate them (pass in vectors, as with sqlite-vec / usearch / lance).

## Install

```toml
[dependencies]
nitrite = "0.4"
nitrite_vector = "0.4"
# a storage backend for the DiskANN path / durability:
nitrite_fjall_adapter = "0.4"
```

## Quick start — collection API

```rust,ignore
use nitrite::nitrite::Nitrite;
use nitrite::common::PersistentCollection;
use nitrite::doc;
use nitrite_vector::{VectorModule, vector_index_options, vector_field, Metric};

// Load the vector module (in-memory HNSW here) at build time.
let db = Nitrite::builder()
    .load_module(VectorModule::builder(3, Metric::Cosine).build())
    .open_or_create(None, None)?;

let docs = db.collection("docs")?;
docs.create_index(vec!["embedding"], &vector_index_options())?;

// Each document's `embedding` field is a numeric array.
docs.insert(doc! { "title": "fox",  "embedding": [1.0f32, 0.0, 0.0] })?;
docs.insert(doc! { "title": "wolf", "embedding": [0.9f32, 0.1, 0.0] })?;

// kNN query via the fluent filter — returns a normal document cursor.
let filter = vector_field("embedding")
    .nearest(vec![1.0, 0.0, 0.0], 5)   // query vector, k
    .ef(64)                            // optional: search width (recall/latency)
    .min_score(0.5)                    // optional: similarity cutoff
    .build();
let cursor = docs.find(filter)?;
```

## RAG store

`RagStore` is a thin layer over a collection: it stores `text` + `embedding` +
arbitrary metadata, does kNN, and combines the result with normal Nitrite
metadata filters, returning documents with scores.

```rust,ignore
use nitrite_vector::{RagStore, Metric};
use nitrite::doc;
use nitrite::filter::field;

// db must be built with a VectorModule (any backend) using the same metric.
let store = RagStore::create(&db, "kb", Metric::Cosine)?;

let id = store.add("the quick brown fox", embedding, doc! { "source": "wiki" })?;

let hits = store
    .search(query_vector, 5)                 // top-5
    .filter(field("source").eq("wiki"))      // combine with metadata (post-filter)
    .min_score(0.75)                         // drop dissimilar hits
    .ef(128)                                 // search width
    .run()?;                                 // Vec<SearchHit { id, text, score, document }>

store.delete(&id)?;
```

`RagStore` also exposes `add_many`, `get`, `len`, `is_empty`, and `collection()`
for advanced use.

## Choosing & configuring a backend

Everything is on `VectorModule::builder(dim, metric)`:

```rust,ignore
use nitrite_vector::{VectorModule, IndexBackend, Precision, Metric};

// Disk-resident DiskANN, sized to the device:
let module = VectorModule::builder(384, Metric::Cosine)
    .backend(IndexBackend::DiskAnn)
    .precision(Precision::F16)          // stored-vector precision
    .degree(64)                         // Vamana out-degree R
    .build_beam(100)                    // construction search width L
    .search_beam(100)                   // default query search width L
    .alpha(1.2)                         // RobustPrune diversity slack
    .pq_subvectors(16)                  // PQ bytes/code (0 disables PQ)
    .pq_train_threshold(10_000)         // train PQ once this many vectors exist
    .consolidate_threshold(1000)        // background delete-consolidation trigger
    .cache_bytes(128 * 1024 * 1024)     // advisory (OS page cache bounds RAM)
    .build();

// In-memory HNSW (default backend):
let module = VectorModule::builder(384, Metric::Cosine)
    .m(16)                              // graph connectivity
    .ef_construction(200)
    .ef_search(64)                      // default query search width
    .build();

// Different dimensions / metrics / backends per index in one database:
let module = VectorModule::builder(384, Metric::Cosine)   // default for all indexes
    .index_config("images", "clip",
        VectorIndexConfig::new(512, Metric::Dot))          // override for images.clip
    .build();
```

### Configuration reference

| Knob | Backend | Meaning | Default |
|------|---------|---------|---------|
| `backend` | both | `Hnsw` or `DiskAnn` | `Hnsw` |
| `precision` | both | `F32` / `F16` / `I8` stored-vector encoding | `F32` |
| `m` | HNSW | graph connectivity `M` | 16 |
| `ef_construction` | HNSW | build search width | 200 |
| `ef_search` | HNSW | default query search width | 64 |
| `degree` | DiskANN | Vamana out-degree `R` | 64 |
| `build_beam` | DiskANN | construction search width `L` | 100 |
| `search_beam` | DiskANN | default query search width `L` | 100 |
| `alpha` | DiskANN | RobustPrune slack (≥ 1.0) | 1.2 |
| `pq_subvectors` | DiskANN | PQ bytes per code; `0` = exact traversal | 16 |
| `pq_train_threshold` | DiskANN | train PQ once N vectors are indexed (runs in the background; queries fall back to exact distances for not-yet-encoded nodes) | 10 000 |
| `consolidate_threshold` | DiskANN | run background consolidation past N deletes; `0` = manual | 1000 |
| `cache_bytes` | DiskANN | advisory RAM budget (see below) | 64 MiB |

Per-query, `.ef(n)` on the fluent filter overrides `ef_search` (HNSW) or
`search_beam` (DiskANN).

Resolved parameters are persisted in each index's header, so a reopened index
keeps the settings it was built with.

## Precision

`Precision` selects the on-disk / stored-vector codec, trading size for exactness:

| Precision | Bytes/dim | Notes |
|-----------|-----------|-------|
| `F32` | 4 | exact (default) |
| `F16` | 2 | IEEE half; ~exact for normalized embeddings |
| `I8`  | 1 | per-vector scalar quantization; ~4× smaller, approximate |

For DiskANN, PQ codes (used only to *guide* traversal) are separate; final
ranking is always an exact re-rank against the stored vectors at the chosen
precision.

## Deletes & consolidation

- **HNSW**: a delete unlinks the node exactly and **reconnects the orphaned
  neighborhood** through the deleted node's other neighbors (diversity-pruned),
  so sustained insert/delete churn does not fragment the graph.
- **DiskANN**: a delete is **correct immediately**: the freed slot is held aside
  (never reused until cleaned), and stale in-edges resolve to a dead sentinel
  that queries skip.
- Once `consolidate_threshold` deletes accumulate, a **background thread**
  (single-flight, gated per chunk so writers and queries interleave) runs a
  FreshDiskANN-style pass: it drops references to deleted nodes, reconnects
  through their surviving neighbors, re-prunes to `degree`, and reclaims the
  slots. Writers and the consolidation pass serialize on a per-index write
  gate, so concurrent mutations cannot lose updates.
- On close (`flush`), a final synchronous consolidation runs so the persisted
  state is clean. You can also call `DiskAnnIndex::consolidate()` manually.

## Persistence & durability

| Backend | Storage | Durability |
|---------|---------|-----------|
| HNSW | Nitrite `NitriteMap` (fjall) | **per-write**: each mutation persists as one atomic batch |
| DiskANN | memory-mapped flat file + checksummed sidecar next to the DB | **checkpointed**: on close, and automatically every ~8k mutations |

Both backends treat the index as *derived data* and **fail safe, never silently
wrong**:

- **HNSW** batches every touched record + header into a single atomic
  `put_all`; loading sanitizes the graph (dangling links pruned, bad records
  dropped), and an unreadable header wipes the index and **rebuilds it
  automatically from the collection** on open.
- **DiskANN** marks its data file *dirty* on the first mutation after a
  checkpoint and clears the flag only after the checksummed sidecar has been
  atomically replaced (tmp + rename) under a matching generation. On open, a
  dirty flag, checksum mismatch, or generation skew is detected, the files are
  wiped, and the index is **rebuilt automatically from the collection**. A
  crash therefore costs a bounded re-index, silent corruption never.

Note that Nitrite writes documents and index entries as separate store
operations; a crash exactly between them can leave one document unindexed.
`collection.rebuild_index(...)` heals this; the automatic rebuild above covers
all index-side damage.

`cache_bytes` is **advisory** for DiskANN: because the store is memory-mapped,
the OS page cache bounds resident memory and reclaims cold pages under pressure
(the index can't OOM from its vector data). The knob is reserved for future
`madvise` hinting.

## Security & privacy

- Vectors are stored **in plaintext** by both backends. Embeddings are
  generally invertible back to their source content — treat them with the same
  sensitivity as the documents themselves.
- DiskANN files live next to the database (named after a **sanitized + hashed**
  form of the collection/field, so hostile names cannot escape the directory)
  and are **not** covered by any encryption or at-rest features a storage
  adapter might provide.
- The DiskANN backend refuses in-memory databases rather than writing
  embeddings into a shared temp directory.
- The sidecar is checksummed and structurally validated on load; corrupt or
  tampered files are wiped and rebuilt, not trusted.

## Performance

Tuning that closes most of the gap to hand-optimized ANN libraries:

- **Portable SIMD** distance kernels via `wide` (f32x8) — pure Rust, **no C
  dependency**, cross-compiles cleanly to ARM NEON for mobile (unlike simsimd).
- **`rustc-hash`** (FxHash) on the query hot path — SipHash is ~5–10× slower for
  the internal integer lookups done thousands of times per query.
- **Allocation-free** vector decode into reused buffers on the hot loop.
- **Lock-free per-query read view** (DiskANN): one read lock per query instead of
  per node, so concurrent queries scale instead of contending on the lock's
  cache line.

Indicative release-build numbers on a 10-core laptop, 384-dim, 2k index:

| Benchmark | Latency |
|-----------|---------|
| HNSW query (k=10) | ~0.10 ms |
| DiskANN query (k=10, PQ + re-rank) | ~0.19 ms |
| distance kernel (128-dim, SIMD) | ~12 ns |
| DiskANN 128 queries, 8 threads | ~4.6× faster than single-threaded |

Numbers vary with dataset, dimension, recall target, and hardware; treat them as
same-class-as-embedded-ANN-libraries, not a head-to-head claim. Run them yourself:

```bash
cargo bench -p nitrite_vector
```

Benchmarks (criterion) cover the distance kernels, build/query throughput for
both backends, and multi-threaded query scaling
([`benches/vector_bench.rs`](benches/vector_bench.rs)).

## Testing

```bash
cargo test -p nitrite_vector          # unit + integration
cargo clippy -p nitrite_vector --all-targets
```

Coverage includes distance/precision/PQ math, HNSW recall vs brute force,
DiskANN recall + disk-residency + persistence + delete + consolidation, and
parity of both backends through the collection / RAG APIs.

## Known limitations

- DiskANN durability is checkpoint-based, not per-write (see above); a crash
  costs an automatic rebuild of that index on next open.
- PQ codebooks are trained once (in the background, off the insert path) and
  not retrained; under heavy distribution drift, traversal quality can degrade
  (final ranking stays exact via re-rank). PQ's ADC guide assumes squared-L2
  ordering, which is monotone for Cosine/Euclidean; for `Metric::Dot` it is a
  heuristic guide only.
- Querying a vector filter without a vector index is an error (a kNN filter
  cannot be evaluated as a per-document predicate).
- With `min_score`, the index over-fetches 4× `k` before applying the cutoff;
  extremely selective cutoffs can still return fewer than `k` hits.
- SIMD is portable (f32x8), not AVX-512-tuned; on AVX-512 servers a hand-tuned
  kernel would still be faster.
- DiskANN traversal maps external ids ↔ dense slots and allocates a small
  neighbor list per node; a fully dense-slot, zero-copy traversal is the next
  step for low-dimensional / very-high-QPS workloads.

## License

Apache-2.0
