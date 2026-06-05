# Nitrite-Rust Hardening Pass — Findings & Changes

**Date:** 2026-06-04
**Scope:** whole workspace (`nitrite`, `nitrite-fjall-adapter`, `nitrite-spatial`,
`nitrite-tantivy-fts`, `nitrite-derive`, `nitrite-bench`, `nitrite-int-test`)
**Decisions taken with the maintainer up front:**
- **Async:** keep the core **synchronous** (no async layer). Rationale below.
- **Delivery:** autonomous end-to-end, report at the end.
- **Durability default:** configurable knob, default **Periodic (fast)**.
- **Crate scope:** all crates.

---

## 1. Should the Nitrite API be async? — No (analysis)

For an embedded database sitting on a **synchronous LSM engine** (fjall), converting the
public API to `async` would **not** improve performance and would likely make the common
path slower:

- The cost centers — fjall memtable inserts, SSTable reads, compaction, and **fsync** — are
  CPU- and blocking-syscall-bound. `async`/`await` does not accelerate that; it overlaps many
  *waiting* I/O operations, which is a network-server pattern, not an embedded-storage one.
- fjall exposes **no async API** and runs its own background threads. An async core would have
  to wrap every call in `spawn_blocking`, adding task-scheduling + thread-handoff overhead per
  operation — a net loss for the dominant single-op case.
- It forces a `tokio` runtime on every consumer (CLI, mobile, sync apps), hurting
  embeddability — the opposite of what an embedded DB wants.
- Precedent: redb, sled, heed/LMDB, rocksdb, and fjall itself are all synchronous.

**Recommendation honored:** core stays synchronous. If async ergonomics are ever needed, add
an *optional, feature-gated async facade* over `spawn_blocking` — never an async core.

---

## 2. Baseline was not green — now it is

`cargo clippy --workspace --all-targets` originally failed with **deny-level correctness
errors** plus ~100+ warnings:

- **Errors (block the build):** `approx_constant` (`3.14`≈π test fixtures),
  `absurd_extreme_comparisons` (`assert!(idx >= 0)` on unsigned, `schema_version <= u32::MAX`).
- **Warnings:** `unwrap`-after-`is_some`, `&Box<T>`, `module_inception`, `type_complexity`,
  deprecated `criterion::black_box`, `field_reassign_with_default`, `assertions_on_constants`,
  `format!`-in-`format!`, identical `if` blocks, `drop` of non-`Drop`, etc.

All fixed. Production-crate lints were fixed *properly* (real code changes); purely cosmetic
test/bench/example lints were relaxed centrally via a `[lints]` table in the non-published
`nitrite-int-test` crate and scoped `#[allow]` on test modules — production code keeps the
strict lint set. **Result: `cargo clippy --workspace --all-targets` is clean (0 warnings).**

---

## 3. Durability — configurable knob, default Periodic

The atomic cross-partition commit (root cause **A** of the prior durability analysis) had
already landed (one `fjall::WriteTransaction` per logical write via a thread-local bridge), and
`close()` already drains (`persist(SyncAll)` + bounded wait on `active_compactions()`), closing
the close/reopen-under-load race. Remaining work for this pass:

- The `durability_on_commit` knob existed internally but had **no public builder method** and
  **defaulted to OnCommit** (fsync per commit). Per the agreed default:
  - Added a public **`Durability { OnCommit, Periodic }`** enum and a
    `FjallModuleBuilder::durability(..)` builder method.
  - **Flipped the default to `Periodic`** and set a **bounded default `fsync_frequency` of
    1000 ms**, so "Periodic" is genuinely periodic (a background fsync bounds the power-loss
    window) rather than "durable only on drop" (which would have left root cause **B** open).
  - Documented the trade-off precisely: every commit is buffered to the OS (survives a
    **process crash**); a clean close fsyncs; `OnCommit` adds a per-commit fsync for power-loss
    safety. (fjall semantics verified against the 2.11.2 source: `manual_journal_persist=false`
    auto-applies `PersistMode::Buffer` per commit; `fsync_ms` drives the background fsync.)

---

## 4. Correctness / memory-safety fixes (storage adapter)

The biggest robustness gap for a production DB: **data-triggered process crashes.**

- **Corrupted / format-incompatible bytes panicked the read path.** `FjallMap`'s reads
  (`get`, `first/last/ceiling/floor/higher/lower_key`, `put_if_absent` return) used the
  **panicking** `Value::from(FjallValue)` conversion, so damaged or foreign on-disk bytes
  **crashed the process** instead of returning an error. Routed all of these through a new
  fallible `decode_value` (`try_into_value` → `NitriteError`).
- **Write path panicked on serialization failure.** `put`/`put_all`/`put_if_absent` used the
  panicking `FjallValue::new`; switched to fallible `try_from_value`.
- **`contains_key` numeric-key consistency bug.** `contains_key` encoded the key **without**
  the numeric normalization that `get`/`put`/`remove` apply, so `contains_key(U64(5))` could
  return `false` for a key that `get(U64(5))` finds (and that a unique-index check stored).
  Fixed to normalize identically. Added a regression test
  (`test_contains_key_normalizes_numeric_types_like_get`).

**Memory safety:** only two `unsafe` blocks exist in production code — the `tx_scope`
thread-local transaction bridge (scoped, per-thread, non-reentrant, no escape — invariants
hold) and the `Vec<u8>→Value::Bytes` `TypeId` specialization in `value.rs` (correct
`forget`/`from_raw_parts` ordering). Both reviewed and sound.

---

## 5. Performance

The real throughput levers are architectural and are in place:

- **One atomic transaction per logical write** (data + all index partitions) instead of N
  independent partition inserts — fewer journal entries and lock acquisitions per commit.
- **Periodic durability by default** removes the per-commit `fsync` from the hot write path
  (the dominant write cost), while a background timer keeps the power-loss window bounded.

### 5.1 Benchmark-driven optimization (email-app workload)

Primary consumer: a production cross-platform **email app** on nitrite + fjall + tantivy.
Baselined the fjall variants with `nitrite-bench` (criterion, reduced sample params on an
external disk — treat absolutes as relative; only deltas matter), 1000-doc workloads:

| Operation (fjall, 1000 docs) | Baseline | After opt |
| --- | --- | --- |
| CRUD insert single | ~27.5 ms | **~24.8 ms (−10%)** |
| CRUD insert batch | ~20.7 ms | ~20.6 ms (flat time) |
| CRUD read / update / delete | 14 / 21.8 / 19.4 ms | unchanged |
| Index create / unique | 38 / 36 ms | — |
| Index indexed-search / full-scan | 13.0 / 13.7 ms | — |
| Transaction commit (per extra tx) | ~0.14 ms (cold-start ~160 ms dominates) | — |

**Optimization applied — eliminate redundant write-path clones (CPU + mobile memory):**
`NitriteStore::with_atomic` required `F: Fn` (re-runnable), forcing every write to **clone its
input** — including a full `Vec<Document>` clone on every batch insert — even though
`run_atomic` invokes the operation **exactly once** (no backend retries; fjall's single-writer
transaction has no conflicts). Relaxed the bound to **`FnOnce`** (bridged through an
`Option::take` over the `FnMut` trait object), so `insert`/`insert_batch`/`update`/`remove`
now **move** their data into the scope. Also moved (instead of cloned) the per-document
rollback-tracking copy in `insert_batch_optimized`. Net effect: single insert ~10% faster and
the transient per-batch full-document-vector clone is gone (peak-memory win on mobile). Batch
*time* is flat because it is dominated by serialization + LSM writes, not the clone.

**Deliberately not done (would need deeper, benchmark-gated work):**
- Reordering index-vs-store phases in `insert_batch_optimized` to drop the remaining
  `entries`/`process_before_write` clones — changes failure/rollback semantics; uncertain time
  gain. Flagged for a focused follow-up.
- Cold-start (~160 ms to lazily create a collection's partitions/catalog on first write) is
  disk-bound directory/fsync cost, paid once; relevant to first-run latency, not steady state.

### 5.2 Range queries were not actually index-accelerated — fixed (high impact for email)

**Symptom:** an indexed range query was barely faster than a full scan
(10k docs: indexed ~39.9 ms vs full scan ~43.2 ms, ~7%).

**Root cause (two places):** for `age >= 30 AND age <= 50` (two bounds on one indexed field),
1. the planner (`plan_index_scan_filter`) took only the **first** filter per index field, so only
   `>= 30` drove the index and `<= 50` became a post-fetch full-scan filter; and
2. the scanner (`IndexScannerInner::scan`) applied only `filters[0]` for a single-field index.

So the index returned ~everything above the lower bound and the read path fetched (and
post-filtered) almost as many documents as a full scan — the index did nothing useful.

**Fix:**
- Planner now collects **all** bounds on a single-field index (compound indexes still take one
  filter per level).
- Scanner combines a lower + upper bound into a **single bounded range scan** that walks only
  `ceiling(lower) .. floor(upper)` — reading just the in-range keys — with a correctness
  fallback to per-bound **intersection** for other same-field combinations.
- Added `Durability`-independent correctness coverage: a regression test asserting the indexed
  result set is **exactly** the full-scan result (inclusive/exclusive/empty/single-value ranges).

**Result (10k docs):**

| Query | Before | After |
| --- | --- | --- |
| Broad range (`age` ∈ [30,50], low-cardinality, ~34% selectivity) | ~39.9 ms (≈ scan) | ~38 ms |
| **Narrow range (1% selectivity, high-cardinality `id`)** | (≈ scan) | **~23.9 ms vs ~40.5 ms scan (~41% faster)** |

The advantage **grows with collection size** (at 1k the fixed query overhead dominates and the
two tie; at 10k the index clearly wins; at real mailbox sizes it dominates) and with
**selectivity** (a narrow date window touches only the matching rows instead of the whole store).

### 5.3 Comprehensive query-engine audit (all filters / scan / plan)

Followed up by auditing every filter's index behaviour, the scan algorithm (simple + compound),
and the find-plan optimizer (AND / OR), to make *all* search paths index-efficient.

**Findings & fixes:**
- **Range on the *compound*-index terminal level** (e.g. `[folder, date]` index with
  `folder == X AND date BETWEEN`): was using only one bound. **Fixed** — the planner now collects
  *all* bounds for the *terminal* index field (prefix fields still take one filter each, as the
  scan cascades one sub-map per matched key), and the scanner's bounded range scan runs at the
  recursed terminal level. Regression test added.
- **`field.between(a, b)` did a full scan** (the `BetweenFilter` has no `apply_on_index` and
  defaults `has_field()` to `false`, so it was skipped in index planning entirely — silently
  slower than the equivalent `gte().and(lte())`). **Fixed** — the planner expands `BetweenFilter`
  into its two bounds (standalone and nested inside an `AND`), so `between` is now bounded-scanned
  identically. Test added.

**Audited and confirmed already efficient / inherently unavoidable (no change needed):**
- `eq` (EqualsFilter) and `in_array` (InFilter): single / N point lookups on the index — fast.
- `!=`, `not in`, `.not()`, `regex`/`like`: full index or collection scan — inherent to negation
  and pattern matching (no btree can avoid it); not a bug.
- `OR`: each branch is planned independently and the index plans are unioned; falls back to a
  single full scan only when a branch is unindexable — correct and reasonable.

**Noted, not changed (semantics/risk):** `DocumentCursor` caches a clone of every yielded
document (to support `reset()`/`size()`/re-iteration), and `count()` materialises every matching
document. For a forward-only consumer (list a folder, count unread) this is redundant CPU +
peak memory. A `count()` that short-circuits on a fully index-covered plan, and an opt-out of
caching for one-shot iteration, are worthwhile follow-ups but touch cursor semantics used across
the codebase — deferred to keep this change set green and low-risk.

---

## 6. Verification

- `cargo clippy --workspace --all-targets`: **clean (0 warnings/errors).**
- Tests: `nitrite` lib 2262 ✓ · `nitrite-fjall-adapter` 221 ✓ · `nitrite-spatial` 429 ✓ ·
  `nitrite-tantivy-fts` 119 ✓ · `nitrite-derive` 11 ✓ · integration suite ✓ (see run notes).
- Feature combos exercised by CI also checked: `--features memory --no-default-features` and
  `--features custom_separator`.

> **Test-harness note (not a production issue):** opening *many* independent fjall keyspaces in
> parallel on a single (external) disk thrashes flush/compaction/fsync I/O — a documented
> harness artifact. Production uses one keyspace. The integration suite is therefore run with
> capped test threads on constrained disks; CI runs it at default parallelism on local SSDs.
