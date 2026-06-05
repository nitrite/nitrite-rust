# nitrite 0.3.0 — KV-separation × atomic-transaction commit regression

**Status:** blocking bug in `nitrite_fjall_adapter` 0.3.0; Inbux is held on `=0.2.0`
pending **0.3.1**. (No Inbux code change can correctly work around it — see §6.)
**Date:** 2026-06-05
**Affects:** `nitrite` / `nitrite_fjall_adapter` **0.3.0**, with `fjall` 2.11.2.
**Reporter:** Inbux engineering (Unit 12 store integration).
**Companion:** `nitrite-fjall-durability-and-concurrency-analysis.md` (the 0.2.0
durability/atomicity analysis that 0.3.0 was published to fix — those fixes are
good; this document is a *new* regression introduced alongside them).

---

## 1. Summary

`nitrite_fjall_adapter` 0.3.0 makes a logical write land in **one fjall
`WriteTransaction`** (the cross-partition atomicity fix). During that transactional
commit, nitrite routes reads through the active transaction via `tx_scope`. One such
read — `first_key_value`, used in unique-index maintenance — **does not resolve
KV-separated (blob) values**: on a partition opened with `kv_separated(true)`, it
decodes the blob handle as if it were the inline value and fails with:

```
Decode(InvalidTag(("MaybeInlineValue", 18)))
```

Any transactional commit that touches a unique index on a KV-separated partition
therefore fails. Inbux's canonical store **requires** `kv_separated(true)` (large mail
blobs — `MessageBody`, attachment payloads, `raw_ref` — are kept out of the LSM hot
path), so 0.3.0 cannot be adopted until this is fixed.

---

## 2. Symptom (exact error)

Every Unit 12 store-backed test fails — **in isolation, deterministically** — on the
first indexed commit (folder discovery writes `Folder`, which has the composite-unique
index `(account_id, path)`):

```
fatal bootstrap error: folder write failed:
  Engine { reason: "Commit failed: Failed to execute commit:
    Failed to get first key from FjallMap:
    FjallError: Storage(Decode(InvalidTag(("MaybeInlineValue", 18)))) " }
```

Unit 06's store tests (36) **pass** with the same `kv_separated(true)` config, because
their writes do not drive the unique-index `first_key` path that the bug lives on.

---

## 3. Reproduction

1. `core/rust/Cargo.toml`: `nitrite = "=0.3.0"`, `nitrite_fjall_adapter = "=0.3.0"`;
   `cargo update -p nitrite -p nitrite_fjall_adapter --precise 0.3.0`.
2. `cargo test --test initial_sync_integration discovery_persists_folders -- --test-threads=1`
   → **fails** with the error in §2.
3. **Bisection:** in `core/rust/src/storage/store/mod.rs::apply_fjall_tuning`, change
   `.kv_separated(tuning.kv_separated)` → `.kv_separated(false)` and re-run →
   **passes**. Revert. This isolates the trigger to `kv_separated(true)`.

The store config in play (`apply_fjall_tuning`): `kv_separated(true)`,
`manual_journal_persist(false)`, `fsync_frequency(100)`, plus cache/buffer sizing.
fjall resolves to **2.11.2** in both Inbux and the upstream `nitrite-rust` lockfile, so
this is **not** a fjall-version mismatch.

---

## 4. Root cause (source-level)

In `nitrite-fjall-adapter` 0.3.0:

- A transaction commit (and any single insert/update/remove + its index updates) runs
  inside one `fjall::WriteTransaction` obtained from a `TxKeyspace`
  (`nitrite-fjall-adapter/src/store.rs:7,257,494` — `use fjall::{… TxKeyspace,
  WriteTransaction}`, `new_write_tx`, `with_atomic`).
- Reads issued during that commit are routed to the active transaction through a
  thread-local bridge, `tx_scope::with_active`. `FjallMapInner::first_key`
  (`nitrite-fjall-adapter/src/map.rs:656-668`):

  ```rust
  fn first_key(&self) -> NitriteResult<Option<Key>> {
      let result = crate::tx_scope::with_active(|tx| match tx {
          Some(tx) => tx.first_key_value(&self.partition), // ← transactional read
          None     => self.partition.first_key_value(),    // ← direct read
      }).map_err(|err| Self::backend_err("get first key from", err))?;
      …
  }
  ```

- The error is raised by `backend_err("get first key from", err)`, i.e.
  `tx.first_key_value(&self.partition)` returned a fjall `Storage(Decode(InvalidTag(
  "MaybeInlineValue", …)))`. `MaybeInlineValue` is fjall's **KV-separation value
  wrapper** (inline bytes vs. blob-log handle). The transactional read path is
  returning/decoding the value **without resolving the blob indirection** that the
  direct `partition.first_key_value()` path handles — so a blob handle is decoded as an
  inline value and the tag (18) is invalid.

**In one line:** `WriteTransaction` reads (`first_key_value`, and by the same mechanism
likely `get` / `range` / `is_empty`) on a `kv_separated` partition do not perform blob
resolution, while the non-transactional `partition.*` reads do. The atomic-commit path
added in 0.3.0 is the first place nitrite reads through `WriteTransaction`, so the
incompatibility only surfaces now.

### Why the 0.3.0 test suite missed it

`nitrite-int-test` has **no** test that combines `kv_separated(true)` **with** a
transactional commit **and** a `first_key`/unique-index path. The adapter's own
`kv_separated(true)` unit tests (`nitrite-fjall-adapter/src/store.rs:810`,
`map.rs:854`) exercise KV-separation only through **direct** partition reads, never
through `tx_scope`/`WriteTransaction`. The new crash-atomic transaction tests, in turn,
do not enable KV-separation. The bug lives precisely in the untested intersection.

---

## 5. Recommended fix (nitrite 0.3.1)

1. **Make transactional reads blob-aware.** Every `tx_scope::with_active` read on a
   KV-separated partition (`first_key`/`first_key_value`, `get`, `range`/iteration,
   `is_empty`/`first_key_value().is_none()` at `map.rs:656-668,763`, and the
   `decode_value`/`FjallValue` path) must resolve the `MaybeInlineValue` blob exactly
   as the direct `partition.*` path does. If fjall's `WriteTransaction` cannot read
   blob-separated partitions, either (a) resolve the blob handle in the adapter after
   the transactional read, or (b) do not enable KV-separation on partitions accessed
   through `WriteTransaction`, or (c) fix it at the fjall layer. Recommendation is to fix it at the fjall layer, as that is the root cause. 
2. **Add the missing regression test** (the one that would have caught this): a
   `kv_separated(true)` store, `begin_transaction()`, insert into a collection with a
   **unique index**, `commit()`, then read back — and a crash/reopen variant. Run it in
   the `Durability`-aware suite so it covers both `OnCommit` and `Periodic`.
3. Audit any other `tx_scope::with_active` read site for the same blob-resolution gap. the same blob-resolution gap likely affects other tx_scope::with_active reads (get, range, is_empty), not just first_key_value — worth auditing all of them in one pass.



---

## 6. Why Inbux cannot work around it

- Disabling `kv_separated` is the only Inbux-side change that makes 0.3.0 green, and it
  is a **performance regression**, not a fix: it moves `MessageBody`, attachment
  payloads, and `raw_ref` into the LSM hot path, hurting exactly the read/compaction
  performance KV-separation exists to protect (Unit 06 design §; `FjallTuning` mandates
  `kv_separated = true` on every profile). That contradicts the production-grade
  performance target.
- The failure is inside nitrite's commit, before any Inbux code can intervene; there is
  no canonical-store API to alter the transactional read path.

So Inbux stays on `nitrite = =0.2.0` (green; the only known-red is the
`cancel_at_safe_boundary_then_resume_completes` test under suite load, which is the
*0.2.0* durability/atomicity bug that 0.3.x fixes — see the companion doc).

---

## 7. Re-integration checklist (when 0.3.1 is published)

1. `core/rust/Cargo.toml`: bump both pins to `=0.3.1`; remove the holding note;
   `cargo update -p nitrite -p nitrite_fjall_adapter --precise 0.3.1`.
2. **Build + decode check:** `cargo test --test initial_sync_integration -- --test-threads=1`
   must pass with `kv_separated(true)` (no `MaybeInlineValue` error), and the previously
   load-flaky `cancel_at_safe_boundary_then_resume_completes` must pass **in-suite**
   (0.3.x atomic transactions + close-drain fix the torn reopen).
3. **Adopt explicit durability** for the canonical store. 0.3.x defaults to
   `Durability::Periodic` (~1s power-loss window). For a production mail client, set
   `FjallModuleBuilder::durability(Durability::OnCommit)` in `apply_fjall_tuning` (the
   `Folder`/`Message` commits are page/batch-sized, so per-commit fsync is well within
   the Unit 03 throughput floor — verify in step 5). Update the `apply_fjall_tuning`
   comment and the `every_profile_is_durability_safe` test accordingly.
4. **Drop now-redundant workarounds.** With `Durability::OnCommit`, the explicit
   `store.persist()` call added at the discovery safe boundary
   (`sync::bootstrap::discovery::persist_discovery`) becomes redundant (every commit
   already fsyncs) — remove it (keep `CanonicalStore::persist()` as a utility, or remove
   if unused). Keep the indexed `existing_folder_id` lookup — it is a correctness/perf
   improvement independent of the engine version, and 0.3.x makes it index-accelerated.
5. **Performance:** run `cargo test --release --test initial_sync_benchmark` and confirm
   ≥ 200 headers/s and ≥ 20 bodies/s (Unit 03 floor) **with `OnCommit`**. 0.3.x also
   index-accelerates range/count queries and short-circuits `count()`/`size()` from the
   index — consider simplifying `committed_counts` to an index-backed `count` once
   confirmed. Re-run the full suite at `--test-threads=1` (the many-Keyspaces parallel
   thrash from the companion doc §6 still applies to tests).
6. fmt + `clippy -D warnings`, regenerate nothing (no bridge change), and update both
   architecture docs to mark the regression resolved.
