# Nitrite-Rust + Fjall — Durability & Concurrency Root-Cause Analysis

**Status:** investigation report (no code changes requested in `nitrite-rust`; the
fixes below are for the `nitrite-rust` maintainer to apply separately).
**Date:** 2026-06-04
**Author:** Inbux engineering (Unit 12 review follow-up)
**Source analyzed:** `/Volumes/External/codebase/nitrite/nitrite-rust`
(`nitrite` core + `nitrite-fjall-adapter`), against `nitrite = 0.2.0` /
`fjall` as bound by Inbux `core/rust/Cargo.toml`.

---

## 1. What prompted this

Two symptoms surfaced while validating Inbux Unit 12 (initial sync) against a real
Fjall-backed canonical store:

1. **Drop-then-reopen inconsistency.** An integration test that commits folders,
   drops the store, reopens it at the same path, and re-runs discovery fails on the
   re-open with:

   ```
   fatal bootstrap error: folder write failed:
     Engine { reason: "Failed to write index entries during insert:
              Unique constraint violated" }
   ```

   The resume path reads existing folders with a full scan
   (`find_all_json("folder")`), gets **nothing back**, mints fresh canonical ids,
   and tries to insert — but the **unique index `(account_id, path)` still holds the
   entries from before the drop**. So after reopen the *index partition* has rows the
   *data partition* does not return. This is reproducible **deterministically when the
   store-backed suite runs under load** (many stores opened/dropped in sequence on an
   external disk) and **passes in isolation**.

2. **"Hang" under parallel store-backed tests.** Running ~13 store-backed
   integration/benchmark tests with the default test-thread parallelism makes the run
   appear to hang for many minutes; a single test completes in ~0.45 s. Forcing
   `--test-threads=1` makes the whole suite finish in ~30 s.

The second is a test-harness artifact (see §6). The first is a genuine
**durability/atomicity** gap in how nitrite commits cross-partition writes. Both are
relevant to the production question: *what happens when several accounts write to one
Fjall store from several threads?* (§5)

> **Update after attempted Inbux-side mitigations.** Two application-layer fixes were
> tried and **do not** resolve symptom #1, which narrows the root cause to the storage
> engine's close/reopen path (see §3.1):
>
> 1. An explicit durable `persist(SyncAll)` immediately after the discovery commit
>    (`CanonicalStore::persist` → `Nitrite::commit` → `ks.persist(PersistMode::SyncAll)`),
>    so the folder topology is fsynced before any later work or drop.
> 2. Replacing the "does this folder already exist?" **full-collection scan**
>    (`find_all_json`) with an **indexed composite lookup** on `(account_id, path)`
>    (`find_one_json_where2`).
>
> With both in place, after a drop+reopen under load the folder rows are **still
> unreadable by either a full scan or the index-backed lookup**, yet the
> `(account_id, path)` **unique index still rejects re-insertion**. So a *clean* store
> drop — which runs `store.commit()` (SyncAll) **and** `store.close()` via
> `NitriteInner::drop` — followed by an immediate reopen can still surface an
> **orphaned index entry whose data-partition row is gone**. That is not merely a
> "didn't fsync in time" window (root cause B); it is a close/reopen consistency bug
> (root cause A, §3.1) that the application layer cannot paper over.

---

## 2. How writes and durability actually work (with code references)

### 2.1 A logical row write fans out to multiple Fjall partitions

Each Nitrite collection and each index is a **separate Fjall partition** inside one
`Keyspace`:

- `nitrite-fjall-adapter/src/store.rs:25-44` — `FjallStore` is "Multi-map (supports
  isolated partitions per collection/index) … within a single Keyspace."

A single document write therefore performs **one data-partition insert plus one insert
per index partition**, each as an independent Fjall operation:

- `nitrite-fjall-adapter/src/map.rs:579` — `FjallMap::put` → `self.partition.insert(...)`
  (a single-key write to one partition).

### 2.2 Transaction commit is a *logical* two-phase commit, not a Fjall atomic batch

- `nitrite/src/transaction/nitrite_transaction.rs:324` — `NitriteTransaction::commit`
  → `perform_commit`.
- `nitrite/src/transaction/nitrite_transaction.rs:358-410` — `perform_commit` iterates
  the touched collections and, **per journal entry**, runs that entry's `commit_cmd()`
  (the buffered insert/update/remove), pushing a logical `rollback_cmd` onto an undo
  stack. Each `commit_cmd()` ultimately calls the per-row `FjallMap::put`/`remove`
  above — i.e. **N rows × (1 data + M index) individual `partition.insert` calls**.

There is **no `fjall` `WriteBatch` / atomic keyspace batch wrapping a transaction's
cross-partition writes.** The "two-phase commit" is nitrite's own redo/undo bookkeeping,
not storage-engine atomicity. (Contrast `FjallMap::put_all`,
`nitrite-fjall-adapter/src/map.rs:604`, which *does* use Fjall's batch API "for
atomicity" — but the per-row collection commit path does not go through it.)

A second important note is in the same function:

- `nitrite/src/transaction/nitrite_transaction.rs:367-372` — *"We don't acquire the
  collection lock here … each commit command will acquire its own lock."* So the commit
  holds **no transaction-wide lock**; locks are taken and released per individual
  operation.

### 2.3 Nothing is fsynced on commit — only on a periodic timer or on Drop

The only `fsync` in the adapter is `persist(SyncAll)`:

- `nitrite-fjall-adapter/src/store.rs:379-388` — `FjallStoreInner::commit` →
  `ks.persist(PersistMode::SyncAll)`. This is the **only** durability barrier.

`NitriteTransaction::commit` (§2.2) **never calls `store.commit()`**. So when
`txn.commit()` returns success, the writes are in Fjall's write buffer + journal but
**not fsynced**. Durability then depends on exactly two things:

1. **A clean Drop of the Nitrite handle**:
   - `nitrite/src/nitrite.rs:805-810` — `NitriteInner::drop` → `store.commit()` then
     `store.close()`.
   - `nitrite-fjall-adapter/src/store.rs:591-606` — `FjallStoreInner::drop` →
     `commit()` if `commit_before_close()`.
2. **Fjall's background journal persistence** (periodic). Inbux configures
   `manual_journal_persist(false)` and `fsync_frequency = 100 ms`
   (`core/rust/src/storage/store/mod.rs` `apply_fjall_tuning`).

Confirmation on the Inbux side: `CanonicalTxn::commit`
(`core/rust/src/storage/store/mod.rs`) wraps writes in
`db.with_session(|s| { let txn = s.begin_transaction()?; …; txn.commit()?; })` and
**never calls the store-level `db.commit()`/persist**. So Inbux, by design, relies on
Drop + the 100 ms periodic fsync for durability.

---

## 3. Root cause A — cross-partition writes are not crash-atomic (and not consistent across a clean close/reopen under load)

Because a transaction's data-partition and index-partition writes are **independent
Fjall operations with no enclosing atomic batch** (§2.1–2.2), the durability boundary
(a periodic fsync, or recovery after an unclean stop) can fall **between** the index
insert and the data insert (or between rows). Recovery then replays a journal prefix
that contains the index write but not the matching data write (or vice versa),
producing exactly the observed state:

> **index partition has `(account_id, path)`; data partition returns no row for it.**

This is the mechanism behind symptom #1: on reopen, `find_all_json("folder")` returns
nothing for the lost data rows, so the resume mints new ids and re-inserts, and the
**surviving unique-index entry rejects the insert** ("Unique constraint violated").

> A correct embedded store must make *all* partitions touched by one logical
> transaction recover together. nitrite delegates persistence to Fjall but issues the
> cross-partition writes individually, so it gets per-operation durability without
> per-transaction atomicity.

### 3.1 The close/reopen path does not guarantee a consistent reopen under load

The attempted mitigations (top-of-doc update) prove this is **not** only the
fsync-timing window of root cause B. Even with an explicit `persist(SyncAll)` after the
folder commit, a **clean** drop — `NitriteInner::drop` → `store.commit()` (SyncAll) +
`store.close()` (`nitrite/src/nitrite.rs:805-810`) — followed by an immediate reopen can
still leave the data partition missing rows that the index partition retains. Likely
contributors in the engine:

- **`close()` / Drop may not drain background flush + compaction workers.** Each
  Keyspace runs background flush and compaction threads (§6). If `close()` returns
  before those threads have flushed memtables and finalized in-flight compactions, the
  on-disk SSTable/manifest state for the *data* partition can be torn relative to the
  *index* partition (which compacts independently). The next `open()` then recovers an
  index entry without its data row.
- **Per-partition recovery is independent.** With each collection and each index in its
  own partition and no transaction-spanning atomic batch (§2.2), recovery has no record
  that "index entry X and data row Y belong to the same committed transaction," so it
  can restore one without the other.

This is consistent with the symptom being **deterministic under suite load** (many prior
store opens leave background work in flight / the disk busy, so `close()` races
finalization) and **absent in isolation** (workers finish before reopen).

---

## 4. Root cause B — acknowledged commits are not durable until Drop / periodic fsync

`txn.commit()` returns "committed" before any `persist()` (§2.3). The data becomes
durable only at the next periodic fsync (≤ `fsync_frequency`) or when
`NitriteInner::drop` runs. Therefore an acknowledged commit is **lost** if the process
ends without running Drop:

- `std::process::exit` / `abort` / `panic = "abort"`,
- an OS kill / power loss inside the fsync window,
- or any path where an `Arc<Nitrite>` outlives the intended scope so Drop never fires.

For a desktop/mobile mail client this is the difference between "we fsynced your move
before telling you it's done" and "we'll fsync within 100 ms, probably." It is also a
contributor to symptom #1: under load, the drop→reopen happens fast enough that the
relevant writes were never fully/consistently persisted before the handle went away.

---

## 5. Root cause C — concurrency: torn cross-partition reads (the multi-account question)

The production question is *"multiple accounts writing from multiple threads to one
Fjall store."* Findings:

- **Fjall itself is internally thread-safe** — concurrent partition access does not
  corrupt data.
- **nitrite serializes per *collection*, not per *transaction*.** Commit takes no
  transaction-wide lock (§2.2, `nitrite_transaction.rs:367`); each operation locks its
  own collection. So a transaction that spans a data partition + several index
  partitions is **not atomic with respect to a concurrent reader** of those partitions:
  a reader can observe the index updated while the data is not (or a multi-row commit
  half-applied).
- Writers to **different** collections proceed concurrently; writers to the **same**
  collection serialize.

### How Inbux's canonical store interacts with this

Inbux runs **one shared `CanonicalStore`** (the `GLOBAL_STORE` singleton). Every
account's bootstrap job commits through `CanonicalTxn::commit`, which:

- takes a process-wide `commit_order` **`Mutex`** (serializes *whole* transactions), and
- runs the engine work under `db.read()` (a **shared** `RwLock` guard, not exclusive).

Consequences for multi-account writes:

1. **No corruption** — Fjall is thread-safe and whole-transaction commits are
   serialized by `commit_order`.
2. **Torn reads are still possible** — `find_*` reads take only `db.read()` and can run
   *concurrently with* an in-flight commit (which also holds `db.read()`), so a reader
   can momentarily observe a half-applied cross-partition transaction (root cause C).
   The reopen-inconsistency (A/B) is the durable version of the same atomicity gap.
3. **Throughput, not hangs** — commits serialize and each becomes durable only at fsync;
   multi-account bulk sync is fsync-bound. There is **no deadlock/hang in production**
   because there is **one** store and **one** Fjall worker pool.

---

## 6. The "parallel test hang" is a test artifact, not a production fault

Each `FjallStore` opens its own `Keyspace`, and a Keyspace spawns background **flush**
and **compaction** worker threads. Inbux sizes these from the device profile
(`apply_fjall_tuning`): `flush_workers ≈ CPUs`, `compaction_workers ≈ CPUs/2`.

Running ~13 store-backed tests in parallel opens ~13 independent Keyspaces ⇒
~`13 × 1.5 × CPUs` background worker threads, all issuing fsyncs against **one
(external) disk**. The result is thread oversubscription + fsync contention that looks
like a hang. Evidence: a single benchmark runs in **0.456 s**; the full suite at
`--test-threads=1` finishes in ~**30 s**; the same suite in parallel does not finish in
9+ minutes.

**Production uses exactly one Keyspace**, so this does not occur there. It is purely a
consequence of opening many independent stores at once.

---

## 7. Recommended fixes

### 7.1 In `nitrite-rust` (for the maintainer)

1. **Make a transaction commit one atomic Fjall batch across all touched partitions.**
   Accumulate every data-partition and index-partition write of a transaction into a
   single `fjall` keyspace batch / `WriteBatch` and apply it atomically. This closes
   root cause **A** (crash atomicity) and makes the commit atomic to readers (**C**).
   The per-row `commit_cmd` path (`perform_commit`) should route through the batch the
   way `put_all` already does (`map.rs:604`), instead of issuing individual
   `partition.insert` calls.

2. **Offer per-commit durability.** Call `ks.persist(PersistMode::SyncAll)` (or a
   lighter `SyncData`) at the end of `NitriteTransaction::commit` when the store is
   configured for synchronous durability, instead of relying solely on Drop + the
   periodic timer. Expose a knob, e.g. `Durability::{OnCommit, Periodic}`, so callers
   that need zero-data-loss (a mail client) can pay the per-commit fsync, and bulk
   importers can opt into periodic. Closes root cause **B**.

3. **Guarantee atomic visibility to concurrent readers.** With (1) the atomic batch
   provides this for committed state; additionally ensure a reader never observes a
   partially-applied transaction mid-commit (a transaction-scoped guard or snapshot
   read). Closes the live-read half of **C**.

4. **Make `close()` / Drop drain background work before returning** (§3.1). `close()`
   must flush all memtables and join in-flight flush/compaction so that a subsequent
   `open()` of the same path observes a fully consistent, fully recovered state — no
   index entry without its data row. This is the specific fix for symptom #1 that the
   application layer cannot provide (an explicit `persist(SyncAll)` before drop is not
   sufficient on its own, as shown by the attempted mitigations).

5. **(Optional) Bound or share background worker threads** across Keyspaces, or document
   the per-Keyspace thread cost, so test/multi-store setups don't oversubscribe (§6).

### 7.2 In Inbux (independent of the nitrite fix — defensive, recommended)

These do not require the nitrite change and would harden Unit 12 today:

- **Make `persist_discovery` resilient to a torn/empty read.** It currently discovers
  existing folder ids via `find_all_json("folder")` + deserialize, then *inserts* with a
  freshly-minted id when the read misses — which collides with a surviving unique-index
  entry. Prefer an indexed natural-key lookup of `(account_id, path)` (e.g.
  `find_one_json_where2`) and **upsert by the natural key**, so a stale/torn read cannot
  produce a duplicate-id insert. This alone would have prevented the symptom-#1 failure
  even with the nitrite bug present.
- **Keep store-backed suites at `--test-threads=1`** (already adopted) until §7.1.4.
- **If intra-process torn reads matter for a given read**, take the `commit_order` mutex
  for that read, or upgrade `CanonicalTxn::commit` to `db.write()` (exclusive) so reads
  never interleave with a commit — at a throughput cost. (Note: this fixes only
  intra-process torn reads, **not** crash atomicity — that needs §7.1.1.)

---

## 8. Evidence index (file:line)

| Claim | Location |
| --- | --- |
| One Keyspace, one partition per collection/index | `nitrite-fjall-adapter/src/store.rs:25-44` |
| Per-row write = single `partition.insert` | `nitrite-fjall-adapter/src/map.rs:579` |
| Batch path exists but only for `put_all` | `nitrite-fjall-adapter/src/map.rs:604` |
| Commit = logical two-phase, per-op locks, no txn-wide lock | `nitrite/src/transaction/nitrite_transaction.rs:324,358-410,367` |
| Only fsync is `persist(SyncAll)` in store `commit()` | `nitrite-fjall-adapter/src/store.rs:379-388` |
| Durability relies on Drop | `nitrite/src/nitrite.rs:805-810`; `nitrite-fjall-adapter/src/store.rs:591-606` |
| Inbux never calls store-level commit per txn | `core/rust/src/storage/store/mod.rs` (`CanonicalTxn::commit`) |
| Inbux serializes whole txns via `commit_order` mutex, reads under shared guard | `core/rust/src/storage/store/mod.rs` (`StoreInner.commit_order`, `db.read()`) |
| Symptom #1 reproduction | `core/rust/tests/initial_sync_integration.rs::cancel_at_safe_boundary_then_resume_completes` (fails in-suite under load, passes isolated) |
