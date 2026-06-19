//! Reproduction + regression test for the Inbux `PERF-014` disk-usage blowup.
//!
//! ## Symptom
//!
//! Inbux's canonical store must keep ≤ 250 MB per 10k messages (attachments
//! excluded). The real gate test (`core/rust/tests/perf_disk_usage_evidence.rs`)
//! measured **761 MiB** (and up to ~1.3 GiB across runs) for 10k *tiny*
//! messages — ~76 KB of on-disk overhead per few-hundred-byte record. That is
//! pure storage-engine amplification, not logical data.
//!
//! ## Root cause (in `fjall`, surfaced through the Nitrite adapter)
//!
//! The on-disk footprint is dominated by the keyspace-wide journal (WAL):
//!
//!   1. **Journal files are preallocated to a fixed 32 MiB**
//!      (`fjall::journal::writer::PRE_ALLOCATED_BYTES`) and a journal is sealed
//!      by *rename*, never truncated back to its real content length — so every
//!      sealed segment occupies a full 32 MiB on disk even when it holds a few
//!      KB.
//!   2. **A sealed journal is only reclaimed once *every* partition has
//!      persisted (flushed memtable → disk segment) past that journal's
//!      watermark** (`JournalManager::maintenance`). During a bulk sync across
//!      many partitions (message / body / attachment / thread / index / account
//!      / folder / checkpoint), mostly-empty 32 MiB journals pile up, and a
//!      low-traffic partition whose memtable never reaches the rotation
//!      threshold pins the whole stack. Neither `Store::compact()` nor
//!      `close()` flushed memtables, so the journals were never reclaimed at the
//!      post-bulk-sync measurement point.
//!
//! ## What this test does
//!
//! It writes a multi-partition, multi-write-per-record workload (the shape of
//! the Inbux bootstrap: header insert → threading update → body insert, plus a
//! couple of low-traffic "metadata" partitions). To make the accumulation
//! deterministic — independent of the background monitor's timing — it raises
//! `max_journaling_size` so the monitor never auto-reduces the journal during
//! the run, and uses a small memtable so rotations are frequent. It then
//! measures the journal footprint **before** compaction (the bloat) and
//! **after** `db.compact()` (which, with the fix, flushes every partition's
//! memtable so the journal manager reclaims the now-unpinned segments).
//!
//! Before the adapter fix this asserts-fails at the post-compaction gate
//! (journals are never reclaimed); after the fix the journal collapses to a
//! single active segment and the total lands far under the gate.

#![cfg(feature = "fjall")]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use std::time::Instant;

use nitrite::collection::Document;
use nitrite::doc;
use nitrite::filter::field;
use nitrite::index::unique_index;
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::{Durability, FjallModule};
use nitrite_int_test::test_util::random_path;

const MESSAGE_COUNT: usize = 10_000;
/// Inbux `PERF-014`: ≤ 250 MB per 10k messages.
const GATE_BYTES: u64 = 250 * 1024 * 1024;
/// A moderately sized per-message field, standing in for the AEAD-sealed
/// canonical fields Inbux persists. Only its size matters here (it drives
/// memtable fill → journal rotations); its content does not.
const SEALED_FIELD_BYTES: usize = 3 * 1024;

/// Opens a Fjall-backed Nitrite store tuned like the Inbux canonical store
/// (`DesktopStandard` profile: KV separation on, LZ4, 10-bit bloom, periodic
/// durability) — but with a deliberately large `max_journaling_size` and a
/// small memtable so the journal-pinning pathology is exercised deterministically
/// rather than depending on the background monitor's reclaim timing.
fn open_db(path: &str) -> Nitrite {
    let module = FjallModule::with_config()
        .db_path(path)
        .kv_separated(true)
        .bloom_filter_bits(10)
        .block_cache_capacity(64 * 1024 * 1024)
        .blob_cache_capacity(32 * 1024 * 1024)
        .max_write_buffer_size(128 * 1024 * 1024)
        // Small memtable → frequent rotations → many sealed (preallocated 32 MiB)
        // journals during the burst.
        .max_memtable_size(2 * 1024 * 1024)
        // Large journal cap → the background monitor never crosses its 50%
        // reclaim threshold mid-run, so the accumulation is deterministic and
        // only `compact()` / `close()` can reclaim the journals.
        .max_journaling_size(8 * 1024 * 1024 * 1024)
        .durability(Durability::Periodic)
        .fsync_frequency(100)
        .manual_journal_persist(false)
        .build();

    Nitrite::builder()
        .load_module(module)
        .open_or_create(None, None)
        .expect("open fjall store")
}

fn message_doc(i: usize, thread_group: &str, sealed: &str) -> Document {
    doc! {
        "id": (format!("01MSG{i:026}")),
        "account_id": "01PERFDISKUSAGE0000000000A",
        "folder_id": "INBOX",
        "provider_message_id": (format!("msg-{i}")),
        "internet_message_id": (format!("<msg-{i}@fixture>")),
        "thread_group_id": thread_group,
        "subject": (format!("Subject msg-{i}")),
        "received_at": (1_900_000_000_000_i64 - i as i64),
        "size_bytes": 2048_i64,
        "flags": 0_i64,
        "has_attachments": false,
        "body_ref": (format!("01BODY{i:025}")),
        // Stand-in for the AEAD-sealed canonical fields.
        "sealed": sealed,
    }
}

fn body_doc(i: usize, sealed: &str) -> Document {
    doc! {
        "id": (format!("01BODY{i:025}")),
        "message_id": (format!("01MSG{i:026}")),
        "preview": (format!("Body of msg-{i}")),
        "language": "en",
        "sealed": sealed,
    }
}

fn dir_size(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                dir_size(&p)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

/// Bytes under the keyspace's `journals/` directory.
fn journal_bytes(root: &Path) -> u64 {
    dir_size(&root.join("journals"))
}

/// Sums on-disk bytes grouped by top-level subdirectory.
fn breakdown(root: &Path) -> BTreeMap<String, u64> {
    let mut by_subdir: BTreeMap<String, u64> = BTreeMap::new();
    fn walk(dir: &Path, top: &str, by_subdir: &mut BTreeMap<String, u64>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                let next_top = if top.is_empty() {
                    p.file_name().unwrap().to_string_lossy().into_owned()
                } else {
                    top.to_string()
                };
                walk(&p, &next_top, by_subdir);
            } else {
                let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                let key = if top.is_empty() {
                    "<root>".to_string()
                } else {
                    top.to_string()
                };
                *by_subdir.entry(key).or_default() += len;
            }
        }
    }
    walk(root, "", &mut by_subdir);
    by_subdir
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[test]
fn disk_usage_per_10k_messages_repro() {
    let path = random_path();
    fs::create_dir_all(&path).ok();
    let root = Path::new(&path);

    let sealed: String = "x".repeat(SEALED_FIELD_BYTES);

    {
        let db = open_db(&path);
        let messages = db.collection("message").expect("open message collection");
        let bodies = db.collection("body").expect("open body collection");
        // Mirror the Inbux canonical store: a unique `id` index on each
        // collection. Without it the per-message threading `update(field("id"))`
        // below falls back to a full collection scan — O(n²) over the run — which
        // is a property of an un-indexed test workload, not of the disk fix. Inbux
        // always carries this index (`$nitrite_index_..._message..id..unique`).
        messages
            .create_index(vec!["id"], &unique_index())
            .expect("create message id index");
        bodies
            .create_index(vec!["id"], &unique_index())
            .expect("create body id index");
        // Low-traffic "metadata" partitions, written once early and never again,
        // mirroring the Inbux account/folder/checkpoint collections that pin the
        // journal stack.
        let account = db.collection("account").expect("open account collection");
        let folder = db.collection("folder").expect("open folder collection");
        account
            .insert(doc! { "id": "acct-1", "name": "Primary" })
            .expect("insert account");
        folder
            .insert(doc! { "id": "INBOX", "name": "Inbox" })
            .expect("insert folder");

        let write_start = Instant::now();
        for i in 0..MESSAGE_COUNT {
            // 1. header page commit — insert the canonical message row.
            let initial_group = format!("01GRP{i:026}");
            messages
                .insert(message_doc(i, &initial_group, &sealed))
                .expect("insert message header");

            // 2. Unit 18 threading pass — rewrite thread_group_id on the same row.
            messages
                .update(
                    field("id").eq(format!("01MSG{i:026}")),
                    &doc! { "thread_group_id": "01THREADGROUPSHARED0000000A" },
                )
                .expect("threading update");

            // 3. body hydration — insert the body row in the second collection.
            bodies.insert(body_doc(i, &sealed)).expect("insert body");
        }
        let write_elapsed = write_start.elapsed();
        println!(
            "[disk-repro] write phase ({MESSAGE_COUNT} msgs, insert+update+body): {:.1}s",
            write_elapsed.as_secs_f64()
        );

        // Footprint as a steady-state mail client would see it while open, right
        // after the bulk sync, *before* the compaction hook fires.
        db.commit().ok();
        let pre = dir_size(root);
        let pre_journals = journal_bytes(root);
        println!(
            "\n[disk-repro] BEFORE compaction: total {:.1} MiB, journals {:.1} MiB",
            mib(pre),
            mib(pre_journals)
        );
        for (k, v) in &breakdown(root) {
            println!("[disk-repro]   {k:<16} {:>10.2} MiB", mib(*v));
        }

        // Post-bulk-sync compaction (the Inbux Unit 12 hook). With the fix this
        // flushes every partition's memtable so the journal manager reclaims the
        // pinned, preallocated journal segments.
        let compact_start = Instant::now();
        db.compact().expect("compact");
        println!(
            "[disk-repro] compact phase (GC + flush-all-partitions): {:.1}s",
            compact_start.elapsed().as_secs_f64()
        );

        let post = dir_size(root);
        let post_journals = journal_bytes(root);
        println!(
            "[disk-repro] AFTER  compaction: total {:.1} MiB, journals {:.1} MiB",
            mib(post),
            mib(post_journals)
        );
        for (k, v) in &breakdown(root) {
            println!("[disk-repro]   {k:<16} {:>10.2} MiB", mib(*v));
        }
        println!("[disk-repro] gate is {:.1} MiB", mib(GATE_BYTES));

        // The reproduction: before compaction the journals alone blow the gate.
        assert!(
            pre_journals > GATE_BYTES,
            "expected the journal bloat to be reproduced (journals {} B > gate {} B); \
             the workload did not accumulate enough pinned journals",
            pre_journals,
            GATE_BYTES
        );

        // The fix: after compaction the journals are reclaimed and the whole
        // store fits comfortably under the gate.
        let total = dir_size(root);
        fs::remove_dir_all(root).ok();
        assert!(
            total <= GATE_BYTES,
            "PERF-014 regression: {total} bytes ({:.1} MiB) for {MESSAGE_COUNT} messages exceeds the {GATE_BYTES}-byte gate after compaction",
            mib(total)
        );
    }
}
