//! Regression tests for the durability / cross-partition consistency fixes.
//!
//! These reproduce the "drop-then-reopen inconsistency" reported while integrating Nitrite
//! with a Fjall-backed canonical store: after committing documents that carry a unique index,
//! dropping the store, and reopening it at the same path, the data partition must still return
//! the committed rows AND the unique index must agree with them (no orphaned index entry whose
//! data row is gone). Before the atomic cross-partition transaction work, a reopen under load
//! could leave the unique index holding `(account_id, path)` entries the data partition no
//! longer returned, so a resume that re-minted ids hit a spurious "Unique constraint violated".

#![cfg(feature = "fjall")]

use nitrite::collection::Document;
use nitrite::doc;
use nitrite::errors::ErrorKind;
use nitrite::filter::{all, field};
use nitrite::index::unique_index;
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;
use nitrite_int_test::test_util::random_path;
use std::fs;

/// Opens (or reopens) a Fjall-backed Nitrite database at `path`.
fn open_db(path: &str) -> Nitrite {
    let storage_module = FjallModule::with_config()
        .db_path(path)
        .low_memory_preset()
        .build();

    Nitrite::builder()
        .load_module(storage_module)
        .open_or_create(None, None)
        .expect("failed to open Fjall-backed Nitrite database")
}

/// Builds folder-like documents with a natural `(account_id, path)` key.
fn folder_docs(count: i64) -> Vec<Document> {
    (0..count)
        .map(|i| {
            doc! {
                "account_id": 12_i64,
                "path": (format!("/inbox/folder_{}", i)),
                "name": (format!("Folder {}", i)),
            }
        })
        .collect()
}

#[test]
fn commit_then_drop_then_reopen_keeps_data_and_unique_index_consistent() {
    let path = random_path();
    let folder_count = 50_i64;

    // --- First session: create a unique index, commit folders inside a transaction, drop. ---
    {
        let db = open_db(&path);
        let collection = db.collection("folder").expect("open collection");
        collection
            .create_index(vec!["account_id", "path"], &unique_index())
            .expect("create unique compound index");

        db.with_session(|session| {
            let txn = session.begin_transaction()?;
            let tx_collection = txn.collection("folder")?;
            tx_collection.insert_many(folder_docs(folder_count))?;
            txn.commit()?;
            Ok(())
        })
        .expect("transactional folder commit");

        // A clean drop runs commit (SyncAll) + drain via NitriteInner/FjallStoreInner drop.
        db.close().expect("close first session");
    }

    // --- Second session: reopen at the same path and assert consistency. ---
    {
        let db = open_db(&path);
        let collection = db.collection("folder").expect("reopen collection");

        // 1. The data partition must return every committed row after reopen.
        let count = collection.find(all()).expect("find all").count();
        assert_eq!(
            count, folder_count as usize,
            "all committed folders must be readable after reopen (data partition intact)"
        );

        // 2. The unique index must agree with the data: an existing natural key still resolves
        //    to its row (no orphaned/missing index entry).
        let existing = collection
            .find(field("path").eq("/inbox/folder_0"))
            .expect("indexed lookup")
            .count();
        assert_eq!(existing, 1, "existing folder must be found via its indexed path");

        // 3. Re-inserting a document with an already-present unique key must be rejected by the
        //    surviving unique index — and, crucially, that key really does still exist in data.
        let dup = collection.insert(doc! {
            "account_id": 12_i64,
            "path": "/inbox/folder_0",
            "name": "Duplicate",
        });
        assert!(
            matches!(
                dup.as_ref().map_err(|e| e.kind()),
                Err(ErrorKind::UniqueConstraintViolation)
            ),
            "duplicate unique key must be rejected, got {:?}",
            dup.map(|_| ())
        );

        // 4. A brand-new unique key must still insert successfully (index is not corrupt).
        collection
            .insert(doc! {
                "account_id": 12_i64,
                "path": "/inbox/folder_new",
                "name": "Fresh",
            })
            .expect("inserting a new unique key must succeed");

        assert_eq!(
            collection.find(all()).expect("find all").count(),
            folder_count as usize + 1,
            "the new folder must be persisted alongside the originals"
        );

        db.close().expect("close second session");
    }

    let _ = fs::remove_dir_all(&path);
}

#[test]
fn failed_unique_insert_leaves_no_orphan_index_entry_across_reopen() {
    let path = random_path();

    {
        let db = open_db(&path);
        let collection = db.collection("account").expect("open collection");
        collection
            .create_index(vec!["email"], &unique_index())
            .expect("create unique index");

        collection
            .insert(doc! { "email": "a@example.com", "name": "A" })
            .expect("first insert");

        // This insert must fail the unique constraint; its data write and the partial index
        // write must both be rolled back atomically, leaving no orphaned index entry.
        let dup = collection.insert(doc! { "email": "a@example.com", "name": "B" });
        assert!(
            matches!(
                dup.as_ref().map_err(|e| e.kind()),
                Err(ErrorKind::UniqueConstraintViolation)
            ),
            "duplicate email must be rejected"
        );

        db.close().expect("close first session");
    }

    {
        let db = open_db(&path);
        let collection = db.collection("account").expect("reopen collection");

        // Exactly one account survives, and the email is reusable only by updating, never by a
        // second insert (i.e. the failed insert left nothing behind in the index).
        assert_eq!(collection.find(all()).expect("find all").count(), 1);
        assert_eq!(
            collection
                .find(field("email").eq("a@example.com"))
                .expect("indexed lookup")
                .count(),
            1
        );

        db.close().expect("close second session");
    }

    let _ = fs::remove_dir_all(&path);
}
