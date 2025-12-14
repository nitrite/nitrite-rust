use std::sync::{Arc, Mutex};
use std::thread;

use fake::faker::name::en::FirstName;
use fake::Fake;
use nitrite::common::{Attributes, Value};
use nitrite::doc;
use nitrite::errors::ErrorKind;
use nitrite::filter::{all, field};
use nitrite::index::{full_text_index, non_unique_index, unique_index};
use nitrite::transaction::TransactionState;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

// ==================== Commit Tests ====================

#[test]
fn test_commit_insert() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let document = doc! {"firstName": "John"};
                tx_col.insert(document)?;

                // Verify in transaction
                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 1);
                // Not visible in main collection yet
                assert_ne!(collection.find(field("firstName").eq("John"))?.count(), 1);

                transaction.commit()?;
                Ok(())
            })?;

            // Now visible in main collection
            assert_eq!(collection.find(field("firstName").eq("John"))?.count(), 1);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_commit_multiple_inserts() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                for i in 0..5 {
                    let name = format!("doc_{}", i);
                    let doc = doc! {"index": i, "name": (name)};
                    tx_col.insert(doc)?;
                }

                assert_eq!(tx_col.find(all())?.count(), 5);
                assert_eq!(collection.find(all())?.count(), 0);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(collection.find(all())?.count(), 5);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rollback_insert() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;
            collection.create_index(vec!["firstName"], &unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let document = doc! {"firstName": "John"};
                let document2 = doc! {"firstName": "Jane", "lastName": "Doe"};
                tx_col.insert(document)?;
                tx_col.insert(document2)?;

                // Create conflict - insert same firstName in main collection
                collection.insert(doc! {"firstName": "Jane"})?;

                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 1);
                assert_ne!(collection.find(field("lastName").eq("Doe"))?.count(), 1);

                // Commit should fail due to unique constraint violation
                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            // After failed commit, verify rollback behavior
            assert_ne!(collection.find(field("firstName").eq("John"))?.count(), 1);
            assert_ne!(collection.find(field("lastName").eq("Doe"))?.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_commit_update() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let update = doc! {"firstName": "John", "lastName": "Doe"};
                tx_col.update_with_options(
                    field("firstName").eq("John"),
                    &update,
                    &nitrite::collection::UpdateOptions::new(true, false),
                )?;

                assert_eq!(tx_col.find(field("lastName").eq("Doe"))?.count(), 1);
                assert_ne!(collection.find(field("lastName").eq("Doe"))?.count(), 1);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(collection.find(field("lastName").eq("Doe"))?.count(), 1);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rollback_update() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;
            collection.create_index(vec!["firstName"], &unique_index())?;
            collection.insert(doc! {"firstName": "Jane"})?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let document = doc! {"firstName": "John"};
                let document2 = doc! {"firstName": "Jane", "lastName": "Doe"};
                tx_col.update_with_options(
                    field("firstName").eq("Jane"),
                    &document2,
                    &nitrite::collection::UpdateOptions::default(),
                )?;
                tx_col.insert(document)?;

                // Create conflict
                collection.insert(doc! {"firstName": "John"})?;

                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 1);
                assert_eq!(tx_col.find(field("lastName").eq("Doe"))?.count(), 1);
                assert_ne!(collection.find(field("lastName").eq("Doe"))?.count(), 1);

                // Commit should fail
                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            // Verify rollback
            assert_eq!(collection.find(field("firstName").eq("Jane"))?.count(), 1);
            assert_ne!(collection.find(field("lastName").eq("Doe"))?.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_commit_remove() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.remove(field("firstName").eq("John"), false)?;

                // In transaction: removed
                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 0);
                // In main collection: still exists
                assert_eq!(collection.find(field("firstName").eq("John"))?.count(), 1);

                transaction.commit()?;
                Ok(())
            })?;

            // Now removed from main collection
            assert_eq!(collection.find(field("firstName").eq("John"))?.count(), 0);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rollback_remove() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;
            collection.create_index(vec!["firstName"], &unique_index())?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.remove(field("firstName").eq("John"), false)?;

                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 0);
                assert_eq!(collection.find(field("firstName").eq("John"))?.count(), 1);

                // Insert a conflicting document
                tx_col.insert(doc! {"firstName": "Jane"})?;
                collection.insert(doc! {"firstName": "Jane"})?;

                // Commit should fail
                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            // Original document should still exist, and conflicting one too
            assert_eq!(collection.find(field("firstName").eq("John"))?.count(), 1);
            assert_eq!(collection.find(field("firstName").eq("Jane"))?.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Index Tests ====================

#[test]
fn test_commit_create_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.create_index(vec!["firstName"], &full_text_index())?;

                assert!(tx_col.has_index(vec!["firstName"])?);
                // Index operations are auto-committed
                assert!(collection.has_index(vec!["firstName"])?);

                transaction.commit()?;
                Ok(())
            })?;

            assert!(collection.has_index(vec!["firstName"])?);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_commit_drop_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;
            collection.create_index(vec!["firstName"], &non_unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.drop_index(vec!["firstName"])?;

                assert!(!tx_col.has_index(vec!["firstName"])?);
                // Index operations are auto-committed
                assert!(!collection.has_index(vec!["firstName"])?);

                transaction.commit()?;
                Ok(())
            })?;

            assert!(!collection.has_index(vec!["firstName"])?);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_commit_drop_all_indices() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;
            collection.create_index(vec!["firstName"], &non_unique_index())?;
            collection.create_index(vec!["lastName"], &non_unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.drop_all_indexes()?;

                assert!(!tx_col.has_index(vec!["firstName"])?);
                assert!(!tx_col.has_index(vec!["lastName"])?);
                // Index operations are auto-committed
                assert!(!collection.has_index(vec!["firstName"])?);
                assert!(!collection.has_index(vec!["lastName"])?);

                transaction.commit()?;
                Ok(())
            })?;

            assert!(!collection.has_index(vec!["firstName"])?);
            assert!(!collection.has_index(vec!["lastName"])?);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Clear Tests ====================

#[test]
fn test_commit_clear() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.clear()?;

                assert_eq!(tx_col.size()?, 0);
                // Clear is auto-committed
                assert_eq!(collection.size()?, 0);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(collection.size()?, 0);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Drop Collection Tests ====================

#[test]
fn test_commit_drop_collection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            let document = doc! {"firstName": "John"};
            collection.insert(document)?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.dispose()?;
                Ok(())
            })?;

            // Drop is auto-committed
            assert!(!db.has_collection("test")?);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Attribute Tests ====================

#[test]
fn test_commit_set_attribute() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let mut attributes = Attributes::new();
                attributes.put("key", Value::from("value"));
                tx_col.set_attributes(attributes)?;

                // Attributes should not be visible before commit
                assert!(collection.attributes()?.is_none());

                transaction.commit()?;
                Ok(())
            })?;

            // Now visible
            let attrs = collection.attributes()?.unwrap();
            assert_eq!(attrs.get("key").unwrap(), &Value::from("value"));
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rollback_set_attribute() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;
            collection.create_index(vec!["firstName"], &unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let mut attributes = Attributes::new();
                attributes.put("key", Value::from("value"));
                tx_col.set_attributes(attributes)?;

                tx_col.insert(doc! {"firstName": "John"})?;
                tx_col.insert(doc! {"firstName": "Jane", "lastName": "Doe"})?;

                assert!(collection.attributes()?.is_none());

                // Create conflict
                collection.insert(doc! {"firstName": "Jane"})?;

                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 1);
                assert_ne!(collection.find(field("lastName").eq("Doe"))?.count(), 1);

                // Commit should fail
                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            // Attributes should not be set
            let attrs = collection.attributes()?;
            if let Some(attrs) = attrs {
                assert!(attrs.get("key").is_none());
            }

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Multi-Collection Tests ====================

#[test]
fn test_transaction_on_different_collections() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let col1 = db.collection("test1")?;
            let col2 = db.collection("test2")?;
            let col3 = db.collection("test3")?;
            col3.create_index(vec!["id"], &unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                let test1 = transaction.collection("test1")?;
                let test2 = transaction.collection("test2")?;
                let test3 = transaction.collection("test3")?;

                for i in 0..10 {
                    let name: String = FirstName().fake();
                    let document = doc! {"firstName": (name.clone()), "id": i};
                    test1.insert(document)?;

                    let name: String = FirstName().fake();
                    let id = i + 10;
                    let document = doc! {"firstName": (name.clone()), "id": id};
                    test2.insert(document)?;

                    let name: String = FirstName().fake();
                    let id = i + 20;
                    let document = doc! {"firstName": (name.clone()), "id": id};
                    test3.insert(document)?;
                }

                assert_eq!(test1.size()?, 10);
                assert_eq!(test2.size()?, 10);
                assert_eq!(test3.size()?, 10);

                assert_eq!(col1.size()?, 0);
                assert_eq!(col2.size()?, 0);
                assert_eq!(col3.size()?, 0);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(col1.size()?, 10);
            assert_eq!(col2.size()?, 10);
            assert_eq!(col3.size()?, 10);

            // Second transaction with conflict
            db.with_session(|session2| {
                let transaction2 = session2.begin_transaction()?;

                let test1_2 = transaction2.collection("test1")?;
                let test2_2 = transaction2.collection("test2")?;
                let test3_2 = transaction2.collection("test3")?;

                for i in 0..10 {
                    let name: String = FirstName().fake();
                    let id = i + 30;
                    let document = doc! {"firstName": (name.clone()), "id": id};
                    test1_2.insert(document)?;

                    let name: String = FirstName().fake();
                    let id = i + 40;
                    let document = doc! {"firstName": (name.clone()), "id": id};
                    test2_2.insert(document)?;

                    let name: String = FirstName().fake();
                    let id = i + 50;
                    let document = doc! {"firstName": (name.clone()), "id": id};
                    test3_2.insert(document)?;
                }

                assert_eq!(test1_2.size()?, 20);
                assert_eq!(test2_2.size()?, 20);
                assert_eq!(test3_2.size()?, 20);

                assert_eq!(col1.size()?, 10);
                assert_eq!(col2.size()?, 10);
                assert_eq!(col3.size()?, 10);

                // Create conflict in col3
                let name: String = FirstName().fake();
                col3.insert(doc! {"firstName": name, "id": 52})?;

                let commit_result = transaction2.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            assert_eq!(col1.size()?, 10);
            assert_eq!(col2.size()?, 10);
            assert_eq!(col3.size()?, 11); // last document added

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Concurrent Tests ====================

#[test]
fn test_concurrent_insert() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let _collection = db.collection("test")?;

            let db_clone = Arc::new(db.clone());
            let completed = Arc::new(Mutex::new(0u32));
            let mut handles = vec![];

            for i in 0..10 {
                let db_ref = Arc::clone(&db_clone);
                let completed_clone = Arc::clone(&completed);

                let handle = thread::spawn(move || {
                    // Create a new session per thread to avoid concurrent access issues
                    let result = db_ref.with_session(|session| {
                        let transaction = session.begin_transaction()?;
                        match transaction.collection("test") {
                            Ok(tx_col) => {
                                let mut insert_ok = true;
                                for j in 0..10 {
                                    let name: String = FirstName().fake();
                                    let document =
                                        doc! {"firstName": name, "threadId": i, "docId": j};
                                    if tx_col.insert(document).is_err() {
                                        insert_ok = false;
                                        break;
                                    }
                                }

                                if insert_ok {
                                    match transaction.commit() {
                                        Ok(_) => {
                                            let mut count = completed_clone.lock().unwrap();
                                            *count += 1;
                                        }
                                        Err(e) => {
                                            eprintln!("Transaction commit failed: {:?}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Collection access failed: {:?}", e);
                            }
                        }
                        Ok(())
                    });

                    if let Err(e) = result {
                        eprintln!("Session failed: {:?}", e);
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let collection = db.collection("test")?;
            // At least some transactions should have succeeded
            let count = *completed.lock().unwrap();
            assert!(count > 0, "At least one transaction should have succeeded");
            assert!(
                collection.size()? >= 10,
                "At least 10 documents should be inserted"
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_concurrent_insert_and_remove() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;
            collection.create_index(vec!["id"], &non_unique_index())?;

            let db_clone = Arc::new(db.clone());
            let completed = Arc::new(Mutex::new(0u32));
            let mut handles = vec![];

            for i in 0..10 {
                let db_ref = Arc::clone(&db_clone);
                let completed_clone = Arc::clone(&completed);
                let fi = i as i64;

                let handle = thread::spawn(move || {
                    // Create separate session per thread
                    let result = db_ref.with_session(|session| {
                        let transaction = session.begin_transaction()?;
                        match transaction.collection("test") {
                            Ok(tx_col) => {
                                for j in 0..10 {
                                    let name: String = FirstName().fake();
                                    let id = j + (fi * 10);
                                    let document = doc! {"firstName": name, "id": id};
                                    if let Err(e) = tx_col.insert(document) {
                                        eprintln!("Insert failed: {:?}", e);
                                        return Ok(());
                                    }
                                }

                                // Remove one document
                                let id_to_remove = 2 + (fi * 10);
                                let _ = tx_col.remove(field("id").eq(id_to_remove), false);

                                match transaction.commit() {
                                    Ok(_) => {
                                        let mut count = completed_clone.lock().unwrap();
                                        *count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!("Transaction commit failed: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Collection access failed: {:?}", e);
                            }
                        }
                        Ok(())
                    });

                    if let Err(e) = result {
                        eprintln!("Session failed: {:?}", e);
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let _collection = db.collection("test")?;
            // Verify some transactions completed
            let count = *completed.lock().unwrap();
            assert!(count > 0, "At least one transaction should have succeeded");

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_concurrent_update() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            // Pre-populate with documents
            for i in 0..10 {
                collection.insert(doc! {"id": i})?;
            }

            let db_clone = Arc::new(db.clone());
            // let completed = Arc::new(AtomicU32::new(0));
            let mut handles = vec![];

            for _ in 0..10 {
                let db_ref = Arc::clone(&db_clone);
                // let completed_clone = Arc::clone(&completed);

                let handle = thread::spawn(move || {
                    // Create a separate session per thread
                    let result = db_ref.with_session(|session| {
                        let transaction = session.begin_transaction()?;
                        match transaction.collection("test") {
                            Ok(tx_col) => {
                                for j in 0..10 {
                                    let name: String = FirstName().fake();
                                    let document = doc! {"firstName": name, "id": j};
                                    if let Err(e) = tx_col.update_with_options(
                                        field("id").eq(j),
                                        &document,
                                        &nitrite::collection::UpdateOptions::new(true, false),
                                    ) {
                                        eprintln!("Update failed: {:?}", e);
                                        return Ok(());
                                    }
                                }

                                match transaction.commit() {
                                    Ok(_) => {
                                        // completed_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    Err(e) => {
                                        eprintln!("Transaction commit failed: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Collection access failed: {:?}", e);
                            }
                        }
                        Ok(())
                    });

                    if let Err(e) = result {
                        eprintln!("Session failed: {:?}", e);
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let collection = db.collection("test")?;
            assert_eq!(collection.size()?, 10);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ==================== Edge Cases and Negative Tests ====================

#[test]
fn test_failure_on_closed_transaction() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let col = transaction.collection("test")?;

                col.insert(doc! {"id": 1})?;
                transaction.commit()?;

                // Transaction is now closed, operations should fail
                let result = col.insert(doc! {"id": 2});
                assert!(result.is_err());
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_commit_after_commit_fails() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.insert(doc! {"id": 1})?;
                transaction.commit()?;

                // Second commit should fail
                let result = transaction.commit();
                assert!(result.is_err());
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rollback_after_commit_succeeds() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.insert(doc! {"id": 1})?;
                transaction.commit()?;

                // Rollback after commit should succeed (transaction already closed)
                let result = transaction.rollback();
                assert!(result.is_ok());
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rollback_idempotent() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.insert(doc! {"id": 1})?;

                transaction.rollback()?;
                transaction.rollback()?;
                transaction.rollback()?;

                // Multiple rollbacks should succeed
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_state_transitions() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                assert_eq!(transaction.state(), TransactionState::Active);

                let tx_col = transaction.collection("test")?;
                tx_col.insert(doc! {"id": 1})?;

                assert_eq!(transaction.state(), TransactionState::Active);

                transaction.commit()?;

                assert_eq!(transaction.state(), TransactionState::Closed);
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_session_close_rolls_back_transactions() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.insert(doc! {"firstName": "John"})?;

                // Close session without commit - with_session handles session.close()
                // The session will be closed automatically after this closure returns
                Ok(())
            })?;

            // Document should not be in main collection (session was closed, transaction rolled back)
            assert_eq!(collection.find(field("firstName").eq("John"))?.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_empty_transaction_commit() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                // Commit without any operations
                let result = transaction.commit();
                assert!(result.is_ok());
                assert_eq!(transaction.state(), TransactionState::Closed);
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_empty_transaction_rollback() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                // Rollback without any operations
                let result = transaction.rollback();
                assert!(result.is_ok());
                assert_eq!(transaction.state(), TransactionState::Closed);
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_collection_isolation() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let collection = db.collection("test")?;

            // Insert into main collection
            collection.insert(doc! {"name": "original"})?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                // Insert in transaction
                tx_col.insert(doc! {"name": "transactional"})?;

                // Transaction should see both documents
                assert_eq!(tx_col.find(all())?.count(), 2);

                // Main collection should only see original
                assert_eq!(collection.find(all())?.count(), 1);

                transaction.rollback()?;
                Ok(())
            })?;

            // After rollback, main collection still only has original
            assert_eq!(collection.find(all())?.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_find_with_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                tx_col.insert(doc! {"firstName": "John", "lastName": "Doe", "age": 30})?;
                tx_col.insert(doc! {"firstName": "Jane", "lastName": "Doe", "age": 25})?;
                tx_col.insert(doc! {"firstName": "Bob", "lastName": "Smith", "age": 35})?;

                // Find by first name
                assert_eq!(tx_col.find(field("firstName").eq("John"))?.count(), 1);

                // Find by last name
                assert_eq!(tx_col.find(field("lastName").eq("Doe"))?.count(), 2);

                // Find by age
                assert_eq!(tx_col.find(field("age").gt(28))?.count(), 2);

                // Complex filter
                assert_eq!(
                    tx_col
                        .find(field("lastName").eq("Doe").and(field("age").lt(30)))?
                        .count(),
                    1
                );

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_get_by_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                let doc = doc! {"name": "Test"};
                let result = tx_col.insert(doc)?;
                let id = &result.affected_nitrite_ids()[0];

                let found = tx_col.get_by_id(id)?;
                assert!(found.is_some());
                assert_eq!(found.unwrap().get("name")?.as_string().unwrap(), "Test");

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_size() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("test")?;

                assert_eq!(tx_col.size()?, 0);

                tx_col.insert(doc! {"a": 1})?;
                assert_eq!(tx_col.size()?, 1);

                tx_col.insert(doc! {"b": 2})?;
                assert_eq!(tx_col.size()?, 2);

                tx_col.insert(doc! {"c": 3})?;
                assert_eq!(tx_col.size()?, 3);

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_collection_name() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_col = transaction.collection("my_test_collection")?;

                assert_eq!(tx_col.name(), "my_test_collection");

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_multiple_sessions() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();

            // Create two separate sessions via with_session
            // First session
            db.with_session(|session1| {
                let tx1 = session1.begin_transaction()?;
                let col1 = tx1.collection("test")?;
                col1.insert(doc! {"source": "tx1"})?;
                assert_eq!(col1.find(field("source").eq("tx1"))?.count(), 1);
                tx1.commit()?;
                Ok(())
            })?;

            // Second session
            db.with_session(|session2| {
                let tx2 = session2.begin_transaction()?;
                let col2 = tx2.collection("test")?;
                col2.insert(doc! {"source": "tx2"})?;
                assert_eq!(col2.find(field("source").eq("tx2"))?.count(), 1);
                tx2.commit()?;
                Ok(())
            })?;

            let collection = db.collection("test")?;
            assert_eq!(collection.size()?, 2);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_pending_operations() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                assert_eq!(transaction.pending_operations(), 0);

                let tx_col = transaction.collection("test")?;
                tx_col.insert(doc! {"a": 1})?;

                // After insert, there should be pending operations
                assert!(transaction.pending_operations() >= 1);

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_transaction_collection_names() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                assert!(transaction.collection_names().is_empty());

                let _col1 = transaction.collection("col1")?;
                let _col2 = transaction.collection("col2")?;
                let _col3 = transaction.collection("col3")?;

                let names = transaction.collection_names();
                assert_eq!(names.len(), 3);
                assert!(names.contains(&"col1".to_string()));
                assert!(names.contains(&"col2".to_string()));
                assert!(names.contains(&"col3".to_string()));

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_session_on_closed_fails() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                session.close()?;

                let result = session.begin_transaction();
                assert!(result.is_err());
                if let Err(e) = result {
                    assert_eq!(*e.kind(), ErrorKind::InvalidOperation);
                }
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_session_active_transactions() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                assert!(session.active_transactions().is_empty());

                let tx1 = session.begin_transaction()?;
                let tx2 = session.begin_transaction()?;

                assert_eq!(session.active_transactions().len(), 2);
                assert!(session
                    .active_transactions()
                    .contains(&tx1.id().to_string()));
                assert!(session
                    .active_transactions()
                    .contains(&tx2.id().to_string()));

                session.close()?;

                assert!(session.active_transactions().is_empty());
                Ok(())
            })?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_unique_transaction_ids() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let tx1 = session.begin_transaction()?;
                let tx2 = session.begin_transaction()?;
                let tx3 = session.begin_transaction()?;

                assert_ne!(tx1.id(), tx2.id());
                assert_ne!(tx2.id(), tx3.id());
                assert_ne!(tx1.id(), tx3.id());
                Ok(())
            })?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_unique_session_ids() {
    run_test(
        || create_test_context(),
        |ctx| {
            let db = ctx.db();
            let mut session_ids = vec![];

            // Create three separate sessions and collect their IDs
            db.with_session(|session1| {
                session_ids.push(session1.id().to_string());
                Ok(())
            })?;

            db.with_session(|session2| {
                session_ids.push(session2.id().to_string());
                Ok(())
            })?;

            db.with_session(|session3| {
                session_ids.push(session3.id().to_string());
                Ok(())
            })?;

            assert_ne!(session_ids[0], session_ids[1]);
            assert_ne!(session_ids[1], session_ids[2]);
            assert_ne!(session_ids[0], session_ids[2]);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
