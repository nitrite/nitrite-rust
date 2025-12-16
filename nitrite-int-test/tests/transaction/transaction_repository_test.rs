use std::sync::{Arc, Mutex};
use std::thread;

use fake::faker::name::en::FirstName;
use fake::Fake;
use nitrite::common::{Attributes, Value};
use nitrite::filter::{all, field};
use nitrite::index::{full_text_index, non_unique_index, unique_index};
use nitrite::repository::ObjectRepository;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

use crate::transaction::{SubEmployee, TxData};

// ==================== Basic Repository Transaction Tests ====================

#[test]
fn test_commit_insert_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                let tx_data = TxData::new(1, "John");
                tx_repo.insert(tx_data)?;

                assert_eq!(tx_repo.find(field("name").eq("John"))?.count(), 1);
                assert_ne!(repository.find(field("name").eq("John"))?.count(), 1);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(repository.find(field("name").eq("John"))?.count(), 1);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_rollback_insert_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.create_index(vec!["name"], &unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                let tx_data1 = TxData::new(1, "John");
                let tx_data2 = TxData::new(2, "Jane");
                tx_repo.insert(tx_data1)?;
                tx_repo.insert(tx_data2)?;

                // Create conflict
                let conflict_data = TxData::new(3, "Molly");
                repository.insert(conflict_data)?;

                // Also insert a document with different name but same conflict potential
                repository.update_one(TxData::new(2, "Jane"), true)?;

                assert_eq!(tx_repo.find(field("name").eq("John"))?.count(), 1);

                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            assert_eq!(repository.find(field("name").eq("John"))?.count(), 0);
            assert_eq!(repository.find(field("name").eq("Molly"))?.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_commit_update_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                let tx_data1 = TxData::new(1, "Jane");
                tx_repo.update_one(tx_data1, true)?;

                assert_eq!(tx_repo.find(field("name").eq("Jane"))?.count(), 1);
                assert_ne!(repository.find(field("name").eq("Jane"))?.count(), 1);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(repository.find(field("name").eq("Jane"))?.count(), 1);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_rollback_update_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.keyed_repository("rollback")?;
            repository.create_index(vec!["name"], &unique_index())?;
            repository.insert(TxData::new(1, "Jane"))?;

            eprintln!(
                "Initial state - Jane count: {}",
                repository.find(field("name").eq("Jane"))?.count()
            );

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.keyed_repository("rollback")?;

                let tx_data1 = TxData::new(2, "John");
                let tx_data2 = TxData::new(1, "Jane Doe");

                eprintln!("Before update in tx");
                let update_result = tx_repo.update_one(tx_data2, false)?;
                eprintln!(
                    "After update in tx - affected_nitrite_ids: {:?}",
                    update_result.affected_nitrite_ids()
                );

                tx_repo.insert(tx_data1)?;
                eprintln!("After insert in tx");

                // Create conflict
                repository.insert(TxData::new(2, "John"))?;
                eprintln!("Created conflict");

                let john_count = tx_repo.find(field("name").eq("John"))?.count();
                eprintln!("John count in tx: {}", john_count);
                assert_eq!(john_count, 1);

                let jane_doe_count_tx = tx_repo.find(field("name").eq("Jane Doe"))?.count();
                eprintln!("Jane Doe count in tx: {}", jane_doe_count_tx);
                assert_eq!(jane_doe_count_tx, 1);

                let jane_doe_count_main = repository.find(field("name").eq("Jane Doe"))?.count();
                eprintln!("Jane Doe count in main: {}", jane_doe_count_main);
                assert_ne!(jane_doe_count_main, 1);

                let commit_result = transaction.commit();
                eprintln!("Commit result: {:?}", commit_result);
                assert!(commit_result.is_err());
                Ok(())
            })?;

            eprintln!(
                "After rollback - Jane count: {}",
                repository.find(field("name").eq("Jane"))?.count()
            );
            eprintln!(
                "After rollback - Jane Doe count: {}",
                repository.find(field("name").eq("Jane Doe"))?.count()
            );

            assert_eq!(repository.find(field("name").eq("Jane"))?.count(), 1);
            assert_ne!(repository.find(field("name").eq("Jane Doe"))?.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_commit_remove_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.remove(field("name").eq("John"), false)?;

                assert_eq!(tx_repo.find(field("name").eq("John"))?.count(), 0);
                assert_eq!(repository.find(field("name").eq("John"))?.count(), 1);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(repository.find(field("name").eq("John"))?.count(), 0);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_rollback_remove_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.create_index(vec!["name"], &unique_index())?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.remove(field("name").eq("John"), false)?;

                assert_eq!(tx_repo.find(field("name").eq("John"))?.count(), 0);
                assert_eq!(repository.find(field("name").eq("John"))?.count(), 1);

                let tx_data2 = TxData::new(2, "Jane");
                tx_repo.insert(tx_data2.clone())?;
                repository.insert(tx_data2)?;

                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            assert_eq!(repository.find(field("name").eq("John"))?.count(), 1);
            assert_eq!(repository.find(field("name").eq("Jane"))?.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

// ==================== Index Tests ====================

#[test]
fn test_commit_create_index_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.create_index(vec!["name"], &full_text_index())?;

                assert!(tx_repo.has_index(vec!["name"])?);
                // Auto-committed
                assert!(repository.has_index(vec!["name"])?);

                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_commit_drop_index_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.create_index(vec!["name"], &non_unique_index())?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.drop_index(vec!["name"])?;

                assert!(!tx_repo.has_index(vec!["name"])?);
                // Auto-committed
                assert!(!repository.has_index(vec!["name"])?);

                transaction.commit()?;
                Ok(())
            })?;

            assert!(!repository.has_index(vec!["name"])?);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_commit_drop_all_indices_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.create_index(vec!["name"], &non_unique_index())?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.drop_all_indexes()?;

                assert!(!tx_repo.has_index(vec!["name"])?);
                assert!(!repository.has_index(vec!["name"])?);

                transaction.commit()?;
                Ok(())
            })?;

            assert!(!repository.has_index(vec!["name"])?);
            Ok(())
        },
        cleanup,
    )
}

// ==================== Clear Tests ====================

#[test]
fn test_commit_clear_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.clear()?;

                assert_eq!(tx_repo.size()?, 0);
                // Auto-committed
                assert_eq!(repository.size()?, 0);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(repository.size()?, 0);
            Ok(())
        },
        cleanup,
    )
}

// ==================== Drop Repository Tests ====================

#[test]
fn test_commit_drop_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.insert(TxData::new(1, "John"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.dispose()?;

                // After drop, the repo should be removed
                let _size_result = tx_repo.size();
                // Auto-committed
                assert!(!db.has_repository::<TxData>()?);

                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

// ==================== Attribute Tests ====================

#[test]
fn test_commit_set_attribute_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                let mut attributes = Attributes::new();
                attributes.put("key", Value::from("value"));
                tx_repo.set_attributes(attributes)?;

                assert!(repository.attributes()?.is_none());

                transaction.commit()?;
                Ok(())
            })?;

            let attrs = repository.attributes()?.unwrap();
            assert_eq!(attrs.get("key").unwrap(), &Value::from("value"));
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_rollback_set_attribute_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.create_index(vec!["name"], &unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                let mut attributes = Attributes::new();
                attributes.put("key", Value::from("value"));
                tx_repo.set_attributes(attributes)?;

                tx_repo.insert(TxData::new(1, "John"))?;
                tx_repo.insert(TxData::new(2, "Jane"))?;

                assert!(repository.attributes()?.is_none());

                // Create conflict
                repository.insert(TxData::new(2, "Jane"))?;

                assert_eq!(tx_repo.find(field("name").eq("John"))?.count(), 1);
                assert_ne!(repository.find(field("name").eq("John"))?.count(), 1);

                let commit_result = transaction.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            let attrs = repository.attributes()?;
            if let Some(attrs) = attrs {
                assert!(attrs.get("key").is_none());
            }

            Ok(())
        },
        cleanup,
    )
}

// ==================== Concurrent Tests ====================

#[test]
fn test_concurrent_insert_and_remove_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;
            repository.create_index(vec!["name"], &non_unique_index())?;

            db.with_session(|session| {
                let completed = Arc::new(Mutex::new(0u32));
                let mut handles = vec![];

                for i in 0..10 {
                    let session_clone = session.clone();
                    let completed_clone = completed.clone();
                    let fi = i as i64;

                    let handle = thread::spawn(move || {
                        let transaction = match session_clone.begin_transaction() {
                            Ok(tx) => tx,
                            Err(_) => return, // Exit thread early if transaction creation fails
                        };
                        let tx_repo: ObjectRepository<TxData> = match transaction.repository() {
                            Ok(repo) => repo,
                            Err(_) => return, // Exit thread early if repository initialization fails
                        };

                        for j in 0..10 {
                            let name: String = FirstName().fake();
                            let id = j + (fi * 10);
                            let _ = tx_repo.insert(TxData::new(id, &name)); // Don't unwrap, just ignore errors
                        }

                        let id_to_remove = 2 + (fi * 10);
                        tx_repo.remove(field("id").eq(id_to_remove), false).ok();

                        match transaction.commit() {
                            Ok(_) => {
                                let mut count = completed_clone.lock().unwrap();
                                *count += 1;
                            }
                            Err(e) => {
                                eprintln!("Transaction failed: {:?}", e);
                            }
                        }
                    });

                    handles.push(handle);
                }

                for handle in handles {
                    handle.join().unwrap();
                }

                let count = *completed.lock().unwrap();
                assert!(count > 0);
                Ok(())
            })?;

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_concurrent_insert_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let _repository: ObjectRepository<TxData> = db.repository()?;

            db.with_session(|session| {
                let completed = Arc::new(Mutex::new(0u32));
                let mut handles = vec![];

                for i in 0..10 {
                    let session_clone = session.clone();
                    let completed_clone = completed.clone();
                    let fi = i as i64;

                    let handle = thread::spawn(move || {
                        let transaction = match session_clone.begin_transaction() {
                            Ok(tx) => tx,
                            Err(_) => return, // Exit thread early if transaction creation fails
                        };
                        let tx_repo: ObjectRepository<TxData> = match transaction.repository() {
                            Ok(repo) => repo,
                            Err(_) => return, // Exit thread early if repository initialization fails
                        };

                        for j in 0..10 {
                            let name: String = FirstName().fake();
                            let id = j + (fi * 10);
                            let _ = tx_repo.insert(TxData::new(id, &name)); // Don't unwrap, just ignore errors
                        }

                        match transaction.commit() {
                            Ok(_) => {
                                let mut count = completed_clone.lock().unwrap();
                                *count += 1;
                            }
                            Err(e) => {
                                eprintln!("Transaction failed: {:?}", e);
                            }
                        }
                    });

                    handles.push(handle);
                }

                for handle in handles {
                    handle.join().unwrap();
                }

                let count = *completed.lock().unwrap();
                assert!(count > 0);
                Ok(())
            })?;

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_concurrent_update_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;

            for j in 0..10 {
                let name: String = FirstName().fake();
                repository.insert(TxData::new(j, &name))?;
            }

            db.with_session(|session| {
                let completed = Arc::new(Mutex::new(0u32));
                let mut handles = vec![];

                for _ in 0..10 {
                    let session_clone = session.clone();
                    let completed_clone = completed.clone();

                    let handle = thread::spawn(move || {
                        let transaction = match session_clone.begin_transaction() {
                            Ok(tx) => tx,
                            Err(_) => return, // Exit thread early if transaction creation fails
                        };
                        let tx_repo: ObjectRepository<TxData> = match transaction.repository() {
                            Ok(repo) => repo,
                            Err(_) => return, // Exit thread early if repository initialization fails
                        };

                        for j in 0..10 {
                            let name: String = FirstName().fake();
                            tx_repo
                                .update_with_options(
                                    field("id").eq(j),
                                    TxData::new(j, &name),
                                    &nitrite::collection::UpdateOptions::default(),
                                )
                                .ok();
                        }

                        match transaction.commit() {
                            Ok(_) => {
                                let mut count = completed_clone.lock().unwrap();
                                *count += 1;
                            }
                            Err(e) => {
                                eprintln!("Transaction failed: {:?}", e);
                            }
                        }
                    });

                    handles.push(handle);
                }

                for handle in handles {
                    handle.join().unwrap();
                }
                Ok(())
            })?;

            let repository: ObjectRepository<TxData> = db.repository()?;
            assert_eq!(repository.size()?, 10);

            Ok(())
        },
        cleanup,
    )
}

// ==================== Mixed Repository and Collection Tests ====================

#[test]
fn test_transaction_on_different_repositories_and_collections() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repo1: ObjectRepository<TxData> = db.repository()?;
            let repo2: ObjectRepository<TxData> = db.keyed_repository("2")?;
            let repo3: ObjectRepository<SubEmployee> = db.repository()?;
            let col1 = db.collection("test1")?;
            col1.create_index(vec!["id"], &unique_index())?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                let tx_repo1: ObjectRepository<TxData> = transaction.repository()?;
                let tx_repo2: ObjectRepository<TxData> = transaction.keyed_repository("2")?;
                let tx_repo3: ObjectRepository<SubEmployee> = transaction.repository()?;
                let test1 = transaction.collection("test1")?;

                for i in 0..10 {
                    let name: String = FirstName().fake();
                    let document = nitrite::doc! {"firstName": (name.clone()), "id": i};
                    test1.insert(document)?;

                    tx_repo1.insert(TxData::new(i, &name))?;
                    tx_repo2.insert(TxData::new(i + 10, &name))?;
                    tx_repo3.insert(SubEmployee::generate())?;
                }

                assert_eq!(test1.size()?, 10);
                assert_eq!(tx_repo1.size()?, 10);
                assert_eq!(tx_repo2.size()?, 10);
                assert_eq!(tx_repo3.size()?, 10);

                assert_eq!(col1.size()?, 0);
                assert_eq!(repo1.size()?, 0);
                assert_eq!(repo2.size()?, 0);
                assert_eq!(repo3.size()?, 0);

                transaction.commit()?;
                Ok(())
            })?;

            assert_eq!(col1.size()?, 10);
            assert_eq!(repo1.size()?, 10);
            assert_eq!(repo2.size()?, 10);
            assert_eq!(repo3.size()?, 10);

            // Second transaction with conflict
            db.with_session(|session| {
                let transaction2 = session.begin_transaction()?;

                let tx_repo1_2: ObjectRepository<TxData> = transaction2.repository()?;
                let tx_repo2_2: ObjectRepository<TxData> = transaction2.keyed_repository("2")?;
                let tx_repo3_2: ObjectRepository<SubEmployee> = transaction2.repository()?;
                let test1_2 = transaction2.collection("test1")?;

                for i in 0..10 {
                    let name: String = FirstName().fake();
                    let document = nitrite::doc! {"firstName": (name.clone()), "id": ((i + 10))};
                    test1_2.insert(document)?;

                    tx_repo1_2.insert(TxData::new(i + 10, &name))?;
                    tx_repo2_2.insert(TxData::new(i + 20, &name))?;
                    tx_repo3_2.insert(SubEmployee::generate())?;
                }

                assert_eq!(test1_2.size()?, 20);
                assert_eq!(tx_repo1_2.size()?, 20);
                assert_eq!(tx_repo2_2.size()?, 20);
                assert_eq!(tx_repo3_2.size()?, 20);

                assert_eq!(col1.size()?, 10);
                assert_eq!(repo1.size()?, 10);
                assert_eq!(repo2.size()?, 10);
                assert_eq!(repo3.size()?, 10);

                // Create conflict in col1
                let name: String = FirstName().fake();
                col1.insert(nitrite::doc! {"firstName": name, "id": 12_i64})?;

                let commit_result = transaction2.commit();
                assert!(commit_result.is_err());
                Ok(())
            })?;

            assert_eq!(col1.size()?, 11); // last doc added
            assert_eq!(repo1.size()?, 10);
            assert_eq!(repo2.size()?, 10);
            assert_eq!(repo3.size()?, 10);

            Ok(())
        },
        cleanup,
    )
}

// ==================== Edge Cases and Negative Tests ====================

#[test]
fn test_failure_on_closed_transaction_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let tx_repo_clone: std::sync::Arc<std::sync::Mutex<Option<ObjectRepository<TxData>>>> =
                std::sync::Arc::new(std::sync::Mutex::new(None));
            let tx_repo_clone2 = tx_repo_clone.clone();

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.insert(TxData::new(1, "John"))?;
                *tx_repo_clone2.lock().unwrap() = Some(tx_repo.clone());
                transaction.commit()?;
                Ok(())
            })?;

            let tx_repo = tx_repo_clone.lock().unwrap().take().unwrap();
            let result = tx_repo.insert(TxData::new(2, "Jane"));
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_keyed_repository_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                let tx_repo1: ObjectRepository<TxData> = transaction.keyed_repository("key1")?;
                let tx_repo2: ObjectRepository<TxData> = transaction.keyed_repository("key2")?;

                tx_repo1.insert(TxData::new(1, "John"))?;
                tx_repo2.insert(TxData::new(2, "Jane"))?;

                assert_eq!(tx_repo1.size()?, 1);
                assert_eq!(tx_repo2.size()?, 1);

                transaction.commit()?;
                Ok(())
            })?;

            let repo1: ObjectRepository<TxData> = db.keyed_repository("key1")?;
            let repo2: ObjectRepository<TxData> = db.keyed_repository("key2")?;

            assert_eq!(repo1.size()?, 1);
            assert_eq!(repo2.size()?, 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_find_with_filter_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.insert(TxData::new(1, "John"))?;
                tx_repo.insert(TxData::new(2, "Jane"))?;
                tx_repo.insert(TxData::new(3, "Bob"))?;

                assert_eq!(tx_repo.find(field("name").eq("John"))?.count(), 1);
                assert_eq!(tx_repo.find(field("id").gt(1_i64))?.count(), 2);
                assert_eq!(tx_repo.find(all())?.count(), 3);

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_get_by_id_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.insert(TxData::new(42, "TestEntity"))?;

                let found = tx_repo.get_by_id(&42_i64)?;
                assert!(found.is_some());
                assert_eq!(found.unwrap().name, "TestEntity");

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_size_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                assert_eq!(tx_repo.size()?, 0);

                tx_repo.insert(TxData::new(1, "A"))?;
                assert_eq!(tx_repo.size()?, 1);

                tx_repo.insert(TxData::new(2, "B"))?;
                assert_eq!(tx_repo.size()?, 2);

                tx_repo.insert(TxData::new(3, "C"))?;
                assert_eq!(tx_repo.size()?, 3);

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_insert_many_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                let entities = vec![
                    TxData::new(1, "A"),
                    TxData::new(2, "B"),
                    TxData::new(3, "C"),
                    TxData::new(4, "D"),
                    TxData::new(5, "E"),
                ];

                tx_repo.insert_many(entities)?;
                assert_eq!(tx_repo.size()?, 5);

                transaction.commit()?;
                Ok(())
            })?;

            let repository: ObjectRepository<TxData> = db.repository()?;
            assert_eq!(repository.size()?, 5);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_update_with_filter_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.insert(TxData::new(1, "Original"))?;

                let updated = TxData::new(1, "Updated");
                tx_repo.update_with_options(
                    field("id").eq(1_i64),
                    updated,
                    &nitrite::collection::UpdateOptions::default(),
                )?;

                let found = tx_repo.get_by_id(&1_i64)?;
                assert!(found.is_some());
                assert_eq!(found.unwrap().name, "Updated");

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_isolation_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let repository: ObjectRepository<TxData> = db.repository()?;

            repository.insert(TxData::new(1, "Original"))?;

            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.insert(TxData::new(2, "Transactional"))?;

                // Transaction sees both
                assert_eq!(tx_repo.find(all())?.count(), 2);

                // Main repository only sees original
                assert_eq!(repository.find(all())?.count(), 1);

                transaction.rollback()?;
                Ok(())
            })?;

            // After rollback, only original remains
            assert_eq!(repository.find(all())?.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_list_indexes_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                tx_repo.create_index(vec!["name"], &non_unique_index())?;
                tx_repo.create_index(vec!["id"], &unique_index())?;

                let indexes = tx_repo.list_indexes()?;
                assert!(indexes.len() >= 2);

                transaction.commit()?;
                Ok(())
            })?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_empty_repository_transaction_commit() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let _tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                // Commit without any operations
                let result = transaction.commit();
                assert!(result.is_ok());
                Ok(())
            })?;

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_empty_repository_transaction_rollback() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let _tx_repo: ObjectRepository<TxData> = transaction.repository()?;

                // Rollback without any operations
                let result = transaction.rollback();
                assert!(result.is_ok());
                Ok(())
            })?;

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_repository_with_complex_entity() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;
                let tx_repo: ObjectRepository<SubEmployee> = transaction.repository()?;

                let emp1 = SubEmployee::generate();
                let emp2 = SubEmployee::generate();
                let emp3 = SubEmployee::generate();

                tx_repo.insert(emp1)?;
                tx_repo.insert(emp2)?;
                tx_repo.insert(emp3)?;

                assert_eq!(tx_repo.size()?, 3);

                transaction.commit()?;
                Ok(())
            })?;

            let repository: ObjectRepository<SubEmployee> = db.repository()?;
            assert_eq!(repository.size()?, 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_multiple_repository_types_in_transaction() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            db.with_session(|session| {
                let transaction = session.begin_transaction()?;

                let tx_data_repo: ObjectRepository<TxData> = transaction.repository()?;
                let sub_emp_repo: ObjectRepository<SubEmployee> = transaction.repository()?;

                tx_data_repo.insert(TxData::new(1, "Data1"))?;
                tx_data_repo.insert(TxData::new(2, "Data2"))?;

                sub_emp_repo.insert(SubEmployee::generate())?;
                sub_emp_repo.insert(SubEmployee::generate())?;
                sub_emp_repo.insert(SubEmployee::generate())?;

                assert_eq!(tx_data_repo.size()?, 2);
                assert_eq!(sub_emp_repo.size()?, 3);

                transaction.commit()?;
                Ok(())
            })?;

            let data_repo: ObjectRepository<TxData> = db.repository()?;
            let emp_repo: ObjectRepository<SubEmployee> = db.repository()?;

            assert_eq!(data_repo.size()?, 2);
            assert_eq!(emp_repo.size()?, 3);

            Ok(())
        },
        cleanup,
    )
}
