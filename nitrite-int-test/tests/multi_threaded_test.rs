// Based on Java MultiThreadedTest.java
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite::index::non_unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_multi_threaded_insert() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let collection = Arc::new(db.collection("test")?);

            let num_threads = 5;
            let inserts_per_thread = 10;
            let barrier = Arc::new(Barrier::new(num_threads));

            let mut handles = vec![];

            for thread_id in 0..num_threads {
                let collection_clone = Arc::clone(&collection);
                let barrier_clone = Arc::clone(&barrier);

                let handle = thread::spawn(move || {
                    // Wait for all threads to be ready
                    barrier_clone.wait();

                    // Each thread inserts documents
                    for i in 0..inserts_per_thread {
                        let value = format!("thread_{}_seq_{}", thread_id, i);
                        let doc_obj = doc!{
                            "thread_id": thread_id,
                            "sequence": i,
                            "value": value
                        };
                        let _ = collection_clone.insert(doc_obj);
                    }
                });

                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                let _ = handle.join();
            }

            // Verify total count
            let cursor = collection.find(all())?;
            let total = cursor.count();
            assert_eq!(total, num_threads * inserts_per_thread);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_multi_threaded_mixed_operations() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            let collection = Arc::new(db.collection("test")?);

            // Create an index
            collection.create_index(vec!["thread_id"], &non_unique_index())?;

            let num_threads = 3;
            let barrier = Arc::new(Barrier::new(num_threads));

            let mut handles = vec![];

            for thread_id in 0..num_threads {
                let collection_clone = Arc::clone(&collection);
                let barrier_clone = Arc::clone(&barrier);

                let handle = thread::spawn(move || {
                    barrier_clone.wait();

                    // Insert some documents
                    for i in 0..5 {
                        let doc_obj = doc!{
                            "thread_id": thread_id,
                            "sequence": i,
                        };
                        let _ = collection_clone.insert(doc_obj);
                    }

                    // Query documents
                    let _ = collection_clone.find(
                        field("thread_id").eq(thread_id)
                    );
                });

                handles.push(handle);
            }

            // Wait for all threads
            for handle in handles {
                let _ = handle.join();
            }

            // Verify
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), num_threads * 5);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_multi_threaded_find() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert initial data
            for i in 0..20 {
                let value = i * 2;
                let doc_obj = doc!{"id": i, "value": value};
                collection.insert(doc_obj)?;
            }

            let collection_arc = Arc::new(collection);
            let num_threads = 4;
            let barrier = Arc::new(Barrier::new(num_threads));

            let mut handles = vec![];

            for _thread_id in 0..num_threads {
                let collection_clone = Arc::clone(&collection_arc);
                let barrier_clone = Arc::clone(&barrier);

                let handle = thread::spawn(move || {
                    barrier_clone.wait();

                    // Each thread performs finds
                    let cursor = collection_clone.find(all()).unwrap();
                    assert!(cursor.count() > 0);
                });

                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.join();
            }

            Ok(())
        },
        cleanup,
    )
}
