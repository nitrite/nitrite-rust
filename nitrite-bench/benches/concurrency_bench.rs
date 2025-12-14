//! Concurrency benchmarks - multi-threaded insert, read, and mixed workload

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite::filter::all;
use nitrite_bench::data_gen::generate_simple_docs;
use nitrite_bench::stores::create_fjall_db;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn bench_concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("Concurrency/Insert");
    group.sample_size(10); // Fewer samples for expensive concurrent tests

    for &doc_count in [1_000, 5_000].iter() {
        for &thread_count in [2, 4, 8].iter() {
            if thread_count > num_cpus::get() * 2 {
                continue;
            }

            let docs_per_thread = doc_count / thread_count;

            group.bench_with_input(
                BenchmarkId::new(format!("{}_threads", thread_count), doc_count),
                &(doc_count, thread_count, docs_per_thread),
                |b, &(doc_count, thread_count, docs_per_thread)| {
                    b.iter_with_setup(
                        || {
                            let ctx = create_fjall_db().unwrap();
                            let db = ctx.db().clone();
                            (ctx, db)
                        },
                        |(_ctx, db)| {
                            let db = Arc::new(db);
                            let success_count = Arc::new(AtomicUsize::new(0));

                            let handles: Vec<_> = (0..thread_count)
                                .map(|_| {
                                    let db = Arc::clone(&db);
                                    let success = Arc::clone(&success_count);
                                    let docs = generate_simple_docs(docs_per_thread);

                                    thread::spawn(move || {
                                        let collection = db.collection("bench").unwrap();
                                        for doc in docs {
                                            if collection.insert(doc).is_ok() {
                                                success.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    })
                                })
                                .collect();

                            for handle in handles {
                                let _ = handle.join();
                            }

                            black_box(success_count.load(Ordering::Relaxed))
                        },
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_concurrent_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Concurrency/Read");
    group.sample_size(10);

    for &doc_count in [1_000, 5_000].iter() {
        for &thread_count in [2, 4, 8].iter() {
            if thread_count > num_cpus::get() * 2 {
                continue;
            }

            let reads_per_thread = (doc_count / thread_count).max(100);

            group.bench_with_input(
                BenchmarkId::new(format!("{}_threads", thread_count), doc_count),
                &(doc_count, thread_count, reads_per_thread),
                |b, &(doc_count, thread_count, reads_per_thread)| {
                    b.iter_with_setup(
                        || {
                            let ctx = create_fjall_db().unwrap();
                            let collection = ctx.db().collection("bench").unwrap();
                            let docs = generate_simple_docs(doc_count);
                            collection.insert_many(docs).unwrap();
                            let db = ctx.db().clone();
                            (ctx, db)
                        },
                        |(_ctx, db)| {
                            let db = Arc::new(db);
                            let success_count = Arc::new(AtomicUsize::new(0));

                            let handles: Vec<_> = (0..thread_count)
                                .map(|_| {
                                    let db = Arc::clone(&db);
                                    let success = Arc::clone(&success_count);

                                    thread::spawn(move || {
                                        let collection = db.collection("bench").unwrap();
                                        for _ in 0..reads_per_thread {
                                            if let Ok(cursor) = collection.find(all()) {
                                                if cursor.take(1).next().is_some() {
                                                    success.fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                    })
                                })
                                .collect();

                            for handle in handles {
                                let _ = handle.join();
                            }

                            black_box(success_count.load(Ordering::Relaxed))
                        },
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("Concurrency/Mixed");
    group.sample_size(10);

    for &doc_count in [1_000, 5_000].iter() {
        for &thread_count in [2, 4, 8].iter() {
            if thread_count > num_cpus::get() * 2 {
                continue;
            }

            let ops_per_thread = (doc_count / thread_count).max(50);

            group.bench_with_input(
                BenchmarkId::new(format!("{}_threads", thread_count), doc_count),
                &(doc_count, thread_count, ops_per_thread),
                |b, &(doc_count, thread_count, ops_per_thread)| {
                    b.iter_with_setup(
                        || {
                            let ctx = create_fjall_db().unwrap();
                            let collection = ctx.db().collection("bench").unwrap();
                            // Pre-populate with half the documents
                            let docs = generate_simple_docs(doc_count / 2);
                            collection.insert_many(docs).unwrap();
                            let db = ctx.db().clone();
                            (ctx, db)
                        },
                        |(_ctx, db)| {
                            let db = Arc::new(db);
                            let success_count = Arc::new(AtomicUsize::new(0));

                            let handles: Vec<_> = (0..thread_count)
                                .map(|_| {
                                    let db = Arc::clone(&db);
                                    let success = Arc::clone(&success_count);

                                    thread::spawn(move || {
                                        let collection = db.collection("bench").unwrap();

                                        for i in 0..ops_per_thread {
                                            let op = i % 4; // 25% writes, 75% reads

                                            match op {
                                                0 => {
                                                    // Write operation
                                                    let docs = generate_simple_docs(1);
                                                    if collection.insert_many(docs).is_ok() {
                                                        success.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                }
                                                _ => {
                                                    // Read operation
                                                    if let Ok(cursor) = collection.find(all()) {
                                                        let _: Vec<_> = cursor.take(10).collect();
                                                        success.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                }
                                            }
                                        }
                                    })
                                })
                                .collect();

                            for handle in handles {
                                let _ = handle.join();
                            }

                            black_box(success_count.load(Ordering::Relaxed))
                        },
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_insert,
    bench_concurrent_read,
    bench_mixed_workload
);
criterion_main!(benches);
