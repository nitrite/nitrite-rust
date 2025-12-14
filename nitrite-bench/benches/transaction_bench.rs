//! Transaction benchmarks - commit, rollback, and multi-operation transactions

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite::filter::field;
use nitrite_bench::stores::create_fjall_db;
use std::hint::black_box;
use uuid::Uuid;

fn bench_transaction_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("Transaction/Commit");
    group.sample_size(20);

    for &tx_count in [10, 50, 100].iter() {
        let docs_per_tx = 5;

        group.bench_with_input(
            BenchmarkId::new("transactions", tx_count),
            &(tx_count, docs_per_tx),
            |b, &(tx_count, docs_per_tx)| {
                b.iter_with_setup(
                    || create_fjall_db().unwrap(),
                    |ctx| {
                        let db = ctx.db();
                        let mut committed = 0;

                        for _ in 0..tx_count {
                            let result = db.with_session(|session| {
                                let tx = session.begin_transaction()?;
                                let collection = tx.collection("bench")?;

                                for _ in 0..docs_per_tx {
                                    let id = Uuid::new_v4().to_string();
                                    let doc = nitrite::doc! {
                                        id: id,
                                        name: "Test Doc",
                                        value: 42i64
                                    };
                                    collection.insert(doc)?;
                                }

                                tx.commit()
                            });

                            if result.is_ok() {
                                committed += 1;
                            }
                        }

                        black_box(committed)
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_transaction_rollback(c: &mut Criterion) {
    let mut group = c.benchmark_group("Transaction/Rollback");
    group.sample_size(20);

    for &tx_count in [10, 50, 100].iter() {
        let docs_per_tx = 5;

        group.bench_with_input(
            BenchmarkId::new("transactions", tx_count),
            &(tx_count, docs_per_tx),
            |b, &(tx_count, docs_per_tx)| {
                b.iter_with_setup(
                    || create_fjall_db().unwrap(),
                    |ctx| {
                        let db = ctx.db();
                        let mut rolled_back = 0;

                        for _ in 0..tx_count {
                            let result = db.with_session(|session| {
                                let tx = session.begin_transaction()?;
                                let collection = tx.collection("bench")?;

                                for _ in 0..docs_per_tx {
                                    let id = Uuid::new_v4().to_string();
                                    let doc = nitrite::doc! {
                                        id: id,
                                        name: "Test Doc",
                                        value: 42i64
                                    };
                                    collection.insert(doc)?;
                                }

                                tx.rollback()
                            });

                            if result.is_ok() {
                                rolled_back += 1;
                            }
                        }

                        black_box(rolled_back)
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_transaction_multi_op(c: &mut Criterion) {
    let mut group = c.benchmark_group("Transaction/MultiOp");
    group.sample_size(20);

    for &tx_count in [10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("transactions", tx_count),
            &tx_count,
            |b, &tx_count| {
                b.iter_with_setup(
                    || {
                        let ctx = create_fjall_db().unwrap();
                        // Pre-populate some data
                        let collection = ctx.db().collection("bench").unwrap();
                        for i in 0..100 {
                            let id = i as i64;
                            let value = i as i64;
                            let doc = nitrite::doc! {
                                id: id,
                                name: "Seed Doc",
                                value: value
                            };
                            let _ = collection.insert(doc);
                        }
                        ctx
                    },
                    |ctx| {
                        let db = ctx.db();
                        let mut success = 0;

                        for _ in 0..tx_count {
                            let result = db.with_session(|session| {
                                let tx = session.begin_transaction()?;
                                let collection = tx.collection("bench")?;

                                // Insert
                                for _ in 0..3 {
                                    let id = Uuid::new_v4().to_string();
                                    let doc = nitrite::doc! {
                                        id: id,
                                        name: "TX Doc",
                                        value: 100i64
                                    };
                                    collection.insert(doc)?;
                                }

                                // Find
                                let _count = collection.find(field("value").gte(0i64))?.count();

                                tx.commit()
                            });

                            if result.is_ok() {
                                success += 1;
                            }
                        }

                        black_box(success)
                    },
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_transaction_commit,
    bench_transaction_rollback,
    bench_transaction_multi_op
);
criterion_main!(benches);
