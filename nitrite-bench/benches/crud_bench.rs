//! CRUD operation benchmarks

use std::hint::black_box;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nitrite::filter::field;
use nitrite::index::non_unique_index;
use nitrite_bench::data_gen::{generate_simple_docs, generate_single_doc};
use nitrite_bench::stores::{create_fjall_db, create_inmemory_db};

/// Indexed `count()` on a warm database: the short-circuit answers from the index id set,
/// whereas the materialized variant fetches and deserializes every matching document (the old
/// behaviour). Both run on the same warmed collection so the difference isolates the
/// optimization rather than cold-start / index-build cost.
fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("Count/Indexed");

    for size in [1_000, 10_000].iter() {
        let docs = generate_simple_docs(*size);

        // Fast path: count() short-circuits to the index id-set length (no document fetch).
        group.bench_with_input(BenchmarkId::new("shortcircuit", size), &docs, |b, docs| {
            let ctx = create_fjall_db().unwrap();
            let coll = ctx.db().collection("bench").unwrap();
            coll.insert_many(docs.clone()).unwrap();
            coll.create_index(vec!["active"], &non_unique_index()).unwrap();
            let _ = coll.find(field("active").eq(true)).unwrap().count(); // warm partitions
            b.iter(|| black_box(coll.find(field("active").eq(true)).unwrap().count()));
        });

        // Old behaviour: materialize every matching document just to count them.
        group.bench_with_input(BenchmarkId::new("materialized", size), &docs, |b, docs| {
            let ctx = create_fjall_db().unwrap();
            let coll = ctx.db().collection("bench").unwrap();
            coll.insert_many(docs.clone()).unwrap();
            coll.create_index(vec!["active"], &non_unique_index()).unwrap();
            let _ = coll.find(field("active").eq(true)).unwrap().count(); // warm partitions
            b.iter(|| {
                // `fold` drives the cursor's `Iterator::next` (fetching each matching document),
                // bypassing the index-covered `count()` short-circuit to measure the old cost.
                black_box(
                    coll.find(field("active").eq(true))
                        .unwrap()
                        .fold(0usize, |n, _| n + 1),
                )
            });
        });
    }

    group.finish();
}

fn bench_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("CRUD/Insert Single");

    for size in [100, 1_000, 10_000].iter() {
        // In-memory benchmark
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("inmemory", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    for i in 0..size {
                        let doc = generate_single_doc(i);
                        collection.insert(doc).unwrap();
                    }
                    black_box(collection.size().unwrap())
                },
            );
        });

        // Fjall benchmark
        group.bench_with_input(BenchmarkId::new("fjall", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    for i in 0..size {
                        let doc = generate_single_doc(i);
                        collection.insert(doc).unwrap();
                    }
                    black_box(collection.size().unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("CRUD/Insert Batch");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_simple_docs(*size);

        group.throughput(Throughput::Elements(*size as u64));

        // In-memory benchmark
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    (ctx, collection, docs.clone())
                },
                |(_ctx, collection, docs)| {
                    collection.insert_many(docs).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });

        // Fjall benchmark
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    (ctx, collection, docs.clone())
                },
                |(_ctx, collection, docs)| {
                    collection.insert_many(docs).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("CRUD/Read");

    for size in [100, 1_000, 10_000].iter() {
        // Setup: pre-populate databases
        let docs = generate_simple_docs(*size);

        // In-memory benchmark
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    // Read all documents
                    let cursor = collection.find(nitrite::filter::all()).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall benchmark
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    let cursor = collection.find(nitrite::filter::all()).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("CRUD/Update");

    for size in [100, 1_000].iter() {
        let docs = generate_simple_docs(*size);

        // In-memory benchmark
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    let update = nitrite::doc! { active: false };
                    collection
                        .update(field("active").eq(true), &update)
                        .unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });

        // Fjall benchmark
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    let update = nitrite::doc! { active: false };
                    collection
                        .update(field("active").eq(true), &update)
                        .unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("CRUD/Delete");

    for size in [100, 1_000].iter() {
        let docs = generate_simple_docs(*size);

        // In-memory benchmark
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    collection.remove(nitrite::filter::all(), false).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });

        // Fjall benchmark
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(_ctx, collection)| {
                    collection.remove(nitrite::filter::all(), false).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_single,
    bench_insert_batch,
    bench_read,
    bench_update,
    bench_delete,
    bench_count
);
criterion_main!(benches);
