//! Indexing benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite::filter::field;
use nitrite::index::{non_unique_index, unique_index};
use nitrite_bench::data_gen::generate_simple_docs;
use nitrite_bench::stores::{create_fjall_db, create_inmemory_db};

fn bench_index_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index/Create");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_simple_docs(*size);

        // In-memory: create index on populated collection
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["age"], &non_unique_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["age"]).unwrap())
                },
            );
        });

        // Fjall: create index on populated collection
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["age"], &non_unique_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["age"]).unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_unique_index_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index/Create Unique");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_simple_docs(*size);

        // In-memory
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Create unique index on id field (which is already unique)
                    collection
                        .create_index(vec!["id"], &unique_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["id"]).unwrap())
                },
            );
        });

        // Fjall
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["id"], &unique_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["id"]).unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_indexed_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index/Indexed Search");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_simple_docs(*size);

        // In-memory: search using indexed field
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    collection
                        .create_index(vec!["age"], &non_unique_index())
                        .unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Search for specific age range
                    let filter = field("age").gte(30i64).and(field("age").lte(50i64));
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: search using indexed field
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    collection
                        .create_index(vec!["age"], &non_unique_index())
                        .unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let filter = field("age").gte(30i64).and(field("age").lte(50i64));
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

fn bench_non_indexed_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index/Non-Indexed Search");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_simple_docs(*size);

        // In-memory: search without index (for comparison)
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    // No index created
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let filter = field("age").gte(30i64).and(field("age").lte(50i64));
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: search without index
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let filter = field("age").gte(30i64).and(field("age").lte(50i64));
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_index_create,
    bench_unique_index_create,
    bench_indexed_search,
    bench_non_indexed_search
);
criterion_main!(benches);
