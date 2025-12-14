//! Tantivy full-text search benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite_bench::data_gen::generate_fts_docs;
use nitrite_bench::stores::{create_fjall_fts_db, create_inmemory_fts_db};
use nitrite_tantivy_fts::{fts_field, fts_index};

fn bench_fts_index_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("FTS/Index Create");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_fts_docs(*size);

        // In-memory with FTS support
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["content"]).unwrap())
                },
            );
        });

        // Fjall with FTS support
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["content"]).unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_fts_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("FTS/Insert with Index");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_fts_docs(*size);

        // In-memory: insert into collection with existing FTS index
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    (ctx, collection, docs.clone())
                },
                |(ctx, collection, docs)| {
                    collection.insert_many(docs).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });

        // Fjall: insert into collection with existing FTS index
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    (ctx, collection, docs.clone())
                },
                |(ctx, collection, docs)| {
                    collection.insert_many(docs).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_fts_single_term_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("FTS/Single Term Search");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_fts_docs(*size);

        // In-memory: single term search
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Search for a common word
                    let filter = fts_field("content").matches("the");
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: single term search
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let filter = fts_field("content").matches("the");
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

fn bench_fts_phrase_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("FTS/Phrase Search");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_fts_docs(*size);

        // In-memory: phrase search
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Phrase search
                    let filter = fts_field("content").phrase("and the");
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: phrase search
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let filter = fts_field("content").phrase("and the");
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

fn bench_fts_multi_field_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("FTS/Multi-Field Index Search");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_fts_docs(*size);

        // In-memory: search with index on title field
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    // Create separate FTS indexes on title and content
                    collection
                        .create_index(vec!["title"], &fts_index())
                        .unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Search in title
                    let filter = fts_field("title").matches("the");
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: search with index on title field
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_fts_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["title"], &fts_index())
                        .unwrap();
                    collection
                        .create_index(vec!["content"], &fts_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let filter = fts_field("title").matches("the");
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
    bench_fts_index_create,
    bench_fts_insert,
    bench_fts_single_term_search,
    bench_fts_phrase_search,
    bench_fts_multi_field_search
);
criterion_main!(benches);
