//! Spatial indexing and search benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite_bench::data_gen::generate_spatial_docs;
use nitrite_bench::stores::{create_fjall_spatial_db, create_inmemory_spatial_db};
use nitrite_spatial::{spatial_field, spatial_index, Geometry, Point};

fn bench_spatial_index_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("Spatial/Index Create");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_spatial_docs(*size);

        // In-memory with spatial support
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["location"]).unwrap())
                },
            );
        });

        // Fjall with spatial support
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    black_box(collection.has_index(vec!["location"]).unwrap())
                },
            );
        });
    }

    group.finish();
}

fn bench_spatial_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("Spatial/Insert with Index");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_spatial_docs(*size);

        // In-memory: insert into collection with existing spatial index
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    (ctx, collection, docs.clone())
                },
                |(ctx, collection, docs)| {
                    collection.insert_many(docs).unwrap();
                    black_box(collection.size().unwrap())
                },
            );
        });

        // Fjall: insert into collection with existing spatial index
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
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

fn bench_spatial_within_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("Spatial/Within Query");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_spatial_docs(*size);

        // In-memory: bounding box query
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Query for points in center 50% of the grid
                    let search_box = Geometry::envelope(250.0, 250.0, 750.0, 750.0);
                    let filter = spatial_field("location").within(search_box);
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: bounding box query
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let search_box = Geometry::envelope(250.0, 250.0, 750.0, 750.0);
                    let filter = spatial_field("location").within(search_box);
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

fn bench_spatial_near_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("Spatial/Near Query");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_spatial_docs(*size);

        // In-memory: proximity query
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    // Find points near center within radius
                    let center = Point::new(500.0, 500.0);
                    let filter = spatial_field("location").near(center, 200.0);
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: proximity query
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let center = Point::new(500.0, 500.0);
                    let filter = spatial_field("location").near(center, 200.0);
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });
    }

    group.finish();
}

fn bench_spatial_knearest_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("Spatial/K-Nearest Query");

    for size in [100, 1_000, 10_000].iter() {
        let docs = generate_spatial_docs(*size);

        // In-memory: k-nearest neighbor query
        group.bench_with_input(BenchmarkId::new("inmemory", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_inmemory_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let center = Point::new(500.0, 500.0);
                    let filter = spatial_field("location").knearest(center, 10).unwrap();
                    let cursor = collection.find(filter).unwrap();
                    black_box(cursor.count())
                },
            );
        });

        // Fjall: k-nearest neighbor query
        group.bench_with_input(BenchmarkId::new("fjall", size), &docs, |b, docs| {
            b.iter_with_setup(
                || {
                    let ctx = create_fjall_spatial_db().unwrap();
                    let collection = ctx.db().collection("bench").unwrap();
                    collection
                        .create_index(vec!["location"], &spatial_index())
                        .unwrap();
                    collection.insert_many(docs.clone()).unwrap();
                    (ctx, collection)
                },
                |(ctx, collection)| {
                    let center = Point::new(500.0, 500.0);
                    let filter = spatial_field("location").knearest(center, 10).unwrap();
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
    bench_spatial_index_create,
    bench_spatial_insert,
    bench_spatial_within_query,
    bench_spatial_near_query,
    bench_spatial_knearest_query
);
criterion_main!(benches);
