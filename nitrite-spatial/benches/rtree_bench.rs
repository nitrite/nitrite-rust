//! R-Tree benchmarks

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use nitrite_spatial::{BoundingBox, DiskRTree, NitriteRTree};
use std::hint::black_box;
use tempfile::tempdir;

fn bench_disk_rtree_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("DiskRTree Insert");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = tempdir().unwrap();
                    let path = dir.path().join("bench.rtree");
                    (DiskRTree::create(&path).unwrap(), dir)
                },
                |(tree, _dir)| {
                    for i in 0..size {
                        let x = (i % 100) as f64;
                        let y = (i / 100) as f64;
                        tree.add(&BoundingBox::new(x, y, x + 1.0, y + 1.0), i as u64)
                            .unwrap();
                    }
                    black_box(tree.size())
                },
            );
        });
    }

    group.finish();
}

fn bench_disk_rtree_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("DiskRTree Search");

    let dir = tempdir().unwrap();
    let path = dir.path().join("bench.rtree");
    let tree = DiskRTree::create(&path).unwrap();

    // Populate tree
    for i in 0..10000 {
        let x = (i % 100) as f64;
        let y = (i / 100) as f64;
        tree.add(&BoundingBox::new(x, y, x + 1.0, y + 1.0), i as u64)
            .unwrap();
    }

    group.bench_function("search_10k", |b| {
        b.iter(|| {
            let query = BoundingBox::new(25.0, 25.0, 75.0, 75.0);
            black_box(tree.find_intersecting_keys(&query).unwrap())
        });
    });

    group.finish();
}

criterion_group!(benches, bench_disk_rtree_insert, bench_disk_rtree_search);
criterion_main!(benches);
