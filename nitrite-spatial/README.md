# Nitrite Spatial

Geospatial indexing and querying module for Nitrite using R-tree data structures.

## Features

- **Spatial Indexes** - R-tree based spatial indexing
- **Geometry Types** - Points, envelopes (bounding boxes)
- **Spatial Queries** - Within, near, and k-nearest neighbor queries

## Usage

### Loading the Module

```rust
use nitrite::nitrite::Nitrite;
use nitrite_spatial::SpatialModule;

let db = Nitrite::builder()
    .load_module(SpatialModule)
    .open_or_create(None, None)
    .expect("Failed to create database");
```

### Creating a Spatial Index

```rust
use nitrite_spatial::spatial_index;

let collection = db.collection("locations").unwrap();
collection.create_index(vec!["location"], &spatial_index()).unwrap();
```

### Inserting Spatial Data

Use `x` and `y` fields to represent point coordinates:

```rust
use nitrite::doc;

let doc = doc! {
    name: "Central Park",
    location: {
        x: (-73.968285),
        y: 40.785091
    }
};
collection.insert(doc).unwrap();
```

### Spatial Queries

#### Within Query (Bounding Box)

```rust
use nitrite_spatial::{spatial_field, Geometry};

// Create a bounding box: (min_x, min_y, max_x, max_y)
let search_box = Geometry::envelope(-74.0, 40.7, -73.9, 40.9);

// Find all points within the bounding box
let filter = spatial_field("location").within(search_box);
let cursor = collection.find(filter).unwrap();
```

#### Near Query

```rust
use nitrite_spatial::{spatial_field, Point};

// Find points near a location within a radius
let center = Point::new(-73.968285, 40.785091);
let filter = spatial_field("location").near(center, 0.1);
let cursor = collection.find(filter).unwrap();
```

## Geometry Types

- `Point::new(x, y)` - A single coordinate
- `Geometry::envelope(min_x, min_y, max_x, max_y)` - A bounding box

## Benchmarks

Run the R-tree benchmark to measure spatial indexing performance:

```bash
# Run all R-tree benchmarks
cargo bench -p nitrite_spatial

# Quick validation (no measurement)
cargo bench -p nitrite_spatial -- --test
```

Benchmark results are saved to `target/criterion/` with HTML reports.

## License

Apache License 2.0
