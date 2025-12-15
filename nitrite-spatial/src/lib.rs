//! # Nitrite Spatial - Spatial Indexing for Nitrite Database
//!
//! This crate provides spatial indexing capabilities for the Nitrite database,
//! including a memory-efficient disk-based R-Tree implementation and spatial
//! query filters.
//!
//! ## Features
//!
//! - **Disk-Based Storage**: Pages stored on disk, loaded on demand
//! - **LRU Cache**: Frequently accessed pages kept in memory
//! - **Memory Efficient**: Only hot data in RAM, cold data on disk
//! - **Persistent**: Data survives process restarts
//! - **Thread Safe**: Concurrent read/write support
//! - **Two-Phase Search**: Fast R-tree bbox search followed by precise geometry refinement
//! - **Spatial Filters**: Intersects, Within, Near, and GeoNear filters
//! - **Fluent API**: Builder pattern for spatial queries
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use nitrite_spatial::{SpatialModule, spatial_field, Point, Geometry};
//! use nitrite::nitrite_builder::NitriteBuilder;
//! use nitrite::common::PersistentCollection;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Register spatial module
//! let db = Nitrite::builder()
//!     .load_module(SpatialModule)
//!     .open_or_create(None, None)?;
//!
//! // Create spatial index on a field
//! let collection = db.collection("places")?;
//! collection.create_index(vec!["location"], &nitrite_spatial::spatial_index())?;
//!
//! // Query with spatial filters
//! let filter = spatial_field("location")
//!     .intersects(Geometry::point(0.0, 0.0));
//!
//! let results = collection.find(filter, None)?;
//! # Ok(())
//! # }
//! ```
//!
//! ## R-Tree API
//!
//! ```rust,no_run
//! use nitrite_spatial::{DiskRTree, BoundingBox, NitriteRTree};
//! use tempfile::NamedTempFile;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a new R-Tree
//! let temp_file = NamedTempFile::new()?;
//! let tree = DiskRTree::create(temp_file.path())?;
//!
//! // Add entries
//! tree.add(&BoundingBox::new(0.0, 0.0, 10.0, 10.0), 1)?;
//!
//! // Find intersecting entries
//! let query = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
//! let results = tree.find_intersecting_keys(&query)?;
//! # Ok(())
//! # }
//! ```

// Core R-Tree modules
pub mod bounding_box;
pub mod disk_rtree;
pub mod nitrite_rtree;

// Spatial indexing modules
pub mod filter;
pub mod fluent;
pub mod geometry;
pub mod geometry_extended;
pub mod hilbert;
pub mod index;
pub mod indexer;
pub mod spatial_module;

// Re-export R-Tree types
pub use bounding_box::BoundingBox;
pub use disk_rtree::{DiskRTree, RTreeStats, SpatialError, SpatialResult};
pub use nitrite_rtree::NitriteRTree;

// Re-export geometry types
pub use geometry::{
    create_geodesic_circle, meters_to_degrees, Coordinate, GeoPoint, Geometry, Point,
};
pub use geometry_extended::{
    parse_geojson, parse_wkt, GeometryValue, LineString, MultiGeometry, PolygonWithHoles,
};

// Re-export filter types
pub use filter::{GeoNearFilter, IntersectsFilter, NearFilter, SpatialFilterOps, WithinFilter};

// Re-export fluent API
pub use fluent::SpatialFluentFilter;

// Re-export indexer types
pub use indexer::SpatialIndexer;

// Re-export filter constant
pub use filter::SPATIAL_INDEX;

pub use spatial_module::SpatialModule;

// Re-export fluent API entry point
pub use fluent::spatial_field;

/// Creates index options for a spatial index.
///
/// This is a convenience function to create `IndexOptions` for spatial indexes.
///
/// # Example
///
/// ```rust,no_run
/// # use nitrite_spatial::spatial_index;
/// let options = spatial_index();
/// // Use with collection.create_index(vec!["location"], &options)?;
/// ```
pub fn spatial_index() -> nitrite::index::IndexOptions {
    nitrite::index::IndexOptions::new(SPATIAL_INDEX)
}
