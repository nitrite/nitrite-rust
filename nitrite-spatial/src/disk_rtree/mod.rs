//! Disk-based R-Tree implementation for memory-efficient spatial indexing.
//!
//! This module provides a custom R-Tree implementation designed from scratch
//! for disk-based storage with:
//! - Page-based node storage with configurable page size
//! - LRU cache for hot pages with memory pressure handling  
//! - Lazy loading of nodes on demand (never loads all data at once)
//! - Efficient range and point queries directly on disk
//!
//! Unlike in-memory R-Trees, this implementation is optimized for datasets
//! that exceed available RAM while maintaining good query performance.

pub mod rtree_types;
pub mod rtree_constants;
pub mod rtree_cache;
pub mod rtree_storage;
pub mod persistence;
mod rtree_impl;

pub use rtree_types::{
    SpatialError, SpatialResult, RTreeStats, RebuildStats, FragmentationMetrics,
    InternalBBox, Node, LeafEntry, ChildRef, FileHeader, PageId, PageWithChecksum, FreePage,
};
pub use rtree_constants::DEFAULT_CACHE_PAGES;
pub use rtree_impl::DiskRTree;
pub use persistence::{
    IntegrityReport, RepairOptions, RepairReport, FreeListManager, MigrationManager,
    VersionMigration, V1ToV2Migration, V2ToV3Migration,
};
