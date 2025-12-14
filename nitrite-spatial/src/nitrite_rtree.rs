//! NitriteRTree trait definition for spatial indexing.

use crate::bounding_box::BoundingBox;
use crate::disk_rtree::rtree_types::{SpatialResult, NitriteIdValue};

/// Represents an R-Tree in the nitrite database.
/// 
/// This trait defines the interface for spatial indexing operations,
/// matching the Java NitriteRTree interface.
pub trait NitriteRTree: Send + Sync {
    /// Adds a key to the rtree.
    fn add(&self, key: &BoundingBox, nitrite_id: NitriteIdValue) -> SpatialResult<()>;

    /// Removes a key from the rtree.
    fn remove(&self, key: &BoundingBox, nitrite_id: NitriteIdValue) -> SpatialResult<bool>;

    /// Finds the intersecting keys from the rtree.
    fn find_intersecting_keys(&self, key: &BoundingBox) -> SpatialResult<Vec<NitriteIdValue>>;

    /// Finds the contained keys from the rtree.
    fn find_contained_keys(&self, key: &BoundingBox) -> SpatialResult<Vec<NitriteIdValue>>;

    /// Finds the K nearest entries to a point using branch-and-bound search.
    fn find_nearest(
        &self,
        center_x: f64,
        center_y: f64,
        k: usize,
        max_distance: Option<f64>,
    ) -> SpatialResult<Vec<(NitriteIdValue, f64)>>;

    /// Finds entries within a specific distance of a point (range query).
    fn find_within_distance(
        &self,
        center_x: f64,
        center_y: f64,
        distance: f64,
    ) -> SpatialResult<Vec<(NitriteIdValue, f64)>> {
        // Default implementation using find_nearest with large k
        self.find_nearest(center_x, center_y, u64::MAX as usize, Some(distance))
    }

    /// Gets the size of the rtree.
    fn size(&self) -> u64;

    /// Closes this RTree instance, flushing all pending changes.
    fn close(&self) -> SpatialResult<()>;

    /// Clears all data from the rtree.
    fn clear(&self) -> SpatialResult<()>;

    /// Drops this instance, removing all data.
    fn drop_tree(&self) -> SpatialResult<()>;
}
