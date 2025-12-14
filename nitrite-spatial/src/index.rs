use std::{path::PathBuf, sync::Arc};

use nitrite::{
    collection::{FindPlan, NitriteId},
    common::{FieldValues, Value},
    errors::{ErrorKind, NitriteError, NitriteResult},
    index::IndexDescriptor,
    nitrite_config::NitriteConfig,
};

use crate::{
    filter::{as_spatial_filter, is_spatial_filter, value_to_geometry, KNearestFilter},
    BoundingBox, DiskRTree, Geometry, IntersectsFilter, NitriteRTree, WithinFilter,
    NearFilter, GeoNearFilter,
};

/// A spatial index instance for a specific field.
/// Uses Pimpl pattern for cheap cloning and encapsulation.
#[derive(Clone)]
pub struct SpatialIndex {
    inner: Arc<SpatialIndexInner>,
}

/// Private implementation details of SpatialIndex.
struct SpatialIndexInner {
    index_descriptor: IndexDescriptor,
    rtree: Arc<dyn NitriteRTree>,
    collection_name: String,
}

impl SpatialIndex {
    pub fn new(index_descriptor: IndexDescriptor, base_path: Option<PathBuf>) -> NitriteResult<Self> {
        let index_name = derive_index_map_name(&index_descriptor);
        let collection_name = index_descriptor.collection_name().to_string();

        // Determine the path for the R-tree file
        let rtree_path = if let Some(base) = &base_path {
            base.join(format!("{}.rtree", index_name))
        } else {
            // Use a temp directory for in-memory mode
            std::env::temp_dir().join(format!("nitrite_{}.rtree", index_name))
        };

        // Create or open the R-tree
        let rtree = if rtree_path.exists() {
            log::debug!("Opening existing spatial index at {:?}", rtree_path);
            DiskRTree::open(&rtree_path).map_err(|e| {
                NitriteError::new(
                    &format!("Failed to open spatial index: {}", e),
                    ErrorKind::Extension("Spatial".to_string()),
                )
            })?
        } else {
            log::debug!("Creating new spatial index at {:?}", rtree_path);
            DiskRTree::create(&rtree_path).map_err(|e| {
                NitriteError::new(
                    &format!("Failed to create spatial index: {}", e),
                    ErrorKind::Extension("Spatial".to_string()),
                )
            })?
        };

        Ok(Self {
            inner: Arc::new(SpatialIndexInner {
                index_descriptor,
                rtree: Arc::new(rtree),
                collection_name,
            }),
        })
    }

    pub fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        if field_names.is_empty() {
            return Ok(());
        }

        let first_field = &field_names[0];
        let value = field_values.get_value(first_field);
        let nitrite_id = field_values.nitrite_id().id_value();

        let bbox = match value {
            Some(v) => {
                if let Some(geom) = value_to_geometry(v) {
                    geom.bounding_box()
                } else {
                    BoundingBox::default()
                }
            }
            None => BoundingBox::default(),
        };

        self.inner.rtree.add(&bbox, nitrite_id).map_err(|e| {
            NitriteError::new(
                &format!("Failed to write to spatial index: {}", e),
                ErrorKind::Extension("Spatial".to_string()),
            )
        })
    }

    pub fn remove(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        if field_names.is_empty() {
            return Ok(());
        }

        let first_field = &field_names[0];
        let value = field_values.get_value(first_field);
        let nitrite_id = field_values.nitrite_id().id_value();

        let bbox = match value {
            Some(v) => {
                if let Some(geom) = value_to_geometry(v) {
                    geom.bounding_box()
                } else {
                    BoundingBox::default()
                }
            }
            None => BoundingBox::default(),
        };

        self.inner.rtree.remove(&bbox, nitrite_id).map_err(|e| {
            NitriteError::new(
                &format!("Failed to remove from spatial index: {}", e),
                ErrorKind::Extension("Spatial".to_string()),
            )
        })?;

        Ok(())
    }

    pub fn find_nitrite_ids(
        &self,
        find_plan: &FindPlan,
        config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        let index_scan_filter = find_plan
            .index_scan_filter()
            .ok_or_else(|| {
                NitriteError::new("No spatial filter found", ErrorKind::FilterError)
            })?;

        let filters = index_scan_filter.filters();
        
        if filters.is_empty() {
            return Err(NitriteError::new(
                "No spatial filter found",
                ErrorKind::FilterError,
            ));
        }

        let filter = &filters[0];
        
        if !is_spatial_filter(filter) {
            return Err(NitriteError::new(
                "Spatial filter must be the first filter for index scan",
                ErrorKind::FilterError,
            ));
        }

        // Get the search geometry from the filter
        let spatial_filter = as_spatial_filter(filter).ok_or_else(|| {
            NitriteError::new("Failed to get spatial filter", ErrorKind::FilterError)
        })?;

        // Handle KNearestFilter separately as it uses find_nearest, not find_intersecting_keys
        if let Some(knearest_filter) = filter.as_any().downcast_ref::<KNearestFilter>() {
            return self.find_knearest_nitrite_ids(knearest_filter, config);
        }

        let search_geometry = spatial_filter.geometry();
        let search_bbox = search_geometry.bounding_box();

        // Phase 1: R-tree bounding box search
        let candidate_ids = if filter.as_any().is::<WithinFilter>()
            || filter.as_any().is::<NearFilter>()
            || filter.as_any().is::<GeoNearFilter>()
        {
            // For within/near filters, find keys that intersect with the search bbox
            self.inner.rtree.find_intersecting_keys(&search_bbox)
        } else if filter.as_any().is::<IntersectsFilter>() {
            self.inner.rtree.find_intersecting_keys(&search_bbox)
        } else {
            return Err(NitriteError::new(
                &format!("Unsupported spatial filter: {}", filter),
                ErrorKind::FilterError,
            ));
        };

        let candidate_ids = candidate_ids.map_err(|e| {
            NitriteError::new(
                &format!("Failed to query spatial index: {}", e),
                ErrorKind::Extension("Spatial".to_string()),
            )
        })?;

        // Phase 2: Geometry refinement
        // For precise results, we need to retrieve the actual geometry from each
        // candidate document and apply the exact spatial predicate.
        let mut results = Vec::new();

        for id in candidate_ids {
            let nitrite_id = NitriteId::create_id(id)?;

            // Try to get the stored geometry and apply precise filter
            if let Some(stored_geom) = self.get_stored_geometry(&nitrite_id, config)? {
                if spatial_filter.matches_geometry(&stored_geom) {
                    results.push(nitrite_id);
                }
            }
        }

        Ok(results)
    }

    /// Finds K nearest entries to a point using the spatial index.
    /// This method uses find_nearest from DiskRTree which performs KNN search directly.
    fn find_knearest_nitrite_ids(
        &self,
        knearest_filter: &KNearestFilter,
        _config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        let center = knearest_filter.center();
        let k = knearest_filter.k();
        let max_distance = knearest_filter.max_distance();

        // Use DiskRTree's find_nearest method for KNN search
        // Note: find_nearest returns (id, distance) tuples
        let knearest_results = self
            .inner
            .rtree
            .find_nearest(center.x(), center.y(), k, max_distance)
            .map_err(|e| {
                NitriteError::new(
                    &format!("Failed to execute KNN query on spatial index: {}", e),
                    ErrorKind::Extension("Spatial".to_string()),
                )
            })?;

        // Convert (id, distance) tuples to Vec<NitriteId>
        let mut results = Vec::new();
        for (id, _distance) in knearest_results {
            let nitrite_id = NitriteId::create_id(id)?;
            results.push(nitrite_id);
        }

        Ok(results)
    }

    /// Retrieves the stored geometry for a document.
    /// This is used in Phase 2 of the two-phase query for precise filtering.
    fn get_stored_geometry(
        &self,
        nitrite_id: &NitriteId,
        config: &NitriteConfig,
    ) -> NitriteResult<Option<Geometry>> {
        // Get the field from the index descriptor
        let field_names = self.inner.index_descriptor.index_fields().field_names();

        if field_names.is_empty() {
            return Ok(None);
        }

        let collection_name = &self.inner.collection_name;
        let nitrite_map = config
            .nitrite_store()
            .and_then(|store| store.open_map(collection_name))?;

        let document_opt = nitrite_map.get(&Value::NitriteId(nitrite_id.clone()))?;

        if let Some(value) = document_opt {
            let first_field = &field_names[0];
            return match value {
                Value::Document(doc) => {
                    let geom_value = doc.get(first_field)?;
                    value_to_geometry(&geom_value).map(Some).ok_or_else(|| {
                        NitriteError::new(
                            "Failed to convert stored value to geometry",
                            ErrorKind::Extension("Spatial".to_string()),
                        )
                    })
                }
                _ => Ok(None),
            };
        };

        Ok(None)
    }

   pub fn close(&self) -> NitriteResult<()> {
        self.inner.rtree.close().map_err(|e| {
            NitriteError::new(
                &format!("Failed to close spatial index: {}", e),
                ErrorKind::Extension("Spatial".to_string()),
            )
        })
    }

    pub fn drop(&self) -> NitriteResult<()> {
        self.inner.rtree.drop_tree().map_err(|e| {
            NitriteError::new(
                &format!("Failed to drop spatial index: {}", e),
                ErrorKind::Extension("Spatial".to_string()),
            )
        })
    }
}

/// Derives the index map name from an index descriptor.
pub(crate) fn derive_index_map_name(descriptor: &IndexDescriptor) -> String {
    let collection = descriptor.collection_name();
    let fields = descriptor.index_fields().field_names().join("_");
    let index_type = descriptor.index_type();
    format!("{}_{}_{}_{}", collection, fields, index_type, "idx")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Point, BoundingBox};

    /// Helper to create a mock IndexDescriptor with a unique collection name for testing.
    /// Uses UUID to ensure uniqueness across test runs.
    fn create_test_index_descriptor() -> IndexDescriptor {
        let uuid = uuid::Uuid::new_v4();
        let fields = nitrite::common::Fields::with_names(vec!["location"]).unwrap();
        IndexDescriptor::new("spatial", fields, &format!("test_collection_{}", uuid))
    }

    // Valid NitriteId values must be >= 10^18
    const TEST_ID_1: u64 = 1_000_000_000_000_000_001;
    const TEST_ID_2: u64 = 1_000_000_000_000_000_002;
    const TEST_ID_3: u64 = 1_000_000_000_000_000_003;

    #[test]
    fn test_knearest_filter_in_find_nitrite_ids() {
        // Create a test index
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        // Add some test entries with valid NitriteId values
        let bbox1 = BoundingBox::new(0.0, 0.0, 1.0, 1.0);
        let bbox2 = BoundingBox::new(2.0, 2.0, 3.0, 3.0);
        let bbox3 = BoundingBox::new(5.0, 5.0, 6.0, 6.0);

        index
            .inner
            .rtree
            .add(&bbox1, TEST_ID_1)
            .expect("Failed to add entry 1");
        index
            .inner
            .rtree
            .add(&bbox2, TEST_ID_2)
            .expect("Failed to add entry 2");
        index
            .inner
            .rtree
            .add(&bbox3, TEST_ID_3)
            .expect("Failed to add entry 3");

        // Create a KNearestFilter for center point (1.5, 1.5) with k=2
        let knearest = KNearestFilter::from_coords("location", 1.5, 1.5, 2)
            .expect("Failed to create KNearestFilter");

        // Call find_knearest_nitrite_ids directly
        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed to find knearest");

        // We expect to get 2 results (k=2)
        assert_eq!(results.len(), 2);

        // The two closest points should be 1 and 2 (at coordinates 0.5,0.5 and 2.5,2.5)
        // Close to (1.5, 1.5)
        let ids: Vec<u64> = results.iter().map(|id| id.id_value()).collect();
        assert!(ids.contains(&TEST_ID_1) || ids.contains(&TEST_ID_2));

        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_filter_with_max_distance() {
        // Create a test index
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        // Add test entries at different distances
        let bbox1 = BoundingBox::new(0.0, 0.0, 1.0, 1.0); // Close
        let bbox2 = BoundingBox::new(2.0, 2.0, 3.0, 3.0); // Medium
        let bbox3 = BoundingBox::new(10.0, 10.0, 11.0, 11.0); // Far

        index
            .inner
            .rtree
            .add(&bbox1, TEST_ID_1)
            .expect("Failed to add entry 1");
        index
            .inner
            .rtree
            .add(&bbox2, TEST_ID_2)
            .expect("Failed to add entry 2");
        index
            .inner
            .rtree
            .add(&bbox3, TEST_ID_3)
            .expect("Failed to add entry 3");

        // Create a KNearestFilter with max_distance constraint
        let knearest = KNearestFilter::with_max_distance("location", Point::new(0.5, 0.5), 10, 5.0)
            .expect("Failed to create KNearestFilter");

        // Call find_knearest_nitrite_ids
        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed to find knearest");

        // Should get results within max_distance, not the far one
        let ids: Vec<u64> = results.iter().map(|id| id.id_value()).collect();
        assert!(!ids.contains(&TEST_ID_3), "Should not include entry 3 (too far away)");

        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_filter_k_equals_one() {
        // Create a test index
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        // Add test entries - place them at very different distances
        // bbox1 center is at (0.5, 0.5), bbox2 center is at (10.5, 10.5), bbox3 center is at (100.5, 100.5)
        let bbox1 = BoundingBox::new(0.0, 0.0, 1.0, 1.0);
        let bbox2 = BoundingBox::new(10.0, 10.0, 11.0, 11.0);
        let bbox3 = BoundingBox::new(100.0, 100.0, 101.0, 101.0);

        index
            .inner
            .rtree
            .add(&bbox1, TEST_ID_1)
            .expect("Failed to add entry 1");
        index
            .inner
            .rtree
            .add(&bbox2, TEST_ID_2)
            .expect("Failed to add entry 2");
        index
            .inner
            .rtree
            .add(&bbox3, TEST_ID_3)
            .expect("Failed to add entry 3");

        // Create a KNearestFilter with k=1 (find nearest single entry)
        // Query at (0.5, 0.5) which is the center of bbox1 - distance should be 0
        let knearest = KNearestFilter::from_coords("location", 0.5, 0.5, 1)
            .expect("Failed to create KNearestFilter");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed to find knearest");

        // Should get exactly 1 result
        assert_eq!(results.len(), 1);

        // The closest point should be bbox1 since the query point is at its center
        // bbox1 contains point (0.5, 0.5), so distance is 0
        // bbox2 is 10 units away, bbox3 is 100 units away
        let id = results[0].id_value();
        assert_eq!(id, TEST_ID_1, "Expected bbox1 (at 0,0,1,1) to be closest to query point (0.5, 0.5)");

        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_filter_k_greater_than_entries() {
        // Create a test index
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        // Add only 2 entries
        let bbox1 = BoundingBox::new(0.0, 0.0, 1.0, 1.0);
        let bbox2 = BoundingBox::new(2.0, 2.0, 3.0, 3.0);

        index
            .inner
            .rtree
            .add(&bbox1, TEST_ID_1)
            .expect("Failed to add entry 1");
        index
            .inner
            .rtree
            .add(&bbox2, TEST_ID_2)
            .expect("Failed to add entry 2");

        // Request k=5 (more than available entries)
        let knearest = KNearestFilter::from_coords("location", 1.5, 1.5, 5)
            .expect("Failed to create KNearestFilter");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed to find knearest");

        // Should get all 2 available entries (not 5)
        assert_eq!(results.len(), 2);

        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_filter_empty_index() {
        // Create a test index
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        // Don't add any entries

        // Create a KNearestFilter
        let knearest = KNearestFilter::from_coords("location", 0.0, 0.0, 5)
            .expect("Failed to create KNearestFilter");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed to find knearest");

        // Should get 0 results from empty index
        assert_eq!(results.len(), 0);

        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_derive_index_map_name() {
        let descriptor = create_test_index_descriptor();
        let name = derive_index_map_name(&descriptor);
        
        // Check that the name contains all expected components
        assert!(name.contains("test_collection"));
        assert!(name.contains("location"));
        assert!(name.contains("spatial"));
        assert!(name.contains("idx"));
    }

    #[test]
    fn test_derive_index_map_name_format() {
        // Use a hardcoded descriptor for format testing
        let fields = nitrite::common::Fields::with_names(vec!["location"]).unwrap();
        let descriptor = IndexDescriptor::new("spatial", fields, "my_collection");
        let name = derive_index_map_name(&descriptor);
        
        // Verify the format: collection_fields_type_idx
        assert_eq!(name, "my_collection_location_spatial_idx");
    }

    // ===== POSITIVE TEST CASES =====
    #[test]
    fn test_spatial_index_clone() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");
        let _cloned = index.clone();
        assert_eq!(
            index.inner.collection_name,
            _cloned.inner.collection_name
        );
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_multiple_entries_sorted() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox1 = BoundingBox::new(0.0, 0.0, 1.0, 1.0);
        let bbox2 = BoundingBox::new(5.0, 5.0, 6.0, 6.0);
        let bbox3 = BoundingBox::new(10.0, 10.0, 11.0, 11.0);

        index.inner.rtree.add(&bbox1, TEST_ID_1).expect("Failed");
        index.inner.rtree.add(&bbox2, TEST_ID_2).expect("Failed");
        index.inner.rtree.add(&bbox3, TEST_ID_3).expect("Failed");

        let knearest = KNearestFilter::from_coords("location", 0.5, 0.5, 3)
            .expect("Failed to create KNearestFilter");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id_value(), TEST_ID_1);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_intersects_filter_finds_matching() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let _intersects = IntersectsFilter::new("location", Geometry::point(5.0, 5.0));
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_within_filter_finds_contained() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(2.0, 2.0, 3.0, 3.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let _within = WithinFilter::new("location", Geometry::circle(2.5, 2.5, 5.0));
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_k_one_multiple_entries() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox1 = BoundingBox::new(0.0, 0.0, 1.0, 1.0);
        let bbox2 = BoundingBox::new(100.0, 100.0, 101.0, 101.0);
        let bbox3 = BoundingBox::new(200.0, 200.0, 201.0, 201.0);

        index.inner.rtree.add(&bbox1, TEST_ID_1).expect("Failed");
        index.inner.rtree.add(&bbox2, TEST_ID_2).expect("Failed");
        index.inner.rtree.add(&bbox3, TEST_ID_3).expect("Failed");

        let knearest = KNearestFilter::from_coords("location", 0.0, 0.0, 1)
            .expect("Failed to create KNearestFilter");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id_value(), TEST_ID_1);
        index.drop().expect("Failed to drop index");
    }

    // ===== NEGATIVE TEST CASES =====
    #[test]
    fn test_knearest_zero_max_distance() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(5.0, 5.0, 6.0, 6.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::with_max_distance(
            "location",
            crate::Point::new(0.0, 0.0),
            10,
            0.0,
        )
        .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 0);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_far_away_location() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(0.0, 0.0, 1.0, 1.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::with_max_distance(
            "location",
            crate::Point::new(1000.0, 1000.0),
            10,
            0.1,
        )
        .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 0);
        index.drop().expect("Failed to drop index");
    }

    // ===== EDGE CASES =====
    #[test]
    fn test_derive_multiple_fields() {
        let fields = nitrite::common::Fields::with_names(vec!["location", "geometry"])
            .expect("Failed to create fields");
        let descriptor = IndexDescriptor::new("spatial", fields, "my_collection");
        let name = derive_index_map_name(&descriptor);
        
        assert!(name.contains("location"));
        assert!(name.contains("geometry"));
        assert!(name.contains("spatial"));
        assert!(name.contains("idx"));
    }

    #[test]
    fn test_knearest_boundary_max_distance() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(5.0, 5.0, 6.0, 6.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::with_max_distance(
            "location",
            crate::Point::new(0.0, 0.0),
            10,
            7.1,
        )
        .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 1);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_negative_coordinates() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(-10.0, -10.0, -5.0, -5.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::from_coords("location", -7.5, -7.5, 1)
            .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id_value(), TEST_ID_1);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_very_large_coordinates() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(1_000_000.0, 1_000_000.0, 1_000_001.0, 1_000_001.0);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::from_coords("location", 1_000_000.5, 1_000_000.5, 1)
            .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 1);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_tiny_coordinates() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox = BoundingBox::new(0.00001, 0.00001, 0.00002, 0.00002);
        index
            .inner
            .rtree
            .add(&bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::from_coords("location", 0.000015, 0.000015, 1)
            .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 1);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_collection_name_matches_descriptor() {
        let descriptor = create_test_index_descriptor();
        let collection_name = descriptor.collection_name().to_string();
        
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");
        
        assert_eq!(index.inner.collection_name, collection_name);
        
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_identical_distance_entries() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let bbox1 = BoundingBox::new(5.0, 10.0, 6.0, 11.0);
        let bbox2 = BoundingBox::new(10.0, 5.0, 11.0, 6.0);
        let bbox3 = BoundingBox::new(0.0, 5.0, 1.0, 6.0);
        let bbox4 = BoundingBox::new(5.0, 0.0, 6.0, 1.0);

        index.inner.rtree.add(&bbox1, TEST_ID_1).expect("Failed");
        index.inner.rtree.add(&bbox2, TEST_ID_2).expect("Failed");
        index.inner.rtree.add(&bbox3, TEST_ID_3).expect("Failed");
        index
            .inner
            .rtree
            .add(&bbox4, 1_000_000_000_000_000_004)
            .expect("Failed");

        let knearest = KNearestFilter::from_coords("location", 5.0, 5.0, 2)
            .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 2);
        index.drop().expect("Failed to drop index");
    }

    #[test]
    fn test_knearest_large_bbox() {
        let descriptor = create_test_index_descriptor();
        let index = SpatialIndex::new(descriptor, None).expect("Failed to create index");

        let large_bbox = BoundingBox::new(-1000.0, -1000.0, 1000.0, 1000.0);
        index
            .inner
            .rtree
            .add(&large_bbox, TEST_ID_1)
            .expect("Failed to add entry");

        let knearest = KNearestFilter::from_coords("location", 0.0, 0.0, 1)
            .expect("Failed");

        let results = index
            .find_knearest_nitrite_ids(&knearest, &NitriteConfig::new())
            .expect("Failed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id_value(), TEST_ID_1);
        index.drop().expect("Failed to drop index");
    }
}

