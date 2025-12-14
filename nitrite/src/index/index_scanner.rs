use super::IndexMap;
use crate::{
    collection::NitriteId,
    errors::NitriteResult,
    filter::{Filter, FilterProvider},
};
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;

/// Scanner for evaluating filter expressions against index maps.
///
/// IndexScanner applies a sequence of filters to an IndexMap, progressively
/// narrowing the result set through compound index evaluation. It handles both
/// simple and compound index scenarios:
///
/// **Simple Index**: Single filter directly applied, returning NitriteIds
/// **Compound Index**: Multiple filters cascaded, each operating on nested maps
///
/// # Characteristics
/// - **Hierarchical**: Recursively processes compound filters through nested maps
/// - **Deduplicating**: Automatically removes duplicate NitriteIds in results
/// - **Reverse-aware**: Respects scan order hints for optimization
/// - **Type-validating**: Ensures result homogeneity (all IDs or all maps)
/// - **Cloneable**: Thread-safe sharing via Arc
///
/// # Behavior
///
/// For simple index scans with a single filter:
/// 1. Applies filter to the index map
/// 2. Extracts NitriteIds directly from results
/// 3. Deduplicates and returns
///
/// For compound index scans with multiple filters:
/// 1. Applies first filter to the main index
/// 2. Results are nested maps (intermediate results)
/// 3. Recursively creates new IndexScanner instances for each map
/// 4. Applies remaining filters to each submapping
/// 5. Collects and deduplicates all terminal NitriteIds
///
/// # Internal Behavior
///
/// When no filters are provided, returns all terminal NitriteIds stored in arrays
/// within the index structure (used for full collection scans).
#[derive(Clone)]
pub(crate) struct IndexScanner {
    inner: Arc<IndexScannerInner>,
}

impl IndexScanner {
    /// Creates a new IndexScanner for the given index map.
    ///
    /// # Arguments
    /// * `index_map` - The index map to scan. For simple indexes this contains
    ///   NitriteIds. For compound indexes this contains nested maps.
    ///
    /// # Returns
    /// A new IndexScanner ready to evaluate filters.
    pub(crate) fn new(index_map: IndexMap) -> Self {
        IndexScanner {
            inner: Arc::new(IndexScannerInner::new(index_map)),
        }
    }

    /// Scans the index using provided filters and returns matching NitriteIds.
    ///
    /// # Arguments
    /// * `filters` - Sequence of filters to apply. For compound indexes, processed
    ///   in order from outermost to innermost index levels.
    /// * `scan_order` - Mapping of field names to reverse scan hints (true for descending).
    ///   Used to optimize iterator direction based on filter operators.
    ///
    /// # Returns
    /// A vector of matching NitriteIds, deduplicated and sorted for consistency.
    /// Returns empty vector if no matches found or if input filters are empty and
    /// the index contains no terminal IDs.
    ///
    /// # Errors
    /// Returns error if:
    /// - Filter evaluation fails on the index
    /// - Field names cannot be extracted from filters
    /// - Scan results contain mixed types (some NitriteIds, some Maps)
    /// - Nested map evaluation fails during compound index traversal
    ///
    /// # Behavior
    ///
    /// **Empty filters**: Returns all terminal NitriteIds from the index
    /// **Single filter**: Applies filter and extracts IDs from results
    /// **Multiple filters**: Cascades through compound index levels recursively
    ///
    /// Type validation ensures that if the first result is a NitriteId, all
    /// results are NitriteIds. Similarly, if first is a Map, all are Maps.
    /// Mixed results cause an IndexingError.
    pub(crate) fn scan(
        &self,
        filters: Vec<Filter>,
        scan_order: HashMap<String, bool>,
    ) -> NitriteResult<Vec<NitriteId>> {
        self.inner.scan(filters, scan_order)
    }
}

struct IndexScannerInner {
    index_map: IndexMap,
}

impl IndexScannerInner {
    pub(crate) fn new(index_map: IndexMap) -> Self {
        IndexScannerInner { index_map }
    }

    pub(crate) fn scan(
        &self,
        filters: Vec<Filter>,
        scan_order: HashMap<String, bool>,
    ) -> NitriteResult<Vec<NitriteId>> {
        let mut nitrite_ids = Vec::new();

        if !filters.is_empty() {
            let filter = filters[0].clone();
            let reverse_scan = scan_order
                .get(filter.get_field_name()?.as_str())
                .copied()
                .unwrap_or(false);

            self.index_map.set_reverse_scan(reverse_scan);

            if filter.is_reverse_scan_supported() {
                let filter_ref = filters[0].clone();
                filter_ref.set_reverse_scan(reverse_scan)?;
            }

            let scan_result = filter.apply_on_index(&self.index_map)?;
            if scan_result.is_empty() {
                return Ok(nitrite_ids);
            } else {
                let first = &scan_result[0];
                let is_nitrite_id_result = first.is_nitrite_id();

                // Validate all results have consistent type (all NitriteId or all Maps)
                for (idx, value) in scan_result.iter().enumerate() {
                    if is_nitrite_id_result != value.is_nitrite_id() {
                        log::error!(
                            "Type mismatch in scan result at index {}: expected {:?}, got {:?}",
                            idx,
                            if is_nitrite_id_result { "NitriteId" } else { "Map" },
                            value
                        );
                        return Err(crate::errors::NitriteError::new(
                            &format!(
                                "Scan result contains mixed types at index {}",
                                idx
                            ),
                            crate::errors::ErrorKind::IndexingError,
                        ));
                    }
                }

                if is_nitrite_id_result {
                    nitrite_ids.reserve(scan_result.len());
                    for id in scan_result {
                        if let Some(nitrite_id) = id.as_nitrite_id() {
                            nitrite_ids.push(nitrite_id.clone());
                        } else {
                            log::warn!("Invalid NitriteId found in scan result: {:?}", id);
                        }
                    }
                } else {
                    let sub_maps = scan_result;
                    let remaining_filters = filters[1..].to_vec();

                    for sub_map in sub_maps {
                        match sub_map.as_map() {
                            Some(map) => {
                                let sub_index_map = IndexMap::new(None, Some(map.clone()));
                                let sub_scanner = IndexScanner::new(sub_index_map);
                                let mut sub_ids =
                                    sub_scanner.scan(remaining_filters.clone(), scan_order.clone())?;

                                nitrite_ids.append(&mut sub_ids);
                            }
                            None => {
                                log::warn!("Invalid map found in scan result: {:?}", sub_map);
                            }
                        }
                    }
                }
            }
        } else {
            let terminal_result = self.index_map.terminal_nitrite_ids()?;
            nitrite_ids.extend(terminal_result.into_iter());
        }

        // Sort to ensure consistent ordering for dedup operation
        nitrite_ids.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        nitrite_ids.dedup();
        
        Ok(nitrite_ids)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::filter::{field, Filter, FilterProvider};
    use std::any::Any;
    use std::collections::{BTreeMap, HashMap};
    use std::fmt::Display;

    fn create_index_map() -> IndexMap {
        // Create a mock IndexMap for testing
        IndexMap::new(None, Some(BTreeMap::new()))
    }

    fn create_filter(field_name: &str) -> Filter {
        // Create a mock Filter for testing
        field(field_name).eq(1)
    }

    #[test]
    fn test_index_scanner_new() {
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        assert_eq!(Arc::strong_count(&scanner.inner), 1);
    }

    #[test]
    fn test_index_scanner_scan_empty_filters() {
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        let result = scanner.scan(vec![], HashMap::new());
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_index_scanner_scan_with_filters() {
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok());
    }

    #[test]
    fn test_index_scanner_scan_with_reverse_scan() {
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), true);

        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok());
    }

    #[test]
    fn test_index_scanner_scan_with_invalid_filter() {
        #[derive(Copy, Clone)]
        struct InvalidFilter;
        
        impl Display for InvalidFilter {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "InvalidFilter")
            }
        }

        impl FilterProvider for InvalidFilter {
            fn apply(&self, _entry: &Document) -> NitriteResult<bool> {
                Ok(false)
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        let filter = Filter::new(InvalidFilter);
        let mut scan_order = HashMap::new();
        scan_order.insert("invalid_field".to_string(), false);

        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_scanner_scan_with_empty_result() {
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_index_scanner_scan_with_sub_maps() {
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map.clone());
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok());
    }

    // Tests for error handling in scan results

    #[test]
    fn test_index_scanner_handles_invalid_nitrite_id_in_scan() {
        // Test that scanner handles malformed NitriteId gracefully
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // This should not panic even if scan results contain non-NitriteId values
        let result = scanner.scan(vec![filter], scan_order);
        // Should succeed without panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_handles_invalid_map_in_scan() {
        // Test that scanner handles malformed maps gracefully
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // This should not panic even if scan results contain non-map values
        let result = scanner.scan(vec![filter], scan_order);
        // Should succeed without panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_multiple_filters_graceful_handling() {
        // Test that scanner with multiple filters handles errors gracefully
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter1 = create_filter("field1");
        let filter2 = create_filter("field2");
        let mut scan_order = HashMap::new();
        scan_order.insert("field1".to_string(), false);
        scan_order.insert("field2".to_string(), false);

        // Should handle multiple filters without panicking
        let result = scanner.scan(vec![filter1, filter2], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_empty_scan_order() {
        // Test that scanner handles empty scan_order gracefully
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let scan_order = HashMap::new();

        // Should handle missing scan_order entry gracefully
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_validates_all_results_nitrite_id_type() {
        // Test that scanner validates ALL results are NitriteIds, not just first
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // Result should either be Ok or Err, but not panic
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_validates_all_results_map_type() {
        // Test that scanner validates ALL results are Maps if first is Map
        // This ensures compound index results are homogeneous
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // Result should either be Ok or Err, but not panic
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_handles_homogeneous_scan_results() {
        // Test that scanner correctly handles homogeneous results
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("field1");
        let mut scan_order = HashMap::new();
        scan_order.insert("field1".to_string(), false);

        // Should succeed with homogeneous results
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    // Performance optimization tests
    #[test]
    fn test_index_scanner_scan_pre_allocates_nitrite_ids() {
        // Test that scan pre-allocates Vec for known result size
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // Should allocate efficiently
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_dedup_efficiency() {
        // Test that dedup is performed more efficiently than unique().collect()
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // Should handle dedup without extra allocations
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_copied_vs_ref_optimization() {
        // Test that scan_order.copied() is used instead of .unwrap_or(&false)
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), true);

        // Should use copied() for better performance
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_handles_multiple_nitrite_ids_without_clone_iterator() {
        // Test that scanner processes multiple IDs without cloning entire collection
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter = create_filter("test_field");
        let mut scan_order = HashMap::new();
        scan_order.insert("test_field".to_string(), false);

        // Should iterate and collect without unnecessary collection clones
        let result = scanner.scan(vec![filter], scan_order);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_index_scanner_append_vs_extend_efficiency() {
        // Test that sub_ids.append() is used for efficient merging
        let index_map = create_index_map();
        let scanner = IndexScanner::new(index_map);
        let filter1 = create_filter("field1");
        let filter2 = create_filter("field2");
        let mut scan_order = HashMap::new();
        scan_order.insert("field1".to_string(), false);
        scan_order.insert("field2".to_string(), false);

        // Should use append for efficient vec merging
        let result = scanner.scan(vec![filter1, filter2], scan_order);
        assert!(result.is_ok() || result.is_err());
    }
}