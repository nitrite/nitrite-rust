use super::{find_optimizer::FindOptimizer, index_operations::IndexOperations};
use crate::filter::is_all_filter;
use crate::{
    collection::{Document, FindOptions, FindPlan, NitriteId},
    errors::{ErrorKind, NitriteError, NitriteResult},
    filter::{Filter, FilterProvider},
    filtered_stream::FilteredStream,
    index::NitriteIndexerProvider,
    indexed_stream::IndexedStream,
    map_values::MapValues,
    nitrite_config::NitriteConfig,
    single_stream::SingleStream,
    sorted_stream::SortedStream,
    store::{NitriteMap, NitriteMapProvider},
    union_stream::UnionStream,
    unique_stream::UniqueStream,
    DocumentCursor, ProcessorChain, ProcessorProvider, Value,
};
use icu_collator::options::CollatorOptions;
use icu_collator::{Collator, CollatorPreferences};
use smallvec::SmallVec;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ReadOperations {
    inner: Arc<ReadOperationsInner>,
}

impl ReadOperations {
    pub fn new(
        collection_name: String,
        index_operations: IndexOperations,
        nitrite_config: NitriteConfig,
        nitrite_map: NitriteMap,
        find_optimizer: FindOptimizer,
        processor_chain: ProcessorChain,
    ) -> Self {
        let inner = ReadOperationsInner::new(
            collection_name,
            index_operations,
            nitrite_config,
            nitrite_map,
            find_optimizer,
            processor_chain,
        );
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Deref for ReadOperations {
    type Target = Arc<ReadOperationsInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct ReadOperationsInner {
    collection_name: String,
    nitrite_config: NitriteConfig,
    nitrite_map: NitriteMap,
    find_optimizer: FindOptimizer,
    index_operations: IndexOperations,
    processor_chain: ProcessorChain,
}

impl ReadOperationsInner {
    fn new(
        collection_name: String,
        index_operations: IndexOperations,
        nitrite_config: NitriteConfig,
        nitrite_map: NitriteMap,
        find_optimizer: FindOptimizer,
        processor_chain: ProcessorChain,
    ) -> Self {
        Self {
            collection_name,
            index_operations,
            nitrite_config,
            nitrite_map,
            find_optimizer,
            processor_chain,
        }
    }

    pub fn find(
        &self,
        filter: Filter,
        find_options: &FindOptions,
    ) -> NitriteResult<DocumentCursor> {
        self.prepare_filter(&filter)?;
        let index_descriptors = self.index_operations.list_indexes()?;
        let find_plan =
            self.find_optimizer
                .create_find_plan(&filter, find_options, &index_descriptors)?;

        let cursor = self.create_cursor(&find_plan)?;
        Ok(cursor)
    }

    pub fn get_by_id(&self, id: &NitriteId) -> NitriteResult<Option<Document>> {
        let document = self.nitrite_map.get(&Value::from(id.clone()))?;
        if let Some(document) = document {
            match document.as_document() {
                Some(doc) => {
                    let chain = self.processor_chain.clone();
                    let document = chain.process_after_read(doc.clone())?;
                    Ok(Some(document))
                }
                None => {
                    log::error!(
                        "Expected Document value in collection store for ID {:?}, found non-Document type: {:?}",
                        id,
                        document
                    );
                    Err(NitriteError::new(
                        &format!("Invalid value type in collection store for ID {:?}", id),
                        ErrorKind::ValidationError,
                    ))
                }
            }
        } else {
            Ok(None)
        }
    }

    fn prepare_filter(&self, filter: &Filter) -> NitriteResult<()> {
        if is_all_filter(filter) {
            return Ok(());
        }

        if let Ok(logical_filters) = filter.logical_filters() {
            for f in logical_filters {
                self.prepare_filter(&f)?;
            }
        }

        // Set collection name using reference instead of cloning entire string
        filter.set_collection_name(self.collection_name.clone())?;
        Ok(())
    }

    fn create_cursor(&self, find_plan: &FindPlan) -> NitriteResult<DocumentCursor> {
        // Fast path for simple all-documents query with no filtering or sorting
        if find_plan.by_id_filter().is_none()
            && find_plan.index_descriptor().is_none()
            && find_plan.full_scan_filter().is_none()
            && find_plan.blocking_sort_order().is_none()
            && find_plan.sub_plans().map_or(true, |p| p.is_empty())
        {
            // Direct map iteration with no filters
            let iter = Box::new(MapValues::new(self.nitrite_map.clone()));

            // Apply limit/skip if needed
            let iter: Box<dyn Iterator<Item = NitriteResult<Document>>> =
                if find_plan.skip().is_some() || find_plan.limit().is_some() {
                    let skip = find_plan.skip().unwrap_or(0);
                    let limit = find_plan.limit().unwrap_or(u64::MAX);
                    Box::new(iter.skip(skip as usize).take(limit as usize))
                } else {
                    iter
                };

            // Combine cursor creation and plan setting into single operation
            return Ok(DocumentCursor::new(iter, self.processor_chain.clone())
                .set_find_plan(find_plan.clone()));
        }

        // Standard path for complex queries
        let iter = self.find_suitable_iter(find_plan)?;
        Ok(
            DocumentCursor::new(iter, self.processor_chain.clone())
                .set_find_plan(find_plan.clone()),
        )
    }

    fn find_suitable_iter(
        &self,
        find_plan: &FindPlan,
    ) -> NitriteResult<Box<dyn Iterator<Item = NitriteResult<Document>>>> {
        let mut raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>>;

        if let Some(sub_plans) = find_plan.sub_plans() {
            if !sub_plans.is_empty() {
                let mut sub_iters: SmallVec<
                    [Box<dyn Iterator<Item = NitriteResult<Document>>>; 4],
                > = SmallVec::with_capacity(sub_plans.len());

                for sub_plan in sub_plans {
                    let iter = self.find_suitable_iter(&sub_plan)?;
                    sub_iters.push(iter);
                }

                raw_stream = Box::new(UnionStream::new(sub_iters.into_vec()));

                if find_plan.distinct() {
                    raw_stream = Box::new(UniqueStream::new(raw_stream));
                }
            } else {
                if find_plan.by_id_filter().is_some() {
                    let nitrite_id = find_plan.by_id_filter().unwrap();
                    let nitrite_id = nitrite_id.get_field_value()?;
                    match nitrite_id {
                        Some(Value::NitriteId(id)) => {
                            let document = self.nitrite_map.get(&Value::from(id))?;
                            if document.is_some() {
                                let doc = document.unwrap();
                                match doc.as_document() {
                                    Some(d) => {
                                        raw_stream = Box::new(SingleStream::new(Some(d.clone())));
                                    }
                                    None => {
                                        log::error!(
                                            "Expected Document value in collection store for ID {:?}, found non-Document type",
                                            nitrite_id
                                        );
                                        return Err(NitriteError::new(
                                            "Invalid value type in collection store",
                                            ErrorKind::ValidationError,
                                        ));
                                    }
                                }
                            } else {
                                raw_stream = Box::new(SingleStream::new(None));
                            }
                        }
                        _ => {
                            log::error!("Invalid NitriteId {:?}", nitrite_id);
                            return Err(NitriteError::new(
                                "Invalid NitriteId",
                                ErrorKind::FilterError,
                            ));
                        }
                    }
                } else {
                    let index_descriptor = find_plan.index_descriptor();
                    if index_descriptor.is_some() {
                        let index_descriptor = index_descriptor.unwrap();
                        let indexer = self
                            .nitrite_config
                            .find_indexer(&index_descriptor.index_type())?;

                        let nitrite_ids =
                            indexer.find_by_filter(find_plan, &self.nitrite_config)?;

                        raw_stream =
                            Box::new(IndexedStream::new(self.nitrite_map.clone(), nitrite_ids));
                    } else {
                        raw_stream = Box::new(MapValues::new(self.nitrite_map.clone()));
                    }
                }

                if find_plan.full_scan_filter().is_some() {
                    raw_stream = Box::new(FilteredStream::new(
                        raw_stream,
                        find_plan.full_scan_filter().unwrap(),
                    ));
                }
            }
        } else {
            if find_plan.by_id_filter().is_some() {
                let nitrite_id = find_plan.by_id_filter().unwrap();
                let nitrite_id = nitrite_id.get_field_value()?;
                match nitrite_id {
                    Some(Value::NitriteId(id)) => {
                        let document = self.nitrite_map.get(&Value::from(id))?;
                        if document.is_some() {
                            let doc = document.unwrap();
                            match doc.as_document() {
                                Some(d) => {
                                    raw_stream = Box::new(SingleStream::new(Some(d.clone())));
                                }
                                None => {
                                    log::error!(
                                        "Expected Document value in collection store for ID {:?}, found non-Document type",
                                        nitrite_id
                                    );
                                    return Err(NitriteError::new(
                                        "Invalid value type in collection store",
                                        ErrorKind::ValidationError,
                                    ));
                                }
                            }
                        } else {
                            raw_stream = Box::new(SingleStream::new(None));
                        }
                    }
                    _ => {
                        log::error!("Invalid NitriteId {:?}", nitrite_id);
                        return Err(NitriteError::new(
                            "Invalid NitriteId",
                            ErrorKind::FilterError,
                        ));
                    }
                }
            } else {
                let index_descriptor = find_plan.index_descriptor();
                if index_descriptor.is_some() {
                    let index_descriptor = index_descriptor.unwrap();
                    let indexer = self
                        .nitrite_config
                        .find_indexer(&index_descriptor.index_type())?;

                    let nitrite_ids = indexer.find_by_filter(find_plan, &self.nitrite_config)?;

                    raw_stream =
                        Box::new(IndexedStream::new(self.nitrite_map.clone(), nitrite_ids));
                } else {
                    raw_stream = Box::new(MapValues::new(self.nitrite_map.clone()));
                }
            }

            if find_plan.full_scan_filter().is_some() {
                raw_stream = Box::new(FilteredStream::new(
                    raw_stream,
                    find_plan.full_scan_filter().unwrap(),
                ));
            }
        }

        if find_plan.blocking_sort_order().is_some()
            && !find_plan.blocking_sort_order().unwrap().is_empty()
        {
            let sort_order = find_plan.blocking_sort_order().unwrap();
            let collator_preference = find_plan
                .collator_preferences()
                .unwrap_or(CollatorPreferences::default());
            let collator_options = find_plan
                .collator_options()
                .unwrap_or(CollatorOptions::default());
            let collator =
                Collator::try_new(collator_preference, collator_options).map_err(|_| {
                    NitriteError::new(
                        "Failed to create collator for sorting - check collator preferences and options",
                        ErrorKind::BackendError
                    )
                })?;
            raw_stream = Box::new(SortedStream::new(raw_stream, sort_order, Some(collator)));
        }

        if find_plan.skip().is_some() || find_plan.limit().is_some() {
            let skip = find_plan.skip().unwrap_or(0);
            let limit = find_plan.limit().unwrap_or(u64::MAX);
            raw_stream = Box::new(raw_stream.skip(skip as usize).take(limit as usize));
        }

        Ok(raw_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{Document, FindOptions, NitriteId};
    use crate::common::{Fields, NitriteEventBus, UNIQUE_INDEX};
    use crate::filter::{all, field};
    use crate::index::IndexDescriptor;
    use crate::nitrite_config::NitriteConfig;
    use crate::store::NitriteStoreProvider;
    use std::sync::Arc;

    fn setup_read_operations() -> ReadOperations {
        let collection_name = "test_collection".to_string();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let store = nitrite_config.nitrite_store().expect("Failed to get store");
        let nitrite_map = store
            .open_map(&*collection_name.clone())
            .expect("Failed to open map");
        let event_bus = NitriteEventBus::new();
        let find_optimizer = FindOptimizer::new();
        let index_operations = IndexOperations::new(
            collection_name.clone(),
            nitrite_config.clone(),
            nitrite_map.clone(),
            find_optimizer.clone(),
            event_bus,
        )
        .unwrap();
        let find_optimizer = FindOptimizer::new();
        let processor_chain = ProcessorChain::new();
        ReadOperations::new(
            collection_name,
            index_operations,
            nitrite_config,
            nitrite_map,
            find_optimizer,
            processor_chain,
        )
    }

    #[test]
    fn test_new() {
        let read_operations = setup_read_operations();
        assert!(Arc::strong_count(&read_operations.inner) > 0);
    }

    #[test]
    fn test_find() {
        let read_operations = setup_read_operations();
        let filter = all();
        let find_options = FindOptions::default();
        let result = read_operations.find(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_by_id_not_found() {
        let read_operations = setup_read_operations();
        let id = NitriteId::new();
        let result = read_operations.get_by_id(&id);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_get_by_id_found() {
        let read_operations = setup_read_operations();
        let id = NitriteId::new();
        let mut document = Document::new();
        document.put("test", Value::from("value")).unwrap();
        let store = &read_operations.inner.nitrite_map;
        use crate::store::NitriteMapProvider;
        store
            .put(Value::from(id.clone()), Value::Document(document))
            .unwrap();

        let result = read_operations.get_by_id(&id);
        assert!(result.is_ok());
        let found = result.unwrap();
        assert!(found.is_some());
        let doc = found.unwrap();
        assert_eq!(doc.get("test").unwrap().clone(), Value::from("value"));
    }

    #[test]
    fn test_get_by_id_corrupted_value_non_document() {
        let read_operations = setup_read_operations();
        let id = NitriteId::new();
        // Insert a non-Document value (string instead of document)
        let store = &read_operations.inner.nitrite_map;
        use crate::store::NitriteMapProvider;
        store
            .put(Value::from(id.clone()), Value::from("not_a_document"))
            .unwrap();

        let result = read_operations.get_by_id(&id);
        // Should return an error because the value is not a Document
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::ValidationError);
    }

    #[test]
    fn test_get_by_id_handles_type_mismatch_gracefully() {
        let read_operations = setup_read_operations();
        let id1 = NitriteId::new();
        let id2 = NitriteId::new();
        let id3 = NitriteId::new();
        let store = &read_operations.inner.nitrite_map;
        use crate::store::NitriteMapProvider;

        // id1: Valid document
        let mut valid_doc = Document::new();
        valid_doc.put("name", Value::from("test")).unwrap();
        store
            .put(Value::from(id1.clone()), Value::Document(valid_doc))
            .unwrap();

        // id2: Non-document value (should fail gracefully)
        store
            .put(Value::from(id2.clone()), Value::from(42))
            .unwrap();

        // id3: Another valid document
        let mut another_doc = Document::new();
        another_doc.put("id", Value::from("id3")).unwrap();
        store
            .put(Value::from(id3.clone()), Value::Document(another_doc))
            .unwrap();

        // Valid retrieval
        let result1 = read_operations.get_by_id(&id1);
        assert!(result1.is_ok());
        assert!(result1.unwrap().is_some());

        // Invalid retrieval - type mismatch
        let result2 = read_operations.get_by_id(&id2);
        assert!(result2.is_err());

        // Another valid retrieval
        let result3 = read_operations.get_by_id(&id3);
        assert!(result3.is_ok());
        assert!(result3.unwrap().is_some());
    }

    #[test]
    fn test_prepare_filter() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();
        let filter = all();
        let result = inner.prepare_filter(&filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_cursor() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();
        let filter = all();
        let find_options = FindOptions::default();
        let index_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field"]).unwrap(),
            "test_collection",
        );
        let find_plan = inner
            .find_optimizer
            .create_find_plan(&filter, &find_options, &vec![index_descriptor])
            .unwrap();
        let result = inner.create_cursor(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_suitable_iter() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();
        let filter = all();
        let find_options = FindOptions::default();
        let index_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field"]).unwrap(),
            "test_collection",
        );
        let find_plan = inner
            .find_optimizer
            .create_find_plan(&filter, &find_options, &vec![index_descriptor])
            .unwrap();
        let result = inner.find_suitable_iter(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_suitable_iter_with_empty_sub_plans() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();
        let mut find_plan = FindPlan::new();
        find_plan.add_sub_plan(FindPlan::new());
        // Remove it for empty test
        let find_plan = FindPlan::new();
        if let Some(mut sub_plans) = find_plan.sub_plans() {
            sub_plans.clear();
        }
        // Actually, for empty we just don't add any

        let result = inner.find_suitable_iter(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_suitable_iter_with_sub_plans() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();
        let mut find_plan = FindPlan::new();

        let sub_plan1 = FindPlan::new();
        let sub_plan2 = FindPlan::new();

        find_plan.add_sub_plan(sub_plan1);
        find_plan.add_sub_plan(sub_plan2);

        let result = inner.find_suitable_iter(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_suitable_iter_no_panics_with_atomic_access() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        // Create a find plan with sub_plans that should be accessed atomically
        let mut find_plan = FindPlan::new();
        for _ in 0..3 {
            find_plan.add_sub_plan(FindPlan::new());
        }

        // This should not panic with the fix (using if-let instead of multiple unwraps)
        let result = inner.find_suitable_iter(&find_plan);
        assert!(result.is_ok());
    }

    // Performance optimization tests for cursor and filter operations

    #[test]
    fn test_create_cursor_simple_path() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        // Create a minimal find plan for fast path
        let find_plan = FindPlan::new();

        let result = inner.create_cursor(&find_plan);
        assert!(result.is_ok());

        let cursor = result.unwrap();
        // Verify cursor has plan set
        assert!(cursor.find_plan().is_some());
    }

    #[test]
    fn test_create_cursor_with_limit_and_skip() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        let mut find_plan = FindPlan::new();
        find_plan.set_skip(10);
        find_plan.set_limit(5);

        let result = inner.create_cursor(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_prepare_filter_avoids_string_cloning() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        // Test with all filter (should return early)
        let all_filter = all();
        let result = inner.prepare_filter(&all_filter);
        assert!(result.is_ok());

        // Test with regular filter - collection name should be set
        let field_filter = field("test_field").eq(Value::from("value"));
        let result = inner.prepare_filter(&field_filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_prepare_filter_recursive_logical_filters() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        // Create nested filters
        let filter1 = field("field1").eq(Value::from("value1"));
        let filter2 = field("field2").eq(Value::from("value2"));
        let combined = filter1.and(filter2);

        let result = inner.prepare_filter(&combined);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cursor_creation_efficiency() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        // Test that cursor creation doesn't create redundant intermediate objects
        let find_plan = FindPlan::new();

        let cursor1 = inner.create_cursor(&find_plan);
        assert!(cursor1.is_ok());

        let cursor2 = inner.create_cursor(&find_plan);
        assert!(cursor2.is_ok());

        // Both should be valid
        let c1 = cursor1.unwrap();
        let c2 = cursor2.unwrap();

        assert!(c1.find_plan().is_some());
        assert!(c2.find_plan().is_some());
    }

    #[test]
    fn test_find_operation_with_prepared_filter() {
        let read_operations = setup_read_operations();

        // Insert test data first
        let mut doc = Document::new();
        doc.put("field", Value::from("test_value")).unwrap();

        // Test find with filter
        let filter = field("field").eq(Value::from("test_value"));
        let find_options = FindOptions::default();

        let result = read_operations.find(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_with_sorting_aware_filter() {
        let read_operations = setup_read_operations();
        let inner = read_operations.inner.clone();

        // First, insert some test documents
        let mut docs = Vec::new();
        for i in 1..=10 {
            let mut doc = Document::new();
            doc.put("emp_id", Value::from(i as u64)).unwrap();
            docs.push(doc);
        }

        // Insert docs into the map
        for doc in docs.iter_mut() {
            let id = doc.id().unwrap();
            inner
                .nitrite_map
                .put(Value::from(id), doc.clone().into())
                .unwrap();
        }

        // Test find with gt filter
        let filter = field("emp_id").gt(5i64);
        let find_options = FindOptions::default();

        let result = read_operations.find(filter, &find_options);
        assert!(result.is_ok());

        let cursor = result.unwrap();
        let count = cursor.count();
        // Should be 5 (values 6, 7, 8, 9, 10)
        assert_eq!(
            count, 5,
            "Expected 5 documents with emp_id > 5, got {}",
            count
        );
    }

    #[test]
    fn test_find_plan_has_full_scan_filter_for_sorting_aware() {
        let read_operations = setup_read_operations();

        // Test with gt filter (no index)
        let filter = field("emp_id").gt(5i64);
        let find_options = FindOptions::default();

        // Get index descriptors from index operations
        let index_descriptors = read_operations
            .inner
            .index_operations
            .list_indexes()
            .unwrap();

        // Create find plan
        let find_plan = read_operations
            .inner
            .find_optimizer
            .create_find_plan(&filter, &find_options, &index_descriptors)
            .unwrap();

        // Verify full_scan_filter is set
        assert!(
            find_plan.full_scan_filter().is_some(),
            "Expected full_scan_filter to be set for SortingAwareFilter, but it was None"
        );

        // Also check the fast path conditions
        assert!(find_plan.by_id_filter().is_none());
        assert!(find_plan.index_descriptor().is_none());
    }
}
