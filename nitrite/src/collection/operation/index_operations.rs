use super::index_manager::IndexManager;
use crate::collection::operation::find_optimizer::{self, FindOptimizer};
use crate::common::{ReadExecutor, WriteExecutor};
use crate::index::NitriteIndexer;
use crate::{
    atomic,
    collection::{CollectionEventInfo, CollectionEventListener, CollectionEvents},
    errors::{ErrorKind, NitriteError, NitriteResult},
    get_document_values,
    index::{IndexDescriptor, NitriteIndexerProvider},
    nitrite_config::NitriteConfig,
    store::{NitriteMap, NitriteMapProvider},
    Atomic, Convertible, Fields, NitriteEventBus, Value,
};
use dashmap::DashMap;
use std::sync::Arc;


#[derive(Clone)]
pub(crate) struct IndexOperations {
    /// Arc-wrapped internal implementation
    inner: Arc<IndexOperationInner>,
}

impl IndexOperations {
    /// Creates a new IndexOperations instance for the given collection.
    ///
    /// # Arguments
    /// * `collection_name` - The name of the collection
    /// * `nitrite_config` - The Nitrite database configuration
    /// * `nitrite_map` - The underlying data map for the collection
    /// * `find_optimizer` - The query optimizer for this collection
    /// * `event_bus` - Event bus for publishing index events
    ///
    /// # Errors
    /// Returns a NitriteResult error if initialization fails
    pub fn new(
        collection_name: String,
        nitrite_config: NitriteConfig,
        nitrite_map: NitriteMap,
        find_optimizer: FindOptimizer,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    ) -> NitriteResult<Self> {
        let inner = IndexOperationInner::new(
            collection_name,
            nitrite_config,
            nitrite_map,
            find_optimizer,
            event_bus,
        )?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Gets the collection name this IndexOperations manages.
    pub fn collection_name(&self) -> String {
        self.inner.collection_name.clone()
    }

    /// Gets the Nitrite configuration used by this IndexOperations.
    pub fn nitrite_config(&self) -> NitriteConfig {
        self.inner.nitrite_config.clone()
    }

    /// Gets the underlying NitriteMap for the collection.
    pub fn nitrite_map(&self) -> NitriteMap {
        self.inner.nitrite_map.clone()
    }

    /// Gets the find optimizer associated with this collection.
    pub fn find_optimizer(&self) -> FindOptimizer {
        self.inner.find_optimizer.clone()
    }

    /// Closes all indexes and releases associated resources.
    pub fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    /// Creates an index on the specified fields.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to create the index on
    /// * `index_type` - The type of index (e.g., "UNIQUE", "NON_UNIQUE")
    ///
    /// # Errors
    /// Returns an error if the index already exists with a different type,
    /// or if index creation fails.
    pub fn create_index(&self, fields: &Fields, index_type: &str) -> NitriteResult<()> {
        self.inner.create_index(fields, index_type)
    }

    /// Builds an index by processing all existing documents.
    ///
    /// # Arguments
    /// * `index_descriptor` - The descriptor of the index to build
    /// * `rebuild` - Whether to drop and recreate the index before building
    ///
    /// # Errors
    /// Returns an error if the index is already being built or if building fails.
    pub fn build_index(
        &self,
        index_descriptor: &IndexDescriptor,
        rebuild: bool,
    ) -> NitriteResult<()> {
        self.inner.build_index(index_descriptor, rebuild)
    }

    /// Drops an index and removes its data.
    ///
    /// # Arguments
    /// * `fields` - The field(s) of the index to drop
    ///
    /// # Errors
    /// Returns an error if the index is currently being built.
    pub fn drop_index(&self, fields: &Fields) -> NitriteResult<()> {
        self.inner.drop_index(fields)
    }

    /// Drops all indexes on this collection.
    ///
    /// # Errors
    /// Returns an error if any index is currently being built.
    pub fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.inner.drop_all_indexes()
    }

    /// Permanently disposes all indexes and the index metadata map.
    ///
    /// This should be used when the collection is being removed entirely.
    /// Unlike `drop_all_indexes()`, this operation cannot be reversed.
    ///
    /// # Errors
    /// Returns an error if any index is currently being built.
    pub fn dispose_all_indexes(&self) -> NitriteResult<()> {
        self.inner.dispose_all_indexes()
    }

    /// Clears all index data while preserving the index structures.
    ///
    /// # Errors
    /// Returns an error if any index is currently being built.
    pub fn clear(&self) -> NitriteResult<()> {
        self.inner.clear()
    }

    /// Checks if an index is currently being built on the specified fields.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to check
    ///
    /// # Returns
    /// `true` if an index exists and is currently being built, `false` otherwise
    pub fn is_indexing(&self, fields: &Fields) -> NitriteResult<bool> {
        self.inner.is_indexing(fields)
    }

    /// Checks if an index entry exists for the specified fields.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to check
    ///
    /// # Returns
    /// `true` if an index exists for these fields, `false` otherwise
    pub fn has_index_entry(&self, fields: &Fields) -> NitriteResult<bool> {
        self.inner.has_index_entry(fields)
    }

    /// Finds the index descriptor for the specified fields.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to find the index for
    ///
    /// # Returns
    /// The IndexDescriptor if found, None otherwise
    pub fn find_index_descriptor(&self, fields: &Fields) -> NitriteResult<Option<IndexDescriptor>> {
        self.inner.find_index_descriptor(fields)
    }

    /// Lists all indexes on this collection.
    ///
    /// # Returns
    /// A vector of all IndexDescriptors for this collection
    pub fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.inner.list_indexes()
    }

    /// Checks if an index needs to be rebuilt.
    ///
    /// An index needs rebuilding if it's marked as dirty and not currently being built.
    ///
    /// # Arguments
    /// * `fields` - The field(s) of the index to check
    ///
    /// # Returns
    /// `true` if the index needs rebuilding, `false` otherwise
    pub fn should_rebuild_index(&self, fields: &Fields) -> NitriteResult<bool> {
        self.inner.should_rebuild_index(fields)
    }
}

/// The internal implementation of IndexOperations.
///
/// This struct contains all the actual state and logic for index operations.
/// It is not directly accessible from outside the crate - all access goes through
/// the public `IndexOperations` interface.
struct IndexOperationInner {
    collection_name: String,
    nitrite_config: NitriteConfig,
    nitrite_map: NitriteMap,
    event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    index_build_tracker: DashMap<Fields, bool>,
    index_manager: Atomic<IndexManager>,
    find_optimizer: FindOptimizer,
    indexer_cache: DashMap<String, NitriteIndexer>,
}

impl IndexOperationInner {
    fn new(
        collection_name: String,
        nitrite_config: NitriteConfig,
        nitrite_map: NitriteMap,
        find_optimizer: FindOptimizer,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    ) -> NitriteResult<Self> {
        let index_manager = IndexManager::new(collection_name.clone(), nitrite_config.clone())?;
        let index_build_tracker = DashMap::new();
        let indexer_cache = DashMap::new();

        Ok(Self {
            collection_name,
            nitrite_config,
            nitrite_map,
            event_bus,
            index_build_tracker,
            index_manager: atomic(index_manager),
            find_optimizer,
            indexer_cache,
        })
    }

    pub fn close(&self) -> NitriteResult<()> {
        self.index_manager.write_with(|manager| manager.close())
    }

    pub fn create_index(&self, fields: &Fields, index_type: &str) -> NitriteResult<()> {
        let index_descriptor = self
            .index_manager
            .read_with(|manager| manager.find_exact_index(fields))?;

        if let Some(index_descriptor) = index_descriptor {
            // if index already there check if it is of same type, if not return error
            if index_descriptor.index_type() != index_type {
                log::error!(
                    "Index already exists on fields {:?} with different type: {}",
                    fields.field_names(),
                    index_descriptor.index_type()
                );
                Err(NitriteError::new(
                    &format!(
                        "Index already exists with different type: {}",
                        index_descriptor.index_type()
                    ),
                    ErrorKind::IndexingError,
                ))
            } else {
                // if index is of same type, return
                Ok(())
            }
        } else {
            // if index not there, create new index
            let index_descriptor = self
                .index_manager
                .read_with(|manager| manager.create_index_descriptor(fields, index_type))?;
            self.build_index(&index_descriptor, false)?;

            self.find_optimizer.invalidate_cache();
            Ok(())
        }
    }

    pub fn build_index(
        &self,
        index_descriptor: &IndexDescriptor,
        rebuild: bool,
    ) -> NitriteResult<()> {
        let fields = index_descriptor.index_fields();
        let build_flag = self.get_build_flag(&fields);

        if !build_flag {
            self.set_build_flag(&fields, true);
            self.build_index_internal(index_descriptor, rebuild)?;
            Ok(())
        } else {
            log::error!(
                "Index is already building for fields: {:?}",
                fields.field_names()
            );
            Err(NitriteError::new(
                &format!(
                    "Index is already building for fields: {:?}",
                    fields.field_names()
                ),
                ErrorKind::IndexingError,
            ))
        }
    }

    pub fn drop_index(&self, fields: &Fields) -> NitriteResult<()> {
        let build_flag = self.get_build_flag(fields);
        if build_flag {
            log::error!("Index is building for fields: {:?}", fields.field_names());
            return Err(NitriteError::new(
                &format!("Index is building for fields: {:?}", fields.field_names()),
                ErrorKind::IndexingError,
            ));
        }

        let index_descriptor = self.find_index_descriptor(fields)?;
        if let Some(index_descriptor) = index_descriptor {
            self.find_optimizer
                .invalidate_index_entries(&index_descriptor);

            let index_type = index_descriptor.index_type();
            let indexer = self.get_indexer(&index_type)?;
            indexer.drop_index(&index_descriptor, &self.nitrite_config)?;

            self.index_manager
                .read_with(|manager| manager.drop_index_descriptor(fields))?;
            self.index_build_tracker.remove(fields);
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn drop_all_indexes(&self) -> NitriteResult<()> {
        for val in self.index_build_tracker.iter() {
            if *val.value() {
                log::error!("Index is building, cannot drop all indexes");
                return Err(NitriteError::new(
                    "Index is building, cannot drop all indexes",
                    ErrorKind::IndexingError,
                ));
            }
        }

        let indexes = self.list_indexes()?;
        for x in &indexes {
            let fields = x.index_fields();
            self.drop_index(&fields)?;
        }

        // Note: drop_index already removed the index entries from index_meta_map and disposed
        // the index maps, so we just need to clear our tracking state and invalidate caches.
        // We do NOT call clear_all() here because the indexes are already dropped.
        self.index_build_tracker.clear();

        // Invalidate all cache entries
        self.find_optimizer.invalidate_cache();

        Ok(())
    }

    /// Dispose all indexes and the index meta map permanently.
    /// Use this when the collection is being removed entirely.
    /// Unlike `drop_all_indexes()`, this does not recreate the IndexManager.
    pub fn dispose_all_indexes(&self) -> NitriteResult<()> {
        for val in self.index_build_tracker.iter() {
            if *val.value() {
                log::error!("Index is building, cannot dispose all indexes");
                return Err(NitriteError::new(
                    "Index is building, cannot dispose all indexes",
                    ErrorKind::IndexingError,
                ));
            }
        }

        let indexes = self.list_indexes()?;
        for x in &indexes {
            let fields = x.index_fields();
            self.drop_index(&fields)?;
        }

        self.index_manager
            .read_with(|manager| manager.dispose_index_meta())?;
        self.index_build_tracker.clear();
        self.index_manager.read_with(|manager| manager.close())?;

        // Invalidate all cache entries
        self.find_optimizer.invalidate_cache();

        Ok(())
    }

    pub fn clear(&self) -> NitriteResult<()> {
        for val in self.index_build_tracker.iter() {
            if *val.value() {
                log::error!("Index is building, cannot clear indexes");
                return Err(NitriteError::new(
                    "Index is building, cannot clear indexes",
                    ErrorKind::IndexingError,
                ));
            }
        }

        self.index_manager
            .read_with(|manager| manager.clear_all())?;
        self.index_build_tracker.clear();
        Ok(())
    }

    pub fn is_indexing(&self, fields: &Fields) -> NitriteResult<bool> {
        let has_index = self
            .index_manager
            .read_with(|manager| manager.has_index_descriptor(fields))?;
        Ok(has_index && self.get_build_flag(fields))
    }

    pub fn has_index_entry(&self, fields: &Fields) -> NitriteResult<bool> {
        Ok(self
            .index_manager
            .read_with(|manager| manager.has_index_descriptor(fields))?)
    }

    pub fn find_index_descriptor(&self, fields: &Fields) -> NitriteResult<Option<IndexDescriptor>> {
        Ok(self
            .index_manager
            .read_with(|manager| manager.find_exact_index(fields))?)
    }

    pub fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        Ok(self
            .index_manager
            .read_with(|manager| manager.get_index_descriptors())?)
    }

    pub fn should_rebuild_index(&self, fields: &Fields) -> NitriteResult<bool> {
        Ok(self
            .index_manager
            .read_with(|manager| manager.is_dirty_index(fields))?
            && !self.get_build_flag(fields))
    }

    fn get_indexer(&self, index_type: &str) -> NitriteResult<NitriteIndexer> {
        // Use entry API for single-lookup caching pattern
        use dashmap::mapref::entry::Entry;
        match self.indexer_cache.entry(index_type.to_string()) {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let indexer = self.nitrite_config.find_indexer(index_type)?;
                vacant.insert(indexer.clone());
                Ok(indexer)
            }
        }
    }

    fn get_build_flag(&self, fields: &Fields) -> bool {
        // Use entry API to avoid redundant clone operations
        use dashmap::mapref::entry::Entry;
        match self.index_build_tracker.entry(fields.clone()) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                vacant.insert(false);
                false
            }
        }
    }

    fn set_build_flag(&self, fields: &Fields, flag: bool) {
        self.index_build_tracker.insert(fields.clone(), flag);
    }

    fn build_index_internal(
        &self,
        index_descriptor: &IndexDescriptor,
        rebuild: bool,
    ) -> NitriteResult<()> {
        let fields = index_descriptor.index_fields();

        // Use RAII pattern for build flag
        struct BuildFlagGuard<'a> {
            fields: &'a Fields,
            ops: &'a IndexOperationInner,
            completed: bool,
        }

        impl<'a> BuildFlagGuard<'a> {
            fn new(fields: &'a Fields, ops: &'a IndexOperationInner) -> Self {
                ops.set_build_flag(fields, true);
                Self {
                    fields,
                    ops,
                    completed: false,
                }
            }

            fn complete(&mut self) {
                self.completed = true;
            }
        }

        impl<'a> Drop for BuildFlagGuard<'a> {
            fn drop(&mut self) {
                if !self.completed {
                    self.ops.set_build_flag(self.fields, false);
                }
            }
        }

        let mut guard = BuildFlagGuard::new(&fields, self);

        let result = (|| {
            self.index_manager
                .read_with(|manager| manager.begin_indexing(&fields))?;

            let index_type = index_descriptor.index_type();
            let indexer = self.get_indexer(&index_type)?;

            if rebuild {
                indexer.drop_index(index_descriptor, &self.nitrite_config)?;
            }

            // Process documents
            for entry in self.nitrite_map.entries()? {
                let (_, value) = entry?;
                if let Value::Document(mut doc) = value {
                    let field_values = get_document_values(&mut doc, &fields)?;
                    indexer.write_index_entry(
                        &field_values,
                        index_descriptor,
                        &self.nitrite_config,
                    )?;
                }
            }

            self.index_manager
                .read_with(|manager| manager.end_indexing(&fields))?;

            Ok(())
        })();

        match result {
            Ok(_) => {
                guard.complete(); // Mark as complete so flag is not cleared in Drop
                self.set_build_flag(&fields, false);
                self.alert(CollectionEvents::IndexEnd, &fields)?;
                Ok(())
            }
            Err(e) => {
                // The guard will reset the flag when dropped
                Err(e)
            }
        }
    }

    fn alert(&self, event_type: CollectionEvents, fields: &Fields) -> NitriteResult<()> {
        let event = CollectionEventInfo::new(
            Some(fields.to_value()?),
            event_type,
            self.collection_name.clone(),
        );

        self.event_bus.publish(event)?;
        Ok(())
    }
}

impl Drop for IndexOperationInner {
    fn drop(&mut self) {
        // Attempt to close the index_manager
        self.index_manager.write_with(|manager| {
            if let Err(e) = manager.close() {
                log::error!("Failed to close index manager: {}", e);
            }
        });

        // Clear trackers
        self.index_build_tracker.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Fields, UNIQUE_INDEX};
    use crate::index::IndexDescriptor;
    use crate::nitrite_config::NitriteConfig;
    use crate::store::NitriteStoreProvider;

    fn setup_index_operations() -> IndexOperations {
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
        IndexOperations::new(
            collection_name,
            nitrite_config,
            nitrite_map,
            find_optimizer,
            event_bus,
        )
        .unwrap()
    }

    fn create_fields() -> Fields {
        Fields::with_names(vec!["field"]).unwrap()
    }

    fn create_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new(UNIQUE_INDEX, create_fields(), "test_collection")
    }

    #[test]
    fn test_new() {
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
        let result = IndexOperations::new(
            collection_name,
            nitrite_config,
            nitrite_map,
            find_optimizer,
            event_bus,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let index_operations = setup_index_operations();
        let result = index_operations.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index() {
        let index_operations = setup_index_operations();
        let fields = create_fields();
        let result = index_operations.create_index(&fields, UNIQUE_INDEX);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_index() {
        let index_operations = setup_index_operations();
        let index_descriptor = create_index_descriptor();
        let result = index_operations.build_index(&index_descriptor, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_index() {
        let index_operations = setup_index_operations();
        let fields = create_fields();
        let result = index_operations.drop_index(&fields);
        assert!(result.is_ok());

        index_operations
            .create_index(&fields, UNIQUE_INDEX)
            .expect("Failed to create index");
        let result = index_operations.drop_index(&fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_all_indexes() {
        let index_operations = setup_index_operations();
        let result = index_operations.drop_all_indexes();
        assert!(result.is_ok());

        let fields1 = create_fields();
        let fields2 = Fields::with_names(vec!["field1", "field2"]).unwrap();

        index_operations
            .create_index(&fields1, UNIQUE_INDEX)
            .expect("Failed to create index");
        index_operations
            .create_index(&fields2, UNIQUE_INDEX)
            .expect("Failed to create index");
        let result = index_operations.drop_all_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear() {
        let index_operations = setup_index_operations();
        let result = index_operations.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_indexing() {
        let index_operations = setup_index_operations();
        let fields = create_fields();
        let result = index_operations.is_indexing(&fields);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_has_index_entry() {
        let index_operations = setup_index_operations();
        let fields = create_fields();
        let result = index_operations.has_index_entry(&fields);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_find_index_descriptor() {
        let index_operations = setup_index_operations();
        let fields = create_fields();
        let result = index_operations.find_index_descriptor(&fields);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_list_indexes() {
        let index_operations = setup_index_operations();
        let result = index_operations.list_indexes();
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_should_rebuild_index() {
        let index_operations = setup_index_operations();
        let fields = create_fields();
        let result = index_operations.should_rebuild_index(&fields);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    // Performance optimization tests for indexer caching

    #[test]
    fn test_get_indexer_cache_hit() {
        let index_operations = setup_index_operations();
        let inner = index_operations.inner.clone();
        
        // First call - cache miss, should populate
        let indexer1 = inner.get_indexer(UNIQUE_INDEX);
        assert!(indexer1.is_ok());
        
        // Second call - cache hit, should return immediately
        let indexer2 = inner.get_indexer(UNIQUE_INDEX);
        assert!(indexer2.is_ok());
        
        // Both should work correctly
        let i1 = indexer1.unwrap();
        let i2 = indexer2.unwrap();
        
        // Verify they're equivalent (same indexer type)
        assert_eq!(i1.index_type(), i2.index_type());
    }

    #[test]
    fn test_get_indexer_multiple_types() {
        let index_operations = setup_index_operations();
        let inner = index_operations.inner.clone();
        
        // Test with UNIQUE_INDEX type
        let unique_indexer = inner.get_indexer(UNIQUE_INDEX);
        assert!(unique_indexer.is_ok());
        
        // Verify cache is populated
        assert!(inner.indexer_cache.contains_key(UNIQUE_INDEX));
    }

    #[test]
    fn test_get_build_flag_entry_api_optimization() {
        let index_operations = setup_index_operations();
        let inner = index_operations.inner.clone();
        let fields = create_fields();
        
        // First call - should initialize to false
        let flag1 = inner.get_build_flag(&fields);
        assert!(!flag1);
        
        // Tracker should now have entry
        assert!(inner.index_build_tracker.contains_key(&fields));
        
        // Second call - should return same value
        let flag2 = inner.get_build_flag(&fields);
        assert_eq!(flag1, flag2);
        
        // Manually set flag to true
        inner.set_build_flag(&fields, true);
        
        // Should now return true
        let flag3 = inner.get_build_flag(&fields);
        assert!(flag3);
    }

    #[test]
    fn test_get_build_flag_multiple_fields() {
        let index_operations = setup_index_operations();
        let inner = index_operations.inner.clone();
        
        let fields1 = Fields::with_names(vec!["field1"]).unwrap();
        let fields2 = Fields::with_names(vec!["field2"]).unwrap();
        
        let flag1 = inner.get_build_flag(&fields1);
        let flag2 = inner.get_build_flag(&fields2);
        
        // Both should initialize to false
        assert!(!flag1);
        assert!(!flag2);
        
        // Should have separate entries
        assert!(inner.index_build_tracker.contains_key(&fields1));
        assert!(inner.index_build_tracker.contains_key(&fields2));
    }

    #[test]
    fn test_indexer_cache_consistency() {
        let index_operations = setup_index_operations();
        let inner = index_operations.inner.clone();
        
        // Verify cache operations are consistent across multiple accesses
        let indexer1 = inner.get_indexer(UNIQUE_INDEX).unwrap();
        let indexer2 = inner.get_indexer(UNIQUE_INDEX).unwrap();
        
        // Should return identical indexer instances (by value)
        assert_eq!(indexer1.index_type(), indexer2.index_type());
    }
}
