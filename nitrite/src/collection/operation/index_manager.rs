use crate::common::{ReadExecutor, WriteExecutor};
use crate::{
    atomic, derive_index_map_name, derive_index_meta_map_name,
    errors::{NitriteError, NitriteResult},
    index::{index_meta::IndexMeta, IndexDescriptor, NitriteIndexerProvider},
    nitrite_config::NitriteConfig,
    store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider},
    Atomic, Convertible, Fields,
};
use std::borrow::Cow;
use std::sync::Arc;


#[derive(Clone)]
pub(crate) struct IndexManager {
    /// Arc-wrapped opaque implementation
    inner: Arc<IndexManagerInner>,
}

impl IndexManager {
    /// Creates a new IndexManager for the given collection.
    ///
    /// # Arguments
    /// * `collection_name` - The name of the collection
    /// * `nitrite_config` - The Nitrite database configuration
    ///
    /// # Errors
    /// Returns a NitriteResult error if initialization fails
    pub fn new(collection_name: String, nitrite_config: NitriteConfig) -> NitriteResult<Self> {
        Ok(Self {
            inner: Arc::new(IndexManagerInner::new(collection_name, nitrite_config)?),
        })
    }

    // ==================== Getter Methods ====================

    /// Gets the Nitrite configuration used by this manager.
    ///
    /// Returns a clone of the configuration object.
    pub fn nitrite_config(&self) -> NitriteConfig {
        self.inner.nitrite_config.clone()
    }

    /// Gets the underlying NitriteStore.
    ///
    /// Returns a clone of the store reference.
    pub fn store(&self) -> NitriteStore {
        self.inner.store.clone()
    }

    /// Gets the collection name this manager handles.
    pub fn collection_name(&self) -> String {
        self.inner.collection_name.to_string()
    }

    /// Gets a clone of the index metadata map.
    pub fn index_meta_map(&self) -> NitriteMap {
        self.inner.index_meta_map.clone()
    }

    /// Gets the index descriptor cache (for testing purposes).
    ///
    /// Returns the current cached value, if any.
    pub fn index_descriptor_cache(&self) -> Option<Vec<IndexDescriptor>> {
        self.inner.index_descriptor_cache.read_with(|it| it.clone())
    }

    /// Checks if an index exists for the given fields.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to check for index existence
    ///
    /// # Returns
    /// `true` if a matching index exists, `false` otherwise
    pub fn has_index_descriptor(&self, fields: &Fields) -> NitriteResult<bool> {
        self.inner.has_index_descriptor(fields)
    }

    /// Gets all index descriptors for this collection.
    ///
    /// # Returns
    /// A vector of all IndexDescriptor objects
    pub fn get_index_descriptors(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.inner.get_index_descriptors()
    }

    /// Finds indexes matching the given fields.
    ///
    /// # Arguments
    /// * `fields` - The fields to match against
    ///
    /// # Returns
    /// A vector of matching IndexDescriptors, sorted by relevance
    pub fn find_matching_index(&self, fields: &Fields) -> NitriteResult<Vec<IndexDescriptor>> {
        self.inner.find_matching_index(fields)
    }

    /// Finds an exact index for the given fields.
    ///
    /// # Arguments
    /// * `fields` - The exact fields to match
    ///
    /// # Returns
    /// The matching IndexDescriptor if found, None otherwise
    pub fn find_exact_index(&self, fields: &Fields) -> NitriteResult<Option<IndexDescriptor>> {
        self.inner.find_exact_index(fields)
    }

    /// Marks an index as dirty, requiring revalidation.
    ///
    /// # Arguments
    /// * `index_descriptor` - The index to mark as dirty
    pub fn mark_index_dirty(&self, index_descriptor: &IndexDescriptor) -> NitriteResult<()> {
        self.inner.mark_index_dirty(index_descriptor)
    }

    /// Closes all open indexes and resources.
    pub fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    /// Clears all index data (but keeps metadata).
    pub fn clear_all(&self) -> NitriteResult<()> {
        self.inner.clear_all()
    }

    /// Checks if an index is marked as dirty.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to check
    ///
    /// # Returns
    /// `true` if the index is dirty, `false` otherwise
    pub fn is_dirty_index(&self, fields: &Fields) -> NitriteResult<bool> {
        self.inner.is_dirty_index(fields)
    }

    /// Creates a new index descriptor and registers it.
    ///
    /// # Arguments
    /// * `fields` - The field(s) to index
    /// * `index_type` - The type of index (e.g., "UNIQUE", "NON_UNIQUE")
    ///
    /// # Returns
    /// The created IndexDescriptor
    pub fn create_index_descriptor(
        &self,
        fields: &Fields,
        index_type: &str,
    ) -> NitriteResult<IndexDescriptor> {
        self.inner.create_index_descriptor(fields, index_type)
    }

    /// Drops an index descriptor (removes metadata but not the actual index).
    ///
    /// # Arguments
    /// * `fields` - The field(s) of the index to drop
    pub fn drop_index_descriptor(&self, fields: &Fields) -> NitriteResult<()> {
        self.inner.drop_index_descriptor(fields)
    }

    /// Disposes all index metadata.
    pub fn dispose_index_meta(&self) -> NitriteResult<()> {
        self.inner.dispose_index_meta()
    }

    /// Begins an indexing operation, marking the index as dirty.
    ///
    /// # Arguments
    /// * `fields` - The field(s) being indexed
    pub fn begin_indexing(&self, fields: &Fields) -> NitriteResult<()> {
        self.inner.begin_indexing(fields)
    }

    /// Ends an indexing operation, marking the index as clean.
    ///
    /// # Arguments
    /// * `fields` - The field(s) that were indexed
    pub fn end_indexing(&self, fields: &Fields) -> NitriteResult<()> {
        self.inner.end_indexing(fields)
    }
}

/// The internal implementation of IndexManager.
///
/// This struct contains all the actual state and logic for index management.
/// It is not directly accessible from outside the crate - all access goes through
/// the public `IndexManager` interface.
pub(crate) struct IndexManagerInner {
    /// Configuration used to create and manage indexes
    nitrite_config: NitriteConfig,
    /// The underlying store for persisting index metadata
    store: NitriteStore,
    /// The name of the collection this manager serves
    collection_name: Cow<'static, str>,
    /// Map storing index metadata (name -> IndexMeta)
    index_meta_map: NitriteMap,
    /// Cache of all index descriptors for fast lookup
    index_descriptor_cache: Atomic<Option<Vec<IndexDescriptor>>>,
}

impl IndexManagerInner {
    fn new(collection_name: String, nitrite_config: NitriteConfig) -> NitriteResult<Self> {
        let store = nitrite_config.nitrite_store()?;
        let index_meta_map = Self::get_index_meta_map(&collection_name, store.clone())?;

        Ok(Self {
            nitrite_config,
            store,
            collection_name: Cow::Owned(collection_name),
            index_meta_map,
            index_descriptor_cache: atomic(None),
        })
    }

    pub fn has_index_descriptor(&self, fields: &Fields) -> NitriteResult<bool> {
        Ok(!self.find_matching_index(fields)?.is_empty())
    }

    pub fn get_index_descriptors(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.ensure_index_descriptor_cache()?;
        Ok(self
            .index_descriptor_cache
            .read_with(|it| it.clone().unwrap_or_default()))
    }

    pub fn find_matching_index(&self, fields: &Fields) -> NitriteResult<Vec<IndexDescriptor>> {
        let index_descriptors = self.get_index_descriptors()?;
        let field_names = fields.field_names();
        let field_count = field_names.len();

        // First filter by field count for faster rejection
        let mut matching_indexes: Vec<IndexDescriptor> = index_descriptors
            .into_iter()
            .filter(|descriptor| descriptor.index_fields().field_names().len() >= field_count)
            .filter(|descriptor| descriptor.index_fields().starts_with(fields))
            .collect();

        // Sort by field count for better relevance
        matching_indexes.sort_by_key(|idx| idx.index_fields().field_names().len());

        Ok(matching_indexes)
    }

    pub fn find_exact_index(&self, fields: &Fields) -> NitriteResult<Option<IndexDescriptor>> {
        let fields = fields.to_value()
            .map_err(|e| NitriteError::new(&format!("Failed to convert fields to value: {}", e), e.kind().clone()))?;
        let index_meta_value = self.index_meta_map.get(&fields)
            .map_err(|e| NitriteError::new(&format!("Failed to retrieve index metadata for fields: {}", e), e.kind().clone()))?;
        match index_meta_value {
            Some(value) => {
                let index_meta = IndexMeta::from_value(&value)
                    .map_err(|e| NitriteError::new(&format!("Failed to deserialize index metadata: {}", e), e.kind().clone()))?;
                Ok(Some(index_meta.index_descriptor()))
            }
            None => Ok(None),
        }
    }

    pub fn mark_index_dirty(&self, index_descriptor: &IndexDescriptor) -> NitriteResult<()> {
        let fields = index_descriptor.index_fields();
        self.mark_dirty(&fields, true)
    }

    pub fn close(&self) -> NitriteResult<()> {
        let mut error_messages = Vec::new();

        if !self.index_meta_map.is_closed()? && !self.index_meta_map.is_dropped()? {
            let index_meta_list = match self.index_meta_map.values() {
                Ok(list) => list,
                Err(e) => {
                    error_messages.push(format!("Failed to get index meta values: {}", e));
                    return Err(e);
                }
            };

            for index_meta_result in index_meta_list {
                match index_meta_result {
                    Ok(index_meta_value) => {
                        if let Ok(index_meta) = IndexMeta::from_value(&index_meta_value) {
                            let index_map_name = index_meta.index_map_name();
                            if let Ok(is_opened) = self.store.is_map_opened(&index_map_name) {
                                if is_opened {
                                    match self.store.open_map(&index_map_name) {
                                        Ok(index_map) => {
                                            if let Err(e) = index_map.close() {
                                                error_messages.push(format!(
                                                    "Failed to close index map {}: {}",
                                                    index_map_name, e
                                                ));
                                            }
                                        }
                                        Err(e) => {
                                            error_messages.push(format!(
                                                "Failed to open index map {}: {}",
                                                index_map_name, e
                                            ));
                                        }
                                    }
                                }
                            } else {
                                error_messages.push(format!(
                                    "Failed to check if index map {} is opened",
                                    index_map_name
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        error_messages.push(format!("Failed to read index meta value: {}", e));
                    }
                }
            }
        }

        if let Err(e) = self.index_meta_map.close() {
            error_messages.push(format!("Failed to close index meta map: {}", e));
            return Err(e);
        }

        if error_messages.is_empty() {
            Ok(())
        } else {
            let error_message = error_messages.join("; ");
            Err(crate::errors::NitriteError::new(
                &error_message,
                crate::errors::ErrorKind::IndexingError,
            ))
        }
    }

    pub fn clear_all(&self) -> NitriteResult<()> {
        let index_meta_list = self.index_meta_map.values()
            .map_err(|e| NitriteError::new(&format!("Failed to retrieve index metadata list for clearing: {}", e), e.kind().clone()))?;

        // Process sequentially - avoids global thread pool contention and is more predictable
        for index_meta_result in index_meta_list {
            let index_meta_value = index_meta_result
                .map_err(|e| NitriteError::new(&format!("Failed to read index metadata entry: {}", e), e.kind().clone()))?;
            let index_meta = IndexMeta::from_value(&index_meta_value)
                .map_err(|e| NitriteError::new(&format!("Failed to deserialize index metadata for clearing: {}", e), e.kind().clone()))?;
            let index_map_name = index_meta.index_map_name();
            let index_map = self.store.open_map(&index_map_name)
                .map_err(|e| NitriteError::new(&format!("Failed to open index map '{}' for clearing: {}", index_map_name, e), e.kind().clone()))?;
            index_map.clear()
                .map_err(|e| NitriteError::new(&format!("Failed to clear index map '{}': {}", index_map_name, e), e.kind().clone()))?;
        }
        Ok(())
    }

    pub fn is_dirty_index(&self, fields: &Fields) -> NitriteResult<bool> {
        let fields = fields.to_value()
            .map_err(|e| NitriteError::new(&format!("Failed to convert fields to value for dirty check: {}", e), e.kind().clone()))?;
        let index_meta_value = self.index_meta_map.get(&fields)
            .map_err(|e| NitriteError::new(&format!("Failed to retrieve index metadata for dirty check: {}", e), e.kind().clone()))?;
        match index_meta_value {
            Some(value) => {
                let index_meta = IndexMeta::from_value(&value)
                    .map_err(|e| NitriteError::new(&format!("Failed to deserialize index metadata for dirty check: {}", e), e.kind().clone()))?;
                Ok(index_meta.is_dirty())
            }
            None => Ok(false),
        }
    }

    pub fn create_index_descriptor(
        &self,
        fields: &Fields,
        index_type: &str,
    ) -> NitriteResult<IndexDescriptor> {
        // validate index
        let indexer = self.nitrite_config.find_indexer(index_type)
            .map_err(|e| NitriteError::new(&format!("Failed to find indexer for type '{}': {}", index_type, e), e.kind().clone()))?;
        indexer.validate_index(fields)
            .map_err(|e| NitriteError::new(&format!("Index validation failed for fields '{}': {}", fields, e), e.kind().clone()))?;

        let index_descriptor =
            IndexDescriptor::new(index_type, fields.clone(), &self.collection_name);
        let index_map_name = derive_index_map_name(&index_descriptor);
        let index_meta = IndexMeta::new(index_descriptor.clone(), index_map_name);
        self.index_meta_map
            .put(fields.to_value()
                .map_err(|e| NitriteError::new(&format!("Failed to convert fields to value for index creation: {}", e), e.kind().clone()))?, 
                index_meta.to_value()
                .map_err(|e| NitriteError::new(&format!("Failed to serialize index metadata: {}", e), e.kind().clone()))?)
            .map_err(|e| NitriteError::new(&format!("Failed to store index metadata in map: {}", e), e.kind().clone()))?;
        self.update_index_descriptor_cache()
            .map_err(|e| NitriteError::new(&format!("Failed to update index descriptor cache: {}", e), e.kind().clone()))?;
        Ok(index_descriptor)
    }

    /// Removes the index descriptor from the index meta map.
    /// Note: This does NOT dispose the actual index map - that should be done by the caller
    /// (typically via `indexer.drop_index()`) before calling this method.
    pub fn drop_index_descriptor(&self, fields: &Fields) -> NitriteResult<()> {
        let fields = fields.to_value()?;
        let index_meta_value = self.index_meta_map.remove(&fields)?;
        if index_meta_value.is_some() {
            // Just update the cache - the actual index map disposal is handled by the caller
            self.update_index_descriptor_cache()?;
        }
        Ok(())
    }

    pub fn dispose_index_meta(&self) -> NitriteResult<()> {
        self.index_meta_map.dispose()?;
        Ok(())
    }

    pub fn begin_indexing(&self, fields: &Fields) -> NitriteResult<()> {
        self.mark_dirty(fields, true)
    }

    pub fn end_indexing(&self, fields: &Fields) -> NitriteResult<()> {
        self.mark_dirty(fields, false)
    }

    fn ensure_index_descriptor_cache(&self) -> NitriteResult<()> {
        let needs_initialization = self.index_descriptor_cache.read_with(|it| it.is_none());
        if needs_initialization {
            let index_descriptors = Self::list_index_descriptors(self.index_meta_map.clone())?;
            self.index_descriptor_cache
                .write_with(|it| *it = Some(index_descriptors));
        }
        Ok(())
    }

    fn update_index_descriptor_cache(&self) -> NitriteResult<()> {
        let index_descriptors = Self::list_index_descriptors(self.index_meta_map.clone())?;
        self.index_descriptor_cache
            .write_with(|it| *it = Some(index_descriptors));
        Ok(())
    }

    fn get_index_meta_map(collection_name: &str, store: NitriteStore) -> NitriteResult<NitriteMap> {
        let index_meta_map_name = derive_index_meta_map_name(collection_name);
        store.open_map(&index_meta_map_name)
    }

    fn list_index_descriptors(index_meta_map: NitriteMap) -> NitriteResult<Vec<IndexDescriptor>> {
        let mut indexes = Vec::new();

        for entry in index_meta_map.entries()? {
            let (_, value) = entry?;
            let index_meta = IndexMeta::from_value(&value)?;
            let index_descriptor = index_meta.index_descriptor();
            indexes.push(index_descriptor);
        }

        Ok(indexes)
    }

    fn mark_dirty(&self, fields: &Fields, dirty: bool) -> NitriteResult<()> {
        let fields = fields.to_value()?;
        let index_meta_value = self.index_meta_map.get(&fields)?;
        match index_meta_value {
            Some(value) => {
                let mut index_meta = IndexMeta::from_value(&value)?;
                index_meta.set_dirty(dirty);

                self.index_meta_map.put(fields, index_meta.to_value()?)?;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Fields, UNIQUE_INDEX};
    use crate::index::IndexDescriptor;
    use crate::nitrite_config::NitriteConfig;

    fn setup_index_manager() -> IndexManager {
        let collection_name = "test_collection".to_string();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("auto_configure failed");
        nitrite_config.initialize().expect("initialize failed");
        IndexManager::new(collection_name, nitrite_config).unwrap()
    }

    fn create_fields() -> Fields {
        Fields::with_names(vec!["field"]).unwrap()
    }

    fn create_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new("UNIQUE", create_fields(), "test_collection")
    }

    #[test]
    fn test_new() {
        let collection_name = "test_collection".to_string();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("auto_configure failed");
        nitrite_config.initialize().expect("initialize failed");
        let result = IndexManager::new(collection_name, nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_index_descriptor() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.has_index_descriptor(&fields);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_get_index_descriptors() {
        let manager = setup_index_manager();
        let result = manager.get_index_descriptors();
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_find_matching_index() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.find_matching_index(&fields);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_find_exact_index() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.find_exact_index(&fields);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_mark_index_dirty() {
        let manager = setup_index_manager();
        let index_descriptor = create_index_descriptor();
        let result = manager.mark_index_dirty(&index_descriptor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let manager = setup_index_manager();
        let result = manager.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear_all() {
        let manager = setup_index_manager();
        let result = manager.clear_all();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_dirty_index() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.is_dirty_index(&fields);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_create_index_descriptor() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.create_index_descriptor(&fields, UNIQUE_INDEX);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_index_descriptor() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.drop_index_descriptor(&fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_dispose_index_meta() {
        let manager = setup_index_manager();
        let result = manager.dispose_index_meta();
        assert!(result.is_ok());
    }

    #[test]
    fn test_begin_indexing() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.begin_indexing(&fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_end_indexing() {
        let manager = setup_index_manager();
        let fields = create_fields();
        let result = manager.end_indexing(&fields);
        assert!(result.is_ok());
    }
}
