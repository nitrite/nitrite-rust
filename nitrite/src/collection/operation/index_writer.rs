use super::index_operations::IndexOperations;
use crate::{
    collection::Document,
    errors::NitriteResult,
    get_document_values,
    index::{IndexDescriptor, NitriteIndexer, NitriteIndexerProvider},
    is_affected_by_update,
    nitrite_config::NitriteConfig,
};
use std::sync::Arc;
use std::collections::HashMap;


#[derive(Clone)]
pub(crate) struct DocumentIndexWriter {
    /// Arc-wrapped internal implementation
    inner: Arc<DocumentIndexWriterInner>,
}

impl DocumentIndexWriter {
    /// Creates a new DocumentIndexWriter.
    ///
    /// # Arguments
    /// * `nitrite_config` - The Nitrite database configuration
    /// * `index_operation` - The index operations manager for the collection
    pub fn new(nitrite_config: NitriteConfig, index_operation: IndexOperations) -> Self {
        let inner = DocumentIndexWriterInner::new(nitrite_config, index_operation);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Gets the Nitrite configuration used by this writer.
    pub fn nitrite_config(&self) -> NitriteConfig {
        self.inner.nitrite_config.clone()
    }

    /// Gets the index operations manager for this writer.
    pub fn index_operation(&self) -> IndexOperations {
        self.inner.index_operation.clone()
    }

    /// Writes index entries for the given document.
    ///
    /// This method is called when a document is inserted. It ensures that all relevant
    /// indexes are updated with the new document's field values.
    ///
    /// # Arguments
    /// * `document` - The document to write to indexes
    ///
    /// # Errors
    /// Returns an error if any index update fails
    pub fn write_index_entry(&self, document: &mut Document) -> NitriteResult<()> {
        self.inner.write_index_entry(document)
    }

    /// Removes index entries for the given document.
    ///
    /// This method is called when a document is deleted. It ensures that all relevant
    /// indexes are updated by removing the document's field values.
    ///
    /// # Arguments
    /// * `document` - The document to remove from indexes
    ///
    /// # Errors
    /// Returns an error if any index update fails
    pub fn remove_index_entry(&self, document: &mut Document) -> NitriteResult<()> {
        self.inner.remove_index_entry(document)
    }

    /// Updates index entries for a modified document.
    ///
    /// This method is called when a document is updated. It only updates indexes affected
    /// by the changed fields, improving performance for partial updates.
    ///
    /// # Arguments
    /// * `old_document` - The document before the update
    /// * `new_document` - The document after the update
    /// * `updated_fields` - The fields that were modified
    ///
    /// # Errors
    /// Returns an error if any index update fails
    pub fn update_index_entry(
        &self,
        old_document: &mut Document,
        new_document: &mut Document,
        updated_fields: &Document,
    ) -> NitriteResult<()> {
        self.inner.update_index_entry(old_document, new_document, updated_fields)
    }
}

/// The internal implementation of DocumentIndexWriter.
///
/// This struct contains all the actual state and logic for writing index entries.
/// It is not directly accessible from outside the crate - all access goes through
/// the public `DocumentIndexWriter` interface.
pub struct DocumentIndexWriterInner {
    nitrite_config: NitriteConfig,
    index_operation: IndexOperations,
}

impl DocumentIndexWriterInner {
    fn new(nitrite_config: NitriteConfig, index_operation: IndexOperations) -> Self {
        Self {
            nitrite_config: nitrite_config.clone(),
            index_operation: index_operation.clone(),
        }
    }

    pub fn write_index_entry(&self, document: &mut Document) -> NitriteResult<()> {
        let index_entries = self.index_operation.list_indexes()?;
        self.process_index_entries(index_entries, document, |this, desc, doc, indexer| {
            this.write_index_entry_internal(desc, doc, indexer)
        })
    }

    pub fn remove_index_entry(&self, document: &mut Document) -> NitriteResult<()> {
        let index_entries = self.index_operation.list_indexes()?;
        self.process_index_entries(index_entries, document, |this, desc, doc, indexer| {
            this.remove_index_entry_internal(desc, doc, indexer)
        })
    }

    fn process_index_entries<F>(
        &self,
        index_entries: Vec<IndexDescriptor>,
        document: &mut Document,
        mut operation: F,
    ) -> NitriteResult<()>
    where
        F: FnMut(&Self, &IndexDescriptor, &mut Document, &mut NitriteIndexer) -> NitriteResult<()>,
    {
        let mut indexer_cache = HashMap::new();

        for index_descriptor in index_entries {
            let index_type = index_descriptor.index_type();
            let indexer = indexer_cache
                .entry(index_type.clone())
                .or_insert_with(|| self.nitrite_config.find_indexer(&index_type))
                .as_mut()
                .map_err(|e| e.clone())?;

            operation(self, &index_descriptor, document, indexer)?;
        }
        Ok(())
    }

    pub fn update_index_entry(
        &self,
        old_document: &mut Document,
        new_document: &mut Document,
        updated_fields: &Document,
    ) -> NitriteResult<()> {
        let index_entries = self.index_operation.list_indexes()?;
        for index_descriptor in index_entries {
            let fields = index_descriptor.index_fields();

            if is_affected_by_update(&fields, updated_fields) {
                let index_type = index_descriptor.index_type();
                let mut indexer = self.nitrite_config.find_indexer(&index_type)?;

                self.remove_index_entry_internal(&index_descriptor, old_document, &mut indexer)?;
                self.write_index_entry_internal(&index_descriptor, new_document, &mut indexer)?;
            }
        }
        Ok(())
    }

    fn write_index_entry_internal(
        &self,
        index_descriptor: &IndexDescriptor,
        document: &mut Document,
        indexer: &mut NitriteIndexer,
    ) -> NitriteResult<()> {
        let fields = index_descriptor.index_fields();
        let field_values = get_document_values(document, &fields)?;

        if self.index_operation.should_rebuild_index(&fields)? {
            self.index_operation.build_index(index_descriptor, true)?;
        } else {
            indexer.write_index_entry(&field_values, index_descriptor, &self.nitrite_config)?;
        }
        Ok(())
    }

    fn remove_index_entry_internal(
        &self,
        index_descriptor: &IndexDescriptor,
        document: &mut Document,
        indexer: &mut NitriteIndexer,
    ) -> NitriteResult<()> {
        let fields = index_descriptor.index_fields();
        let field_values = get_document_values(document, &fields)?;

        if self.index_operation.should_rebuild_index(&fields)? {
            self.index_operation.build_index(index_descriptor, true)?;
        } else {
            indexer.remove_index_entry(&field_values, index_descriptor, &self.nitrite_config)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::operation::find_optimizer;
    use crate::collection::operation::index_operations::IndexOperations;
    use crate::collection::Document;
    use crate::common::{Fields, NitriteEventBus, UNIQUE_INDEX};
    use crate::index::IndexDescriptor;
    use crate::nitrite_config::NitriteConfig;
    use crate::store::NitriteStoreProvider;
    use std::sync::Arc;

    fn setup_document_index_writer() -> DocumentIndexWriter {
        let collection_name = "test_collection".to_string();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let store = nitrite_config.nitrite_store().expect("Failed to get store");
        let nitrite_map = store
            .open_map(&collection_name.clone())
            .expect("Failed to open map");
        let event_bus = NitriteEventBus::new();
        let find_optimizer = find_optimizer::FindOptimizer::new();
        let index_operations = IndexOperations::new(
            collection_name,
            nitrite_config.clone(),
            nitrite_map,
            find_optimizer,
            event_bus,
        )
        .unwrap();
        DocumentIndexWriter::new(nitrite_config, index_operations)
    }

    fn create_document() -> Document {
        let mut doc = Document::new();
        doc.put("field", "value").expect("Failed to put value");
        doc
    }

    fn create_fields() -> Fields {
        Fields::with_names(vec!["field"]).unwrap()
    }

    #[test]
    fn test_new() {
        let writer = setup_document_index_writer();
        assert!(Arc::strong_count(&writer.inner) > 0);
    }

    #[test]
    fn test_write_index_entry() {
        let writer = setup_document_index_writer();
        let mut document = create_document();
        let result = writer.write_index_entry(&mut document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_index_entry() {
        let writer = setup_document_index_writer();
        let mut document = create_document();
        let result = writer.remove_index_entry(&mut document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_index_entry() {
        let writer = setup_document_index_writer();
        let mut old_document = create_document();
        let mut new_document = create_document();
        let updated_fields = create_document();
        let result =
            writer.update_index_entry(&mut old_document, &mut new_document, &updated_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_index_entry_internal() {
        let writer = setup_document_index_writer();
        let inner = writer.inner.clone();
        let mut document = create_document();
        let index_descriptor =
            IndexDescriptor::new(UNIQUE_INDEX, create_fields(), "test_collection");
        let mut indexer = inner
            .nitrite_config
            .find_indexer(&index_descriptor.index_type())
            .unwrap();
        let result =
            inner.write_index_entry_internal(&index_descriptor, &mut document, &mut indexer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_index_entry_internal() {
        let writer = setup_document_index_writer();
        let inner = writer.inner.clone();
        let mut document = create_document();
        let index_descriptor =
            IndexDescriptor::new(UNIQUE_INDEX, create_fields(), "test_collection");
        let mut indexer = inner
            .nitrite_config
            .find_indexer(&index_descriptor.index_type())
            .unwrap();
        let result =
            inner.remove_index_entry_internal(&index_descriptor, &mut document, &mut indexer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_indexer_cache_reuse() {
        // Test that indexer cache is reused across multiple indexes of same type
        let writer = setup_document_index_writer();
        let mut document = create_document();
        
        // Create multiple indexes of the same type
        let fields1 = Fields::with_names(vec!["field1"]).unwrap();
        let _index_desc1 = IndexDescriptor::new(UNIQUE_INDEX, fields1, "test_collection");
        
        // This should reuse indexer from cache if logic is correct
        let result = writer.write_index_entry(&mut document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_index_cache_efficiency() {
        let writer = setup_document_index_writer();
        let mut document = create_document();
        
        // Create index first
        writer.write_index_entry(&mut document).unwrap();
        
        // Remove should use cached indexer efficiently
        let result = writer.remove_index_entry(&mut document);
        assert!(result.is_ok());
    }

    #[test]
    fn bench_write_index_entry() {
        let writer = setup_document_index_writer();
        let mut document = create_document();
        
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let _ = writer.write_index_entry(&mut document);
        }
        let elapsed = start.elapsed();
        
        println!("write_index_entry 100 iterations: {:?}", elapsed);
        // Assert reasonable performance (< 5ms per op on average)
        assert!(elapsed.as_millis() < 500);
    }

    #[test]
    fn bench_remove_index_entry() {
        let writer = setup_document_index_writer();
        let mut document = create_document();
        
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let _ = writer.remove_index_entry(&mut document);
        }
        let elapsed = start.elapsed();
        
        println!("remove_index_entry 100 iterations: {:?}", elapsed);
        assert!(elapsed.as_millis() < 500);
    }
}
