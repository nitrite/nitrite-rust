use crate::{
    common::{LockHandle, LockRegistry}, create_unique_filter, errors::{ErrorKind, NitriteError, NitriteResult}, filter::{is_all_filter, Filter}, nitrite_config::NitriteConfig, store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider}, AttributeAware, EventAware, Fields, NitriteEventBus, PersistentCollection, Processor
};
use std::sync::atomic::{AtomicBool, Ordering};

use super::{operation::CollectionOperations, NitriteCollectionProvider, UpdateOptions};

pub(crate) struct DefaultNitriteCollection {
    collection_name: String,
    nitrite_map: NitriteMap,
    store: NitriteStore,
    operations: CollectionOperations,
    dropped: AtomicBool,
    lock_handle: LockHandle,
}

impl DefaultNitriteCollection {
    pub fn new(
        collection_name: &str,
        nitrite_map: NitriteMap,
        nitrite_config: NitriteConfig,
        lock_handle: LockHandle,
    ) -> NitriteResult<Self> {
        let store = nitrite_config.nitrite_store()?;
        let event_bus = NitriteEventBus::new();

        let operations = CollectionOperations::new(
            collection_name,
            nitrite_map.clone(),
            nitrite_config.clone(),
            event_bus,
        )?;

        Ok(Self {
            collection_name: collection_name.to_string(),
            nitrite_map: nitrite_map.clone(),
            store: store.clone(),
            operations,
            dropped: AtomicBool::from(false),
            lock_handle,
        })
    }

    fn ensure_opened(&self) -> NitriteResult<()> {
        // Check dropped state first (cheapest check)
        if self.dropped.load(Ordering::Relaxed) {
            log::error!("Collection '{}' is dropped and cannot be accessed", self.collection_name);
            return Err(NitriteError::new(
                &format!("Collection '{}' is dropped and cannot be accessed", self.collection_name),
                ErrorKind::InvalidOperation,
            ));
        }

        // Check store state
        if self.store.is_closed()? {
            log::error!("Store is closed; cannot access collection '{}'", self.collection_name);
            return Err(NitriteError::new(
                "Nitrite store is closed. Close all collections and reopen the database to continue operations",
                ErrorKind::InvalidOperation,
            ));
        }

        // Check map state only if necessary
        let map_closed = self.nitrite_map.is_closed()?;
        if map_closed {
            log::error!("NitriteMap for collection '{}' is closed", self.collection_name);
            return Err(NitriteError::new(
                &format!("Collection '{}' underlying map is closed and cannot be accessed", self.collection_name),
                ErrorKind::InvalidOperation,
            ));
        }

        let map_dropped = self.nitrite_map.is_dropped()?;
        if map_dropped {
            log::error!("NitriteMap for collection '{}' is dropped", self.collection_name);
            return Err(NitriteError::new(
                &format!("Collection '{}' underlying map is dropped; cannot perform further operations", self.collection_name),
                ErrorKind::InvalidOperation,
            ));
        }

        Ok(())
    }
}

impl EventAware for DefaultNitriteCollection {
    fn subscribe(
        &self,
        handler: super::CollectionEventListener,
    ) -> NitriteResult<Option<crate::SubscriberRef>> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.subscribe(handler)
    }

    fn unsubscribe(&self, subscriber: crate::SubscriberRef) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.unsubscribe(subscriber)
    }
}

impl AttributeAware for DefaultNitriteCollection {
    fn attributes(&self) -> NitriteResult<Option<crate::Attributes>> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        self.operations.attributes()
    }

    fn set_attributes(&self, attributes: crate::Attributes) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.set_attributes(attributes)
    }
}

impl PersistentCollection for DefaultNitriteCollection {
    fn add_processor(&self, processor: Processor) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.add_processor(processor);
        Ok(())
    }

    fn create_index(
        &self,
        field_names: Vec<&str>,
        index_options: &crate::index::IndexOptions,
    ) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        let fields = Fields::with_names(field_names)?;
        self.operations
            .create_index(&fields, &index_options.index_type())
    }

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        let fields = Fields::with_names(field_names)?;
        let index_descriptor = self.operations.find_index(&fields)?;

        let index_descriptor = match index_descriptor {
            Some(descriptor) => descriptor,
            None => {
                log::error!("Index not found for fields {}", fields);
                return Err(NitriteError::new(
                    "Index not found",
                    ErrorKind::IndexingError,
                ));
            }
        };

        if self.operations.is_indexing(&fields)? {
            log::error!("Indexing is in progress for fields {}", fields);
            return Err(NitriteError::new(
                "Indexing is in progress",
                ErrorKind::IndexingError,
            ));
        }

        self.operations.rebuild_index(&index_descriptor)
    }

    fn list_indexes(&self) -> NitriteResult<Vec<crate::index::IndexDescriptor>> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        self.operations.list_indexes()
    }

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        let fields = Fields::with_names(field_names)?;
        self.operations.has_index(&fields)
    }

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        let fields = Fields::with_names(field_names)?;
        self.operations.is_indexing(&fields)
    }

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        let fields = Fields::with_names(field_names)?;
        self.operations.drop_index(&fields)
    }

    fn drop_all_indexes(&self) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.drop_all_indexes()
    }

    fn clear(&self) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.clear()
    }

    fn dispose(&self) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.dispose()?;
        self.dropped.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        let _guard = self.lock_handle.read();
        Ok(self.dropped.load(Ordering::Relaxed) || self.nitrite_map.is_dropped()?)
    }

    fn is_open(&self) -> NitriteResult<bool> {
        let _guard = self.lock_handle.read();
        Ok(!self.store.is_closed()?
            && !self.dropped.load(Ordering::Relaxed)
            && !self.nitrite_map.is_dropped()?
            && !self.nitrite_map.is_closed()?
            && !self.nitrite_map.is_dropped()?)
    }

    fn size(&self) -> NitriteResult<u64> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        self.operations.size()
    }

    fn close(&self) -> NitriteResult<()> {
        let _guard = self.lock_handle.write();
        self.operations.close()?;
        Ok(())
    }

    fn store(&self) -> NitriteResult<NitriteStore> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        Ok(self.store.clone())
    }
}

impl NitriteCollectionProvider for DefaultNitriteCollection {
    fn insert(
        &self,
        document: super::Document,
    ) -> NitriteResult<super::operation::WriteResult> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.insert(document)
    }

    fn insert_many(
        &self,
        documents: Vec<super::Document>,
    ) -> NitriteResult<super::operation::WriteResult> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.insert_batch(documents)
    }

    fn update_with_options(
        &self,
        filter: Filter,
        update: &super::Document,
        update_options: &UpdateOptions,
    ) -> NitriteResult<super::operation::WriteResult> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.update(filter, update, update_options)
    }

    fn update_one(
        &self,
        document: &super::Document,
        insert_if_absent: bool,
    ) -> NitriteResult<super::operation::WriteResult> {
        let mut document = document.clone();
        
        // create_unique_filter creates a new id in the document if it does not have one
        // so it can't be called before checking if the document has an id
        
        if insert_if_absent {
            let filter = create_unique_filter(&mut document)?;
            self.update_with_options(filter, &document, &UpdateOptions::new(true, false))
        } else if document.has_id() {
            let filter = create_unique_filter(&mut document)?;
            self.update_with_options(filter, &document, &UpdateOptions::new(false, false))
        } else {
            log::error!("Document does not have id");
            Err(NitriteError::new(
                "Document does not have id",
                ErrorKind::NotIdentifiable,
            ))
        }
    }

    fn update_by_id(
        &self,
        id: &super::NitriteId,
        update: &super::Document,
        insert_if_absent: bool,
    ) -> NitriteResult<super::operation::WriteResult> {
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.update_by_id(id, update, insert_if_absent)
    }

    fn remove(
        &self,
        filter: Filter,
        just_once: bool,
    ) -> NitriteResult<super::operation::WriteResult> {
        if is_all_filter(&filter) && just_once {
            log::error!("Cannot remove all documents with just once as true");
            return Err(NitriteError::new(
                "Cannot remove all documents with just once as true",
                ErrorKind::InvalidOperation,
            ));
        }
        
        let _guard = self.lock_handle.write();
        self.ensure_opened()?;
        self.operations.remove(filter, just_once)
    }

    fn remove_one(
        &self,
        document: &super::Document,
    ) -> NitriteResult<super::operation::WriteResult> {
        let _guard = self.lock_handle.write();
        if document.has_id() {
            self.ensure_opened()?;
            self.operations.remove_document(document)
        } else {
            log::error!("Document does not have id");
            Err(NitriteError::new(
                "Document does not have id",
                ErrorKind::NotIdentifiable,
            ))
        }
    }

    fn find(&self, filter: Filter) -> NitriteResult<crate::DocumentCursor> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        self.operations.find(filter, &super::FindOptions::new())
    }

    fn find_with_options(
        &self,
        filter: Filter,
        find_options: &super::FindOptions,
    ) -> NitriteResult<crate::DocumentCursor> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        self.operations.find(filter, find_options)
    }

    fn get_by_id(&self, id: &super::NitriteId) -> NitriteResult<Option<super::Document>> {
        let _guard = self.lock_handle.read();
        self.ensure_opened()?;
        self.operations.get_by_id(id)
    }

    fn name(&self) -> String {
        self.collection_name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{CollectionEventListener, Document, FindOptions, NitriteId};
    use crate::common::ProcessorProvider;
    use crate::filter::field;
    use crate::index::{unique_index, IndexOptions};
    use crate::nitrite_config::NitriteConfig;

    fn setup_collection() -> DefaultNitriteCollection {
        let nitrite_config = NitriteConfig::default();
        nitrite_config.auto_configure().expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let store = nitrite_config.nitrite_store().expect("Failed to get store");
        let nitrite_map = store.open_map("test_collection").expect("Failed to open map");
        let lock_registry = LockRegistry::default();
        let lock_handle = lock_registry.get_lock("test_collection");
        DefaultNitriteCollection::new("test_collection", nitrite_map, nitrite_config, lock_handle).expect("Failed to create collection")
    }

    #[test]
    fn test_new() {
        let collection = setup_collection();
        assert_eq!(collection.name(), "test_collection");
    }

    #[test]
    fn test_ensure_opened() {
        let collection = setup_collection();
        assert!(collection.ensure_opened().is_ok());
    }

    #[test]
    fn test_subscribe() {
        let collection = setup_collection();
        let handler = |_| {Ok(())};
        let listener = CollectionEventListener::new(handler);
        let result = collection.subscribe(listener);
        assert!(result.is_ok());
    }

    #[test]
    fn test_unsubscribe() {
        let collection = setup_collection();
        let handler = |_| {Ok(())};
        let listener = CollectionEventListener::new(handler);
        let subscriber = collection.subscribe(listener).unwrap();
        let result = collection.unsubscribe(subscriber.unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn test_attributes() {
        let collection = setup_collection();
        let result = collection.attributes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_attributes() {
        let collection = setup_collection();
        let attributes = crate::Attributes::new();
        let result = collection.set_attributes(attributes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_processor() {
        let collection = setup_collection();
        let processor = Processor::new(TestProcessor);
        let result = collection.add_processor(processor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index() {
        let collection = setup_collection();
        let result = collection.create_index(vec!["field1"], &IndexOptions::default());
        assert!(result.is_ok());
    }

    #[test]
    fn test_rebuild_index() {
        let collection = setup_collection();
        let _ = collection.create_index(vec!["field1"], &IndexOptions::default()).expect("Failed to create index");
        let result = collection.rebuild_index(vec!["field1"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_indexes() {
        let collection = setup_collection();
        let result = collection.list_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_index() {
        let collection = setup_collection();
        let result = collection.has_index(vec!["field1"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_indexing() {
        let collection = setup_collection();
        let result = collection.is_indexing(vec!["field1"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_index() {
        let collection = setup_collection();
        let result = collection.drop_index(vec!["field1"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_all_indexes() {
        let collection = setup_collection();
        let result = collection.drop_all_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear() {
        let collection = setup_collection();
        let result = collection.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy() {
        let collection = setup_collection();
        let result = collection.dispose();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_dropped() {
        let collection = setup_collection();
        let result = collection.is_dropped();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_open() {
        let collection = setup_collection();
        let result = collection.is_open();
        assert!(result.is_ok());
    }

    #[test]
    fn test_size() {
        let collection = setup_collection();
        let result = collection.size();
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let collection = setup_collection();
        let result = collection.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_store() {
        let collection = setup_collection();
        let result = collection.store();
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert() {
        let collection = setup_collection();
        let document = Document::new();
        let result = collection.insert(document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_batch() {
        let collection = setup_collection();
        let documents = vec![Document::new(), Document::new()];
        let result = collection.insert_many(documents);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update() {
        let collection = setup_collection();
        let filter = field("field1").eq("value1");
        let document = Document::new();
        let update_options = UpdateOptions::default();
        let result = collection.update_with_options(filter, &document, &update_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_one() {
        let collection = setup_collection();
        let document = Document::new();
        let result = collection.update_one(&document, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove() {
        let collection = setup_collection();
        let filter = field("field1").eq("value1");
        let result = collection.remove(filter, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_document() {
        let collection = setup_collection();
        let mut document = Document::new();
        let _ = document.id();
        let result = collection.remove_one(&document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find() {
        let collection = setup_collection();
        let filter = field("field1").eq("value1");
        let result = collection.find(filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_options() {
        let collection = setup_collection();
        let filter = field("field1").eq("value1");
        let find_options = FindOptions::default();
        let result = collection.find_with_options(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_by_id() {
        let collection = setup_collection();
        let id = NitriteId::new();
        let result = collection.get_by_id(&id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ensure_opened_early_exit() {
        let collection = setup_collection();
        
        // Test that ensure_opened exits fast for opened collection
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = collection.ensure_opened();
        }
        let elapsed = start.elapsed();
        
        println!("1000 ensure_opened calls: {:?}", elapsed);
        // Should be very fast with atomic load
        assert!(elapsed.as_millis() < 100);
    }

    #[test]
    fn test_ensure_opened_after_drop() {
        let collection = setup_collection();
        let _ = collection.dispose();
        
        // After dropping, ensure_opened should fail fast
        let result = collection.ensure_opened();
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_operations_efficiency() {
        let collection = setup_collection();
        let mut documents = vec![];
        
        for i in 0..10 {
            let mut doc = Document::new();
            let _ = doc.put("index", i);
            documents.push(doc);
        }
        
        let start = std::time::Instant::now();
        let _ = collection.insert_many(documents);
        let elapsed = start.elapsed();
        
        println!("Batch insert 10 docs: {:?}", elapsed);
        assert!(elapsed.as_millis() < 200);
    }

    #[test]
    fn test_index_insert() {
        let collection = setup_collection();
        let _ = collection.create_index(vec!["field1"], &unique_index()).expect("Failed to create index");
        
        let mut documents = vec![];
        for i in 0..10 {
            let mut doc = Document::new();
            let _ = doc.put("field1", format!("value{}", i));
            documents.push(doc);
        }
        
        let start = std::time::Instant::now();
        let _ = collection.insert_many(documents);
        let elapsed = start.elapsed();
        
        println!("Insert 10 indexed docs: {:?}", elapsed);
        assert!(elapsed.as_millis() < 300);
    }
    
    struct TestProcessor;
    
    impl ProcessorProvider for TestProcessor {
        fn name(&self) -> String {
            "TestProcessor".to_string()
        }

        fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
            let mut doc = doc.clone();
            doc.put("processed", true)?;
            Ok(doc)
        }

        fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
            let mut doc = doc.clone();
            doc.remove("processed")?;
            Ok(doc)
        }
    }
}