use super::{
    find_optimizer::FindOptimizer, index_operations::IndexOperations,
    index_writer::DocumentIndexWriter, read_operations::ReadOperations,
    write_operations::WriteOperations, write_result::WriteResult,
};
use crate::{
    collection::{
        CollectionEventInfo, CollectionEventListener, Document, FindOptions, NitriteId, UpdateOptions,
    },
    errors::NitriteResult,
    filter::Filter,
    index::IndexDescriptor,
    nitrite_config::NitriteConfig,
    store::{NitriteMap, NitriteMapProvider, NitriteStoreProvider},
    AttributeAware, Attributes, DocumentCursor, Fields, NitriteEventBus, Processor, ProcessorChain,
    SubscriberRef,
};
use std::sync::Arc;
use std::{borrow::Cow, ops::Deref};


pub(crate) struct CollectionOperations {
    nitrite_map: NitriteMap,
    event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    processor_chain: ProcessorChain,
    index_operations: IndexOperations,
    write_operations: WriteOperations,
    read_operations: ReadOperations,
}

impl CollectionOperations {
    pub fn new(
        collection_name: &str,
        nitrite_map: NitriteMap,
        nitrite_config: NitriteConfig,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    ) -> NitriteResult<Self> {
        let processor_chain = ProcessorChain::new();

        let collection_name_cow = Cow::from(collection_name);

        let find_optimizer = FindOptimizer::new();

        let index_operations = IndexOperations::new(
            collection_name_cow.to_string(),
            nitrite_config.clone(),
            nitrite_map.clone(),
            find_optimizer.clone(),
            event_bus.clone(),
        )?;

        let read_operations = ReadOperations::new(
            collection_name_cow.into_owned(),
            index_operations.clone(),
            nitrite_config.clone(),
            nitrite_map.clone(),
            find_optimizer,
            processor_chain.clone(),
        );

        let index_writer =
            DocumentIndexWriter::new(nitrite_config.clone(), index_operations.clone());

        let write_operations = WriteOperations::new(
            index_writer.clone(),
            read_operations.clone(),
            event_bus.clone(),
            nitrite_map.clone(),
            processor_chain.clone(),
        );

        Ok(Self {
            nitrite_map,
            event_bus,
            processor_chain,
            index_operations,
            write_operations,
            read_operations,
        })
    }

    pub fn subscribe(&self, handler: CollectionEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.event_bus.register(handler)
    }

    pub fn unsubscribe(&self, subscriber: SubscriberRef) -> NitriteResult<()> {
        self.event_bus.deregister(subscriber)
    }

    pub fn add_processor(&self, processor: Processor) {
        self.processor_chain.add_processor(processor);
    }

    pub fn create_index(&self, fields: &Fields, index_type: &str) -> NitriteResult<()> {
        self.index_operations.create_index(fields, index_type)
    }

    pub fn find_index(&self, fields: &Fields) -> NitriteResult<Option<IndexDescriptor>> {
        self.index_operations.find_index_descriptor(fields)
    }

    pub fn rebuild_index(&self, index_descriptor: &IndexDescriptor) -> NitriteResult<()> {
        self.index_operations.build_index(index_descriptor, true)
    }

    pub fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.index_operations.list_indexes()
    }

    pub fn has_index(&self, fields: &Fields) -> NitriteResult<bool> {
        self.index_operations.has_index_entry(fields)
    }

    pub fn is_indexing(&self, fields: &Fields) -> NitriteResult<bool> {
        self.index_operations.is_indexing(fields)
    }

    pub fn drop_index(&self, fields: &Fields) -> NitriteResult<()> {
        self.index_operations.drop_index(fields)
    }

    pub fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.index_operations.drop_all_indexes()
    }

    pub fn insert(&self, document: Document) -> NitriteResult<WriteResult> {
        self.write_operations.insert(document)
    }

    pub fn insert_batch(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        self.write_operations.insert_batch(documents)
    }

    pub fn update(
        &self,
        filter: Filter,
        update: &Document,
        update_options: &UpdateOptions,
    ) -> NitriteResult<WriteResult> {
        self.write_operations.update(filter, update, update_options)
    }

    /// Updates a document directly by its NitriteId without filter-based lookup.
    /// This is an O(1) operation as it directly accesses the document by its key.
    pub fn update_by_id(
        &self,
        id: &NitriteId,
        update: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        self.write_operations.update_by_id(id, update, insert_if_absent)
    }

    pub fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult> {
        self.write_operations.remove(filter, just_once)
    }

    pub fn remove_document(&self, document: &Document) -> NitriteResult<WriteResult> {
        self.write_operations.remove_document(document)
    }

    pub fn find(
        &self,
        filter: Filter,
        find_options: &FindOptions,
    ) -> NitriteResult<DocumentCursor> {
        self.read_operations.find(filter, find_options)
    }

    pub fn get_by_id(&self, id: &NitriteId) -> NitriteResult<Option<Document>> {
        self.read_operations.get_by_id(id)
    }

    pub fn dispose(&self) -> NitriteResult<()> {
        self.index_operations.dispose_all_indexes()?;
        self.dispose_nitrite_map()?;
        self.event_bus.close()?;
        Ok(())
    }

    pub fn size(&self) -> NitriteResult<u64> {
        self.nitrite_map.size()
    }

    pub fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.nitrite_map.attributes()
    }

    pub fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.nitrite_map.set_attributes(attributes)
    }

    pub fn close(&self) -> NitriteResult<()> {
        self.index_operations.close()?;
        self.nitrite_map.close()?;
        self.event_bus.close()?;
        Ok(())
    }

    pub fn clear(&self) -> NitriteResult<()> {
        self.index_operations.clear()?;
        self.nitrite_map.clear()
    }

    fn dispose_nitrite_map(&self) -> NitriteResult<()> {
        let store = self.nitrite_map.get_store()?;
        let catalog = store.store_catalog()?;
        catalog.remove(self.nitrite_map.get_name()?.as_str())?;
        self.nitrite_map.dispose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::insert_if_absent;
    use crate::common::{NitritePluginProvider, NON_UNIQUE_INDEX, UNIQUE_INDEX};
    use crate::{
        doc,
        filter::field
        ,
    };
    use rand::rngs::OsRng;
    use rand::Rng;
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;
    use std::thread;

    fn setup_collection_operations() -> CollectionOperations {
        let nitrite_config = NitriteConfig::new();
        nitrite_config.auto_configure().expect("Auto configure failed");
        nitrite_config.initialize().expect("Initialize failed");
        let store = nitrite_config.nitrite_store().expect("Nitrite store failed");
        let nitrite_map = store.open_map("test_collection").expect("Open map failed");      
        
        let event_bus = NitriteEventBus::new();
        
        CollectionOperations::new("test_collection", nitrite_map, nitrite_config, event_bus).unwrap()
    }

    #[test]
    fn test_insert_document() {
        let collection = setup_collection_operations();
        let document = doc!{"field": "value"};
        let result = collection.insert(document);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().collect::<Vec<_>>().len(), 1);
        assert_eq!(collection.size().unwrap(), 1);
    }

    #[test]
    fn test_insert_batch_documents() {
        let collection = setup_collection_operations();
        let documents = vec![doc!{"field1": "value1"}, doc!{"field2": "value2"}];
        let result = collection.insert_batch(documents);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().collect::<Vec<_>>().len(), 2);
        assert_eq!(collection.size().unwrap(), 2);
    }

    #[test]
    fn test_update_document_insert_if_absent() {
        let collection = setup_collection_operations();
        let filter = field("field").eq("value");
        let update = doc!{"field": "value1"};
        let update_options = insert_if_absent();
        assert_eq!(collection.size().unwrap(), 0);
        let result = collection.update(filter, &update, &update_options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().collect::<Vec<_>>().len(), 1);
        assert_eq!(collection.size().unwrap(), 1);
    }

    #[test]
    fn test_update_document() {
        let collection = setup_collection_operations();
        let document = doc!{"field": "value"};
        collection.insert(document).expect("Insert failed");
        let filter = field("field").eq("value");
        let update = doc!{"field": "value1"};
        let update_options = UpdateOptions::default();
        let result = collection.update(filter, &update, &update_options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().collect::<Vec<_>>().len(), 1);
        assert_eq!(collection.size().unwrap(), 1);

        let cursor = collection
            .find(field("field").eq("value1"), &FindOptions::default())
            .expect("Find failed");
        assert_eq!(cursor.count(), 1);
    }

    #[test]
    fn test_remove_document() {
        let collection = setup_collection_operations();
        let document = doc!{"field": "value"};
        collection.insert(document).expect("Insert failed");
        let filter = field("field").eq("value");
        let result = collection.remove(filter, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_document() {
        let collection = setup_collection_operations();
        let document = doc!{"field": "value"};
        collection.insert(document).expect("Insert failed");
        let filter = field("field").eq("value");
        let find_options = FindOptions::default();
        let result = collection.find(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_by_id() {
        let collection = setup_collection_operations();
        let mut doc = doc!{"field": "value"};
        let id = doc.id().unwrap();
        collection.insert(doc).expect("Insert failed");
        let result = collection.get_by_id(&id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index() {
        let collection = setup_collection_operations();
        let fields = Fields::with_names(vec!["field"]).expect("Fields creation failed");
        let has_index = collection.has_index(&fields).expect("Has index failed");
        assert!(!has_index);
        let result = collection.create_index(&fields, UNIQUE_INDEX);
        assert!(result.is_ok());
        let has_index = collection.has_index(&fields).expect("Has index failed");
        assert!(has_index);
    }

    #[test]
    fn test_find_index() {
        let collection = setup_collection_operations();
        let fields = Fields::with_names(vec!["field"]).expect("Fields creation failed");
        let result = collection.find_index(&fields);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        let result = collection.create_index(&fields, UNIQUE_INDEX);
        assert!(result.is_ok());
        assert!(collection.find_index(&fields).unwrap().is_some());
    }

    #[test]
    fn test_rebuild_index() {
        let collection = setup_collection_operations();
        let index_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field"]).unwrap(),
            "test_collection",
        );
        let result = collection.rebuild_index(&index_descriptor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_indexes() {
        let collection = setup_collection_operations();
        let result = collection.list_indexes();
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
        
        let fields = Fields::with_names(vec!["field"]).expect("Fields creation failed");
        let result = collection.create_index(&fields, UNIQUE_INDEX);
        assert!(result.is_ok());
        let indexes = collection.list_indexes().expect("List indexes failed");
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_drop_index() {
        let collection = setup_collection_operations();
        let fields = Fields::with_names(vec!["field"]).expect("Fields creation failed");
        let result = collection.drop_index(&fields);
        assert!(result.is_ok());
        
        let result = collection.create_index(&fields, UNIQUE_INDEX);
        assert!(result.is_ok());

        let has_index = collection.has_index(&fields).expect("Has index failed");
        assert!(has_index);
        
        let result = collection.drop_index(&fields);
        assert!(result.is_ok());
        
        let has_index = collection.has_index(&fields).expect("Has index failed");
        assert!(!has_index);
    }

    #[test]
    fn test_drop_all_indexes() {
        let collection = setup_collection_operations();
        
        let fields1 = Fields::with_names(vec!["field1"]).expect("Fields creation failed");
        let fields2 = Fields::with_names(vec!["field2"]).expect("Fields creation failed");
        
        let result = collection.create_index(&fields1, UNIQUE_INDEX);
        assert!(result.is_ok());
        
        let result = collection.create_index(&fields2, NON_UNIQUE_INDEX);
        assert!(result.is_ok());
        
        let indexes = collection.list_indexes().expect("List indexes failed");
        assert_eq!(indexes.len(), 2);
        
        let result = collection.drop_all_indexes();
        assert!(result.clone().is_ok());
        
        let indexes = collection.list_indexes().expect("List indexes failed");
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_dispose_collection() {
        let collection = setup_collection_operations();
        let result = collection.dispose();
        assert!(result.is_ok());
    }

    #[test]
    fn test_size() {
        let collection = setup_collection_operations();
        let result = collection.size();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
        
        let document = doc!{"field": "value"};
        collection.insert(document).expect("Insert failed");
        let result = collection.size();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_attributes() {
        let collection = setup_collection_operations();
        let result = collection.attributes();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_set_attributes() {
        let collection = setup_collection_operations();
        let attributes = Attributes::new();
        let result = collection.set_attributes(attributes);
        assert!(result.is_ok());
        
        let result = collection.attributes();
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_clear() {
        let collection = setup_collection_operations();
        let result = collection.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_multithreaded_insert() {
        let collection = Arc::new(setup_collection_operations());
        let mut handles = vec![];

        for _ in 0..10 {
            let collection = Arc::clone(&collection);
            let handle = thread::spawn(move || {
                let document = Document::new();
                let result = collection.insert(document);
                assert!(result.is_ok());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(collection.size().unwrap(), 10);
    }

    #[test]
    fn test_multithreaded_update() {
        let collection = Arc::new(setup_collection_operations());
        let mut handles = vec![];

        for _ in 0..10 {
            let collection = Arc::clone(&collection);
            let handle = thread::spawn(move || {
                let filter = field("field").eq("value");
                let update = doc!{"field": (OsRng.gen::<u64>().to_string())};
                
                let update_options = insert_if_absent();
                let result = collection.update(filter, &update, &update_options);
                assert!(result.is_ok());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(collection.size().unwrap(), 10);
    }

    #[test]
    fn test_multithreaded_remove() {
        let collection = Arc::new(setup_collection_operations());
        let mut handles = vec![];
        
        let document = doc!{"field": "value"};
        collection.insert(document).expect("Insert failed");

        for _ in 0..10 {
            let collection = Arc::clone(&collection);
            let handle = thread::spawn(move || {
                let filter = field("field").eq("value");
                let result = collection.remove(filter, false);
                assert!(result.is_ok());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(collection.size().unwrap(), 0);
    }
    
    #[test]
    fn test_subscriber() {
        let counter = Arc::new(AtomicI32::new(0));
        let counter_ref = Arc::clone(&counter);
        let collection = setup_collection_operations();
        let subscriber = CollectionEventListener::new(Box::new(move |_| {
            counter_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }));
        let subscriber_ref = collection.subscribe(subscriber).expect("Subscribe failed");
        let document = doc!{"field": "value"};
        collection.insert(document).expect("Insert failed");
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
        collection.unsubscribe(subscriber_ref.unwrap()).expect("Unsubscribe failed");
    }
    
    #[test]
    fn test_close() {
        let collection = setup_collection_operations();
        let result = collection.close();
        assert!(result.is_ok());
    }
}
