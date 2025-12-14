use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use super::core::{ChangeType, Command, JournalEntry, TransactionContext};
use crate::collection::operation::{CollectionOperations, WriteResult};
use crate::collection::{
    CollectionEventInfo, CollectionEventListener, Document, FindOptions, NitriteCollection, NitriteCollectionProvider, NitriteId, UpdateOptions
};
use crate::common::{
    create_unique_filter, AttributeAware, Attributes, EventAware,
    NitriteEventBus, PersistentCollection, Processor, DOC_ID,
};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::filter::{all, field, is_all_filter};
use crate::index::{IndexDescriptor, IndexOptions};
use crate::store::NitriteStore;

#[derive(Clone)]
pub(crate) struct TransactionalCollection {
    inner: Arc<TransactionalCollectionInner>,
}

impl TransactionalCollection {
    pub fn new(
        primary: NitriteCollection,
        context: TransactionContext,
        store: NitriteStore,
        operations: CollectionOperations,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    ) -> Self {
        let inner = TransactionalCollectionInner::new(
            primary,
            context,
            store,
            operations,
            event_bus,
        );
        TransactionalCollection {
            inner: Arc::new(inner),
        }
    }
}

impl PersistentCollection for TransactionalCollection {
    fn add_processor(&self, processor: Processor) -> NitriteResult<()> {
        self.inner.add_processor(processor)
    }

    fn create_index(
        &self,
        field_names: Vec<&str>,
        index_options: &IndexOptions,
    ) -> NitriteResult<()> {
        self.inner.create_index(field_names, index_options)
    }

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.inner.rebuild_index(field_names)
    }

    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.inner.list_indexes()
    }

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.inner.has_index(field_names)
    }

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.inner.is_indexing(field_names)
    }

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.inner.drop_index(field_names)
    }

    fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.inner.drop_all_indexes()
    }

    fn clear(&self) -> NitriteResult<()> {
        self.inner.clear()
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.inner.dispose()
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        self.inner.is_dropped()
    }

    fn is_open(&self) -> NitriteResult<bool> {
        self.inner.is_open()
    }

    fn size(&self) -> NitriteResult<u64> {
        self.inner.size()
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn store(&self) -> NitriteResult<NitriteStore> {
        self.inner.store()
    }
}

impl EventAware for TransactionalCollection {
    fn subscribe(
        &self,
        handler: crate::collection::CollectionEventListener,
    ) -> NitriteResult<Option<crate::SubscriberRef>> {
        self.inner.subscribe(handler)
    }

    fn unsubscribe(&self, subscriber: crate::SubscriberRef) -> NitriteResult<()> {
        self.inner.unsubscribe(subscriber)
    }
}

impl AttributeAware for TransactionalCollection {
    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.inner.attributes()
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.inner.set_attributes(attributes)
    }
}

impl NitriteCollectionProvider for TransactionalCollection {
    fn insert(&self, document: Document) -> NitriteResult<WriteResult> {
        self.inner.insert(document)
    }

    fn insert_many(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        self.inner.insert_batch(documents)
    }

    fn update_with_options(
        &self,
        filter: crate::filter::Filter,
        update: &Document,
        update_options: &crate::collection::UpdateOptions,
    ) -> NitriteResult<WriteResult> {
        self.inner
            .update_with_options(filter, update, update_options)
    }

    fn update_one(
        &self,
        document: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        self.inner.update_one(document, insert_if_absent)
    }

    fn update_by_id(
        &self,
        id: &NitriteId,
        update: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        self.inner.update_by_id(id, update, insert_if_absent)
    }

    fn remove(&self, filter: crate::filter::Filter, just_once: bool) -> NitriteResult<WriteResult> {
        self.inner.remove(filter, just_once)
    }

    fn remove_one(&self, document: &Document) -> NitriteResult<WriteResult> {
        self.inner.remove_one(document)
    }

    fn find(&self, filter: crate::filter::Filter) -> NitriteResult<crate::common::DocumentCursor> {
        self.inner.find(filter)
    }

    fn find_with_options(
        &self,
        _filter: crate::filter::Filter,
        find_options: &crate::collection::FindOptions,
    ) -> NitriteResult<crate::common::DocumentCursor> {
        self.inner.find_with_options(_filter, find_options)
    }

    fn get_by_id(&self, id: &NitriteId) -> NitriteResult<Option<Document>> {
        self.inner.get_by_id(id)
    }

    fn name(&self) -> String {
        self.inner.name()
    }
}

struct TransactionalCollectionInner {
    primary: NitriteCollection,
    context: TransactionContext,
    store: NitriteStore,
    dropped: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    operations: CollectionOperations,
}

impl TransactionalCollectionInner {
    pub fn new(
        primary: NitriteCollection,
        context: TransactionContext,
        store: NitriteStore,
        operations: CollectionOperations,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    ) -> Self {
        TransactionalCollectionInner {
            primary,
            context,
            store,
            dropped: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
            event_bus,
            operations,
        }
    }

    fn check_open(&self) -> NitriteResult<()> {
        let is_closed = self.closed.load(std::sync::atomic::Ordering::Acquire);
        if is_closed {
            return Err(NitriteError::new(
                "Collection is closed",
                ErrorKind::InvalidOperation,
            ));
        }

        if self.is_dropped()? {
            return Err(NitriteError::new(
                "Collection is dropped",
                ErrorKind::InvalidOperation,
            ));
        }

        if !self.context.is_active() {
            return Err(NitriteError::new(
                "No active transaction",
                ErrorKind::InvalidOperation,
            ));
        }

        Ok(())
    }

    fn add_processor(&self, processor: Processor) -> NitriteResult<()> {
        self.primary.add_processor(processor)
    }

    fn create_index(
        &self,
        field_names: Vec<&str>,
        index_options: &IndexOptions,
    ) -> NitriteResult<()> {
        self.check_open()?;
        
        // Auto-committed: execute immediately on primary collection
        // The index will be automatically updated when documents are committed
        self.primary.create_index(field_names, index_options)?;
        
        Ok(())
    }

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.check_open()?;
        self.primary.rebuild_index(field_names)
    }

    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.check_open()?;
        self.primary.list_indexes()
    }

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.check_open()?;
        self.primary.has_index(field_names)
    }

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.check_open()?;
        self.primary.is_indexing(field_names)
    }

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.check_open()?;
        
        // Auto-committed: execute immediately on primary collection
        self.primary.drop_index(field_names)?;
        
        Ok(())
    }

    fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.check_open()?;
        self.primary.drop_all_indexes()
    }

    fn clear(&self) -> NitriteResult<()> {
        self.check_open()?;
        self.operations.clear()?;
        self.primary.clear()
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.check_open()?;
        self.primary.dispose()?;
        self.dropped
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        Ok(self.dropped.load(std::sync::atomic::Ordering::Acquire))
    }

    fn is_open(&self) -> NitriteResult<bool> {
        let is_closed = self.closed.load(std::sync::atomic::Ordering::Acquire);
        Ok(!is_closed && !self.is_dropped()?)
    }

    fn size(&self) -> NitriteResult<u64> {
        self.check_open()?;
        // Use direct map size calculation instead of loading all documents
        // via find(all())?.size() which causes O(n) memory usage
        self.operations.size()
    }

    fn close(&self) -> NitriteResult<()> {
        self.check_open()?;
        self.event_bus.close()?;
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn store(&self) -> NitriteResult<NitriteStore> {
        Ok(self.store.clone())
    }

    fn subscribe(
        &self,
        handler: crate::collection::CollectionEventListener,
    ) -> NitriteResult<Option<crate::SubscriberRef>> {
        self.check_open()?;
        self.event_bus.register(handler)
    }

    fn unsubscribe(&self, subscriber: crate::SubscriberRef) -> NitriteResult<()> {
        self.check_open()?;
        self.event_bus.deregister(subscriber)
    }

    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.check_open()?;
        self.operations.attributes()
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.check_open()?;
        self.operations.set_attributes(attributes.clone())?;

        // Capture the original attributes for rollback
        let original_attrs = self.primary.attributes()?.unwrap_or_default();
        let primary_collection = self.primary.clone();
        let attrs_for_commit = attributes.clone();
        let attrs_for_rollback = original_attrs.clone();
        let primary_for_rollback = self.primary.clone();

        // Commit: apply the new attributes to the primary collection
        let commit: Command = Arc::new(move || {
            primary_collection.set_attributes(attrs_for_commit.clone())?;
            Ok(())
        });

        // Rollback: restore the original attributes
        let rollback: Command = Arc::new(move || {
            primary_for_rollback.set_attributes(attrs_for_rollback.clone())?;
            Ok(())
        });

        let entry = JournalEntry::new(ChangeType::SetAttributes, Some(commit), Some(rollback));
        self.context.add_entry(entry)?;
        Ok(())
    }

    fn insert(&self, document: Document) -> NitriteResult<WriteResult> {
        self.check_open()?;

        // Generate ID before inserting (like Java does)
        let mut document = document;
        let _nitrite_id = document.id()?;

        // Clone the document with ID for commit closure
        let doc_for_commit = document.clone();
        let inserted_id = doc_for_commit.clone().id()?;

        let result = self.operations.insert(document)?;

        let primary = self.primary.clone();
        let primary_for_rollback = self.primary.clone();

        // Commit: insert the document into the primary collection
        let commit: Command = Arc::new(move || {
            primary.insert(doc_for_commit.clone())?;
            Ok(())
        });

        // Rollback: remove the inserted document using its ID
        let rollback: Command = Arc::new(move || {
            let filter = crate::filter::by_id(inserted_id.clone());
            primary_for_rollback.remove(filter, true)?;
            Ok(())
        });

        let entry = JournalEntry::new(ChangeType::Insert, Some(commit), Some(rollback));
        self.context.add_entry(entry)?;
        Ok(result)
    }

    fn insert_batch(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        self.check_open()?;

        // Generate IDs for all documents before inserting (like Java does)
        let mut documents: Vec<Document> = documents;
        let mut inserted_ids = Vec::with_capacity(documents.len());
        for doc in &mut documents {
            let id = doc.id()?;
            inserted_ids.push(id);
        }

        // Clone the documents with IDs for commit closure
        let docs_for_commit = documents.clone();

        let result = self.operations.insert_batch(documents)?;

        let primary = self.primary.clone();
        let primary_for_rollback = self.primary.clone();
        let ids_for_rollback = inserted_ids.clone();

        // Commit: insert all documents into the primary collection
        let commit: Command = Arc::new(move || {
            primary.insert_many(docs_for_commit.clone())?;
            Ok(())
        });

        // Rollback: remove all inserted documents using their IDs
        let rollback: Command = Arc::new(move || {
            let filter = field(DOC_ID).in_array(ids_for_rollback.clone());
            primary_for_rollback.remove(filter, false)?;
            Ok(())
        });

        let entry = JournalEntry::new(ChangeType::Insert, Some(commit), Some(rollback));
        self.context.add_entry(entry)?;
        Ok(result)
    }

    fn update_with_options(
        &self,
        filter: crate::filter::Filter,
        update: &Document,
        update_options: &crate::collection::UpdateOptions,
    ) -> NitriteResult<WriteResult> {
        self.check_open()?;

        // Find all matching documents BEFORE the update to enable rollback
        let matched_documents: Vec<Document> = self
            .primary
            .find(filter.clone())?
            .map(|x| x.ok())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        let result = self
            .operations
            .update(filter.clone(), update, update_options)?;

        let primary = self.primary.clone();
        let filter_for_commit = filter.clone();
        let update_for_commit = update.clone();
        let insert_if_absent = update_options.is_insert_if_absent();
        let just_once = update_options.is_just_once();
        let primary_for_rollback = self.primary.clone();
        let filter_for_rollback = filter.clone();
        let docs_for_rollback = matched_documents.clone();

        // Commit: apply the update to matching documents
        let commit: Command = Arc::new(move || {
            let opts = crate::collection::UpdateOptions::new(insert_if_absent, just_once);
            primary.update_with_options(filter_for_commit.clone(), &update_for_commit, &opts)?;
            Ok(())
        });

        // Rollback: restore the original documents
        let rollback: Command = Arc::new(move || {
            // Remove the updated documents
            primary_for_rollback.remove(filter_for_rollback.clone(), false)?;
            // Re-insert the original documents
            for doc in docs_for_rollback.clone() {
                primary_for_rollback.insert(doc)?;
            }
            Ok(())
        });

        let entry = JournalEntry::new(ChangeType::Update, Some(commit), Some(rollback));
        self.context.add_entry(entry)?;
        Ok(result)
    }

    fn update_one(
        &self,
        document: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        let mut document = document.clone();

        if insert_if_absent {
            let filter = create_unique_filter(&mut document)?;
            self.update_with_options(filter, &document, &UpdateOptions::new(true, false))
        } else {
            if document.has_id() {
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
    }

    fn update_by_id(
        &self,
        id: &NitriteId,
        update: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        self.check_open()?;

        // Get the original document for rollback purposes
        let original_doc = self.operations.get_by_id(id)?;
        
        // Perform the update
        let result = self.operations.update_by_id(id, update, insert_if_absent)?;

        if result.affected_nitrite_ids().is_empty() {
            // Nothing was updated, no need for journal entry
            return Ok(result);
        }

        let primary = self.primary.clone();
        let id_for_commit = id.clone();
        let update_for_commit = update.clone();
        let primary_for_rollback = self.primary.clone();
        let id_for_rollback = id.clone();
        let original_for_rollback = original_doc.clone();
        let was_insert = original_doc.is_none();

        // Commit: apply the same update to the primary collection
        let commit: Command = Arc::new(move || {
            primary.update_by_id(&id_for_commit, &update_for_commit, true)?;
            Ok(())
        });

        // Rollback: restore the original document or remove if it was inserted
        let rollback: Command = Arc::new(move || {
            if was_insert {
                // Document was inserted, so remove it
                if let Some(doc) = primary_for_rollback.get_by_id(&id_for_rollback)? {
                    primary_for_rollback.remove_one(&doc)?;
                }
            } else if let Some(ref orig) = original_for_rollback {
                // Document existed, restore it
                primary_for_rollback.update_by_id(&id_for_rollback, orig, false)?;
            }
            Ok(())
        });

        let entry = JournalEntry::new(ChangeType::Update, Some(commit), Some(rollback));
        self.context.add_entry(entry)?;
        Ok(result)
    }

    fn remove(&self, filter: crate::filter::Filter, just_once: bool) -> NitriteResult<WriteResult> {
        if is_all_filter(&filter) && just_once {
            log::error!("Cannot remove all documents with just once as true");
            return Err(NitriteError::new(
                "Cannot remove all documents with just once as true",
                ErrorKind::InvalidOperation,
            ));
        }

        self.check_open()?;

        // Find all matching documents BEFORE removal to enable rollback
        let matched_documents: Vec<Document> = self
            .primary
            .find(filter.clone())?
            .map(|x| x.ok())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        let result = self.operations.remove(filter.clone(), just_once)?;

        let primary = self.primary.clone();
        let filter_for_commit = filter.clone();
        let primary_for_rollback = self.primary.clone();
        let docs_for_rollback = matched_documents.clone();

        // Commit: remove the matching documents
        let commit: Command = Arc::new(move || {
            primary.remove(filter_for_commit.clone(), just_once)?;
            Ok(())
        });

        // Rollback: re-insert the original documents
        let rollback: Command = Arc::new(move || {
            primary_for_rollback.insert_many(docs_for_rollback.clone())?;
            Ok(())
        });

        let entry = JournalEntry::new(ChangeType::Remove, Some(commit), Some(rollback));
        self.context.add_entry(entry)?;
        Ok(result)
    }

    fn remove_one(&self, document: &Document) -> NitriteResult<WriteResult> {
        if !document.has_id() {
            log::error!("Document does not have id");
            return Err(NitriteError::new(
                "Document does not have id",
                ErrorKind::NotIdentifiable,
            ));
        }

        self.check_open()?;

        // Get the document ID to find the exact document to remove
        let doc_id = document.clone().id()?;

        // Capture the original document BEFORE removal to enable rollback
        // Check in the transactional operations, not the primary collection
        return match self.operations.get_by_id(&doc_id)? {
            None => {
                log::error!("Document not found");
                return Err(NitriteError::new("Document not found", ErrorKind::NotFound));
            }
            Some(original_doc) => {
                let result = self.operations.remove_document(document)?;

                let primary = self.primary.clone();
                let primary_for_rollback = self.primary.clone();
                let doc_for_rollback = original_doc.clone();
                let doc_for_commit = document.clone();

                // Commit: remove the document
                let commit: Command = Arc::new(move || {
                    primary.remove_one(&doc_for_commit)?;
                    Ok(())
                });

                // Rollback: re-insert the original document
                let rollback: Command = Arc::new(move || {
                    primary_for_rollback.insert(doc_for_rollback.clone())?;
                    Ok(())
                });

                let entry = JournalEntry::new(ChangeType::Remove, Some(commit), Some(rollback));
                self.context.add_entry(entry)?;
                Ok(result)
            }
        };
    }

    fn find(&self, filter: crate::filter::Filter) -> NitriteResult<crate::common::DocumentCursor> {
        self.check_open()?;
        self.operations.find(filter, &FindOptions::new())
    }

    fn find_with_options(
        &self,
        _filter: crate::filter::Filter,
        find_options: &crate::collection::FindOptions,
    ) -> NitriteResult<crate::common::DocumentCursor> {
        self.check_open()?;
        self.operations.find(_filter, find_options)
    }

    fn get_by_id(&self, id: &NitriteId) -> NitriteResult<Option<Document>> {
        self.check_open()?;
        self.operations.get_by_id(id)
    }

    fn name(&self) -> String {
        self.primary.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LockRegistry;
    use crate::doc;
    use crate::index::unique_index;
    use crate::nitrite::Nitrite;
    use crate::transaction::NitriteTransaction;

    fn create_test_db() -> Nitrite {
        Nitrite::builder().open_or_create(None, None).unwrap()
    }

    // Tests that use NitriteTransaction to exercise TransactionalCollection

    // ==================== Insert Tests via Transaction ====================

    #[test]
    fn test_insert_document_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_insert").unwrap();
        
        let doc = doc!{"name": "John", "age": 30};
        
        let result = coll.insert(doc);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 1);
    }

    #[test]
    fn test_insert_many_documents_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_insert_many").unwrap();
        
        let docs = vec![
            doc!{"name": "A"},
            doc!{"name": "B"},
            doc!{"name": "C"},
        ];
        
        let result = coll.insert_many(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 3);
    }

    #[test]
    fn test_insert_returns_nitrite_ids() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_ids").unwrap();
        
        let doc = doc!{"name": "Test"};
        let result = coll.insert(doc).unwrap();
        
        let ids = result.affected_nitrite_ids();
        assert_eq!(ids.len(), 1);
    }

    // ==================== Find Tests via Transaction ====================

    #[test]
    fn test_find_all_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_find_all").unwrap();
        
        coll.insert(doc!{"name": "A"}).unwrap();
        coll.insert(doc!{"name": "B"}).unwrap();
        
        let cursor = coll.find(all()).unwrap();
        assert_eq!(cursor.count(), 2);
    }

    #[test]
    fn test_find_with_filter_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_find_filter").unwrap();
        
        coll.insert(doc!{"name": "John", "age": 30}).unwrap();
        coll.insert(doc!{"name": "Jane", "age": 25}).unwrap();
        
        let cursor = coll.find(field("name").eq("John")).unwrap();
        assert_eq!(cursor.count(), 1);
    }

    #[test]
    fn test_find_empty_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_find_empty").unwrap();
        
        let cursor = coll.find(all()).unwrap();
        assert_eq!(cursor.count(), 0);
    }

    #[test]
    fn test_find_with_options_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_find_opts").unwrap();
        
        coll.insert(doc!{"name": "A"}).unwrap();
        coll.insert(doc!{"name": "B"}).unwrap();
        coll.insert(doc!{"name": "C"}).unwrap();
        
        let options = FindOptions::new().skip(1).limit(1);
        let cursor = coll.find_with_options(all(), &options).unwrap();
        assert_eq!(cursor.count(), 1);
    }

    // ==================== Get By ID Tests ====================

    #[test]
    fn test_get_by_id_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_get_by_id").unwrap();
        
        let doc = doc!{"name": "John"};
        let result = coll.insert(doc).unwrap();
        let id = &result.affected_nitrite_ids()[0];
        
        let found = coll.get_by_id(id).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().get("name").unwrap().as_string().unwrap(), "John");
    }

    #[test]
    fn test_get_by_id_nonexistent_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_get_by_id_none").unwrap();
        
        let fake_id = NitriteId::new();
        let found = coll.get_by_id(&fake_id).unwrap();
        assert!(found.is_none());
    }

    // ==================== Update Tests ====================

    #[test]
    fn test_update_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_update").unwrap();
        
        coll.insert(doc!{"name": "John", "age": 30}).unwrap();
        
        let update = doc!{"age": 31};
        let opts = UpdateOptions::new(false, false);
        let result = coll.update_with_options(field("name").eq("John"), &update, &opts);
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_one_upsert_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_upsert").unwrap();
        
        // Update with insert_if_absent = true (upsert)
        let doc = doc!{"name": "NewPerson", "age": 25};
        let result = coll.update_one(&doc, true);
        
        assert!(result.is_ok());
        
        // Verify it was inserted
        let cursor = coll.find(field("name").eq("NewPerson")).unwrap();
        assert_eq!(cursor.count(), 1);
    }

    #[test]
    fn test_update_one_without_id_fails_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_update_no_id").unwrap();
        
        let doc = doc!{"name": "John", "age": 30};
        let result = coll.update_one(&doc, false);
        
        assert!(result.is_err());
    }

    // ==================== Remove Tests ====================

    #[test]
    fn test_remove_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_remove").unwrap();
        
        coll.insert(doc!{"name": "John"}).unwrap();
        coll.insert(doc!{"name": "Jane"}).unwrap();
        
        let result = coll.remove(field("name").eq("John"), false);
        assert!(result.is_ok());
        
        let count = coll.find(all()).unwrap().count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_remove_just_once_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_remove_once").unwrap();
        
        coll.insert(doc!{"type": "A"}).unwrap();
        coll.insert(doc!{"type": "A"}).unwrap();
        coll.insert(doc!{"type": "A"}).unwrap();
        
        let result = coll.remove(field("type").eq("A"), true);
        assert!(result.is_ok());
        
        // Should have removed only one
        let count = coll.find(all()).unwrap().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_remove_all_with_just_once_fails_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_remove_all_once").unwrap();
        
        coll.insert(doc!{"name": "Test"}).unwrap();
        
        let result = coll.remove(all(), true);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_one_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_remove_one").unwrap();
        
        coll.insert(doc!{"name": "John"}).unwrap();
        
        let cursor = coll.find(field("name").eq("John")).unwrap();
        let docs: Vec<_> = cursor.collect();
        let doc_to_remove = docs[0].clone().unwrap();
        
        let result = coll.remove_one(&doc_to_remove);
        assert!(result.is_ok());
        
        let count = coll.find(all()).unwrap().count();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_remove_one_without_id_fails_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_remove_one_no_id").unwrap();
        
        let doc = doc!{"name": "John"};
        let result = coll.remove_one(&doc);
        
        assert!(result.is_err());
    }

    // ==================== Size Tests ====================

    #[test]
    fn test_size_empty_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_size_empty").unwrap();
        
        let size = coll.size().unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn test_size_with_documents_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_size").unwrap();
        
        coll.insert(doc!{"a": 1}).unwrap();
        coll.insert(doc!{"b": 2}).unwrap();
        
        let size = coll.size().unwrap();
        assert_eq!(size, 2);
    }

    // ==================== Index Tests ====================

    #[test]
    fn test_create_index_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_idx").unwrap();
        
        let result = coll.create_index(vec!["name"], &IndexOptions::default());
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_index_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_has_idx").unwrap();
        
        coll.create_index(vec!["name"], &IndexOptions::default()).unwrap();
        
        let has = coll.has_index(vec!["name"]).unwrap();
        assert!(has);
    }

    #[test]
    fn test_list_indexes_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_list_idx").unwrap();
        
        coll.create_index(vec!["field1"], &IndexOptions::default()).unwrap();
        
        let indexes = coll.list_indexes().unwrap();
        assert!(!indexes.is_empty());
    }

    #[test]
    fn test_drop_index_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_drop_idx").unwrap();
        
        coll.create_index(vec!["name"], &IndexOptions::default()).unwrap();
        
        let result = coll.drop_index(vec!["name"]);
        assert!(result.is_ok());
        
        let has = coll.has_index(vec!["name"]).unwrap();
        assert!(!has);
    }

    #[test]
    fn test_drop_all_indexes_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_drop_all_idx").unwrap();
        
        coll.create_index(vec!["name"], &IndexOptions::default()).unwrap();
        coll.create_index(vec!["age"], &IndexOptions::default()).unwrap();
        
        let result = coll.drop_all_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_indexing_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_is_indexing").unwrap();
        
        let result = coll.is_indexing(vec!["nonexistent"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_rebuild_index_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_rebuild").unwrap();
        
        coll.create_index(vec!["name"], &IndexOptions::default()).unwrap();
        
        let result = coll.rebuild_index(vec!["name"]);
        assert!(result.is_ok());
    }

    // ==================== Clear Tests ====================

    #[test]
    fn test_clear_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_clear").unwrap();
        
        coll.insert(doc!{"a": 1}).unwrap();
        coll.insert(doc!{"b": 2}).unwrap();
        
        coll.clear().unwrap();
        
        let size = coll.size().unwrap();
        assert_eq!(size, 0);
    }

    // ==================== Store Tests ====================

    #[test]
    fn test_store_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_store").unwrap();
        
        let store = coll.store();
        assert!(store.is_ok());
    }

    // ==================== Attribute Tests ====================

    #[test]
    fn test_attributes_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_attrs").unwrap();
        
        let attrs = coll.attributes().unwrap();
        // May or may not have attributes initially
        assert!(attrs.is_none() || attrs.is_some());
    }

    #[test]
    fn test_set_attributes_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_set_attrs").unwrap();
        
        let attrs = Attributes::default();
        let result = coll.set_attributes(attrs);
        
        assert!(result.is_ok());
    }

    // ==================== Name Tests ====================

    #[test]
    fn test_collection_name_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("my_collection").unwrap();
        
        assert_eq!(coll.name(), "my_collection");
    }

    // ==================== Open/Dropped State Tests ====================

    #[test]
    fn test_is_open_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_is_open").unwrap();
        
        assert!(coll.is_open().unwrap());
    }

    #[test]
    fn test_is_dropped_in_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_is_dropped").unwrap();
        
        assert!(!coll.is_dropped().unwrap());
    }

    // ==================== Commit/Rollback Integration ====================

    #[test]
    fn test_insert_and_commit() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_commit").unwrap();
        coll.insert(doc!{"name": "Test"}).unwrap();
        
        // Commit should succeed
        let result = tx.commit();
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_and_rollback() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_rollback").unwrap();
        coll.insert(doc!{"name": "Test"}).unwrap();
        
        // Rollback should succeed
        let result = tx.rollback();
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_operations_and_commit() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_multi_commit").unwrap();
        coll.create_index(vec!["name"], &unique_index()).unwrap();
        
        // Insert documents
        coll.insert(doc!{"name": "John", "age": 30}).unwrap();
        coll.insert(doc!{"name": "Jane", "age": 25}).unwrap();
        
        // Update one
        let update = doc!{"age": 31};
        coll.update_with_options(
            field("name").eq("John"), 
            &update, 
            &UpdateOptions::new(false, false)
        ).unwrap();
        
        // Commit
        let result = tx.commit();
        assert!(result.is_ok());
    }

    #[test]
    fn test_operations_after_closed_transaction_fail() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();
        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        
        let coll = tx.collection("test_closed_ops").unwrap();
        
        // Commit closes the transaction
        tx.commit().unwrap();
        
        // Operations should fail now
        let result = coll.insert(doc!{"name": "Test"});
        assert!(result.is_err());
    }
}
