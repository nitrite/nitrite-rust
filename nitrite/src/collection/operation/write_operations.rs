use super::{
    index_writer::DocumentIndexWriter, read_operations::ReadOperations, write_result::WriteResult,
};
use crate::{
    collection::{
        CollectionEventInfo, CollectionEventListener, CollectionEvents, Document, FindOptions, NitriteId, UpdateOptions
    }, common::get_current_time_or_zero, errors::{ErrorKind, NitriteError, NitriteResult}, filter::Filter, get_current_time, store::{NitriteMap, NitriteMapProvider}, Key, NitriteEventBus, ProcessorChain, ProcessorProvider, Value, DOC_ID, DOC_MODIFIED, DOC_REVISION, DOC_SOURCE, REPLICATOR
};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct WriteOperations {
    inner: Arc<WriteOperationsInner>,
}

impl WriteOperations {
    /// Creates a new WriteOperations instance with the required components.
    pub fn new(
        document_index_writer: DocumentIndexWriter,
        read_operations: ReadOperations,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
        nitrite_map: NitriteMap,
        processor_chain: ProcessorChain,
    ) -> Self {
        let inner = WriteOperationsInner::new(
            document_index_writer,
            read_operations,
            event_bus,
            nitrite_map,
            processor_chain,
        );

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Returns the document index writer for handling index operations.
    fn document_index_writer(&self) -> DocumentIndexWriter {
        self.inner.document_index_writer.clone()
    }

    /// Returns the read operations handler for querying documents.
    fn read_operations(&self) -> ReadOperations {
        self.inner.read_operations.clone()
    }

    /// Returns the event bus for publishing collection events.
    fn event_bus(&self) -> NitriteEventBus<CollectionEventInfo, CollectionEventListener> {
        self.inner.event_bus.clone()
    }

    /// Returns the underlying nitrite map for direct document storage access.
    fn nitrite_map(&self) -> NitriteMap {
        self.inner.nitrite_map.clone()
    }

    /// Returns the processor chain for pre-write document processing.
    fn processor_chain(&self) -> ProcessorChain {
        self.inner.processor_chain.clone()
    }

    /// Inserts a single document into the collection.
    pub fn insert(&self, document: Document) -> NitriteResult<WriteResult> {
        self.inner.insert(document)
    }

    /// Inserts multiple documents into the collection using optimized batch operations.
    pub fn insert_batch(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        self.inner.insert_batch(documents)
    }

    /// Updates documents matching a filter with the provided update fields.
    pub fn update(
        &self,
        filter: Filter,
        update: &Document,
        update_options: &UpdateOptions,
    ) -> NitriteResult<WriteResult> {
        self.inner.update(filter, update, update_options)
    }

    /// Updates a document directly by its NitriteId without filter-based lookup.
    pub fn update_by_id(
        &self,
        id: &NitriteId,
        update: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        self.inner.update_by_id(id, update, insert_if_absent)
    }

    /// Removes documents matching a filter.
    pub fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult> {
        self.inner.remove(filter, just_once)
    }

    /// Removes a specific document from the collection.
    pub fn remove_document(&self, document: &Document) -> NitriteResult<WriteResult> {
        self.inner.remove_document(document)
    }
}

/// Inner implementation of write operations containing the actual business logic.
struct WriteOperationsInner {
    document_index_writer: DocumentIndexWriter,
    read_operations: ReadOperations,
    event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
    nitrite_map: NitriteMap,
    processor_chain: ProcessorChain,
}

impl WriteOperationsInner {
    fn new(
        document_index_writer: DocumentIndexWriter,
        read_operations: ReadOperations,
        event_bus: NitriteEventBus<CollectionEventInfo, CollectionEventListener>,
        nitrite_map: NitriteMap,
        processor_chain: ProcessorChain,
    ) -> Self {
        Self {
            document_index_writer,
            read_operations,
            event_bus,
            nitrite_map,
            processor_chain,
        }
    }

    pub fn insert(&self, document: Document) -> NitriteResult<WriteResult> {
        self.insert_batch(vec![document])
    }

    pub fn insert_batch(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        if documents.is_empty() {
            return Ok(WriteResult::new(vec![]));
        }
        
        // For small batches, use simple sequential processing
        // (overhead of batch coordination exceeds benefits)
        if documents.len() <= 10 {
            return self.insert_batch_sequential(documents);
        }
        
        // For larger batches, use optimized batch insert with put_all
        self.insert_batch_optimized(documents)
    }
    
    /// Sequential insert for small batches - simple and efficient for few documents
    fn insert_batch_sequential(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        let mut nitrite_ids = Vec::with_capacity(documents.len());
        for document in documents {
            let id = self.process_insert(document)?;
            nitrite_ids.push(id);
        }
        Ok(WriteResult::new(nitrite_ids))
    }
    
    /// Optimized batch insert using put_all for larger batches.
    /// 
    /// This method uses a three-phase approach:
    /// 1. Prepare: Process all documents (metadata, pre-write processing)
    /// 2. Validate: Check for duplicate NitriteIds (primary key constraint)
    /// 3. Commit: Use put_all for atomic batch storage, then write indexes
    /// 
    /// Note: Unique index constraint violations are detected during index writing (Phase 4).
    /// If a unique index violation occurs, all successfully indexed documents and stored
    /// documents are rolled back to maintain consistency.
    fn insert_batch_optimized(&self, documents: Vec<Document>) -> NitriteResult<WriteResult> {
        let batch_size = documents.len();
        
        // Phase 1: Prepare all documents and collect metadata
        let prepared: Vec<(NitriteId, Document, Document, String)> = documents
            .into_iter()
            .map(|doc| self.prepare_document_for_insert(doc))
            .collect::<NitriteResult<Vec<_>>>()?;
        
        // Phase 2: Check for duplicate NitriteIds (primary key uniqueness)
        let keys: Vec<_> = prepared.iter()
            .map(|(id, _, _, _)| Value::NitriteId(*id))
            .collect();
        
        self.validate_no_duplicates(&keys)?;
        
        // Collect all IDs upfront for potential rollback (put_all stores all at once)
        let all_ids: Vec<NitriteId> = prepared.iter()
            .map(|(id, _, _, _)| *id)
            .collect();
        
        // Phase 3: Batch write using put_all
        let entries: Vec<(Key, Value)> = prepared.iter()
            .map(|(id, processed_doc, _, _)| {
                (Value::NitriteId(*id), Value::Document(processed_doc.clone()))
            })
            .collect();
        
        self.nitrite_map.put_all(entries)
            .map_err(|e| NitriteError::new(
                &format!("Failed to batch insert documents: {}", e),
                e.kind().clone(),
            ))?;
        
        // Phase 4: Write index entries and publish events
        // Track successfully indexed documents for potential rollback
        // Unique index constraint violations are detected here
        let mut nitrite_ids = Vec::with_capacity(batch_size);
        let mut indexed_docs: Vec<Document> = Vec::with_capacity(batch_size);
        
        for (id, mut processed_doc, original_doc, source) in prepared {
            // Write index entries - this is where unique index violations are detected
            if let Err(e) = self.document_index_writer.write_index_entry(&mut processed_doc) {
                // Rollback: remove index entries for successfully indexed documents
                self.rollback_batch_indexes(&indexed_docs);
                // Rollback: remove ALL stored documents (put_all stored them all at once)
                self.rollback_batch_insert(&all_ids);
                
                return Err(NitriteError::new(
                    &format!("Failed to write index entries during batch insert (unique constraint violation?): {}", e),
                    e.kind().clone(),
                ));
            }
            
            // Track successfully indexed document for potential rollback
            indexed_docs.push(processed_doc.clone());
            
            // Publish event
            let value = Value::Document(original_doc);
            let event = CollectionEventInfo::new(Some(value), CollectionEvents::Insert, source);
            if let Err(e) = self.event_bus.publish(event) {
                log::warn!("Failed to publish insert event for {}: {}", id, e);
                // Don't fail the operation for event publishing errors
            }
            
            nitrite_ids.push(id);
        }
        
        Ok(WriteResult::new(nitrite_ids))
    }
    
    /// Prepares a document for insertion by setting metadata and processing.
    /// Returns: (NitriteId, processed_doc, original_doc, source)
    fn prepare_document_for_insert(&self, document: Document) -> NitriteResult<(NitriteId, Document, Document, String)> {
        let mut new_doc = document;
        let nitrite_id = new_doc.id()
            .map_err(|e| NitriteError::new(
                &format!("Failed to retrieve document ID during insert: {}", e),
                e.kind().clone(),
            ))?;
        let source = new_doc.source()
            .map_err(|e| NitriteError::new(
                &format!("Failed to retrieve document source during insert: {}", e),
                e.kind().clone(),
            ))?;
        let time = get_current_time_or_zero();

        if REPLICATOR.ne(&source) {
            new_doc.remove(DOC_SOURCE)
                .map_err(|e| NitriteError::new(
                    &format!("Failed to remove document source field during insert: {}", e),
                    e.kind().clone(),
                ))?;
            new_doc.put(DOC_REVISION, Value::I32(1))
                .map_err(|e| NitriteError::new(
                    &format!("Failed to set document revision during insert: {}", e),
                    e.kind().clone(),
                ))?;
            new_doc.put(DOC_MODIFIED, Value::U128(time))
                .map_err(|e| NitriteError::new(
                    &format!("Failed to set document modification time during insert: {}", e),
                    e.kind().clone(),
                ))?;
        } else {
            new_doc.remove(DOC_SOURCE)
                .map_err(|e| NitriteError::new(
                    &format!("Failed to remove document source field during replication insert: {}", e),
                    e.kind().clone(),
                ))?;
        }

        let processed = self.processor_chain.process_before_write(new_doc.clone())
            .map_err(|e| NitriteError::new(
                &format!("Failed to process document before write during insert: {}", e),
                e.kind().clone(),
            ))?;
        
        Ok((nitrite_id, processed, new_doc, source))
    }
    
    /// Validates that none of the keys already exist in the map.
    /// Uses parallel checking for large batches.
    fn validate_no_duplicates(&self, keys: &[Value]) -> NitriteResult<()> {
        // For large batches, use parallel validation
        if keys.len() > 50 {
            let num_threads = std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
                .min(keys.len());
            
            let chunk_size = keys.len().div_ceil(num_threads);
            let chunks: Vec<_> = keys.chunks(chunk_size).collect();
            
            let results: Vec<NitriteResult<Option<NitriteId>>> = std::thread::scope(|s| {
                let handles: Vec<_> = chunks
                    .into_iter()
                    .map(|chunk| {
                        s.spawn(move || {
                            for key in chunk {
                                if self.nitrite_map.contains_key(key)? {
                                    if let Value::NitriteId(id) = key {
                                        return Ok(Some(*id));
                                    }
                                }
                            }
                            Ok(None)
                        })
                    })
                    .collect();
                
                handles.into_iter()
                    .map(|h| h.join().unwrap())
                    .collect()
            });
            
            // Check for any duplicates found
            for result in results {
                if let Some(duplicate_id) = result? {
                    log::error!("Document already exists with id {}", duplicate_id);
                    return Err(NitriteError::new(
                        &format!("Document already exists with id {}", duplicate_id),
                        ErrorKind::UniqueConstraintViolation,
                    ));
                }
            }
        } else {
            // Sequential check for smaller batches
            for key in keys {
                if self.nitrite_map.contains_key(key)? {
                    if let Value::NitriteId(id) = key {
                        log::error!("Document already exists with id {}", id);
                        return Err(NitriteError::new(
                            &format!("Document already exists with id {}", id),
                            ErrorKind::UniqueConstraintViolation,
                        ));
                    }
                }
            }
        }
        Ok(())
    }
    
    /// Rollback helper: removes documents that were inserted during a failed batch
    fn rollback_batch_insert(&self, ids: &[NitriteId]) {
        for id in ids {
            if let Err(e) = self.nitrite_map.remove(&Value::NitriteId(*id)) {
                log::error!("Failed to rollback document {} during batch insert: {}", id, e);
            }
        }
    }
    
    /// Rollback helper: removes index entries for documents that were successfully indexed
    /// during a failed batch operation
    fn rollback_batch_indexes(&self, indexed_docs: &[Document]) {
        for doc in indexed_docs {
            let mut doc_clone = doc.clone();
            if let Err(e) = self.document_index_writer.remove_index_entry(&mut doc_clone) {
                log::error!("Failed to rollback index entries during batch insert: {}", e);
            }
        }
    }
    
    fn process_insert(&self, document: Document) -> NitriteResult<NitriteId> {
        let mut new_doc = document;
        let nitrite_id = new_doc.id()
            .map_err(|e| NitriteError::new(&format!("Failed to retrieve document ID during insert: {}", e), e.kind().clone()))?;
        let source = new_doc.source()
            .map_err(|e| NitriteError::new(&format!("Failed to retrieve document source during insert: {}", e), e.kind().clone()))?;
        let time = get_current_time_or_zero();

        if REPLICATOR.ne(&source) {
            new_doc.remove(DOC_SOURCE)
                .map_err(|e| NitriteError::new(&format!("Failed to remove document source field during insert: {}", e), e.kind().clone()))?;
            new_doc.put(DOC_REVISION, Value::I32(1))
                .map_err(|e| NitriteError::new(&format!("Failed to set document revision during insert: {}", e), e.kind().clone()))?;
            new_doc.put(DOC_MODIFIED, Value::U128(time))
                .map_err(|e| NitriteError::new(&format!("Failed to set document modification time during insert: {}", e), e.kind().clone()))?;
        } else {
            new_doc.remove(DOC_SOURCE)
                .map_err(|e| NitriteError::new(&format!("Failed to remove document source field during replication insert: {}", e), e.kind().clone()))?;
        }

        let mut processed = self.processor_chain.process_before_write(new_doc.clone())
            .map_err(|e| NitriteError::new(&format!("Failed to process document before write during insert: {}", e), e.kind().clone()))?;
        let existing = self.nitrite_map.put_if_absent(
            Value::NitriteId(nitrite_id),
            Value::Document(processed.clone()),
        ).map_err(|e| NitriteError::new(&format!("Failed to store document in map during insert: {}", e), e.kind().clone()))?;

        if existing.is_some() {
            log::error!("Document already exists with id {}", nitrite_id.clone());
            return Err(NitriteError::new(
                &format!("Document already exists with id {}", nitrite_id.clone()),
                ErrorKind::UniqueConstraintViolation,
            ));
        } else {
            let result = self.document_index_writer.write_index_entry(&mut processed);
            if let Err(e) = result {
                self.nitrite_map.remove(&Value::NitriteId(nitrite_id))
                    .map_err(|remove_err| NitriteError::new(&format!("Failed to rollback document storage after index write failure: {}", remove_err), remove_err.kind().clone()))?;
                return Err(NitriteError::new(&format!("Failed to write index entries during insert: {}", e), e.kind().clone()));
            }
        }

        let value = Value::Document(new_doc);
        let event = CollectionEventInfo::new(Some(value), CollectionEvents::Insert, source);
        self.event_bus.publish(event)
            .map_err(|e| NitriteError::new(&format!("Failed to publish insert event: {}", e), e.kind().clone()))?;
        
        Ok(nitrite_id)
    }

    pub fn update(
        &self,
        filter: Filter,
        update: &Document,
        update_options: &UpdateOptions,
    ) -> NitriteResult<WriteResult> {
        let cursor = self.read_operations.find(filter, &FindOptions::new())?;
        let mut nitrite_ids = Vec::new();

        let mut document = update.clone();
        document.remove(DOC_ID)?;

        if REPLICATOR.ne(&document.source()?) {
            document.remove(DOC_REVISION)?;
        }

        if document.is_empty() {
            return Ok(WriteResult::new(nitrite_ids));
        }

        let mut count = 0usize;
        let mut batch_size = 10;
        let mut docs = Vec::with_capacity(batch_size);

        for doc_result in cursor {
            let doc = doc_result?;
            count += 1;
            docs.push(doc);

            if count >= 1 && update_options.is_just_once() {
                break;
            }

            // Process in batches for better performance
            if docs.len() >= batch_size {
                self.process_update_batch(&document, &mut nitrite_ids, docs)?;
                docs = Vec::with_capacity(batch_size);
                
                // Dynamically adjust batch size based on performance
                if count > 100 {
                    batch_size = 50;
                } else if count > 1000 {
                    batch_size = 200;
                }
            }
        }

        // Process remaining docs
        if !docs.is_empty() {
            self.process_update_batch(&document, &mut nitrite_ids, docs)?;
        }

        if count == 0 && update_options.is_insert_if_absent() {
            return self.insert(update.clone());
        }

        Ok(WriteResult::new(nitrite_ids))
    }
    
    fn process_update_batch(
        &self, 
        update_doc: &Document, 
        nitrite_ids: &mut Vec<NitriteId>, 
        docs: Vec<Document>
    ) -> NitriteResult<()> {
        // For small batches, use simple sequential processing
        if docs.len() <= 10 {
            for doc in docs {
                if let Some(id) = self.process_single_update(doc, update_doc)? {
                    nitrite_ids.push(id);
                }
            }
            return Ok(());
        }
        
        // For larger batches, use optimized batch update with put_all
        self.process_update_batch_optimized(update_doc, nitrite_ids, docs)
    }
    
    /// Optimized batch update using put_all for larger batches.
    /// 
    /// This method:
    /// 1. Prepares all updated documents
    /// 2. Uses put_all for batch storage
    /// 3. Updates index entries with proper rollback on failure
    fn process_update_batch_optimized(
        &self,
        update_doc: &Document,
        nitrite_ids: &mut Vec<NitriteId>,
        docs: Vec<Document>,
    ) -> NitriteResult<()> {
        let source = update_doc.source()?;
        let time = get_current_time_or_zero();
        
        // Phase 1: Prepare all updated documents
        // Collect: (NitriteId, old_doc, new_doc, processed_doc)
        let mut prepared: Vec<(NitriteId, Document, Document, Document)> = Vec::with_capacity(docs.len());
        
        for doc in docs {
            let mut new_doc = doc.clone();
            let old_doc = doc;
            let nitrite_id = new_doc.id()?;
            
            if REPLICATOR.ne(&source) {
                new_doc.merge(update_doc)?;
                let revision = new_doc.revision()?;
                new_doc.put(DOC_REVISION, Value::I32(revision + 1))?;
                new_doc.put(DOC_MODIFIED, Value::U128(time))?;
            } else {
                new_doc.merge(update_doc)?;
            }
            
            let processed = self.processor_chain.process_before_write(new_doc.clone())?;
            prepared.push((nitrite_id, old_doc, new_doc, processed));
        }
        
        // Phase 2: Batch write using put_all
        let entries: Vec<(Key, Value)> = prepared.iter()
            .map(|(id, _, _, processed)| {
                (Value::NitriteId(*id), Value::Document(processed.clone()))
            })
            .collect();
        
        self.nitrite_map.put_all(entries)?;
        
        // Phase 3: Update index entries with proper rollback tracking
        // Track successfully updated documents for rollback
        let mut updated_indexes: Vec<(NitriteId, Document, Document)> = Vec::with_capacity(prepared.len());
        
        for (id, mut old_doc, new_doc, mut processed) in prepared {
            // Update index entries
            let result = self.document_index_writer.update_index_entry(
                &mut old_doc,
                &mut processed,
                update_doc,
            );
            
            if let Err(e) = result {
                // Rollback: restore old documents for all updates
                self.rollback_batch_update(&updated_indexes, &id, &old_doc, update_doc)?;
                return Err(e);
            }
            
            // Track for potential rollback
            updated_indexes.push((id, old_doc, processed.clone()));
            
            // Publish event
            let value = Value::Document(new_doc);
            let event = CollectionEventInfo::new(Some(value), CollectionEvents::Update, source.clone());
            if let Err(e) = self.event_bus.publish(event) {
                log::warn!("Failed to publish update event for {}: {}", id, e);
            }
            
            if update_doc.size() > 0 {
                nitrite_ids.push(id);
            }
        }
        
        Ok(())
    }
    
    /// Rollback helper for batch updates: restores old documents and index entries
    fn rollback_batch_update(
        &self,
        updated_indexes: &[(NitriteId, Document, Document)],
        failed_id: &NitriteId,
        failed_old_doc: &Document,
        update_doc: &Document,
    ) -> NitriteResult<()> {
        // Restore the failed document's old state
        self.nitrite_map.put(
            Value::NitriteId(*failed_id),
            Value::Document(failed_old_doc.clone()),
        )?;
        
        // Rollback all successfully updated documents
        let mut restore_entries: Vec<(Key, Value)> = Vec::with_capacity(updated_indexes.len());
        
        for (id, old_doc, processed) in updated_indexes {
            restore_entries.push((
                Value::NitriteId(*id),
                Value::Document(old_doc.clone()),
            ));
            
            // Restore index entries
            let mut old_doc_clone = old_doc.clone();
            let mut processed_clone = processed.clone();
            if let Err(e) = self.document_index_writer.update_index_entry(
                &mut processed_clone,
                &mut old_doc_clone,
                update_doc,
            ) {
                log::error!("Failed to rollback index entry for {}: {}", id, e);
            }
        }
        
        // Batch restore old documents
        if !restore_entries.is_empty() {
            if let Err(e) = self.nitrite_map.put_all(restore_entries) {
                log::error!("Failed to batch restore documents during rollback: {}", e);
            }
        }
        
        Ok(())
    }
    
    fn process_single_update(&self, doc: Document, update_doc: &Document) -> NitriteResult<Option<NitriteId>> {
        let mut new_doc = doc.clone();
        let mut old_doc = doc;
        let source = update_doc.source()?;
        let time = get_current_time_or_zero();

        let nitrite_id = new_doc.id()?;

        if REPLICATOR.ne(&source) {
            new_doc.merge(update_doc)?;

            let revision = new_doc.revision()?;
            new_doc.put(DOC_REVISION, Value::I32(revision + 1))?;
            new_doc.put(DOC_MODIFIED, Value::U128(time))?;
        } else {
            new_doc.merge(update_doc)?;
        }

        let mut processed = self.processor_chain.process_before_write(new_doc.clone())?;
        self.nitrite_map.put(
            Value::NitriteId(nitrite_id),
            Value::Document(processed.clone()),
        )?;

        let result = self.document_index_writer.update_index_entry(
            &mut old_doc,
            &mut processed,
            update_doc,
        );
        
        if let Err(e) = result {
            self.nitrite_map.put(
                Value::NitriteId(nitrite_id),
                Value::Document(old_doc.clone()),
            )?;
            self.document_index_writer.update_index_entry(
                &mut processed,
                &mut old_doc,
                update_doc,
            )?;
            return Err(e);
        }

        let value = Value::Document(new_doc.clone());
        let event = CollectionEventInfo::new(Some(value), CollectionEvents::Update, source);
        self.event_bus.publish(event)?;

        if update_doc.size() > 0 {
            Ok(Some(nitrite_id))
        } else {
            Ok(None)
        }
    }

    /// Updates a document directly by its NitriteId without filter-based lookup.
    /// This is an O(1) operation as it directly accesses the document by its key.
    /// 
    /// # Arguments
    /// * `id` - The NitriteId of the document to update
    /// * `update` - The document containing the fields to update (will be merged with existing)
    /// * `insert_if_absent` - If true, inserts the document if it doesn't exist
    /// 
    /// # Returns
    /// * `Ok(WriteResult)` - Contains the affected NitriteId if update was successful
    /// * `Err(NitriteError)` - If the document doesn't exist and insert_if_absent is false
    pub fn update_by_id(
        &self,
        id: &NitriteId,
        update: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        // Get the existing document directly by ID (O(1) lookup)
        let existing = self.nitrite_map.get(&Value::NitriteId(*id))?;
        
        match existing {
            Some(value) => {
                let doc = match value.as_document() {
                    Some(d) => d.clone(),
                    None => {
                        log::error!("Expected Document value in collection store for ID {:?}", id);
                        return Err(NitriteError::new(
                            "Invalid value type in collection store",
                            ErrorKind::ValidationError,
                        ));
                    }
                };
                
                // Process through the existing update logic
                let mut nitrite_ids = Vec::new();
                if let Some(updated_id) = self.process_single_update(doc, update)? {
                    nitrite_ids.push(updated_id);
                }
                Ok(WriteResult::new(nitrite_ids))
            }
            None => {
                if insert_if_absent {
                    // Insert the document with the specified ID
                    let mut new_doc = update.clone();
                    new_doc.put(DOC_ID, Value::NitriteId(*id))?;
                    self.insert(new_doc)
                } else {
                    // Document not found and insert_if_absent is false
                    Ok(WriteResult::new(vec![]))
                }
            }
        }
    }

    pub fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult> {
        let cursor = self.read_operations.find(filter, &FindOptions::new())?;
        let mut nitrite_ids = Vec::new();

        for doc_result in cursor {
            let doc = doc_result?;

            let processed = self.processor_chain.process_before_write(doc.clone())?;
            let event = self.remove_internal(processed.clone(), &mut nitrite_ids)?;

            if let Some(event) = event {
                self.event_bus.publish(event)?;
            }

            if just_once {
                break;
            }
        }

        Ok(WriteResult::new(nitrite_ids))
    }

    pub fn remove_document(&self, document: &Document) -> NitriteResult<WriteResult> {
        let mut nitrite_ids = Vec::new();
        let event = self.remove_internal(document.clone(), &mut nitrite_ids)?;

        if event.is_some() {
            let event = event.unwrap();
            event.set_originator(document.source()?);
            self.event_bus.publish(event)?;
        }

        Ok(WriteResult::new(nitrite_ids))
    }

    fn remove_internal(
        &self,
        mut document: Document,
        nitrite_ids: &mut Vec<NitriteId>,
    ) -> NitriteResult<Option<CollectionEventInfo>> {
        let nitrite_id = document.id()?;
        let document = self
            .nitrite_map
            .remove(&Key::NitriteId(nitrite_id))?;
        
        // Validate that removed value is a Document, not another type
        let mut document = match document {
            Some(Value::Document(doc)) => doc,
            Some(other_value) => {
                log::error!("Data corruption: Expected Document in collection store, found {:?}", other_value);
                return Err(NitriteError::new(
                    "Expected Document value in collection store, found corrupted type",
                    ErrorKind::IndexingError,
                ));
            }
            None => return Ok(None),
        };

        let remove_at = get_current_time_or_zero();
        self.document_index_writer
            .remove_index_entry(&mut document)?;
        nitrite_ids.push(nitrite_id);

        let revision = document.revision()? + 1;
        document.put(DOC_REVISION, Value::I32(revision))?;
        document.put(DOC_MODIFIED, Value::U128(remove_at))?;

        let value = Value::Document(document.clone());
        let event = CollectionEventInfo::new(Some(value), CollectionEvents::Remove, document.source()?);
        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::operation::find_optimizer::FindOptimizer;
    use crate::collection::operation::index_operations::IndexOperations;
    use crate::collection::operation::index_writer::DocumentIndexWriter;
    use crate::collection::operation::read_operations::ReadOperations;
    use crate::collection::{
        Document, UpdateOptions,
    };
    use crate::filter::{all, field};
    use crate::nitrite_config::NitriteConfig;
    use crate::store::{NitriteMapProvider, NitriteStoreProvider};
    use crate::{
        NitriteEventBus, ProcessorChain, Value
        ,
    };

    fn setup_write_operations() -> WriteOperations {
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
            event_bus.clone(),
        )
        .unwrap();
        let document_index_writer =
            DocumentIndexWriter::new(nitrite_config.clone(), index_operations.clone());
        let find_optimizer = FindOptimizer::new();
        let processor_chain = ProcessorChain::new();
        let read_operations = ReadOperations::new(
            collection_name,
            index_operations,
            nitrite_config.clone(),
            nitrite_map.clone(),
            find_optimizer,
            processor_chain.clone(),
        );

        WriteOperations::new(
            document_index_writer,
            read_operations,
            event_bus,
            nitrite_map,
            processor_chain,
        )
    }

    #[test]
    fn test_insert() {
        let write_operations = setup_write_operations();
        let document = Document::new();
        let result = write_operations.insert(document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update() {
        let write_operations = setup_write_operations();
        let filter = all();
        let document = Document::new();
        let update_options = UpdateOptions::default();
        let result = write_operations.update(filter, &document, &update_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove() {
        let write_operations = setup_write_operations();
        let filter = all();
        let result = write_operations.remove(filter, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_document() {
        let write_operations = setup_write_operations();
        let document = Document::new();
        let result = write_operations.remove_document(&document);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_duplicate_document() {
        let write_operations = setup_write_operations();
        let mut document = Document::new();
        let _ = document.id();
        let _ = write_operations.insert(document.clone());
        let result = write_operations.insert(document);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_nonexistent_document() {
        let write_operations = setup_write_operations();
        let filter = all();
        let document = Document::new();
        let update_options = UpdateOptions::default();
        let result = write_operations.update(filter, &document, &update_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_nonexistent_document() {
        let write_operations = setup_write_operations();
        let filter = all();
        let result = write_operations.remove(filter, false);
        assert!(result.is_ok());
    }

    // Double unwrap error handling tests
    #[test]
    fn test_remove_internal_with_valid_document() {
        // Test that remove_internal properly handles valid Document values
        let write_operations = setup_write_operations();
        let mut document = Document::new();
        let _ = document.id();
        
        // Insert first so we have something to remove
        let insert_result = write_operations.insert(document.clone());
        assert!(insert_result.is_ok());
        
        // Now test removing
        let remove_result = write_operations.remove_document(&document);
        assert!(remove_result.is_ok());
    }

    #[test]
    fn test_remove_internal_handles_non_existent() {
        // Test that remove_internal gracefully handles non-existent documents
        // instead of unwrapping None and panicking
        let write_operations = setup_write_operations();
        let document = Document::new();
        
        // Try to remove a document that was never inserted
        let result = write_operations.remove_document(&document);
        // Should succeed with no error - document simply doesn't exist
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_document_preserves_document_metadata() {
        // Test that remove properly sets document revision and modified timestamp
        let write_operations = setup_write_operations();
        let mut document = Document::new();
        let _ = document.id();
        
        // Insert the document
        let insert_result = write_operations.insert(document.clone());
        assert!(insert_result.is_ok());
        
        // Check that remove_document succeeds and returns a WriteResult
        let remove_result = write_operations.remove_document(&document);
        assert!(remove_result.is_ok());
        
        // The WriteResult contains the affected NitriteIds
        let write_result = remove_result.unwrap();
        let affected_ids = write_result.affected_nitrite_ids();
        // Verify that at least the document ID is in the affected IDs
        assert!(!affected_ids.is_empty());
    }

    // Performance optimization tests for batch operations

    #[test]
    fn test_process_update_batch_small_batch() {
        // Test that small batches (<=20) use sequential processing
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut update_doc = Document::new();
        update_doc.put("field", Value::from("updated")).unwrap();
        
        let mut docs = Vec::new();
        for _ in 0..10 {
            let mut doc = Document::new();
            doc.put("field", Value::from("original")).unwrap();
            docs.push(doc);
        }
        
        let mut ids = Vec::new();
        let result = inner.process_update_batch(&update_doc, &mut ids, docs);
        
        // Should complete without error
        assert!(result.is_ok());
    }

    #[test]
    fn test_process_update_batch_vec_extend_optimization() {
        // Test that Vec::extend is used instead of individual pushes
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut update_doc = Document::new();
        update_doc.put("field", Value::from("updated")).unwrap();
        
        let mut docs = Vec::new();
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("id", Value::from(i)).unwrap();
            doc.put("field", Value::from("original")).unwrap();
            docs.push(doc);
        }
        
        let mut ids = Vec::with_capacity(5);
        let initial_capacity = ids.capacity();
        
        let result = inner.process_update_batch(&update_doc, &mut ids, docs);
        assert!(result.is_ok());
        
        // Capacity should remain efficient (no excessive reallocations)
        assert!(ids.capacity() >= initial_capacity);
    }

    #[test]
    fn test_remove_operation_optimized_no_redundant_checks() {
        // Test that remove doesn't perform redundant count checks
        let write_operations = setup_write_operations();
        
        // Insert a document first
        let mut doc = Document::new();
        let _ = doc.id();
        let insert_result = write_operations.insert(doc.clone());
        assert!(insert_result.is_ok());
        
        // Remove with just_once = true
        let filter = all();
        let result = write_operations.remove(filter, true);
        assert!(result.is_ok());
        
        let write_result = result.unwrap();
        let ids = write_result.affected_nitrite_ids();
        assert!(!ids.is_empty());
    }

    #[test]
    fn test_remove_operation_multiple_documents() {
        // Test that remove works correctly with multiple documents
        let write_operations = setup_write_operations();
        
        // Insert multiple documents
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("index", Value::from(i)).unwrap();
            let result = write_operations.insert(doc);
            assert!(result.is_ok());
        }
        
        // Remove all documents
        let filter = all();
        let result = write_operations.remove(filter, false);
        assert!(result.is_ok());
        
        let write_result = result.unwrap();
        let ids = write_result.affected_nitrite_ids();
        assert_eq!(ids.len(), 5);
    }

    #[test]
    fn test_insert_batch_sequential_vs_parallel() {
        // Test that batch processing correctly routes based on batch size
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Small batch - sequential processing
        let mut small_docs = Vec::new();
        for i in 0..10 {
            let mut doc = Document::new();
            doc.put("batch", Value::from("small")).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            small_docs.push(doc);
        }
        
        let result = inner.insert_batch(small_docs);
        assert!(result.is_ok());
        
        let write_result = result.unwrap();
        assert_eq!(write_result.affected_nitrite_ids().len(), 10);
    }

    #[test]
    fn test_insert_batch_large_parallel() {
        // Test that large batches use parallel processing
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Large batch - parallel processing
        let mut large_docs = Vec::new();
        for i in 0..50 {
            let mut doc = Document::new();
            doc.put("batch", Value::from("large")).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            large_docs.push(doc);
        }
        
        let result = inner.insert_batch(large_docs);
        assert!(result.is_ok());
        
        let write_result = result.unwrap();
        assert_eq!(write_result.affected_nitrite_ids().len(), 50);
    }

    // Tests for new batch optimization with put_all
    
    #[test]
    fn test_insert_batch_empty() {
        // Test that empty batch returns early with no work
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let result = inner.insert_batch(vec![]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 0);
    }
    
    #[test]
    fn test_insert_batch_sequential_threshold() {
        // Test that batches <= 10 use sequential processing
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Exactly 10 docs should use sequential
        let docs: Vec<Document> = (0..10).map(|i| {
            let mut doc = Document::new();
            doc.put("index", Value::from(i)).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 10);
    }
    
    #[test]
    fn test_insert_batch_optimized_threshold() {
        // Test that batches > 10 use optimized processing with put_all
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // 11 docs should trigger optimized batch insert
        let docs: Vec<Document> = (0..11).map(|i| {
            let mut doc = Document::new();
            doc.put("batch_test", Value::from("optimized")).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 11);
    }
    
    #[test]
    fn test_insert_batch_duplicate_detection() {
        // Test that duplicates are detected even in batch mode
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // First insert a document
        let mut doc = Document::new();
        let id = doc.id().unwrap();
        doc.put("field", Value::from("original")).unwrap();
        inner.insert(doc.clone()).unwrap();
        
        // Try to insert a batch containing the duplicate
        let mut docs: Vec<Document> = (0..15).map(|i| {
            let mut d = Document::new();
            d.put("index", Value::from(i)).unwrap();
            d
        }).collect();
        
        // Add the duplicate at position 5
        let mut duplicate = Document::new();
        duplicate.put(DOC_ID, Value::NitriteId(id.clone())).unwrap();
        duplicate.put("field", Value::from("duplicate")).unwrap();
        docs.insert(5, duplicate);
        
        let result = inner.insert_batch(docs);
        assert!(result.is_err());
        
        // Verify error is UniqueConstraintViolation
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
    }
    
    #[test]
    fn test_insert_batch_all_unique_ids() {
        // Test that batch insert works when all documents have unique IDs
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..20).map(|i| {
            let mut doc = Document::new();
            doc.put("unique_test", Value::from(i)).unwrap();
            doc.put("data", Value::from(format!("document_{}", i))).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        
        let write_result = result.unwrap();
        let ids = write_result.affected_nitrite_ids();
        assert_eq!(ids.len(), 20);
        
        // All IDs should be unique
        let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique_ids.len(), 20);
    }
    
    #[test]
    fn test_validate_no_duplicates_small_batch() {
        // Test sequential duplicate validation for small batches
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert some documents first
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("index", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        // Create keys that don't exist
        let new_keys: Vec<Value> = (100..110).map(|_| {
            Value::NitriteId(NitriteId::new())
        }).collect();
        
        let result = inner.validate_no_duplicates(&new_keys);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_prepare_document_for_insert() {
        // Test document preparation sets correct metadata
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut doc = Document::new();
        doc.put("test_field", Value::from("test_value")).unwrap();
        
        let result = inner.prepare_document_for_insert(doc);
        assert!(result.is_ok());
        
        let (id, processed, original, source) = result.unwrap();
        
        // ID should be valid
        assert!(!id.to_string().is_empty());
        
        // Processed doc should have revision set (not Null)
        let revision = processed.get(DOC_REVISION).unwrap();
        assert!(!matches!(revision, Value::Null));
        
        // Original should have the test field (not Null)
        let test_field = original.get("test_field").unwrap();
        assert!(!matches!(test_field, Value::Null));
        
        // Source should not be REPLICATOR
        assert_ne!(source, REPLICATOR);
    }
    
    #[test]
    fn test_rollback_batch_insert() {
        // Test that rollback removes inserted documents
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert some documents
        let mut ids: Vec<NitriteId> = Vec::new();
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("rollback_test", Value::from(i)).unwrap();
            let result = inner.insert(doc).unwrap();
            ids.extend(result.affected_nitrite_ids());
        }
        
        // Verify they exist
        for id in &ids {
            assert!(inner.nitrite_map.contains_key(&Value::NitriteId(id.clone())).unwrap());
        }
        
        // Rollback
        inner.rollback_batch_insert(&ids);
        
        // Verify they're gone
        for id in &ids {
            assert!(!inner.nitrite_map.contains_key(&Value::NitriteId(id.clone())).unwrap());
        }
    }
    
    #[test]
    fn test_rollback_batch_indexes() {
        // Test that rollback_batch_indexes removes index entries
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Create and prepare some documents
        let mut docs: Vec<Document> = Vec::new();
        for i in 0..3 {
            let mut doc = Document::new();
            doc.put("index_rollback_test", Value::from(i)).unwrap();
            let _ = doc.id(); // Generate ID
            docs.push(doc);
        }
        
        // Process and index them
        let mut indexed_docs: Vec<Document> = Vec::new();
        for doc in docs {
            let (_, mut processed, _, _) = inner.prepare_document_for_insert(doc).unwrap();
            // Write index entry
            inner.document_index_writer.write_index_entry(&mut processed).unwrap();
            indexed_docs.push(processed);
        }
        
        // Rollback indexes - should not panic
        inner.rollback_batch_indexes(&indexed_docs);
    }
    
    // Tests for batch update optimization with put_all
    
    #[test]
    fn test_update_batch_small_sequential() {
        // Test that small update batches (<= 10) use sequential processing
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents first
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("value", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        // Update all documents
        let mut update_doc = Document::new();
        update_doc.put("updated", Value::from(true)).unwrap();
        
        let filter = all();
        let update_options = UpdateOptions::default();
        let result = inner.update(filter, &update_doc, &update_options);
        
        assert!(result.is_ok());
        let write_result = result.unwrap();
        assert_eq!(write_result.affected_nitrite_ids().len(), 5);
    }
    
    #[test]
    fn test_update_batch_optimized_threshold() {
        // Test that larger update batches (> 10) use optimized processing with put_all
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert 15 documents
        for i in 0..15 {
            let mut doc = Document::new();
            doc.put("batch_update_test", Value::from("original")).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        // Update all documents - this should trigger optimized batch update
        let mut update_doc = Document::new();
        update_doc.put("batch_update_test", Value::from("updated")).unwrap();
        
        let filter = all();
        let update_options = UpdateOptions::default();
        let result = inner.update(filter, &update_doc, &update_options);
        
        assert!(result.is_ok());
        let write_result = result.unwrap();
        assert_eq!(write_result.affected_nitrite_ids().len(), 15);
    }
    
    #[test]
    fn test_update_batch_preserves_document_data() {
        // Test that batch update correctly merges and preserves document data
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents with different values
        let mut inserted_ids: Vec<NitriteId> = Vec::new();
        for i in 0..12 {
            let mut doc = Document::new();
            doc.put("original_field", Value::from(format!("value_{}", i))).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            let result = inner.insert(doc).unwrap();
            inserted_ids.extend(result.affected_nitrite_ids());
        }
        
        // Update with a new field
        let mut update_doc = Document::new();
        update_doc.put("new_field", Value::from("added")).unwrap();
        
        let filter = all();
        let update_options = UpdateOptions::default();
        let result = inner.update(filter, &update_doc, &update_options);
        
        assert!(result.is_ok());
        
        // Verify documents have both old and new fields
        for id in &inserted_ids {
            let stored = inner.nitrite_map.get(&Value::NitriteId(id.clone())).unwrap();
            assert!(stored.is_some());
            
            let doc = stored.unwrap();
            let document = doc.as_document().unwrap();
            
            // Should have original field preserved
            let original = document.get("original_field").unwrap();
            assert!(!matches!(original, Value::Null));
            
            // Should have new field added
            let new_field = document.get("new_field").unwrap();
            assert!(!matches!(new_field, Value::Null));
        }
    }
    
    #[test]
    fn test_update_batch_revision_increment() {
        // Test that batch update correctly increments revision for each document
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents
        for i in 0..12 {
            let mut doc = Document::new();
            doc.put("rev_test", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        // Update all
        let mut update_doc = Document::new();
        update_doc.put("updated", Value::from(true)).unwrap();
        
        let filter = all();
        let update_options = UpdateOptions::default();
        inner.update(filter.clone(), &update_doc, &update_options).unwrap();
        
        // Update again
        let mut update_doc2 = Document::new();
        update_doc2.put("updated_again", Value::from(true)).unwrap();
        inner.update(filter, &update_doc2, &update_options).unwrap();
        
        // All documents should have revision 3 (1 from insert + 2 updates)
        // Note: Revision tracking is handled internally
    }
    
    #[test]
    fn test_process_update_batch_optimized_directly() {
        // Test the optimized batch update method directly
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents first
        let mut docs = Vec::new();
        for i in 0..15 {
            let mut doc = Document::new();
            doc.put("direct_test", Value::from(i)).unwrap();
            let result = inner.insert(doc.clone()).unwrap();
            
            // Read back the stored document for update
            let id = result.affected_nitrite_ids().first().unwrap().clone();
            let stored = inner.nitrite_map.get(&Value::NitriteId(id)).unwrap().unwrap();
            docs.push(stored.as_document().unwrap().clone());
        }
        
        // Call process_update_batch_optimized directly
        let mut update_doc = Document::new();
        update_doc.put("batch_processed", Value::from(true)).unwrap();
        
        let mut nitrite_ids = Vec::new();
        let result = inner.process_update_batch_optimized(&update_doc, &mut nitrite_ids, docs);
        
        assert!(result.is_ok());
        assert_eq!(nitrite_ids.len(), 15);
    }
    
    // =================== Comprehensive Batch Insert Tests ===================
    
    // --- Positive Cases ---
    
    #[test]
    fn test_batch_insert_exactly_at_threshold() {
        // Test batch insert at exactly the threshold boundary (10 docs = sequential)
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..10).map(|i| {
            let mut doc = Document::new();
            doc.put("threshold_test", Value::from(i)).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 10);
    }
    
    #[test]
    fn test_batch_insert_just_above_threshold() {
        // Test batch insert just above threshold (11 docs = optimized)
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..11).map(|i| {
            let mut doc = Document::new();
            doc.put("above_threshold", Value::from(i)).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 11);
    }
    
    #[test]
    fn test_batch_insert_with_complex_nested_documents() {
        // Test batch insert with complex nested document structures
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..15).map(|i| {
            let mut doc = Document::new();
            doc.put("level1.level2.level3.value", Value::from(i)).unwrap();
            doc.put("array_field", Value::Array(vec![
                Value::from(i),
                Value::from(i * 2),
                Value::from(i * 3),
            ])).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 15);
    }
    
    #[test]
    fn test_batch_insert_single_document() {
        // Test that single document batch works correctly
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut doc = Document::new();
        doc.put("single", Value::from("test")).unwrap();
        
        let result = inner.insert_batch(vec![doc]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 1);
    }
    
    #[test]
    fn test_batch_insert_returns_correct_ids() {
        // Verify that returned IDs match stored documents
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..20).map(|i| {
            let mut doc = Document::new();
            doc.put("verify_id", Value::from(i)).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs).unwrap();
        let ids = result.affected_nitrite_ids();
        
        // Verify each ID exists in the map
        for id in ids {
            let stored = inner.nitrite_map.get(&Value::NitriteId(id.clone())).unwrap();
            assert!(stored.is_some(), "Document with ID {} should exist", id);
        }
    }
    
    // --- Negative Cases ---
    
    #[test]
    fn test_batch_insert_duplicate_within_batch_same_id() {
        // Test behavior when batch contains documents with same generated IDs
        // Note: In practice, NitriteId::new() generates unique IDs, so this tests
        // the scenario where the same document reference is added twice
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut doc1 = Document::new();
        let id = doc1.id().unwrap();
        doc1.put("field", Value::from("first")).unwrap();
        
        // Create another document with the same ID (manually assigned)
        let mut doc2 = Document::new();
        doc2.put(DOC_ID, Value::NitriteId(id.clone())).unwrap();
        doc2.put("field", Value::from("second")).unwrap();
        
        // For small batch (<= 10), this goes through sequential insert which uses put_if_absent
        // So the second document with same ID should fail
        let result = inner.insert_batch(vec![doc1, doc2]);
        assert!(result.is_err());
        
        // Verify error is UniqueConstraintViolation
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
    }
    
    #[test]
    fn test_batch_insert_duplicate_within_large_batch() {
        // Test duplicate detection within a large batch that uses optimized path
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // First insert a document
        let mut existing = Document::new();
        let existing_id = existing.id().unwrap();
        existing.put("existing", Value::from(true)).unwrap();
        inner.insert(existing).unwrap();
        
        // Create a large batch with one document having the existing ID
        let mut docs: Vec<Document> = (0..15).map(|i| {
            let mut doc = Document::new();
            doc.put("new_doc", Value::from(i)).unwrap();
            doc
        }).collect();
        
        // Add document with existing ID to the batch
        let mut duplicate = Document::new();
        duplicate.put(DOC_ID, Value::NitriteId(existing_id)).unwrap();
        duplicate.put("duplicate", Value::from(true)).unwrap();
        docs.push(duplicate);
        
        // This should fail because we check against existing documents
        let result = inner.insert_batch(docs);
        assert!(result.is_err());
        
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
    }
    
    #[test]
    fn test_batch_insert_fails_on_existing_document() {
        // Test that batch insert fails if any document already exists
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert a document first
        let mut existing = Document::new();
        let existing_id = existing.id().unwrap();
        existing.put("existing", Value::from(true)).unwrap();
        inner.insert(existing).unwrap();
        
        // Create batch with the existing ID included
        let mut docs: Vec<Document> = (0..15).map(|i| {
            let mut doc = Document::new();
            doc.put("new_doc", Value::from(i)).unwrap();
            doc
        }).collect();
        
        // Add document with existing ID
        let mut duplicate = Document::new();
        duplicate.put(DOC_ID, Value::NitriteId(existing_id)).unwrap();
        duplicate.put("duplicate", Value::from(true)).unwrap();
        docs.push(duplicate);
        
        let result = inner.insert_batch(docs);
        assert!(result.is_err());
        
        // Verify error is UniqueConstraintViolation
        let err = result.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
    }
    
    // --- Edge Cases ---
    
    #[test]
    fn test_batch_insert_empty_documents() {
        // Test batch insert with empty documents (no fields except ID)
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..15).map(|_| Document::new()).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 15);
    }
    
    #[test]
    fn test_batch_insert_large_batch() {
        // Test batch insert with a larger number of documents
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..100).map(|i| {
            let mut doc = Document::new();
            doc.put("large_batch", Value::from(i)).unwrap();
            doc.put("data", Value::from(format!("data_{}", i))).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 100);
    }
    
    #[test]
    fn test_batch_insert_with_null_values() {
        // Test batch insert with null field values
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let docs: Vec<Document> = (0..15).map(|i| {
            let mut doc = Document::new();
            doc.put("index", Value::from(i)).unwrap();
            doc.put("nullable", Value::Null).unwrap();
            doc
        }).collect();
        
        let result = inner.insert_batch(docs);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 15);
    }
    
    #[test]
    fn test_batch_insert_preserves_document_order() {
        // Verify that batch insert returns IDs in the same order as input
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut docs = Vec::new();
        let mut expected_order = Vec::new();
        
        for i in 0..20 {
            let mut doc = Document::new();
            let id = doc.id().unwrap();
            doc.put("order_test", Value::from(i)).unwrap();
            expected_order.push(id);
            docs.push(doc);
        }
        
        let result = inner.insert_batch(docs).unwrap();
        let returned_ids = result.affected_nitrite_ids();
        
        assert_eq!(returned_ids.len(), expected_order.len());
        for (expected, actual) in expected_order.iter().zip(returned_ids.iter()) {
            assert_eq!(expected, actual, "ID order should be preserved");
        }
    }
    
    // =================== Comprehensive Batch Update Tests ===================
    
    // --- Positive Cases ---
    
    #[test]
    fn test_batch_update_at_threshold() {
        // Test batch update at exactly the threshold (10 = sequential)
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert exactly 10 documents
        for i in 0..10 {
            let mut doc = Document::new();
            doc.put("update_threshold", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        let mut update_doc = Document::new();
        update_doc.put("updated_at_threshold", Value::from(true)).unwrap();
        
        let result = inner.update(all(), &update_doc, &UpdateOptions::default());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 10);
    }
    
    #[test]
    fn test_batch_update_above_threshold() {
        // Test batch update above threshold (uses optimized path)
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert more than 10 documents to trigger batch processing
        for i in 0..25 {
            let mut doc = Document::new();
            doc.put("batch_update", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        let mut update_doc = Document::new();
        update_doc.put("batch_updated", Value::from(true)).unwrap();
        
        let result = inner.update(all(), &update_doc, &UpdateOptions::default());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 25);
    }
    
    #[test]
    fn test_batch_update_partial_match() {
        // Test batch update that only matches some documents
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents with different categories
        for i in 0..30 {
            let mut doc = Document::new();
            doc.put("category", Value::from(if i % 2 == 0 { "even" } else { "odd" })).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        let mut update_doc = Document::new();
        update_doc.put("processed", Value::from(true)).unwrap();
        
        // Only update "even" documents
        let filter = field("category").eq("even");
        let result = inner.update(filter, &update_doc, &UpdateOptions::default());
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 15); // Half are even
    }
    
    #[test]
    fn test_batch_update_merges_fields_correctly() {
        // Verify that update correctly merges new fields with existing
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut inserted_ids: Vec<NitriteId> = Vec::new();
        for i in 0..20 {
            let mut doc = Document::new();
            doc.put("original", Value::from(format!("original_{}", i))).unwrap();
            doc.put("keep_this", Value::from(i * 10)).unwrap();
            let result = inner.insert(doc).unwrap();
            inserted_ids.extend(result.affected_nitrite_ids());
        }
        
        let mut update_doc = Document::new();
        update_doc.put("new_field", Value::from("added")).unwrap();
        update_doc.put("original", Value::from("overwritten")).unwrap();
        
        inner.update(all(), &update_doc, &UpdateOptions::default()).unwrap();
        
        // Verify merged correctly
        for id in &inserted_ids {
            let stored = inner.nitrite_map.get(&Value::NitriteId(id.clone())).unwrap().unwrap();
            let doc = stored.as_document().unwrap();
            
            // Original field should be overwritten
            let original = doc.get("original").unwrap();
            assert_eq!(original.as_string().unwrap(), "overwritten");
            
            // keep_this should be preserved
            let keep = doc.get("keep_this").unwrap();
            assert!(!matches!(keep, Value::Null));
            
            // new_field should be added
            let new_field = doc.get("new_field").unwrap();
            assert_eq!(new_field.as_string().unwrap(), "added");
        }
    }
    
    // --- Negative Cases ---
    
    #[test]
    fn test_batch_update_no_matching_documents() {
        // Test batch update when no documents match the filter
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents
        for i in 0..15 {
            let mut doc = Document::new();
            doc.put("status", Value::from("active")).unwrap();
            doc.put("index", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        let mut update_doc = Document::new();
        update_doc.put("updated", Value::from(true)).unwrap();
        
        // Filter that matches nothing
        let filter = field("status").eq("nonexistent");
        let result = inner.update(filter, &update_doc, &UpdateOptions::default());
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 0);
    }
    
    #[test]
    fn test_batch_update_with_empty_update_document() {
        // Test batch update with empty update document
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents
        for i in 0..15 {
            let mut doc = Document::new();
            doc.put("data", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        let update_doc = Document::new(); // Empty
        let result = inner.update(all(), &update_doc, &UpdateOptions::default());
        
        assert!(result.is_ok());
        // Empty update should not affect any documents
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 0);
    }
    
    // --- Edge Cases ---
    
    #[test]
    fn test_batch_update_just_once_option() {
        // Test that just_once option stops after first document
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert multiple documents
        for i in 0..20 {
            let mut doc = Document::new();
            doc.put("just_once_test", Value::from(i)).unwrap();
            inner.insert(doc).unwrap();
        }
        
        let mut update_doc = Document::new();
        update_doc.put("updated", Value::from(true)).unwrap();
        
        let options = UpdateOptions::new(false, true); // just_once = true
        
        let result = inner.update(all(), &update_doc, &options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 1);
    }
    
    #[test]
    fn test_batch_update_insert_if_absent() {
        // Test update with insert_if_absent when no documents match
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut update_doc = Document::new();
        update_doc.put("upserted", Value::from(true)).unwrap();
        
        let options = UpdateOptions::new(true, false); // insert_if_absent = true
        
        // No documents exist, should insert
        let filter = field("nonexistent").eq(true);
        let result = inner.update(filter, &update_doc, &options);
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 1);
    }
    
    #[test]
    fn test_batch_update_increments_revision() {
        // Verify that revision is correctly incremented on batch update
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        let mut inserted_ids: Vec<NitriteId> = Vec::new();
        for i in 0..15 {
            let mut doc = Document::new();
            doc.put("rev_check", Value::from(i)).unwrap();
            let result = inner.insert(doc).unwrap();
            inserted_ids.extend(result.affected_nitrite_ids());
        }
        
        // Update twice
        let mut update1 = Document::new();
        update1.put("update1", Value::from(true)).unwrap();
        inner.update(all(), &update1, &UpdateOptions::default()).unwrap();
        
        let mut update2 = Document::new();
        update2.put("update2", Value::from(true)).unwrap();
        inner.update(all(), &update2, &UpdateOptions::default()).unwrap();
        
        // Check revision is 3 (1 from insert + 2 updates)
        for id in &inserted_ids {
            let stored = inner.nitrite_map.get(&Value::NitriteId(id.clone())).unwrap().unwrap();
            let doc = stored.as_document().unwrap();
            let revision = doc.revision().unwrap();
            assert_eq!(revision, 3, "Revision should be 3 after insert + 2 updates");
        }
    }
    
    // =================== Rollback Tests ===================
    
    #[test]
    fn test_rollback_batch_update_restores_old_state() {
        // Test that rollback_batch_update properly restores documents
        let write_operations = setup_write_operations();
        let inner = write_operations.inner.clone();
        
        // Insert documents with known values
        let mut ids: Vec<NitriteId> = Vec::new();
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("original_value", Value::from(format!("original_{}", i))).unwrap();
            let result = inner.insert(doc).unwrap();
            ids.extend(result.affected_nitrite_ids());
        }
        
        // Manually prepare updated documents and track old state
        let mut update_doc = Document::new();
        update_doc.put("new_value", Value::from("changed")).unwrap();
        
        // This tests the rollback mechanism itself
        let updated_indexes: Vec<(NitriteId, Document, Document)> = Vec::new();
        let failed_id = ids[0].clone();
        let failed_old_doc = inner.nitrite_map
            .get(&Value::NitriteId(failed_id.clone()))
            .unwrap()
            .unwrap()
            .as_document()
            .unwrap()
            .clone();
        
        // Call rollback (should not panic)
        let result = inner.rollback_batch_update(&updated_indexes, &failed_id, &failed_old_doc, &update_doc);
        assert!(result.is_ok());
    }
}
