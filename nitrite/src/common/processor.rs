use crate::collection::Document;
use crate::common::persistent_collection::PersistentCollection;
use crate::errors::NitriteResult;
use dashmap::DashMap;
use std::sync::Arc;

/// Contract for implementing document processors.
///
/// # Purpose
/// Defines the interface for processors that transform documents at specific points in the
/// document lifecycle. Processors allow applications to intercept, validate, or modify
/// documents before they are persisted to the database or after they are retrieved from storage.
///
/// # Trait Methods
/// - `name()`: Returns a unique identifier for the processor
/// - `process_before_write()`: Transforms document before database persistence
/// - `process_after_read()`: Transforms document after database retrieval
///
/// # Thread Safety
/// Implementations must be `Send + Sync` to work with the concurrent processor chain.
///
/// # Usage
/// Implementations transform documents by modifying fields. For example, a processor might
/// add a processed flag before writing or remove internal fields after reading.
pub trait ProcessorProvider: Send + Sync {
    /// Returns the unique name of this processor.
    ///
    /// # Returns
    /// A string identifier used to identify and remove this processor from chains.
    fn name(&self) -> String;

    /// Processes a document before it is written to the database.
    ///
    /// # Arguments
    /// * `doc` - The document to process before persistence.
    ///
    /// # Returns
    /// `Ok(Document)` with the processed document, or an error if processing fails.
    ///
    /// # Behavior
    /// Called immediately before a document is written to persistent storage. Can modify,
    /// validate, or enrich the document. If this method returns an error, the write operation fails.
    fn process_before_write(&self, doc: Document) -> NitriteResult<Document>;

    /// Processes a document after it is read from the database.
    ///
    /// # Arguments
    /// * `doc` - The document retrieved from persistent storage.
    ///
    /// # Returns
    /// `Ok(Document)` with the processed document, or an error if processing fails.
    ///
    /// # Behavior
    /// Called immediately after a document is retrieved from persistent storage. Can transform,
    /// filter, or decode the document. If this method returns an error, the read operation fails.
    fn process_after_read(&self, doc: Document) -> NitriteResult<Document>;
}

/// Wraps a document processor implementation.
///
/// # Purpose
/// Provides a type-erased, cloneable wrapper around any `ProcessorProvider` implementation.
/// Uses `Arc` for efficient reference-counted sharing and polymorphic dispatch.
///
/// # Characteristics
/// - **Type-erased**: Works with any `ProcessorProvider` implementation
/// - **Cloneable**: Can be cloned to share across multiple processor chains
/// - **Thread-safe**: Arc enables safe concurrent access
/// - **Zero-cost delegation**: Transparent forwarding to inner implementation
///
/// # Usage
/// Created from a concrete processor implementation via `Processor::new()`, then
/// added to a `ProcessorChain` to participate in document transformation pipelines.
#[derive(Clone)]
pub struct Processor {
    inner: Arc<dyn ProcessorProvider>,
}

impl Processor {
    /// Creates a new processor from an implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete `ProcessorProvider` implementation.
    ///
    /// # Returns
    /// A new `Processor` wrapping the implementation in an `Arc`.
    ///
    /// # Behavior
    /// Wraps the provided processor implementation, making it suitable for use in
    /// processor chains. The inner implementation is shared via Arc, allowing efficient
    /// cloning and concurrent access.
    ///
    /// # Example
    /// A processor that adds a flag before writing and removes it after reading:
    /// ```text
    /// struct MyProcessor;
    ///
    /// impl ProcessorProvider for MyProcessor {
    ///     fn name(&self) -> String {
    ///         "MyProcessor".to_string()
    ///     }
    ///
    ///     fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
    ///         let mut doc = doc.clone();
    ///         doc.put("processed", true)?;
    ///         Ok(doc)
    ///     }
    ///
    ///     fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
    ///         let mut doc = doc.clone();
    ///         doc.remove("processed")?;
    ///         Ok(doc)
    ///     }
    /// }
    ///
    /// let processor = Processor::new(MyProcessor);
    /// ```
    pub fn new<T: ProcessorProvider + 'static>(inner: T) -> Self {
        Processor { inner: Arc::new(inner) }
    }

    /// Returns the processor's name.
    ///
    /// # Returns
    /// The unique identifier string for this processor.
    ///
    /// # Behavior
    /// Delegates to the inner processor's `name()` method.
    pub fn name(&self) -> String {
        self.inner.name()
    }

    /// Processes a document before it is written to the database.
    ///
    /// # Arguments
    /// * `doc` - The document to process before persistence.
    ///
    /// # Returns
    /// `Ok(Document)` with the processed document, or an error if processing fails.
    ///
    /// # Behavior
    /// Delegates to the inner processor's `process_before_write()` method.
    /// Called by processor chains during write operations.
    pub fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
        self.inner.process_before_write(doc)
    }

    /// Processes a document after it is read from the database.
    ///
    /// # Arguments
    /// * `doc` - The document retrieved from persistent storage.
    ///
    /// # Returns
    /// `Ok(Document)` with the processed document, or an error if processing fails.
    ///
    /// # Behavior
    /// Delegates to the inner processor's `process_after_read()` method.
    /// Called by processor chains during read operations.
    pub fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
        self.inner.process_after_read(doc)
    }
}

/// Manages multiple document processors in a processing pipeline.
///
/// This struct maintains an ordered chain of processors that are applied
/// sequentially to documents. Processors are identified by name and can be
/// added or removed dynamically. The chain applies all registered processors
/// in order when processing documents before writing or after reading.
///
/// # Responsibilities
///
/// * **Processor Management**: Adds and removes processors from the chain
/// * **Before-Write Processing**: Applies processors to documents before database writes
/// * **After-Read Processing**: Applies processors to documents after database reads
/// * **Processor Lookup**: Identifies processors by name to enable removal
/// * **Optimization**: Fast path for empty processor chains with no overhead
#[derive(Clone)]
pub struct ProcessorChain {
    inner: Arc<ProcessorChainInner>,
}

impl ProcessorChain {
    /// Creates a new empty processor chain.
    ///
    /// # Returns
    /// A new `ProcessorChain` with no processors registered.
    ///
    /// # Behavior
    /// Initializes an empty processor chain. Processors must be added via `add_processor()`
    /// for the chain to perform any transformations. A chain with no processors has negligible
    /// overhead when processing documents.
    pub fn new() -> Self {
        ProcessorChain {
            inner: Arc::new(ProcessorChainInner::new()),
        }
    }

    /// Adds a processor to the end of the chain.
    ///
    /// # Arguments
    /// * `processor` - The processor to add to the chain.
    ///
    /// # Returns
    /// Nothing. Processors are added directly without validation.
    ///
    /// # Behavior
    /// Inserts the processor into the chain by its name. If a processor with the same name
    /// already exists, it is replaced. Processors are applied in insertion order during
    /// document transformation.
    pub fn add_processor(&self, processor: Processor) {
        self.inner.add_processor(processor);
    }

    /// Removes a processor from the chain by name.
    ///
    /// # Arguments
    /// * `processor_name` - The name of the processor to remove (must match `processor.name()`).
    ///
    /// # Returns
    /// Nothing. No error if the processor doesn't exist.
    ///
    /// # Behavior
    /// Removes the processor with the matching name from the chain. Silently succeeds
    /// if no processor with that name is found. Other processors continue applying in order.
    pub fn remove_processor(&self, processor_name: &str) {
        self.inner.remove_processor(processor_name);
    }

    /// Applies all processors in the chain to a document before writing.
    ///
    /// # Arguments
    /// * `doc` - The document to process before persistence.
    ///
    /// # Returns
    /// `Ok(Document)` with the processed document after all processors have been applied,
    /// or an error if any processor fails.
    ///
    /// # Behavior
    /// Sequentially applies each processor's `process_before_write()` method to the document.
    /// If any processor returns an error, the chain stops and returns that error immediately.
    /// If no processors are registered, returns the document unchanged.
    pub fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
        self.inner.process_before_write(doc)
    }

    /// Applies all processors in the chain to a document after reading.
    ///
    /// # Arguments
    /// * `doc` - The document retrieved from persistent storage.
    ///
    /// # Returns
    /// `Ok(Document)` with the processed document after all processors have been applied,
    /// or an error if any processor fails.
    ///
    /// # Behavior
    /// Sequentially applies each processor's `process_after_read()` method to the document.
    /// If any processor returns an error, the chain stops and returns that error immediately.
    /// If no processors are registered, returns the document unchanged.
    pub fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
        self.inner.process_after_read(doc)
    }
}

impl ProcessorProvider for ProcessorChain {
    fn name(&self) -> String {
        "ProcessorChain".to_string()
    }

    fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
        self.inner.process_before_write(doc)
    }

    fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
        self.inner.process_after_read(doc)
    }
}

/// Inner implementation of the processor chain.
///
/// # Purpose
/// PIMPL (Pointer to Implementation) pattern for `ProcessorChain`. Provides the actual
/// processor management and transformation logic behind the public API.
///
/// # Characteristics
/// - **Thread-safe**: Uses `DashMap` for lock-free concurrent access
/// - **Fast-path optimization**: Empty chains return documents unchanged with no overhead
/// - **Sequential processing**: Applies processors in order, stopping on first error
/// - **Efficient replacement**: Adding a processor with existing name replaces the old one
pub(crate) struct ProcessorChainInner {
    processors: DashMap<String, Processor>,
}

impl ProcessorChainInner {
    fn new() -> Self {
        Self {
            processors: DashMap::new(),
        }
    }

    #[inline]
    fn add_processor(&self, processor: Processor) {
        self.processors.insert(processor.name(), processor);
    }

    #[inline]
    fn remove_processor(&self, processor_name: &str) {
        self.processors.remove(processor_name);
    }

    #[inline]
    fn name(&self) -> String {
        "ProcessorChain".to_string()
    }

    #[inline]
    fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
        // Fast path: no processors, return early
        if self.processors.is_empty() {
            return Ok(doc);
        }
        
        let mut processed_doc = doc.clone();
        for processor in self.processors.iter() {
            processed_doc = processor.process_before_write(processed_doc)?;
        }
        Ok(processed_doc)
    }

    #[inline]
    fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
        // Fast path: no processors, return early
        if self.processors.is_empty() {
            return Ok(doc);
        }
        
        let mut processed_doc = doc.clone();
        for processor in self.processors.iter() {
            processed_doc = processor.process_after_read(processed_doc)?;
        }
        Ok(processed_doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::errors::{ErrorKind, NitriteError};

    struct MockProcessor;

    impl ProcessorProvider for MockProcessor {
        fn name(&self) -> String {
            "MockProcessor".to_string()
        }

        fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
            let mut new_doc = doc.clone();
            new_doc.put("processed", "before_write")?;
            Ok(new_doc)
        }

        fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
            let mut new_doc = doc.clone();
            new_doc.put("processed", "after_read")?;
            Ok(new_doc)
        }
    }

    #[test]
    fn test_processor_new() {
        let processor = Processor::new(MockProcessor);
        assert_eq!(processor.name(), "MockProcessor");
    }

    #[test]
    fn test_processor_process_before_write() {
        let processor = Processor::new(MockProcessor);
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let processed_doc = processor.process_before_write(doc).unwrap();
        assert_eq!(processed_doc.get("processed").unwrap(), "before_write".into());
    }

    #[test]
    fn test_processor_process_after_read() {
        let processor = Processor::new(MockProcessor);
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let processed_doc = processor.process_after_read(doc).unwrap();
        assert_eq!(processed_doc.get("processed").unwrap(), "after_read".into());
    }

    #[test]
    fn test_processor_chain_new() {
        let processor_chain = ProcessorChain::new();
        assert_eq!(processor_chain.name(), "ProcessorChain");
    }

    #[test]
    fn test_processor_chain_add_processor() {
        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(MockProcessor);
        processor_chain.add_processor(processor.clone());
        assert_eq!(processor_chain.inner.processors.len(), 1);
    }

    #[test]
    fn test_processor_chain_remove_processor() {
        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(MockProcessor);
        processor_chain.add_processor(processor.clone());
        processor_chain.remove_processor("MockProcessor");
        assert_eq!(processor_chain.inner.processors.len(), 0);
    }

    #[test]
    fn test_processor_chain_process_before_write() {
        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(MockProcessor);
        processor_chain.add_processor(processor.clone());
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let processed_doc = processor_chain.process_before_write(doc).unwrap();
        assert_eq!(processed_doc.get("processed").unwrap(), "before_write".into());
    }

    #[test]
    fn test_processor_chain_process_after_read() {
        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(MockProcessor);
        processor_chain.add_processor(processor.clone());
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let processed_doc = processor_chain.process_after_read(doc).unwrap();
        assert_eq!(processed_doc.get("processed").unwrap(), "after_read".into());
    }

    #[test]
    fn test_processor_chain_process_before_write_no_processors() {
        let processor_chain = ProcessorChain::new();
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let processed_doc = processor_chain.process_before_write(doc.clone()).unwrap();
        assert_eq!(processed_doc, doc);
    }

    #[test]
    fn test_processor_chain_process_after_read_no_processors() {
        let processor_chain = ProcessorChain::new();
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let processed_doc = processor_chain.process_after_read(doc.clone()).unwrap();
        assert_eq!(processed_doc, doc);
    }

    #[test]
    fn test_processor_chain_process_before_write_error() {
        struct ErrorProcessor;

        impl ProcessorProvider for ErrorProcessor {
            fn name(&self) -> String {
                "ErrorProcessor".to_string()
            }

            fn process_before_write(&self, _doc: Document) -> NitriteResult<Document> {
                Err(NitriteError::new("Error in process_before_write", ErrorKind::IOError))
            }

            fn process_after_read(&self, _doc: Document) -> NitriteResult<Document> {
                Ok(Document::new())
            }
        }

        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(ErrorProcessor);
        processor_chain.add_processor(processor.clone());
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let result = processor_chain.process_before_write(doc);
        assert!(result.is_err());
    }

    #[test]
    fn test_processor_chain_process_after_read_error() {
        struct ErrorProcessor;

        impl ProcessorProvider for ErrorProcessor {
            fn name(&self) -> String {
                "ErrorProcessor".to_string()
            }

            fn process_before_write(&self, _doc: Document) -> NitriteResult<Document> {
                Ok(Document::new())
            }

            fn process_after_read(&self, _doc: Document) -> NitriteResult<Document> {
                Err(NitriteError::new("Error in process_after_read", ErrorKind::IOError))
            }
        }

        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(ErrorProcessor);
        processor_chain.add_processor(processor.clone());
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        let result = processor_chain.process_after_read(doc);
        assert!(result.is_err());
    }

    #[test]
    fn test_processor_chain_add_duplicate_processor() {
        let processor_chain = ProcessorChain::new();
        let processor = Processor::new(MockProcessor);
        processor_chain.add_processor(processor.clone());
        processor_chain.add_processor(processor.clone());
        assert_eq!(processor_chain.inner.processors.len(), 1);
    }

    #[test]
    fn test_processor_chain_remove_nonexistent_processor() {
        let processor_chain = ProcessorChain::new();
        processor_chain.remove_processor("NonExistentProcessor");
        assert_eq!(processor_chain.inner.processors.len(), 0);
    }

    #[test]
    fn bench_processor_chain_creation() {
        for _ in 0..1000 {
            let _ = ProcessorChain::new();
        }
    }

    #[test]
    fn bench_processor_chain_add_remove() {
        let chain = ProcessorChain::new();
        let processor = Processor::new(MockProcessor);
        
        for _ in 0..100 {
            chain.add_processor(processor.clone());
            chain.remove_processor("MockProcessor");
        }
    }

    #[test]
    fn bench_processor_chain_process_no_processors() {
        let chain = ProcessorChain::new();
        let mut doc = Document::new();
        doc.put("key", "value").unwrap();
        
        for _ in 0..500 {
            let _ = chain.process_before_write(doc.clone());
            let _ = chain.process_after_read(doc.clone());
        }
    }
}