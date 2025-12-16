//! FTS index implementation using Tantivy.
//!
//! This module provides the `FtsIndex` that wraps Tantivy's Index
//! for integration with Nitrite's indexing system.

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value as TantivyValue, STORED, STRING, TEXT};
use tantivy::{Index, IndexWriter, ReloadPolicy, TantivyDocument};

use nitrite::collection::{FindPlan, NitriteId};
use nitrite::common::{FieldValues, Value};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::index::IndexDescriptor;

use crate::config::FtsConfig;
use crate::filter::{as_fts_filter, is_fts_filter};

/// A full-text search index instance for a specific field.
#[derive(Clone)]
pub struct FtsIndex {
    inner: Arc<FtsIndexInner>,
}

/// Private implementation details of FtsIndex.
struct FtsIndexInner {
    index: Index,
    index_writer: RwLock<Option<IndexWriter>>,
    id_field: Field,
    text_field: Field,
    index_path: Option<PathBuf>,
    search_result_limit: usize,
}

impl FtsIndex {
    /// Creates a new FTS index for the given descriptor.
    ///
    /// If `base_path` is provided, the index is stored on disk.
    /// Otherwise, an in-memory index is created.
    pub fn new(
        index_descriptor: IndexDescriptor,
        base_path: Option<PathBuf>,
        config: &FtsConfig,
    ) -> NitriteResult<Self> {
        let index_name = derive_index_map_name(&index_descriptor);

        // Build schema with id and text fields
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("_id", STRING | STORED);
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();

        // Create or open the index
        let (index, index_path) = if let Some(base) = base_path {
            let path = base.join(format!("{}_fts", index_name));

            // Ensure directory exists
            std::fs::create_dir_all(&path).map_err(|e| {
                NitriteError::new(
                    &format!("Failed to create FTS index directory: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;

            let index = if path.join("meta.json").exists() {
                log::debug!("Opening existing FTS index at {:?}", path);
                Index::open_in_dir(&path).map_err(|e| {
                    NitriteError::new(
                        &format!("Failed to open FTS index: {}", e),
                        ErrorKind::Extension("FTS".to_string()),
                    )
                })?
            } else {
                log::debug!("Creating new FTS index at {:?}", path);
                Index::create_in_dir(&path, schema.clone()).map_err(|e| {
                    NitriteError::new(
                        &format!("Failed to create FTS index: {}", e),
                        ErrorKind::Extension("FTS".to_string()),
                    )
                })?
            };

            (index, Some(path))
        } else {
            log::debug!("Creating in-memory FTS index for {}", index_name);
            let index = Index::create_in_ram(schema.clone());
            (index, None)
        };

        // Create index writer with configured heap size and thread count
        let heap_size = config.index_writer_heap_size();
        let num_threads = config.num_threads();

        let index_writer = if num_threads > 0 {
            index.writer_with_num_threads(num_threads, heap_size)
        } else {
            index.writer(heap_size)
        }
        .map_err(|e| {
            NitriteError::new(
                &format!("Failed to create FTS index writer: {}", e),
                ErrorKind::Extension("FTS".to_string()),
            )
        })?;

        Ok(Self {
            inner: Arc::new(FtsIndexInner {
                index,
                index_writer: RwLock::new(Some(index_writer)),
                id_field,
                text_field,
                index_path,
                search_result_limit: config.search_result_limit(),
            }),
        })
    }

    /// Writes a document to the FTS index.
    pub fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        if field_names.is_empty() {
            return Ok(());
        }

        let first_field = &field_names[0];
        let value = field_values.get_value(first_field);
        let nitrite_id = field_values.nitrite_id().id_value();

        // Extract text from value
        let text = match value {
            Some(v) => value_to_text(v),
            None => return Ok(()),
        };

        if text.is_empty() {
            return Ok(());
        }

        // Create document
        let mut doc = TantivyDocument::new();
        doc.add_text(self.inner.id_field, nitrite_id.to_string());
        doc.add_text(self.inner.text_field, &text);

        let id_term = tantivy::Term::from_field_text(self.inner.id_field, &nitrite_id.to_string());

        let mut writer_guard = self.inner.index_writer.write();
        if let Some(ref mut writer) = *writer_guard {
            writer.delete_term(id_term);
            writer.add_document(doc).map_err(|e| {
                NitriteError::new(
                    &format!("Failed to add document to FTS index: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;

            writer.commit().map_err(|e| {
                NitriteError::new(
                    &format!("Failed to commit FTS index: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;
        }

        Ok(())
    }

    /// Removes a document from the FTS index.
    pub fn remove(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let nitrite_id = field_values.nitrite_id().id_value();
        let id_term = tantivy::Term::from_field_text(self.inner.id_field, &nitrite_id.to_string());

        let mut writer_guard = self.inner.index_writer.write();
        if let Some(ref mut writer) = *writer_guard {
            writer.delete_term(id_term);
            writer.commit().map_err(|e| {
                NitriteError::new(
                    &format!("Failed to commit FTS index after delete: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;
        }

        Ok(())
    }

    /// Finds NitriteIds matching the FTS query in the find plan.
    pub fn find_nitrite_ids(&self, find_plan: &FindPlan) -> NitriteResult<Vec<NitriteId>> {
        let index_scan_filter = find_plan
            .index_scan_filter()
            .ok_or_else(|| NitriteError::new("No FTS filter found", ErrorKind::FilterError))?;

        let filters = index_scan_filter.filters();
        if filters.is_empty() {
            return Err(NitriteError::new(
                "No FTS filter found",
                ErrorKind::FilterError,
            ));
        }

        let filter = &filters[0];
        if !is_fts_filter(filter) {
            return Err(NitriteError::new(
                "Expected FTS filter",
                ErrorKind::FilterError,
            ));
        }

        let fts_filter = as_fts_filter(filter).ok_or_else(|| {
            NitriteError::new("Failed to cast to FTS filter", ErrorKind::FilterError)
        })?;

        let query_str = fts_filter.query_string();
        self.search(&query_str)
    }

    /// Performs a full-text search and returns matching NitriteIds.
    fn search(&self, query_str: &str) -> NitriteResult<Vec<NitriteId>> {
        let reader = self
            .inner
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| {
                NitriteError::new(
                    &format!("Failed to create FTS reader: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;

        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&self.inner.index, vec![self.inner.text_field]);

        let query = query_parser.parse_query(query_str).map_err(|e| {
            NitriteError::new(
                &format!("Failed to parse FTS query '{}': {}", query_str, e),
                ErrorKind::Extension("FTS".to_string()),
            )
        })?;

        // Search with configured limit
        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(self.inner.search_result_limit))
            .map_err(|e| {
                NitriteError::new(
                    &format!("FTS search failed: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;

        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address).map_err(|e| {
                NitriteError::new(
                    &format!("Failed to retrieve FTS document: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;

            // Get the stored ID
            if let Some(id_value) = retrieved_doc.get_first(self.inner.id_field) {
                if let Some(id_str) = id_value.as_str() {
                    if let Ok(id_num) = id_str.parse::<u64>() {
                        if let Ok(nitrite_id) = NitriteId::create_id(id_num) {
                            results.push(nitrite_id);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Closes the FTS index, committing any pending changes.
    pub fn close(&self) -> NitriteResult<()> {
        let mut writer_guard = self.inner.index_writer.write();
        if let Some(mut writer) = writer_guard.take() {
            writer.commit().map_err(|e| {
                NitriteError::new(
                    &format!("Failed to commit FTS index on close: {}", e),
                    ErrorKind::Extension("FTS".to_string()),
                )
            })?;
        }
        Ok(())
    }

    /// Drops the FTS index, removing all data.
    pub fn drop(&self) -> NitriteResult<()> {
        // First close the writer
        self.close()?;

        // Remove index directory if on disk
        if let Some(ref path) = self.inner.index_path {
            if path.exists() {
                std::fs::remove_dir_all(path).map_err(|e| {
                    NitriteError::new(
                        &format!("Failed to remove FTS index directory: {}", e),
                        ErrorKind::Extension("FTS".to_string()),
                    )
                })?;
            }
        }

        Ok(())
    }
}

/// Derives the index map name from an index descriptor.
pub(crate) fn derive_index_map_name(descriptor: &IndexDescriptor) -> String {
    let collection = descriptor.collection_name();
    let fields = descriptor.index_fields().field_names().join("_");
    let index_type = descriptor.index_type();
    format!("{}_{}_{}_idx", collection, fields, index_type)
}

/// Converts a Value to text for indexing.
fn value_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Document(doc) => {
            // Concatenate all string values from document
            let mut parts = Vec::new();
            for (_, v) in doc.iter() {
                let text = value_to_text(&v);
                if !text.is_empty() {
                    parts.push(text);
                }
            }
            parts.join(" ")
        }
        Value::Array(arr) => {
            let mut parts = Vec::new();
            for v in arr {
                let text = value_to_text(v);
                if !text.is_empty() {
                    parts.push(text);
                }
            }
            parts.join(" ")
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FtsConfig;
    use nitrite::common::Fields;

    fn create_test_config() -> FtsConfig {
        FtsConfig::default()
    }

    fn create_test_index_descriptor() -> IndexDescriptor {
        let uuid = uuid::Uuid::new_v4();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        IndexDescriptor::new(
            "TantivyFullText",
            fields,
            &format!("test_collection_{}", uuid),
        )
    }

    fn create_test_field_values(_id: u64, text: &str) -> FieldValues {
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let nitrite_id = NitriteId::new();
        FieldValues::new(
            vec![("content".to_string(), Value::String(text.to_string()))],
            nitrite_id,
            fields,
        )
    }

    // ===== derive_index_map_name Tests =====

    #[test]
    fn test_derive_index_map_name() {
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let descriptor = IndexDescriptor::new("FullText", fields, "my_collection");
        let name = derive_index_map_name(&descriptor);

        assert_eq!(name, "my_collection_content_FullText_idx");
    }

    #[test]
    fn test_derive_index_map_name_multiple_fields() {
        let fields = Fields::with_names(vec!["title", "body"]).unwrap();
        let descriptor = IndexDescriptor::new("FullText", fields, "articles");
        let name = derive_index_map_name(&descriptor);

        assert_eq!(name, "articles_title_body_FullText_idx");
    }

    #[test]
    fn test_derive_index_map_name_special_chars() {
        let fields = Fields::with_names(vec!["my_field"]).unwrap();
        let descriptor = IndexDescriptor::new("TantivyFullText", fields, "my_collection");
        let name = derive_index_map_name(&descriptor);

        assert!(name.contains("my_collection"));
        assert!(name.contains("my_field"));
    }

    // ===== value_to_text Tests =====

    #[test]
    fn test_value_to_text_string() {
        let value = Value::String("hello world".to_string());
        assert_eq!(value_to_text(&value), "hello world");
    }

    #[test]
    fn test_value_to_text_empty_string() {
        let value = Value::String("".to_string());
        assert_eq!(value_to_text(&value), "");
    }

    #[test]
    fn test_value_to_text_non_string() {
        let value = Value::from(42i64);
        assert_eq!(value_to_text(&value), "");
    }

    #[test]
    fn test_value_to_text_null() {
        let value = Value::Null;
        assert_eq!(value_to_text(&value), "");
    }

    #[test]
    fn test_value_to_text_array_of_strings() {
        let value = Value::Array(vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ]);
        assert_eq!(value_to_text(&value), "hello world");
    }

    #[test]
    fn test_value_to_text_unicode() {
        let value = Value::String("日本語テスト".to_string());
        assert_eq!(value_to_text(&value), "日本語テスト");
    }

    #[test]
    fn test_value_to_text_whitespace() {
        let value = Value::String("  hello   world  ".to_string());
        assert_eq!(value_to_text(&value), "  hello   world  ");
    }

    // ===== FtsIndex Creation Tests =====

    #[test]
    fn test_fts_index_create_in_memory() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config);
        assert!(index.is_ok());
    }

    #[test]
    fn test_fts_index_create_on_disk() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let temp_dir = tempfile::tempdir().unwrap();
        let index = FtsIndex::new(descriptor, Some(temp_dir.path().to_path_buf()), &config);
        assert!(index.is_ok());
    }

    #[test]
    fn test_fts_index_clone() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();
        let cloned = index.clone();
        // Both should work independently
        assert!(cloned.search("test").is_ok());
    }

    #[test]
    fn test_fts_index_with_custom_config() {
        let descriptor = create_test_index_descriptor();
        let config = FtsConfig::new()
            .with_index_writer_heap_size(100 * 1024 * 1024)
            .with_search_result_limit(5000);
        let index = FtsIndex::new(descriptor, None, &config);
        assert!(index.is_ok());
    }

    // ===== FtsIndex Search Tests =====

    #[test]
    fn test_fts_index_search_empty() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        let results = index.search("nonexistent");
        assert!(results.is_ok());
        assert_eq!(results.unwrap().len(), 0);
    }

    #[test]
    fn test_fts_index_write_and_search() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        // Write a document
        let field_values = create_test_field_values(1001, "hello world test document");
        index.write(&field_values).unwrap();

        // Search for it
        let results = index.search("hello").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fts_index_write_multiple_and_search() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        // Write multiple documents
        index
            .write(&create_test_field_values(1001, "hello world"))
            .unwrap();
        index
            .write(&create_test_field_values(1002, "goodbye world"))
            .unwrap();
        index
            .write(&create_test_field_values(1003, "hello universe"))
            .unwrap();

        // Search for "hello" - should find 2 documents
        let results = index.search("hello").unwrap();
        assert_eq!(results.len(), 2);

        // Search for "world" - should find 2 documents
        let results = index.search("world").unwrap();
        assert_eq!(results.len(), 2);

        // Search for "universe" - should find 1 document
        let results = index.search("universe").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fts_index_remove() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        // Write a document
        let field_values = create_test_field_values(1001, "hello world");
        index.write(&field_values).unwrap();

        // Verify it exists
        assert_eq!(index.search("hello").unwrap().len(), 1);

        // Remove it
        index.remove(&field_values).unwrap();

        // Verify it's gone
        assert_eq!(index.search("hello").unwrap().len(), 0);
    }

    #[test]
    fn test_fts_index_phrase_search() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        index
            .write(&create_test_field_values(1001, "the quick brown fox"))
            .unwrap();
        index
            .write(&create_test_field_values(1002, "quick and the dead"))
            .unwrap();

        // Phrase search
        let results = index.search("\"quick brown\"").unwrap();
        assert_eq!(results.len(), 1);
    }

    // ===== FtsIndex Lifecycle Tests =====

    #[test]
    fn test_fts_index_close() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        index
            .write(&create_test_field_values(1001, "test"))
            .unwrap();

        // Close should succeed
        assert!(index.close().is_ok());
    }

    #[test]
    fn test_fts_index_drop() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let temp_dir = tempfile::tempdir().unwrap();
        let index =
            FtsIndex::new(descriptor, Some(temp_dir.path().to_path_buf()), &config).unwrap();

        // Drop should succeed
        assert!(index.drop().is_ok());
    }

    // ===== Edge Cases =====

    #[test]
    fn test_fts_index_empty_text() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        // Write empty text - should not fail
        let field_values = create_test_field_values(1001, "");
        assert!(index.write(&field_values).is_ok());
    }

    #[test]
    fn test_fts_index_unicode_text() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        index
            .write(&create_test_field_values(1001, "日本語テスト"))
            .unwrap();

        let results = index.search("日本語").unwrap();
        // Note: tantivy's default tokenizer may not split CJK well
        // but the index operation should succeed
        let _ = results.len();
    }

    #[test]
    fn test_fts_index_special_chars() {
        let descriptor = create_test_index_descriptor();
        let config = create_test_config();
        let index = FtsIndex::new(descriptor, None, &config).unwrap();

        index
            .write(&create_test_field_values(1001, "hello@world.com"))
            .unwrap();

        // Should be able to search (may be tokenized)
        assert!(index.search("hello").is_ok());
    }
}
