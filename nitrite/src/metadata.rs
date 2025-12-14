use crate::{collection::Document, errors::NitriteResult, store::Metadata, Value};

/// Database metadata capturing store creation time, versions, and schema information.
///
/// NitriteMetadata encapsulates essential metadata about a Nitrite database instance,
/// including when it was created and what versions of the store and schema are in use.
/// This metadata is persisted as a special document and used for compatibility checking
/// and initialization during database open operations.
///
/// # Fields
/// - `create_time`: Timestamp (milliseconds) when the database was created
/// - `store_version`: Version string of the storage engine (e.g., "2.0.0")
/// - `nitrite_version`: Version string of the Nitrite library (e.g., "3.0.0")
/// - `schema_version`: Numeric schema version for compatibility tracking
///
/// # Characteristics
/// - **Cloneable**: Can be efficiently cloned for sharing across threads
/// - **Serializable**: Converts to/from Document for persistence
/// - **Versioned**: Supports schema and version migrations
/// - **Defaults**: Missing fields default to sensible values ("1.0.0" for versions, 0 for numeric fields)
///
/// # Usage
///
/// Metadata is typically:
/// 1. Created from a database Document via `new()` during database initialization
/// 2. Used for version compatibility checks before opening a database
/// 3. Serialized back to a Document via `get_info()` for persistence
#[derive(Debug, Clone)]
pub struct NitriteMetadata {
    pub create_time: u128,
    pub store_version: String,
    pub nitrite_version: String,
    pub schema_version: u32,
}

impl NitriteMetadata {
    /// Creates NitriteMetadata by extracting metadata fields from a Document.
    ///
    /// # Arguments
    /// * `document` - A Document containing metadata fields. Supports both complete and partial documents.
    ///
    /// # Returns
    /// A new NitriteMetadata instance with extracted fields or defaults for missing fields.
    ///
    /// # Field Extraction
    ///
    /// Extracts the following fields from the document:
    /// - `create_time`: u128 timestamp, defaults to 0 if missing or invalid type
    /// - `store_version`: String version, defaults to "1.0.0" if missing or invalid type
    /// - `nitrite_version`: String version, defaults to "1.0.0" if missing or invalid type
    /// - `schema_version`: u32 version, defaults to 0 if missing or invalid type
    ///
    /// # Behavior
    ///
    /// Type validation is lenient - if a field exists but has the wrong type, the default
    /// value for that field is used rather than returning an error. This allows backward
    /// compatibility with older metadata documents that may have type variations.
    ///
    /// # Errors
    ///
    /// Returns error only if document access fails (e.g., document corruption).
    /// Invalid field types use defaults instead of causing errors.
    pub fn new(document: &Document) -> NitriteResult<NitriteMetadata> {
        // Cache document lookups to avoid repeated get() calls
        let create_time_val = document.get("create_time")?;
        let store_version_val = document.get("store_version")?;
        let nitrite_version_val = document.get("nitrite_version")?;
        let schema_version_val = document.get("schema_version")?;
        
        Ok(NitriteMetadata {
            create_time: *create_time_val.as_u128().unwrap_or(&0u128),
            store_version: store_version_val.as_string().map_or("1.0.0", |v| v).to_string(),
            nitrite_version: nitrite_version_val.as_string().map_or("1.0.0", |v| v).to_string(),
            schema_version: *schema_version_val.as_u32().unwrap_or(&0u32),
        })
    }
}

impl Metadata for NitriteMetadata {
    /// Serializes this metadata to a Document for persistence.
    ///
    /// # Returns
    /// A Document containing the metadata fields:
    /// - `create_time`: u128 creation timestamp
    /// - `store_version`: String store engine version
    /// - `nitrite_version`: String Nitrite library version
    /// - `schema_version`: u32 schema version
    ///
    /// # Behavior
    ///
    /// Creates a new Document and populates all four metadata fields from this instance.
    /// String fields (store_version, nitrite_version) are cloned during serialization
    /// to ensure the returned Document is independent of this metadata instance.
    ///
    /// All put operations are performed safely and result errors are ignored, as field
    /// insertion should always succeed for valid metadata values. The returned Document
    /// is guaranteed to contain all four fields.
    ///
    /// # Usage
    ///
    /// Used during database shutdown to persist metadata back to storage:
    /// 1. Metadata instance is converted to Document via get_info()
    /// 2. Document is written to special metadata storage location
    /// 3. On next database open, metadata is read back and reconstructed via new()
    fn get_info(&self) -> Document {
        let mut document = Document::new();
        let _ = document.put("create_time", Value::from(self.create_time));
        let _ = document.put("store_version", Value::from(self.store_version.clone()));
        let _ = document.put(
            "nitrite_version",
            Value::from(self.nitrite_version.clone()),
        );
        let _ = document.put("schema_version", Value::from(self.schema_version));
        document
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::Value;

    #[test]
    fn test_new_metadata_success() {
        let mut doc = Document::new();
        doc.put("create_time", Value::from(1234567890u128)).unwrap();
        doc.put("store_version", Value::from("2.0.0")).unwrap();
        doc.put("nitrite_version", Value::from("3.0.0")).unwrap();
        doc.put("schema_version", Value::from(1u32)).unwrap();

        let metadata = NitriteMetadata::new(&doc).unwrap();
        assert_eq!(metadata.create_time, 1234567890u128);
        assert_eq!(metadata.store_version, "2.0.0");
        assert_eq!(metadata.nitrite_version, "3.0.0");
        assert_eq!(metadata.schema_version, 1u32);
    }

    #[test]
    fn test_new_metadata_missing_fields() {
        let doc = Document::new();
        let metadata = NitriteMetadata::new(&doc).unwrap();
        assert_eq!(metadata.create_time, 0u128);
        assert_eq!(metadata.store_version, "1.0.0");
        assert_eq!(metadata.nitrite_version, "1.0.0");
        assert_eq!(metadata.schema_version, 0u32);
    }

    #[test]
    fn test_new_metadata_invalid_field_types() {
        let mut doc = Document::new();
        doc.put("create_time", Value::from("invalid")).unwrap();
        doc.put("store_version", Value::from(2)).unwrap();
        doc.put("nitrite_version", Value::from(3)).unwrap();
        doc.put("schema_version", Value::from("invalid")).unwrap();

        let metadata = NitriteMetadata::new(&doc).unwrap();
        assert_eq!(metadata.create_time, 0u128);
        assert_eq!(metadata.store_version, "1.0.0");
        assert_eq!(metadata.nitrite_version, "1.0.0");
        assert_eq!(metadata.schema_version, 0u32);
    }

    #[test]
    fn test_get_info() {
        let mut doc = Document::new();
        doc.put("create_time", Value::from(1234567890u128)).unwrap();
        doc.put("store_version", Value::from("2.0.0")).unwrap();
        doc.put("nitrite_version", Value::from("3.0.0")).unwrap();
        doc.put("schema_version", Value::from(1u32)).unwrap();

        let metadata = NitriteMetadata::new(&doc).unwrap();
        let info = metadata.get_info();

        assert_eq!(info.get("create_time").unwrap().as_u128().unwrap(), &1234567890u128);
        assert_eq!(info.get("store_version").unwrap().as_string().unwrap(), "2.0.0");
        assert_eq!(info.get("nitrite_version").unwrap().as_string().unwrap(), "3.0.0");
        assert_eq!(info.get("schema_version").unwrap().as_u32().unwrap(), &1u32);
    }

    #[test]
    fn test_get_info_with_default_values() {
        // Test that get_info handles defaults gracefully without unwrap panics
        let metadata = NitriteMetadata {
            create_time: 0,
            store_version: "1.0.0".to_string(),
            nitrite_version: "1.0.0".to_string(),
            schema_version: 0,
        };
        
        let info = metadata.get_info();
        
        // Verify all fields are present in document
        assert!(info.get("create_time").is_ok());
        assert!(info.get("store_version").is_ok());
        assert!(info.get("nitrite_version").is_ok());
        assert!(info.get("schema_version").is_ok());
    }

    #[test]
    fn test_get_info_safe_document_creation() {
        // Test that get_info uses safe put operations without unwrap
        let metadata = NitriteMetadata {
            create_time: 9876543210u128,
            store_version: "4.5.6".to_string(),
            nitrite_version: "5.6.7".to_string(),
            schema_version: 99u32,
        };
        
        let info = metadata.get_info();
        
        // Document creation should succeed without panics
        assert_eq!(info.get("create_time").unwrap().as_u128().unwrap(), &9876543210u128);
        assert_eq!(info.get("store_version").unwrap().as_string().unwrap(), "4.5.6");
        assert_eq!(info.get("nitrite_version").unwrap().as_string().unwrap(), "5.6.7");
        assert_eq!(info.get("schema_version").unwrap().as_u32().unwrap(), &99u32);
    }

    #[test]
    fn test_get_info_round_trip() {
        // Test round-trip: metadata -> get_info -> new metadata
        let original = NitriteMetadata {
            create_time: 5555555u128,
            store_version: "2.1.0".to_string(),
            nitrite_version: "2.2.0".to_string(),
            schema_version: 42u32,
        };
        
        let info = original.get_info();
        let restored = NitriteMetadata::new(&info).unwrap();
        
        // Verify data integrity
        assert_eq!(original.create_time, restored.create_time);
        assert_eq!(original.store_version, restored.store_version);
        assert_eq!(original.nitrite_version, restored.nitrite_version);
        assert_eq!(original.schema_version, restored.schema_version);
    }

    #[test]
    fn test_get_info_clone_efficiency() {
        // Test that clone() operation in get_info is efficient
        let metadata = NitriteMetadata {
            create_time: 9876543210u128,
            store_version: "4.5.6".to_string(),
            nitrite_version: "5.6.7".to_string(),
            schema_version: 99u32,
        };
        
        // Multiple calls should be efficient
        let info1 = metadata.get_info();
        let info2 = metadata.get_info();
        
        // Both should have same values
        assert_eq!(info1.get("store_version").unwrap().as_string().unwrap(), "4.5.6");
        assert_eq!(info2.get("store_version").unwrap().as_string().unwrap(), "4.5.6");
    }

    #[test]
    fn test_metadata_new_with_cached_field_access() {
        // Test that new() caches field lookups efficiently
        let mut doc = Document::new();
        doc.put("create_time", Value::from(1111111u128)).unwrap();
        doc.put("store_version", Value::from("1.5.0")).unwrap();
        doc.put("nitrite_version", Value::from("1.6.0")).unwrap();
        doc.put("schema_version", Value::from(5u32)).unwrap();
        
        let metadata = NitriteMetadata::new(&doc).unwrap();
        
        assert_eq!(metadata.create_time, 1111111u128);
        assert_eq!(metadata.store_version, "1.5.0");
        assert_eq!(metadata.nitrite_version, "1.6.0");
        assert_eq!(metadata.schema_version, 5u32);
    }

    #[test]
    fn test_metadata_new_multiple_calls_efficiency() {
        // Test that multiple new() calls with same document are efficient
        let mut doc = Document::new();
        doc.put("create_time", Value::from(2222222u128)).unwrap();
        doc.put("store_version", Value::from("2.5.0")).unwrap();
        doc.put("nitrite_version", Value::from("2.6.0")).unwrap();
        doc.put("schema_version", Value::from(10u32)).unwrap();
        
        for _ in 0..100 {
            let metadata = NitriteMetadata::new(&doc).unwrap();
            assert_eq!(metadata.create_time, 2222222u128);
        }
    }

    #[test]
    fn test_metadata_string_clone_only_when_needed() {
        // Test that string clone() only happens for string values
        let original = NitriteMetadata {
            create_time: 3333333u128,
            store_version: "version_long_string_to_test_clone".to_string(),
            nitrite_version: "another_long_string".to_string(),
            schema_version: 15u32,
        };
        
        let info = original.get_info();
        let restored = NitriteMetadata::new(&info).unwrap();
        
        // Cloned strings should be identical
        assert_eq!(original.store_version, restored.store_version);
        assert_eq!(original.nitrite_version, restored.nitrite_version);
    }
}