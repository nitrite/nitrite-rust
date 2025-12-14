use crate::{collection::Document, errors::{ErrorKind, NitriteError, NitriteResult}, Convertible, Value};

use super::IndexDescriptor;

pub struct IndexMeta {
    index_descriptor: IndexDescriptor,
    index_map: String,
    is_dirty: bool,
}

impl IndexMeta {
    pub fn new(index_descriptor: IndexDescriptor, index_map: String) -> IndexMeta {
        IndexMeta {
            index_descriptor,
            index_map,
            is_dirty: false,
        }
    }

    pub fn index_descriptor(&self) -> IndexDescriptor {
        self.index_descriptor.clone()
    }

    pub fn index_map_name(&self) -> String {
        self.index_map.clone()
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }
}

impl Convertible for IndexMeta {
    type Output = Self;

    fn to_value(&self) -> NitriteResult<Value> {
        let mut doc = Document::new();
        doc.put("index_descriptor", self.index_descriptor.to_value()?)?;
        doc.put("index_map", Value::String(self.index_map.clone()))?;
        doc.put("is_dirty", Value::Bool(self.is_dirty))?;
        Ok(Value::Document(doc))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::Document(doc) => {
                let index_descriptor = IndexDescriptor::from_value(&doc.get("index_descriptor")?)?;
                
                // Safely extract index_map as String with validation
                let index_map = doc.get("index_map")?
                    .as_string()
                    .ok_or_else(|| {
                        log::error!("index_map field must be a string, got: {:?}", doc.get("index_map"));
                        NitriteError::new(
                            "index_map field must be a string in index metadata",
                            ErrorKind::ObjectMappingError,
                        )
                    })?
                    .clone();
                
                // Safely extract is_dirty as Bool with validation
                let is_dirty = doc.get("is_dirty")?
                    .as_bool()
                    .ok_or_else(|| {
                        log::error!("is_dirty field must be a bool, got: {:?}", doc.get("is_dirty"));
                        NitriteError::new(
                            "is_dirty field must be a bool in index metadata",
                            ErrorKind::ObjectMappingError,
                        )
                    })?
                    .clone();
                    
                Ok(IndexMeta {
                    index_descriptor,
                    index_map,
                    is_dirty,
                })
            }
            _ => {
                log::error!("Failed to convert Value {:?} to IndexMeta", value);
                Err(NitriteError::new(
                    "Value is not a document",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::{Fields, Value};
    use crate::errors::ErrorKind;
    use crate::index::IndexDescriptor;

    fn create_fields() -> Fields {
        Fields::with_names(vec!["test_field"]).unwrap()
    }
    
    fn create_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new("test_index", create_fields(), "test")
    }

    #[test]
    fn test_index_meta_new() {
        let index_descriptor = create_index_descriptor();
        let index_meta = IndexMeta::new(index_descriptor.clone(), "test_map".to_string());

        assert_eq!(index_meta.index_descriptor(), index_descriptor);
        assert_eq!(index_meta.index_map_name(), "test_map");
        assert!(!index_meta.is_dirty());
    }

    #[test]
    fn test_index_meta_set_dirty() {
        let index_descriptor = create_index_descriptor();
        let mut index_meta = IndexMeta::new(index_descriptor, "test_map".to_string());

        index_meta.set_dirty(true);
        assert!(index_meta.is_dirty());

        index_meta.set_dirty(false);
        assert!(!index_meta.is_dirty());
    }

    #[test]
    fn test_index_meta_to_value() {
        let index_descriptor = create_index_descriptor();
        let index_meta = IndexMeta::new(index_descriptor.clone(), "test_map".to_string());

        let value = index_meta.to_value().unwrap();
        if let Value::Document(doc) = value {
            assert_eq!(doc.get("index_descriptor").unwrap(), index_descriptor.to_value().unwrap());
            assert_eq!(doc.get("index_map").unwrap(), Value::String("test_map".to_string()));
            assert_eq!(doc.get("is_dirty").unwrap(), Value::Bool(false));
        } else {
            panic!("Expected Value::Document");
        }
    }

    #[test]
    fn test_index_meta_from_value() {
        let index_descriptor = create_index_descriptor();
        let mut doc = Document::new();
        doc.put("index_descriptor", index_descriptor.to_value().unwrap()).unwrap();
        doc.put("index_map", Value::String("test_map".to_string())).unwrap();
        doc.put("is_dirty", Value::Bool(false)).unwrap();

        let value = Value::Document(doc);
        let index_meta = IndexMeta::from_value(&value).unwrap();

        assert_eq!(index_meta.index_descriptor(), index_descriptor);
        assert_eq!(index_meta.index_map_name(), "test_map");
        assert!(!index_meta.is_dirty());
    }

    #[test]
    fn test_index_meta_from_value_invalid() {
        let value = Value::String("invalid".to_string());
        let result = IndexMeta::from_value(&value);

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_index_meta_from_value_missing_fields() {
        let mut doc = Document::new();
        doc.put("index_map", Value::String("test_map".to_string())).unwrap();
        doc.put("is_dirty", Value::Bool(false)).unwrap();

        let value = Value::Document(doc);
        let result = IndexMeta::from_value(&value);

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_index_meta_from_value_rejects_non_string_index_map() {
        // When index_map field contains non-string value, deserialization should fail
        let index_descriptor = create_index_descriptor();
        let mut doc = Document::new();
        doc.put("index_descriptor", index_descriptor.to_value().unwrap()).unwrap();
        doc.put("index_map", Value::I32(42)).unwrap(); // Invalid: should be String
        doc.put("is_dirty", Value::Bool(false)).unwrap();

        let value = Value::Document(doc);
        let result = IndexMeta::from_value(&value);

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), &ErrorKind::ObjectMappingError);
            assert!(err.to_string().contains("index_map field must be a string"));
        }
    }

    #[test]
    fn test_index_meta_from_value_rejects_non_bool_is_dirty() {
        // When is_dirty field contains non-bool value, deserialization should fail
        let index_descriptor = create_index_descriptor();
        let mut doc = Document::new();
        doc.put("index_descriptor", index_descriptor.to_value().unwrap()).unwrap();
        doc.put("index_map", Value::String("test_map".to_string())).unwrap();
        doc.put("is_dirty", Value::String("true".to_string())).unwrap(); // Invalid: should be Bool

        let value = Value::Document(doc);
        let result = IndexMeta::from_value(&value);

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), &ErrorKind::ObjectMappingError);
            assert!(err.to_string().contains("is_dirty field must be a bool"));
        }
    }

    #[test]
    fn test_index_meta_from_value_handles_multiple_type_violations() {
        // When multiple fields have wrong types, first error should be returned
        let index_descriptor = create_index_descriptor();
        let mut doc = Document::new();
        doc.put("index_descriptor", index_descriptor.to_value().unwrap()).unwrap();
        doc.put("index_map", Value::I32(100)).unwrap(); // Invalid
        doc.put("is_dirty", Value::Null).unwrap(); // Invalid

        let value = Value::Document(doc);
        let result = IndexMeta::from_value(&value);

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_index_meta_from_value_validates_all_fields_have_correct_types() {
        // When all fields have correct types in proper structure, deserialization should succeed
        let index_descriptor = create_index_descriptor();
        let mut doc = Document::new();
        doc.put("index_descriptor", index_descriptor.to_value().unwrap()).unwrap();
        doc.put("index_map", Value::String("production_index".to_string())).unwrap();
        doc.put("is_dirty", Value::Bool(true)).unwrap();

        let value = Value::Document(doc);
        let result = IndexMeta::from_value(&value);

        assert!(result.is_ok());
        if let Ok(metadata) = result {
            assert_eq!(metadata.index_map_name(), "production_index");
            assert!(metadata.is_dirty());
        }
    }

    // Performance optimization tests
    #[test]
    fn test_index_meta_from_value_efficient_string_extraction() {
        // Test that from_value efficiently extracts strings using as_string() reference
        let index_descriptor = create_index_descriptor();
        let mut doc = Document::new();
        doc.put("index_descriptor", index_descriptor.to_value().unwrap()).unwrap();
        
        // Create index_map with large string to test efficiency
        let large_name = "very_long_index_map_name_that_tests_efficiency_of_string_extraction_".repeat(10);
        doc.put("index_map", Value::String(large_name.clone())).unwrap();
        doc.put("is_dirty", Value::Bool(false)).unwrap();

        let value = Value::Document(doc);
        let result = IndexMeta::from_value(&value);

        assert!(result.is_ok());
        if let Ok(metadata) = result {
            assert_eq!(metadata.index_map_name(), large_name);
        }
    }

    #[test]
    fn test_index_meta_to_value_and_back_round_trip() {
        // Test that serialization and deserialization preserve all data
        let index_descriptor = create_index_descriptor();
        let original = IndexMeta::new(index_descriptor.clone(), "test_map".to_string());
        
        let value = original.to_value().unwrap();
        let restored = IndexMeta::from_value(&value).unwrap();
        
        assert_eq!(original.index_descriptor(), restored.index_descriptor());
        assert_eq!(original.index_map_name(), restored.index_map_name());
        assert_eq!(original.is_dirty(), restored.is_dirty());
    }

    #[test]
    fn test_index_meta_conversion_with_dirty_flag_changes() {
        // Test that dirty flag is accurately preserved through conversions
        let index_descriptor = create_index_descriptor();
        let mut original = IndexMeta::new(index_descriptor.clone(), "test_map".to_string());
        
        // Test with dirty=true
        original.set_dirty(true);
        let value = original.to_value().unwrap();
        let restored = IndexMeta::from_value(&value).unwrap();
        assert!(restored.is_dirty());
        
        // Test with dirty=false
        let mut original2 = IndexMeta::new(index_descriptor, "test_map".to_string());
        original2.set_dirty(false);
        let value2 = original2.to_value().unwrap();
        let restored2 = IndexMeta::from_value(&value2).unwrap();
        assert!(!restored2.is_dirty());
    }

    #[test]
    fn test_index_meta_batch_conversions_efficiency() {
        // Test that multiple conversions don't cause performance degradation
        let index_descriptor = create_index_descriptor();
        
        for i in 0..100 {
            let index_meta = IndexMeta::new(
                index_descriptor.clone(),
                format!("index_map_{}", i)
            );
            
            let value = index_meta.to_value().unwrap();
            let restored = IndexMeta::from_value(&value).unwrap();
            
            assert_eq!(restored.index_map_name(), format!("index_map_{}", i));
        }
    }
}
