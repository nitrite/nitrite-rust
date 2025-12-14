use crate::collection::Document;
use crate::common::{Value, TAG_MAP_METADATA};
use std::collections::HashSet;

pub trait Metadata {
    fn get_info(&self) -> Document;
}

pub(crate) struct MapMeta {
    pub(crate) map_names: HashSet<String>,
}

impl MapMeta {
    pub fn new(metadata: &Document) -> Self {
        let mut map_names = HashSet::new();
        if let Ok(names) = metadata.get(TAG_MAP_METADATA) {
            if let Some(names) = names.as_array() {
                map_names.reserve(names.len());
                for name in names {
                    // Validate that array elements are strings before unwrapping
                    // This prevents panics on corrupted metadata
                    if let Some(name_str) = name.as_string() {
                        map_names.insert(name_str.to_string());
                    } else {
                        log::warn!("Non-string value in map metadata, skipping: {:?}", name);
                    }
                }
            }
        }

        MapMeta { map_names }
    }
}

impl Metadata for MapMeta {
    fn get_info(&self) -> Document {
        let mut doc = Document::new();
        let mut names_vec = Vec::with_capacity(self.map_names.len());
        for name in &self.map_names {
            names_vec.push(Value::String(name.clone()));
        }
        let names = Value::Array(names_vec);
        doc.put(TAG_MAP_METADATA, names).unwrap();
        doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::{Value, TAG_MAP_METADATA};

    #[test]
    fn test_map_meta_new_with_valid_metadata() {
        let mut doc = Document::new();
        let names = Value::Array(vec![
            Value::String("map1".to_string()),
            Value::String("map2".to_string()),
        ]);
        doc.put(TAG_MAP_METADATA, names).unwrap();

        let map_meta = MapMeta::new(&doc);
        assert!(map_meta.map_names.contains("map1"));
        assert!(map_meta.map_names.contains("map2"));
    }

    #[test]
    fn test_map_meta_new_with_empty_metadata() {
        let doc = Document::new();
        let map_meta = MapMeta::new(&doc);
        assert!(map_meta.map_names.is_empty());
    }

    #[test]
    fn test_map_meta_new_with_invalid_metadata() {
        let mut doc = Document::new();
        let invalid_value = Value::String("invalid".to_string());
        doc.put(TAG_MAP_METADATA, invalid_value).unwrap();

        let map_meta = MapMeta::new(&doc);
        assert!(map_meta.map_names.is_empty());
    }

    #[test]
    fn test_map_meta_get_info() {
        let mut doc = Document::new();
        let names = Value::Array(vec![
            Value::String("map1".to_string()),
            Value::String("map2".to_string()),
        ]);
        doc.put(TAG_MAP_METADATA, names).unwrap();

        let map_meta = MapMeta::new(&doc);
        let info = map_meta.get_info();
        let temp = info.get(TAG_MAP_METADATA).unwrap();
        let retrieved_names = temp.as_array().unwrap();

        assert_eq!(retrieved_names.len(), 2);
        assert!(retrieved_names.contains(&Value::String("map1".to_string())));
        assert!(retrieved_names.contains(&Value::String("map2".to_string())));
    }

    #[test]
    fn test_map_meta_get_info_with_empty_map_names() {
        let doc = Document::new();
        let map_meta = MapMeta::new(&doc);
        let info = map_meta.get_info();
        let temp = info.get(TAG_MAP_METADATA).unwrap();
        let retrieved_names = temp.as_array().unwrap();

        assert!(retrieved_names.is_empty());
    }

    #[test]
    fn test_map_meta_with_mixed_array_types() {
        // Test that MapMeta gracefully handles corrupted metadata with mixed types
        let mut doc = Document::new();
        let mixed_array = Value::Array(vec![
            Value::String("map1".to_string()),
            Value::I32(42),  // Non-string element - should be skipped
            Value::String("map2".to_string()),
            Value::Null,  // Another non-string - should be skipped
        ]);
        doc.put(TAG_MAP_METADATA, mixed_array).unwrap();

        let map_meta = MapMeta::new(&doc);
        
        // Should only include the string values
        assert!(map_meta.map_names.contains("map1"));
        assert!(map_meta.map_names.contains("map2"));
        // Integer and Null values should not be included
        assert_eq!(map_meta.map_names.len(), 2);
    }

    #[test]
    fn test_map_meta_with_all_non_string_elements() {
        // Test that MapMeta handles array with all non-string elements
        let mut doc = Document::new();
        let non_string_array = Value::Array(vec![
            Value::I32(1),
            Value::I32(2),
            Value::Null,
        ]);
        doc.put(TAG_MAP_METADATA, non_string_array).unwrap();

        let map_meta = MapMeta::new(&doc);
        
        // Should skip all non-string elements and have empty set
        assert!(map_meta.map_names.is_empty());
    }

    #[test]
    fn test_map_meta_with_single_non_string_element() {
        // Test that MapMeta handles single corrupted element
        let mut doc = Document::new();
        let corrupted_array = Value::Array(vec![
            Value::I32(999),  // Non-string value
        ]);
        doc.put(TAG_MAP_METADATA, corrupted_array).unwrap();

        let map_meta = MapMeta::new(&doc);
        
        // Should gracefully skip the non-string element
        assert!(map_meta.map_names.is_empty());
    }

    #[test]
    fn test_map_meta_preserves_valid_strings_with_corruption() {
        // Test that MapMeta preserves valid strings even with corrupted entries
        let mut doc = Document::new();
        let corrupted_array = Value::Array(vec![
            Value::String("valid_map".to_string()),
            Value::F64(3.14),  // Corrupted entry
            Value::String("another_valid".to_string()),
            Value::I32(100),  // Another corrupted entry
        ]);
        doc.put(TAG_MAP_METADATA, corrupted_array).unwrap();

        let map_meta = MapMeta::new(&doc);
        
        // Should include only valid string entries
        assert!(map_meta.map_names.contains("valid_map"));
        assert!(map_meta.map_names.contains("another_valid"));
        assert_eq!(map_meta.map_names.len(), 2);
    }

    #[test]
    fn test_map_meta_reserve_efficiency() {
        // Test that reserve() pre-allocates capacity efficiently
        let mut doc = Document::new();
        let names = Value::Array(vec![
            Value::String("map1".to_string()),
            Value::String("map2".to_string()),
            Value::String("map3".to_string()),
            Value::String("map4".to_string()),
            Value::String("map5".to_string()),
        ]);
        doc.put(TAG_MAP_METADATA, names).unwrap();

        let map_meta = MapMeta::new(&doc);
        
        // Should reserve capacity for 5 elements, reducing reallocation
        assert_eq!(map_meta.map_names.len(), 5);
        // Verify capacity is at least as much as needed
        assert!(map_meta.map_names.capacity() >= 5);
    }

    #[test]
    fn test_map_meta_get_info_pre_allocation() {
        // Test that get_info() uses Vec::with_capacity for efficient allocation
        let mut doc = Document::new();
        let names = Value::Array(vec![
            Value::String("first".to_string()),
            Value::String("second".to_string()),
            Value::String("third".to_string()),
        ]);
        doc.put(TAG_MAP_METADATA, names).unwrap();

        let map_meta = MapMeta::new(&doc);
        let info = map_meta.get_info();
        
        let temp = info.get(TAG_MAP_METADATA).unwrap();
        let retrieved_names = temp.as_array().unwrap();
        
        // Should have pre-allocated exactly the needed capacity
        assert_eq!(retrieved_names.len(), 3);
        assert_eq!(retrieved_names.capacity(), 3);
    }

    #[test]
    fn test_large_map_meta_efficiency() {
        // Test efficiency with larger dataset
        let mut doc = Document::new();
        let mut names_vec = Vec::with_capacity(100);
        for i in 0..100 {
            names_vec.push(Value::String(format!("map_{}", i)));
        }
        let names = Value::Array(names_vec);
        doc.put(TAG_MAP_METADATA, names).unwrap();

        let map_meta = MapMeta::new(&doc);
        
        // Should efficiently handle 100 entries with pre-allocation
        assert_eq!(map_meta.map_names.len(), 100);
        
        let info = map_meta.get_info();
        let temp = info.get(TAG_MAP_METADATA).unwrap();
        let retrieved = temp.as_array().unwrap();
        assert_eq!(retrieved.len(), 100);
    }

    #[test]
    fn test_map_meta_empty_array_efficiency() {
        // Test that empty arrays don't allocate unnecessary capacity
        let mut doc = Document::new();
        let empty_names = Value::Array(vec![]);
        doc.put(TAG_MAP_METADATA, empty_names).unwrap();

        let map_meta = MapMeta::new(&doc);
        assert!(map_meta.map_names.is_empty());
        
        let info = map_meta.get_info();
        let temp = info.get(TAG_MAP_METADATA).unwrap();
        let retrieved = temp.as_array().unwrap();
        assert_eq!(retrieved.len(), 0);
        assert_eq!(retrieved.capacity(), 0);
    }
}