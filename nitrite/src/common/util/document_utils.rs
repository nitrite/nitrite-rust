use std::collections::BTreeMap;

use crate::{
    collection::Document,
    errors::NitriteResult,
    filter::{by_id, Filter},
    FieldValues, Fields, Value,
};

/// Creates an empty document.
pub fn empty_document() -> Document {
    Document::new()
}

/// Creates a document from a [BTreeMap].
pub fn document_from_map(map: &BTreeMap<String, Value>) -> NitriteResult<Document> {
    // recursively create document from map
    // and validate the key as well
    let mut doc = Document::new();
    for (key, value) in map.iter() {
        match value {
            Value::Document(obj) => {
                // recursively create document from nested map
                doc.put(key, Value::Document(obj.clone()))?;
            }
            Value::Array(arr) => {
                // Preallocate with exact capacity to avoid reallocation
                let mut nested_arr = Vec::with_capacity(arr.len());
                for v in arr.iter() {
                    // if array contains nested object, then recursively create document
                    match v {
                        Value::Document(obj) => {
                            nested_arr.push(Value::Document(obj.clone()));
                        }
                        _ => {
                            nested_arr.push(v.clone());
                        }
                    }
                }
                // put the array in the document
                doc.put(key, Value::Array(nested_arr))?;
            }
            _ => {
                // for all other types, just put the value in the document
                doc.put(key, value.clone())?;
            }
        }
    }
    Ok(doc)
}

/// Creates a document with a single key-value pair.
pub fn create_document(key: &str, value: Value) -> NitriteResult<Document> {
    let mut doc = Document::new();
    doc.put(key, value)?;
    Ok(doc)
}

pub(crate) fn get_document_values(
    document: &mut Document,
    fields: &Fields,
) -> NitriteResult<FieldValues> {
    let nitrite_id = document.id()?;
    // Preallocate with exact field count to avoid reallocation
    let mut values = Vec::with_capacity(fields.field_names().len());

    for field in fields.field_names() {
        let value = document.get(&field)?;
        values.push((field, value));
    }

    Ok(FieldValues::new(values, nitrite_id, fields.clone()))
}

pub(crate) fn is_affected_by_update(fields: &Fields, updated_fields: &Document) -> bool {
    for field in fields.field_names() {
        if updated_fields.contains_key(&field) {
            return true;
        }
    }
    false
}

pub(crate) fn create_unique_filter(document: &mut Document) -> NitriteResult<Filter> {
    Ok(by_id(document.id()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::filter::is_equals_filter;
    use crate::Value;
    use std::collections::BTreeMap;

    #[test]
    fn test_empty_document() {
        let doc = empty_document();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_document_from_map() {
        let mut map = BTreeMap::new();
        map.insert("key1".to_string(), Value::String("value1".to_string()));
        map.insert("key2".to_string(), Value::I32(42));
        let doc = document_from_map(&map).unwrap();
        assert_eq!(doc.get("key1").unwrap(), Value::String("value1".to_string()));
        assert_eq!(doc.get("key2").unwrap(), Value::I32(42));
    }

    #[test]
    fn test_create_document() {
        let doc = create_document("key", Value::String("value".to_string())).unwrap();
        assert_eq!(doc.get("key").unwrap(), Value::String("value".to_string()));
    }

    #[test]
    fn test_get_document_values() {
        let mut doc = Document::new();
        doc.put("field1", Value::String("value1".to_string())).unwrap();
        let fields = Fields::with_names(vec!["field1"]).expect("Failed to create fields");
        let values = get_document_values(&mut doc, &fields).unwrap();
        assert_eq!(values.get_value("field1").unwrap(), &Value::String("value1".to_string()));
    }

    #[test]
    fn test_is_affected_by_update() {
        let fields = Fields::with_names(vec!["field1"]).expect("Failed to create fields");
        let mut updated_fields = Document::new();
        updated_fields.put("field1", Value::String("value1".to_string())).unwrap();
        assert!(is_affected_by_update(&fields, &updated_fields));
    }

    #[test]
    fn test_create_unique_filter() {
        let mut doc = Document::new();
        let _ = doc.id();
        let filter = create_unique_filter(&mut doc).unwrap();
        assert!(is_equals_filter(&filter));
    }

    #[test]
    fn bench_document_from_map_small() {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let mut map = BTreeMap::new();
            map.insert("key1".to_string(), Value::String("value1".to_string()));
            map.insert("key2".to_string(), Value::I32(42));
            let _ = document_from_map(&map);
        }
        let elapsed = start.elapsed();
        println!(
            "document_from_map small (1000x 2-field map): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }

    #[test]
    fn bench_document_from_map_large() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let mut map = BTreeMap::new();
            for i in 0..50 {
                map.insert(format!("field{}", i), Value::String(format!("value{}", i)));
            }
            let _ = document_from_map(&map);
        }
        let elapsed = start.elapsed();
        println!(
            "document_from_map large (100x 50-field map): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
    }

    #[test]
    fn bench_get_document_values() {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let mut doc = Document::new();
            doc.put("field1", Value::String("value1".to_string())).unwrap();
            doc.put("field2", Value::I32(42)).unwrap();
            let fields = Fields::with_names(vec!["field1", "field2"]).expect("Failed to create fields");
            let _ = get_document_values(&mut doc, &fields);
        }
        let elapsed = start.elapsed();
        println!(
            "get_document_values (1000x 2-field extraction): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }
}