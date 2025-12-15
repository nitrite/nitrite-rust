use crate::{
    errors::{ErrorKind, NitriteError, NitriteResult},
    index::IndexDescriptor,
    Value, INDEX_META_PREFIX, INDEX_PREFIX, INTERNAL_NAME_SEPARATOR,
};

pub(crate) fn derive_index_map_name(descriptor: &IndexDescriptor) -> String {
    // Preallocate with reasonable capacity to avoid reallocations
    // Format: $nitrite_index|collection|fields|type (typical ~40-80 chars)
    let mut name = String::with_capacity(80);
    name.push_str(INDEX_PREFIX);
    name.push_str(INTERNAL_NAME_SEPARATOR);
    name.push_str(&descriptor.collection_name());
    name.push_str(INTERNAL_NAME_SEPARATOR);
    name.push_str(&descriptor.index_fields().encoded_names());
    name.push_str(INTERNAL_NAME_SEPARATOR);
    name.push_str(&descriptor.index_type());
    name
}

pub(crate) fn derive_index_meta_map_name(collection_name: &str) -> String {
    // Format: $nitrite_index_meta|collection (typical ~20-40 chars)
    let mut name = String::with_capacity(48);
    name.push_str(INDEX_META_PREFIX);
    name.push_str(INTERNAL_NAME_SEPARATOR);
    name.push_str(collection_name);
    name
}

pub(crate) fn validate_index_field(value: Option<&Value>, field_name: &str) -> NitriteResult<()> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Array(arr)) => validate_array_index_field(arr, field_name),
        Some(Value::Bytes(_)) => {
            log::error!("Byte field {} cannot be indexed", field_name);
            Err(NitriteError::new(
                &format!("Byte field {} cannot be indexed", field_name),
                ErrorKind::IndexingError,
            ))
        },
        Some(value) => {
            if !value.is_comparable() {
                log::error!("Field {} does not have comparable value {}", field_name, value);
                Err(NitriteError::new(
                    &format!("Field {} does not have comparable value {}", field_name, value),
                    ErrorKind::IndexingError,
                ))
            } else {
                Ok(())
            }
        }
    }
}

pub(crate) fn validate_string_array_index_field(
    array: &Vec<Value>,
    field_name: &str,
) -> NitriteResult<()> {
    // Quick path: if array is empty, it's valid
    if array.is_empty() {
        return Ok(());
    }
    
    for value in array {
        match *value {
            Value::Null => {
                continue;
            }
            Value::Array(_) => {
                log::error!("Nested array field {} is not supported", field_name);
                return Err(NitriteError::new(
                    "Nested array is not supported",
                    ErrorKind::IndexingError,
                ));
            }
            Value::String(_) => {
                continue;
            }
            _ => {
                log::error!(
                    "Each value in the array field {} should be string",
                    field_name
                );
                return Err(NitriteError::new(
                    &format!(
                        "Each value in the array field {} should be string",
                        field_name
                    ),
                    ErrorKind::IndexingError,
                ));
            }
        }
    }
    Ok(())
}

fn validate_array_index_field(array: &Vec<Value>, field_name: &str) -> NitriteResult<()> {
    // Quick path: if array is empty, it's valid
    if array.is_empty() {
        return Ok(());
    }
    
    for value in array {
        match value {
            &Value::Null => {
                continue;
            }
            &Value::Array(_) => {
                log::error!("Nested array field {} is not supported", field_name);
                return Err(NitriteError::new(
                    "Nested array is not supported",
                    ErrorKind::IndexingError,
                ));
            }
            v => {
                if !v.is_comparable() {
                    log::error!(
                        "Each value in the array field {} should be comparable",
                        field_name
                    );
                    return Err(NitriteError::new(
                        &format!(
                            "Each value in the array field {} should be comparable",
                            field_name
                        ),
                        ErrorKind::IndexingError,
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Fields;
    use crate::index::IndexDescriptor;
    use crate::Value;

    #[test]
    fn test_derive_index_map_name() {
        let descriptor = IndexDescriptor::new(
            "test_collection",
            Fields::with_names(vec!["field1"]).expect("Failed to create fields"),
            "test_type",
        );
        let name = derive_index_map_name(&descriptor);
        assert_eq!(name, "$nitrite_index|test_type|field1|test_collection");
    }

    #[test]
    fn test_derive_index_meta_map_name() {
        let name = derive_index_meta_map_name("test_collection");
        assert_eq!(name, "$nitrite_index_meta|test_collection");
    }

    #[test]
    fn test_validate_index_field() {
        assert!(validate_index_field(Some(&Value::I32(42)), "field1").is_ok());
        assert!(validate_index_field(Some(&Value::String("test".to_string())), "field1").is_ok());
        assert!(validate_index_field(Some(&Value::Array(vec![Value::I32(42)])), "field1").is_ok());
        assert!(validate_index_field(Some(&Value::Array(vec![Value::Null])), "field1").is_ok());
        assert!(validate_index_field(
            Some(&Value::Array(vec![Value::Array(vec![Value::I32(42)])])),
            "field1"
        )
        .is_err());
        assert!(validate_index_field(Some(&Value::Null), "field1").is_ok());
        assert!(validate_index_field(None, "field1").is_ok());
    }

    #[test]
    fn test_validate_string_array_index_field() {
        assert!(validate_string_array_index_field(
            &vec![Value::String("test".to_string())],
            "field1"
        )
        .is_ok());
        assert!(validate_string_array_index_field(&vec![Value::Null], "field1").is_ok());
        assert!(validate_string_array_index_field(
            &vec![Value::Array(vec![Value::String("test".to_string())])],
            "field1"
        )
        .is_err());
        assert!(validate_string_array_index_field(&vec![Value::I32(42)], "field1").is_err());
    }

    #[test]
    fn test_validate_array_index_field() {
        assert!(validate_array_index_field(&vec![Value::I32(42)], "field1").is_ok());
        assert!(validate_array_index_field(&vec![Value::Null], "field1").is_ok());
        assert!(
            validate_array_index_field(&vec![Value::Array(vec![Value::I32(42)])], "field1")
                .is_err()
        );
        assert!(
            validate_array_index_field(&vec![Value::String("test".to_string())], "field1").is_ok()
        );
    }

    #[test]
    fn bench_derive_index_map_name() {
        let descriptor = IndexDescriptor::new(
            "test_collection",
            Fields::with_names(vec!["field1"]).expect("Failed to create fields"),
            "test_type",
        );
        
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = derive_index_map_name(&descriptor);
        }
        let elapsed = start.elapsed();
        println!(
            "derive_index_map_name (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_derive_index_meta_map_name() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = derive_index_meta_map_name("test_collection");
        }
        let elapsed = start.elapsed();
        println!(
            "derive_index_meta_map_name (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_validate_string_array_index_field() {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let arr = vec![
                Value::String("test1".to_string()),
                Value::String("test2".to_string()),
                Value::String("test3".to_string()),
                Value::Null,
                Value::String("test4".to_string()),
            ];
            let _ = validate_string_array_index_field(&arr, "field1");
        }
        let elapsed = start.elapsed();
        println!(
            "validate_string_array_index_field (1000x 5-element array): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }

    #[test]
    fn bench_validate_array_index_field() {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let arr = vec![
                Value::I32(1),
                Value::I32(2),
                Value::I32(3),
                Value::Null,
                Value::String("test".to_string()),
            ];
            let _ = validate_array_index_field(&arr, "field1");
        }
        let elapsed = start.elapsed();
        println!(
            "validate_array_index_field (1000x 5-element array): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }
}
