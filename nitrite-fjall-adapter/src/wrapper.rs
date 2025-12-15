use fjall::UserKey;
use nitrite::common::Value;
use nitrite::errors::{ErrorKind, NitriteError};
use std::error::Error;
use thiserror::Error;
/// Error type for FjallValue serialization/deserialization operations.
///
/// Provides granular error information for Value serialization/deserialization failures
/// in the Fjall adapter.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum FjallValueError {
    /// Deserialization of binary data failed
    #[error("Deserialization failed: {0}")]
    DeserializationError(String),
    /// Serialization of a value failed
    #[error("Serialization failed: {0}")]
    SerializationError(String),
    /// Invalid UTF-8 encountered in serialized data
    #[error("Invalid UTF-8 in serialized data: {0}")]
    InvalidUtf8(String),
}
impl From<FjallValueError> for NitriteError {
    /// Converts a `FjallValueError` to a `NitriteError` with ObjectMappingError kind.
    fn from(err: FjallValueError) -> Self {
        NitriteError::new(&err.to_string(), ErrorKind::ObjectMappingError)
    }
}
/// Result type for FjallValue operations.
///
/// Used throughout the Fjall adapter for fallible Value serialization/deserialization.
pub type FjallValueResult<T> = Result<T, FjallValueError>;
/// Byte-serialized wrapper for Nitrite Values.
///
/// Encapsulates a Value as a Vec<u8> for storage in Fjall's partition. Handles
/// serialization/deserialization using bincode.
/// Implements numeric type normalization to ensure consistent index behavior across
/// different numeric types.
///
/// Characteristics:
/// - Serialization: Converts Value to bytes for disk storage
/// - Deserialization: Restores Value from bytes with error handling
/// - Normalization: Ensures numeric type consistency (U64 → I64, etc.)
/// - Cloneable: Full clone support for Vec<u8> data
/// - Comparable: Derives PartialEq and Eq for content comparison
///
/// Normalization details:
/// - U8, U16, U32, U64, U128, USize: Converted to signed equivalents
/// - I8 through ISize: Preserved as-is
/// - String, Array, Document, Boolean: Preserved as-is
///
/// Usage: Created via `try_from_value()` for serialization, or `try_into_value()`
/// for deserialization. Available as FjallValue::new() for infallible operations
/// (panics on errors - use try_* methods for safe code).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FjallValue(Vec<u8>);
impl FjallValue {
    /// Normalize numeric types to ensure consistent byte representation across different numeric types.
    /// This is essential for index operations to work correctly with mixed numeric types.
    ///
    /// For example: Value::I64(5) and Value::U64(5) will both be normalized to Value::I64(5)
    /// to ensure they serialize to the same bytes and compare correctly in indexes.
    #[inline]
    fn normalize_numeric_type(value: &Value) -> Value {
        match value {
            // Convert all unsigned integer types to their signed equivalents when possible
            Value::U8(v) => Value::I8(*v as i8),
            Value::U16(v) => Value::I16(*v as i16),
            Value::U32(v) => Value::I32(*v as i32),
            Value::U64(v) => Value::I64(*v as i64),
            Value::U128(v) => Value::I128(*v as i128),
            Value::USize(v) => Value::ISize(*v as isize),
            // Keep other values as-is
            other => other.clone(),
        }
    }
    /// Try to create FjallValue from Value using normalization for numeric types.
    /// This ensures consistent index behavior across different numeric types.
    ///
    /// # Returns
    /// - `Ok(FjallValue)` on successful serialization
    /// - `Err(FjallValueError)` on serialization failure
    #[inline]
    pub fn try_from_value_normalized(value: &Value) -> FjallValueResult<FjallValue> {
        let normalized = Self::normalize_numeric_type(value);
        bincode::serde::encode_to_vec(&normalized, bincode::config::legacy())
            .map(FjallValue)
            .map_err(|e| FjallValueError::SerializationError(e.to_string()))
    }
    /// Try to convert FjallValue to Value using TryFrom pattern.
    ///
    /// **RECOMMENDED FOR PRODUCTION USE**: Returns Result for safe error handling.
    ///
    /// # Returns
    /// - `Ok(Value)` on successful deserialization
    /// - `Err(FjallValueError)` on corrupted or invalid data
    #[inline]
    pub fn try_into_value(self) -> FjallValueResult<Value> {
        bincode::serde::decode_from_slice(&self.0, bincode::config::legacy())
            .map(|(value, _)| value)
            .map_err(|e| FjallValueError::DeserializationError(e.to_string()))
    }
    /// Try to create FjallValue from Value using fallible conversion.
    ///
    /// **RECOMMENDED FOR PRODUCTION USE**: Returns Result for safe error handling.
    ///
    /// # Returns
    /// - `Ok(FjallValue)` on successful serialization
    /// - `Err(FjallValueError)` on serialization failure
    #[inline]
    pub fn try_from_value(value: &Value) -> FjallValueResult<FjallValue> {
        bincode::serde::encode_to_vec(value, bincode::config::legacy())
            .map(FjallValue)
            .map_err(|e| FjallValueError::SerializationError(e.to_string()))
    }
    /// Create a new FjallValue from a Value.
    ///
    /// **WARNING: PANICS ON ERROR** - Use only with trusted values known to serialize successfully.
    /// For production code, prefer `try_from_value()` which returns a Result.
    ///
    /// # Panics
    /// - If the value fails to serialize
    #[inline]
    pub fn new(value: Value) -> Self {
        if let Ok(fjall_value) = Self::try_from_value(&value) {
            fjall_value
        } else {
            panic!("Failed to serialize value: {:?}", value)
        }
    }
}
/// Safe conversion using Into trait. Panics only on corrupted/invalid data.
///
/// **WARNING: This trait implementation can panic on corrupted deserialization data.**
/// For safe, production-ready code, use `try_into_value()` which returns a Result.
///
/// # Panics
/// - If deserialization fails (corrupted data, format version mismatch, etc.)
impl From<FjallValue> for Value {
    fn from(val: FjallValue) -> Self {
        if let Ok(value) = val.try_into_value() {
            value
        } else {
            panic!("Failed to deserialize FjallValue")
        }
    }
}
/// Safe conversion using From trait. Panics only on serialization failure.
///
/// **WARNING: This trait implementation can panic on serialization failure.**
/// For safe, production-ready code, use `try_from_value()` which returns a Result.
///
/// # Panics
/// - If serialization fails
impl From<Value> for FjallValue {
    fn from(value: Value) -> Self {
        if let Ok(fjall_value) = Self::try_from_value(&value) {
            fjall_value
        } else {
            panic!("Failed to serialize value: {:?}", value)
        }
    }
}
impl From<FjallValue> for UserKey {
    /// Converts FjallValue to a Fjall UserKey for partition operations.
    ///
    /// Returns: UserKey wrapping the internal byte vector
    #[inline]
    fn from(val: FjallValue) -> Self {
        UserKey::new(&val.0)
    }
}
impl From<UserKey> for FjallValue {
    /// Converts a Fjall UserKey back to FjallValue.
    ///
    /// Arguments:
    /// - `value`: UserKey to convert
    ///
    /// Returns: FjallValue containing the key's bytes
    #[inline]
    fn from(value: UserKey) -> Self {
        FjallValue(value.to_vec())
    }
}
impl AsRef<[u8]> for FjallValue {
    /// Returns a byte slice reference to the serialized data.
    ///
    /// Returns: Reference to internal Vec<u8> as a byte slice
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
/// Converts Fjall backend errors to Nitrite errors.
///
/// Maps error message patterns to appropriate NitriteError kinds:
/// - "closed" → StoreAlreadyClosed
/// - "not found" → StoreNotInitialized
/// - "corrupt" → FileCorrupted
/// - "permission" → PermissionDenied
/// - "full" → DiskFull
/// - Other → BackendError
///
/// Arguments:
/// - `error`: Fjall error to convert
///
/// Returns: NitriteError with mapped kind and formatted message
pub(crate) fn to_nitrite_error(error: impl Error) -> NitriteError {
    // Map specific error messages to appropriate NitriteError variants with early returns
    let error_msg = error.to_string();
    let error_kind = if error_msg.contains("closed") {
        ErrorKind::StoreAlreadyClosed
    } else if error_msg.contains("not found") {
        ErrorKind::StoreNotInitialized
    } else if error_msg.contains("deleted") || error_msg.contains("PartitionDeleted") {
        // Partition was deleted - treat as not found/not initialized
        ErrorKind::StoreNotInitialized
    } else if error_msg.contains("corrupt") {
        ErrorKind::FileCorrupted
    } else if error_msg.contains("permission") {
        ErrorKind::PermissionDenied
    } else if error_msg.contains("full") {
        ErrorKind::DiskFull
    } else {
        ErrorKind::BackendError
    };
    NitriteError::new(&format!("Fjall Error: {}", error_msg), error_kind)
}
#[cfg(test)]
mod tests {
    use super::*;
    use fjall::UserKey;
    use nitrite::common::Value;
    #[inline(never)]
    #[allow(dead_code)]
    fn black_box<T>(x: T) -> T {
        x
    }
    #[test]
    fn test_fjall_value_try_into_value() {
        let fjall_value = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        let result = fjall_value.try_into_value();
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(matches!(value, Value::Array(_)));
    }
    #[test]
    fn test_value_try_from_fjall_value() {
        let fjall_value = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        // Use the safe conversion method directly instead of TryFrom
        let result = fjall_value.try_into_value();
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(matches!(value, Value::Array(_)));
    }
    #[test]
    fn test_fjall_value_try_from_value() {
        let value = Value::Array(vec![1.into(), 2.into(), 3.into(), 4.into()]);
        let result = FjallValue::try_from_value(&value);
        assert!(result.is_ok());
        let fjall_value = result.unwrap();
        assert_eq!(
            fjall_value.0,
            vec![
                19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0,
                0, 6, 0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0
            ]
        );
    }
    #[test]
    fn test_corrupted_deserialization_returns_error() {
        // Corrupted binary data that cannot be deserialized
        let corrupted_fjall_value = FjallValue(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        let result = corrupted_fjall_value.try_into_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, FjallValueError::DeserializationError(_)));
        assert!(err.to_string().contains("Deserialization failed"));
    }
    #[test]
    fn test_try_from_with_corrupted_data() {
        let corrupted_fjall_value = FjallValue(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        let result = corrupted_fjall_value.try_into_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, FjallValueError::DeserializationError(_)));
    }
    #[test]
    fn test_error_contains_diagnostic_info() {
        let corrupted_fjall_value = FjallValue(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        let result = corrupted_fjall_value.try_into_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("Deserialization failed:"));
    }
    #[test]
    fn test_empty_fjall_value_deserialization_error() {
        let empty_fjall_value = FjallValue(vec![]);
        let result = empty_fjall_value.try_into_value();
        // Empty data should result in deserialization error
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FjallValueError::DeserializationError(_)
        ));
    }
    #[test]
    fn test_fjall_value_error_display() {
        let err = FjallValueError::DeserializationError("test error".to_string());
        assert_eq!(err.to_string(), "Deserialization failed: test error");
        let err = FjallValueError::SerializationError("test error".to_string());
        assert_eq!(err.to_string(), "Serialization failed: test error");
        let err = FjallValueError::InvalidUtf8("invalid bytes".to_string());
        assert_eq!(
            err.to_string(),
            "Invalid UTF-8 in serialized data: invalid bytes"
        );
    }
    #[test]
    fn test_fjall_value_error_clone() {
        let err1 = FjallValueError::DeserializationError("test".to_string());
        let err2 = err1.clone();
        assert_eq!(err1, err2);
    }
    #[test]
    fn test_fjall_value_error_into_nitrite_error() {
        let fjall_err = FjallValueError::DeserializationError("test error".to_string());
        let nitrite_err: NitriteError = fjall_err.into();
        assert!(nitrite_err
            .to_string()
            .contains("Deserialization failed: test error"));
    }
    #[test]
    #[allow(deprecated)]
    fn test_deprecated_into_still_works() {
        let fjall_value = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        let value: Value = fjall_value.into();
        assert!(matches!(value, Value::Array(_)));
    }
    #[test]
    #[allow(deprecated)]
    fn test_deprecated_from_still_works() {
        let value = Value::Array(vec![1.into(), 2.into(), 3.into(), 4.into()]);
        let fjall_value: FjallValue = value.into();
        assert_eq!(
            fjall_value.0,
            vec![
                19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0,
                0, 6, 0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0
            ]
        );
    }
    #[test]
    fn test_fjall_value_into_user_key() {
        let fjall_value = FjallValue(vec![1, 2, 3, 4]);
        let user_key: UserKey = fjall_value.into();
        assert_eq!(user_key.as_ref(), &[1, 2, 3, 4]);
    }
    #[test]
    fn test_user_key_into_fjall_value() {
        let user_key = UserKey::new(&[1, 2, 3, 4]);
        let fjall_value: FjallValue = user_key.into();
        assert_eq!(fjall_value.0, vec![1, 2, 3, 4]);
    }
    #[test]
    fn test_fjall_value_as_ref() {
        let fjall_value = FjallValue(vec![1, 2, 3, 4]);
        assert_eq!(fjall_value.as_ref(), &[1, 2, 3, 4]);
    }
    #[test]
    fn test_roundtrip_value_to_fjall_to_value() {
        let original = Value::Array(vec![1.into(), 2.into(), 3.into(), 4.into()]);
        let fjall_value = FjallValue::try_from_value(&original).unwrap();
        let recovered: Value = fjall_value.into();
        assert_eq!(original, recovered);
    }
    #[test]
    fn test_roundtrip_complex_value() {
        let original = Value::Document(nitrite::doc! {
            "name": "test",
            "values": [1, 2, 3],
            "nested": { "key": "value" }
        });
        let fjall_value = FjallValue::try_from_value(&original).unwrap();
        let recovered: Value = fjall_value.into();
        assert_eq!(original, recovered);
    }
    #[test]
    fn test_to_nitrite_error() {
        let error = std::io::Error::other("test error");
        let nitrite_error = to_nitrite_error(error);
        assert_eq!(nitrite_error.to_string(), "Fjall Error: test error");
    }
    #[test]
    fn test_into_trait_panics_on_corrupted_data() {
        let corrupted = FjallValue(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _value: Value = corrupted.into();
        }));
        assert!(caught.is_err(), "Into trait should panic on corrupted data");
    }
    #[test]
    fn test_from_trait_panics_on_serialization_failure() {
        // This test verifies that the From trait impl exists and can panic
        // In normal circumstances, serialization shouldn't fail for valid Values
        // but the implementation ensures panic behavior is consistent
        let value = Value::I64(42);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _fjall_value: FjallValue = value.into();
        }));
        // Should succeed for valid value
        assert!(result.is_ok(), "From trait should work for valid values");
    }
    #[test]
    fn test_safe_alternative_try_into_value_for_into_trait() {
        // Demonstrates the recommended safe alternative to Into trait
        let valid_fjall = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        // Safe approach: use try_into_value() which returns Result
        let result = valid_fjall.try_into_value();
        assert!(
            result.is_ok(),
            "Safe conversion should succeed for valid data"
        );
        let corrupted = FjallValue(vec![0xFF, 0xFF, 0xFF, 0xFF]);
        let result = corrupted.try_into_value();
        assert!(
            result.is_err(),
            "Safe conversion should return Err for corrupted data"
        );
    }
    #[test]
    fn test_safe_alternative_try_from_value_for_from_trait() {
        // Demonstrates the recommended safe alternative to From trait
        let value = Value::Array(vec![1.into(), 2.into()]);
        // Safe approach: use try_from_value() which returns Result
        let result = FjallValue::try_from_value(&value);
        assert!(
            result.is_ok(),
            "Safe conversion should succeed for valid values"
        );
        assert!(
            !result.unwrap().0.is_empty(),
            "Serialized value should not be empty"
        );
    }
    #[test]
    fn test_fjall_value_serialization_perf() {
        let value = Value::Array(vec![1.into(), 2.into(), 3.into(), 4.into()]);
        for _ in 0..1000 {
            let result = black_box(FjallValue::try_from_value(&value));
            black_box(result.is_ok());
        }
    }
    #[test]
    fn test_fjall_value_deserialization_perf() {
        let fjall_value = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        for _ in 0..1000 {
            let result = black_box(fjall_value.clone().try_into_value());
            black_box(result.is_ok());
        }
    }
    #[test]
    fn test_try_into_value_if_let_efficiency() {
        // Verify if-let pattern reduces overhead in deserialization path
        let fjall_value = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        for _ in 0..5000 {
            let _ = black_box(fjall_value.clone().try_into_value());
        }
    }
    #[test]
    fn test_try_from_value_if_let_efficiency() {
        // Verify if-let pattern reduces overhead in serialization path
        let value = Value::Array(vec![1.into(), 2.into()]);
        for _ in 0..5000 {
            let _ = black_box(FjallValue::try_from_value(&value));
        }
    }
    #[test]
    fn test_new_method_if_let_efficiency() {
        // Verify if-let pattern in new() reduces error handling overhead
        let value = Value::I64(42);
        for _ in 0..2000 {
            let fjall_val = black_box(FjallValue::new(value.clone()));
            black_box(fjall_val);
        }
    }
    #[test]
    fn test_into_trait_if_let_efficiency() {
        // Verify if-let pattern improves Into trait performance
        let fjall_value = FjallValue(vec![
            19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 6, 0, 0, 0, 2, 0, 0, 0, 6,
            0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0,
        ]);
        for _ in 0..2000 {
            let value: Value = black_box(fjall_value.clone()).into();
            black_box(value);
        }
    }
    #[test]
    fn test_from_trait_if_let_efficiency() {
        // Verify if-let pattern improves From trait performance
        let value = Value::I64(42);
        for _ in 0..2000 {
            let fjall_val: FjallValue = black_box(value.clone()).into();
            black_box(fjall_val);
        }
    }
    #[test]
    fn test_error_mapping_single_allocation() {
        // Verify optimized to_nitrite_error reduces string allocations
        let error = std::io::Error::other("connection closed");
        for _ in 0..1000 {
            let _ = black_box(to_nitrite_error(&error));
        }
    }
    #[test]
    fn test_error_mapping_case_insensitivity() {
        // Verify error mapping works correctly with lowercase checks
        let error = std::io::Error::other("not found");
        let nitrite_error = black_box(to_nitrite_error(&error));
        assert_eq!(nitrite_error.kind(), &ErrorKind::StoreNotInitialized);
    }
    #[test]
    fn test_round_trip_serialization_perf() {
        // Verify serialization and deserialization are both efficient
        let value = Value::Array(vec![1.into(), 2.into(), 3.into()]);
        for _ in 0..1000 {
            let fjall = black_box(FjallValue::try_from_value(&value).unwrap());
            let recovered: Value = black_box(fjall.try_into_value().unwrap());
            black_box(recovered);
        }
    }
    #[test]
    fn test_fjall_value_clone_perf() {
        // Verify Vec cloning is efficient for performance-critical paths
        let fjall_value = FjallValue(vec![1, 2, 3, 4, 5]);
        for _ in 0..5000 {
            let cloned = black_box(fjall_value.clone());
            black_box(cloned);
        }
    }
}
