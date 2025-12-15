use crate::collection::Document;
use crate::collection::NitriteId;
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use argon2::password_hash::Decimal;
use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;

/// Compare two integers represented as u128 for equality.
/// This handles cross-type comparison by converting to a common type.
#[inline]
fn num_eq_int(a: u128, b: u128) -> bool {
    a == b
}

/// Compare two floats for equality with proper NaN handling.
#[inline]
fn num_eq_float(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        true
    } else {
        a == b
    }
}

/// Compare two integers represented as u128.
#[inline]
fn num_cmp_int(a: u128, b: u128) -> std::cmp::Ordering {
    a.cmp(&b)
}

/// Compare two floats with proper NaN and total ordering.
#[inline]
fn num_cmp_float(a: f64, b: f64) -> std::cmp::Ordering {
    // Handle NaN: treat NaN as greater than all other values
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// Represents a [Document] value. It can be a simple value like [Value::I32], [Value::String] or
/// a complex value like [Value::Document] or [Value::Array].
///
/// # Purpose
/// Provides a unified representation for all value types that can be stored in Nitrite documents.
/// Supports native Rust types (integers, floats, strings, booleans) and complex types
/// (documents, arrays, maps), as well as Nitrite-specific types (NitriteId).
///
/// # Variants
/// - Null: Absence of a value
/// - Bool(bool): Boolean true/false
/// - I8-U128: Integer types with various bit widths (8, 16, 32, 64, 128 bits, signed/unsigned)
/// - ISize/USize: Platform-dependent integer types
/// - F32/F64: Floating point types (32-bit and 64-bit)
/// - Char(char): Single Unicode character
/// - String(String): Text value
/// - Document(Document): Nested document/object
/// - Array(Vec<Value>): Ordered collection of values
/// - Map(BTreeMap<Value, Value>): Key-value mapping structure
/// - NitriteId(NitriteId): Database-generated unique identifier
/// - Bytes(Vec<u8>): Binary data (not indexable/queryable)
/// - Unknown: Unrecognized value type
///
/// # Characteristics
/// - **Flexible**: Supports any JSON-compatible type plus Nitrite-specific types
/// - **Type-safe**: Each variant explicitly represents its type
/// - **Comparable**: Implements Ord for sorting and comparisons
/// - **Serializable**: Can be serialized/deserialized with serde
/// - **Default**: Defaults to Null
///
/// # Usage
/// Create values using From trait, from() helper, or val! macro:
/// ```text
/// let v1: Value = 42.into();           // From i32
/// let v2 = Value::from("hello");       // From &str
/// let v3 = val!(true);                 // Using macro
/// let doc = doc! { "age": 42, "name": "Alice" };
/// ```
///
/// Access values using as_* methods (returns Option if type matches):
/// ```text
/// if let Some(name) = doc.get("name").and_then(|v| v.as_string()) {
///     println!("Name: {}", name);
/// }
/// ```
#[derive(Clone, Default, serde::Deserialize, serde::Serialize)]
pub enum Value {
    /// Represents a null value.
    #[default]
    Null,
    /// Represents a boolean value.
    Bool(bool),
    /// Represents a signed 8-bit integer value.
    I8(i8),
    /// Represents an unsigned 8-bit integer value.
    U8(u8),
    /// Represents a signed 16-bit integer value.
    I16(i16),
    /// Represents an unsigned 16-bit integer value.
    U16(u16),
    /// Represents a signed 32-bit integer value.
    I32(i32),
    /// Represents an unsigned 32-bit integer value.
    U32(u32),
    /// Represents a signed 64-bit integer value.
    I64(i64),
    /// Represents an unsigned 64-bit integer value.
    U64(u64),
    /// Represents a signed 128-bit integer value.
    I128(i128),
    /// Represents an unsigned 128-bit integer value.
    U128(u128),
    /// Represents a signed isize value.
    ISize(isize),
    /// Represents an unsigned isize value.
    USize(usize),
    /// Represents a 32-bit floating point value.
    F32(f32),
    /// Represents a 64-bit floating point value.
    F64(f64),
    /// Represents a character value.
    Char(char),
    /// Represents a string value.
    String(String),
    /// Represents a document value.
    Document(Document),
    /// Represents an array value.
    Array(Vec<Value>),
    /// Represents a map.
    Map(BTreeMap<Value, Value>),
    /// Represents a NitriteId value.
    NitriteId(NitriteId),
    /// Represents a byte array value.
    /// It will be used for binary data. It cannot be indexed or queried.
    Bytes(Vec<u8>),
    /// Represents an unknown value.
    Unknown,
}

/// Type alias for map and document keys.
///
/// # Purpose
/// Alias for `Value` used as keys in map structures. Since `Value` is used for both
/// document values and map keys, this type alias provides semantic clarity that a
/// `Value` is being used as a key rather than a regular value.
///
/// # Characteristics
/// - **Same as Value**: Supports all Value types as keys
/// - **Comparable**: Implements Ord for key ordering
/// - **Hashable**: Implements Hash for use in hash-based structures
/// - **Generic**: Works with any Value variant that implements the required traits
///
/// # Usage
/// Used with the `key!` macro for convenient key creation:
/// ```text
/// let k = key!("field_name");  // Create a string key
/// let k = key!(42);             // Create an integer key
/// ```
pub type Key = Value;

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_debug_string(0))
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_pretty_json(0))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        if self.is_integer() && other.is_integer() {
            let self_int = self.as_integer();
            let other_int = other.as_integer();

            if let (Some(self_int), Some(other_int)) = (self_int, other_int) {
                return num_eq_int(self_int, other_int);
            }
        }

        if self.is_decimal() && other.is_decimal() {
            let self_decimal = self.as_decimal();
            let other_decimal = other.as_decimal();

            if let (Some(self_decimal), Some(other_decimal)) = (self_decimal, other_decimal) {
                return num_eq_float(self_decimal, other_decimal);
            }
        }

        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => *a == *b,
            (Value::Char(a), Value::Char(b)) => *a == *b,
            (Value::String(a), Value::String(b)) => *a == *b,
            (Value::Document(a), Value::Document(b)) => *a == *b,
            (Value::Array(a), Value::Array(b)) => *a == *b,
            (Value::Map(a), Value::Map(b)) => *a == *b,
            (Value::NitriteId(a), Value::NitriteId(b)) => *a == *b,
            (Value::Bytes(a), Value::Bytes(b)) => *a == *b,
            (Value::Unknown, Value::Unknown) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.is_integer() && other.is_integer() {
            let self_int = self.as_integer();
            let other_int = other.as_integer();

            if let (Some(self_int), Some(other_int)) = (self_int, other_int) {
                return num_cmp_int(self_int, other_int);
            }
        }

        if self.is_decimal() && other.is_decimal() {
            let self_decimal = self.as_decimal();
            let other_decimal = other.as_decimal();

            if let (Some(self_decimal), Some(other_decimal)) = (self_decimal, other_decimal) {
                return num_cmp_float(self_decimal, other_decimal);
            }
        }

        match (self, other) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Char(a), Value::Char(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Document(a), Value::Document(b)) => a.cmp(b),
            (Value::Array(a), Value::Array(b)) => a.cmp(b),
            (Value::Map(a), Value::Map(b)) => a.cmp(b),
            (Value::NitriteId(a), Value::NitriteId(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Unknown, Value::Unknown) => std::cmp::Ordering::Equal,
            _ => self.to_string().cmp(&other.to_string()), // fallback to string comparison
        }
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => (&"null_value").hash(state),
            Value::Bool(v) => v.hash(state),
            Value::I8(v) => v.hash(state),
            Value::U8(v) => v.hash(state),
            Value::I16(v) => v.hash(state),
            Value::U16(v) => v.hash(state),
            Value::I32(v) => v.hash(state),
            Value::U32(v) => v.hash(state),
            Value::I64(v) => v.hash(state),
            Value::U64(v) => v.hash(state),
            Value::I128(v) => v.hash(state),
            Value::U128(v) => v.hash(state),
            Value::ISize(v) => v.hash(state),
            Value::USize(v) => v.hash(state),
            Value::F32(v) => v.to_bits().hash(state),
            Value::F64(v) => v.to_bits().hash(state),
            Value::Char(v) => v.hash(state),
            Value::String(v) => v.hash(state),
            Value::Document(v) => v.hash(state),
            Value::Array(v) => v.hash(state),
            Value::Map(v) => v.hash(state),
            Value::NitriteId(v) => v.hash(state),
            Value::Bytes(v) => v.hash(state),
            Value::Unknown => (&"unknown_value").hash(state),
        }
    }
}

impl Value {
    /// Creates a new [Value] from the given value using runtime type inspection.
    ///
    /// # Arguments
    /// * `value` - Any Rust value implementing `Any` trait.
    ///
    /// # Returns
    /// `Ok(Value)` containing the converted value, or an error if type is unsupported.
    ///
    /// # Behavior
    /// Uses Rust's `Any` trait for runtime type identification. Converts native Rust types
    /// to corresponding Value variants. Returns an error for unsupported types.
    /// Supports all primitive types, String, Document, Vec<Value>, BTreeMap, NitriteId, and Vec<u8>.
    pub fn new<T: Any>(value: T) -> NitriteResult<Value> {
        let any = &value as &dyn Any;

        if any.downcast_ref::<()>().is_some() {
            Ok(Value::Null)
        } else if let Some(v) = any.downcast_ref::<bool>() {
            Ok(Value::Bool(*v))
        } else if let Some(v) = any.downcast_ref::<i8>() {
            Ok(Value::I8(*v))
        } else if let Some(v) = any.downcast_ref::<u8>() {
            Ok(Value::U8(*v))
        } else if let Some(v) = any.downcast_ref::<i16>() {
            Ok(Value::I16(*v))
        } else if let Some(v) = any.downcast_ref::<u16>() {
            Ok(Value::U16(*v))
        } else if let Some(v) = any.downcast_ref::<i32>() {
            Ok(Value::I32(*v))
        } else if let Some(v) = any.downcast_ref::<u32>() {
            Ok(Value::U32(*v))
        } else if let Some(v) = any.downcast_ref::<i64>() {
            Ok(Value::I64(*v))
        } else if let Some(v) = any.downcast_ref::<u64>() {
            Ok(Value::U64(*v))
        } else if let Some(v) = any.downcast_ref::<i128>() {
            Ok(Value::I128(*v))
        } else if let Some(v) = any.downcast_ref::<u128>() {
            Ok(Value::U128(*v))
        } else if let Some(v) = any.downcast_ref::<isize>() {
            Ok(Value::ISize(*v))
        } else if let Some(v) = any.downcast_ref::<usize>() {
            Ok(Value::USize(*v))
        } else if let Some(v) = any.downcast_ref::<f32>() {
            Ok(Value::F32(*v))
        } else if let Some(v) = any.downcast_ref::<f64>() {
            Ok(Value::F64(*v))
        } else if let Some(v) = any.downcast_ref::<char>() {
            Ok(Value::Char(*v))
        } else if let Some(v) = any.downcast_ref::<String>() {
            Ok(Value::String(v.clone()))
        } else if let Some(v) = any.downcast_ref::<Document>() {
            Ok(Value::Document(v.clone()))
        } else if let Some(v) = any.downcast_ref::<Vec<Value>>() {
            Ok(Value::Array(v.clone()))
        } else if let Some(v) = any.downcast_ref::<BTreeMap<Value, Value>>() {
            Ok(Value::Map(v.clone()))
        } else if let Some(v) = any.downcast_ref::<NitriteId>() {
            Ok(Value::NitriteId(*v))
        } else if let Some(v) = any.downcast_ref::<Vec<u8>>() {
            Ok(Value::Bytes(v.clone()))
        } else if let Some(v) = any.downcast_ref::<Value>() {
            Ok(v.clone())
        } else {
            log::error!("Unknown type to convert to Value");
            Err(NitriteError::new(
                "Unknown type to convert to Value",
                ErrorKind::ObjectMappingError,
            ))
        }
    }

    /// Creates a new [Value] from the given value that implements [`Into<Value>`].
    ///
    /// # Arguments
    /// * `value` - Any type implementing `Into<Value>`.
    ///
    /// # Returns
    /// A new `Value` converted from the input.
    ///
    /// # Behavior
    /// Direct conversion using the Into trait. Preferred for known types that have
    /// From<T> for Value implementations. More efficient than `new()` as it avoids
    /// runtime type inspection.
    ///
    /// # Example Usage
    /// From actual nitrite-int-test patterns:
    /// ```text
    /// Value::from(42)                    // i32
    /// Value::from("hello")               // &str
    /// Value::from(true)                  // bool
    /// Value::from(format!("val_{}", i)) // String
    /// ```
    pub fn from<T: Into<Value>>(value: T) -> Value {
        value.into()
    }

    /// Creates a new [Value] from the given [Option] value. If the value is [Some], it will be
    /// converted to [Value]. If the value is [None], it will be converted to [Value::Null].
    ///
    /// # Arguments
    /// * `value` - An Optional value.
    ///
    /// # Returns
    /// `Value::Null` if input is None, otherwise the converted Some value.
    ///
    /// # Behavior
    /// Converts None to Null and Some(T) to Value. Useful for handling optional fields
    /// in documents where missing values should be Null.
    pub fn from_option<T: Into<Value>>(value: Option<T>) -> Value {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }

    /// Creates a new [Value] from the vector of values.
    ///
    /// # Arguments
    /// * `values` - A vector of values that implement `Into<Value>`.
    ///
    /// # Returns
    /// A `Value::Array` containing the converted values.
    ///
    /// # Behavior
    /// Converts each element in the vector using Into trait and wraps them in Value::Array.
    /// More convenient than manually creating Value::Array for common cases.
    pub fn from_vec<T: Into<Value>>(values: Vec<T>) -> Value {
        Value::Array(values.into_iter().map(|v| v.into()).collect())
    }

    /// Returns the boolean value if the [Value] is [Value::Bool].
    #[inline]
    pub fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Bool(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the i8 value if the [Value] is [Value::I8].
    #[inline]
    pub fn as_i8(&self) -> Option<&i8> {
        match self {
            Value::I8(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the u8 value if the [Value] is [Value::U8].
    #[inline]
    pub fn as_u8(&self) -> Option<&u8> {
        match self {
            Value::U8(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the i16 value if the [Value] is [Value::I16].
    #[inline]
    pub fn as_i16(&self) -> Option<&i16> {
        match self {
            Value::I16(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the u16 value if the [Value] is [Value::U16].
    #[inline]
    pub fn as_u16(&self) -> Option<&u16> {
        match self {
            Value::U16(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the i32 value if the [Value] is [Value::I32].
    #[inline]
    pub fn as_i32(&self) -> Option<&i32> {
        match self {
            Value::I32(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the u32 value if the [Value] is [Value::U32].
    #[inline]
    pub fn as_u32(&self) -> Option<&u32> {
        match self {
            Value::U32(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the i64 value if the [Value] is [Value::I64].
    #[inline]
    pub fn as_i64(&self) -> Option<&i64> {
        match self {
            Value::I64(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the u64 value if the [Value] is [Value::U64].
    #[inline]
    pub fn as_u64(&self) -> Option<&u64> {
        match self {
            Value::U64(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the i128 value if the [Value] is [Value::I128].
    #[inline]
    pub fn as_i128(&self) -> Option<&i128> {
        match self {
            Value::I128(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the u128 value if the [Value] is [Value::U128].
    #[inline]
    pub fn as_u128(&self) -> Option<&u128> {
        match self {
            Value::U128(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the isize value if the [Value] is [Value::ISize].
    #[inline]
    pub fn as_isize(&self) -> Option<&isize> {
        match self {
            Value::ISize(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the usize value if the [Value] is [Value::USize].
    #[inline]
    pub fn as_usize(&self) -> Option<&usize> {
        match self {
            Value::USize(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the f32 value if the [Value] is [Value::F32].
    #[inline]
    pub fn as_f32(&self) -> Option<&f32> {
        match self {
            Value::F32(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the f64 value if the [Value] is [Value::F64].
    #[inline]
    pub fn as_f64(&self) -> Option<&f64> {
        match self {
            Value::F64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_integer(&self) -> Option<u128> {
        match self {
            Value::I8(v) => Some(*v as u128),
            Value::U8(v) => Some(*v as u128),
            Value::I16(v) => Some(*v as u128),
            Value::U16(v) => Some(*v as u128),
            Value::I32(v) => Some(*v as u128),
            Value::U32(v) => Some(*v as u128),
            Value::I64(v) => Some(*v as u128),
            Value::U64(v) => Some(*v as u128),
            Value::I128(v) => Some(*v as u128),
            Value::U128(v) => Some(*v),
            Value::ISize(v) => Some(*v as u128),
            Value::USize(v) => Some(*v as u128),
            _ => None,
        }
    }

    #[inline]
    pub fn as_decimal(&self) -> Option<f64> {
        match self {
            Value::F32(v) => Some(*v as f64),
            Value::F64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the char value if the [Value] is [Value::Char].
    #[inline]
    pub fn as_char(&self) -> Option<&char> {
        match self {
            Value::Char(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the string value if the [Value] is [Value::String].
    ///
    /// # Returns
    /// `Some(&String)` if this is a string value, `None` otherwise.
    ///
    /// # Behavior
    /// Type-safe string accessor. Used in actual tests to extract string values after
    /// retrieval from documents. Returns a reference to the contained String without cloning.
    ///
    /// # Example Usage
    /// From actual nitrite-int-test patterns:
    /// ```text
    /// if let Some(name) = value.as_string() {
    ///     println!("Name: {}", name);
    /// }
    /// ```
    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the object value if the [Value] is [Value::Document].
    ///
    /// # Returns
    /// `Some(&Document)` if this is a document value, `None` otherwise.
    ///
    /// # Behavior
    /// Type-safe document accessor. Used to extract nested documents or to work with
    /// complex structures. Returns a reference to the contained Document without cloning.
    ///
    /// # Example Usage
    /// From actual nitrite-int-test patterns:
    /// ```text
    /// let document = value.as_document().unwrap();
    /// let nested_field = document.get("nested_key");
    /// ```
    #[inline]
    pub fn as_document(&self) -> Option<&Document> {
        match self {
            Value::Document(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the mutable object value if the [Value] is [Value::Document].
    #[inline]
    pub fn as_document_mut(&mut self) -> Option<&mut Document> {
        match self {
            Value::Document(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the array value if the [Value] is [Value::Array].
    ///
    /// # Returns
    /// `Some(&Vec<Value>)` if this is an array value, `None` otherwise.
    ///
    /// # Behavior
    /// Type-safe array accessor. Returns a reference to the contained Vec without cloning.
    /// Useful for iterating over array elements or checking array length.
    #[inline]
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the mutable array value if the [Value] is [Value::Array].
    #[inline]
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Value>> {
        match self {
            Value::Array(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_map(&self) -> Option<&BTreeMap<Value, Value>> {
        match self {
            Value::Map(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_map_mut(&mut self) -> Option<&mut BTreeMap<Value, Value>> {
        match self {
            Value::Map(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the [NitriteId] value if the [Value] is [Value::NitriteId].
    ///
    /// # Returns
    /// `Some(&NitriteId)` if this is a NitriteId value, `None` otherwise.
    ///
    /// # Behavior
    /// Type-safe NitriteId accessor. Used to extract database-generated unique identifiers.
    /// Returns a reference without cloning.
    #[inline]
    pub fn as_nitrite_id(&self) -> Option<&NitriteId> {
        match self {
            Value::NitriteId(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the byte array value if the [Value] is [Value::Bytes].
    ///
    /// # Returns
    /// `Some(&Vec<u8>)` if this is a bytes value, `None` otherwise.
    ///
    /// # Behavior
    /// Type-safe bytes accessor. Bytes values cannot be indexed or queried, used only
    /// for binary data storage. Returns a reference without cloning.
    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    /// Checks if the [Value] is [Value::Null].
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Checks if the [Value] is [Value::Bool].
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Checks if the [Value] is [Value::I8].
    #[inline]
    pub fn is_i8(&self) -> bool {
        matches!(self, Value::I8(_))
    }

    /// Checks if the [Value] is [Value::U8].
    #[inline]
    pub fn is_u8(&self) -> bool {
        matches!(self, Value::U8(_))
    }

    /// Checks if the [Value] is [Value::I16].
    #[inline]
    pub fn is_i16(&self) -> bool {
        matches!(self, Value::I16(_))
    }

    /// Checks if the [Value] is [Value::U16].
    #[inline]
    pub fn is_u16(&self) -> bool {
        matches!(self, Value::U16(_))
    }

    /// Checks if the [Value] is [Value::I32].
    #[inline]
    pub fn is_i32(&self) -> bool {
        matches!(self, Value::I32(_))
    }

    /// Checks if the [Value] is [Value::U32].
    #[inline]
    pub fn is_u32(&self) -> bool {
        matches!(self, Value::U32(_))
    }

    /// Checks if the [Value] is [Value::I64].
    #[inline]
    pub fn is_i64(&self) -> bool {
        matches!(self, Value::I64(_))
    }

    /// Checks if the [Value] is [Value::U64].
    #[inline]
    pub fn is_u64(&self) -> bool {
        matches!(self, Value::U64(_))
    }

    /// Checks if the [Value] is [Value::I128].
    #[inline]
    pub fn is_i128(&self) -> bool {
        matches!(self, Value::I128(_))
    }

    /// Checks if the [Value] is [Value::U128].
    #[inline]
    pub fn is_u128(&self) -> bool {
        matches!(self, Value::U128(_))
    }

    /// Checks if the [Value] is [Value::ISize].
    #[inline]
    pub fn is_isize(&self) -> bool {
        matches!(self, Value::ISize(_))
    }

    /// Checks if the [Value] is [Value::USize].
    #[inline]
    pub fn is_usize(&self) -> bool {
        matches!(self, Value::USize(_))
    }

    /// Checks if the [Value] is [Value::F32].
    #[inline]
    pub fn is_f32(&self) -> bool {
        matches!(self, Value::F32(_))
    }

    /// Checks if the [Value] is [Value::F64].
    #[inline]
    pub fn is_f64(&self) -> bool {
        matches!(self, Value::F64(_))
    }

    /// Checks if the [Value] is [Value::String].
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Checks if the [Value] is [Value::Document].
    #[inline]
    pub fn is_document(&self) -> bool {
        matches!(self, Value::Document(_))
    }

    /// Checks if the [Value] is [Value::Array].
    #[inline]
    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    /// Checks if the [Value] is [Value::Map].
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    /// Checks if the [Value] is [Value::NitriteId].
    #[inline]
    pub fn is_nitrite_id(&self) -> bool {
        matches!(self, Value::NitriteId(_))
    }

    /// Checks if the [Value] is [Value::Bytes].
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytes(_))
    }

    /// Checks if the [Value] is [Value::Unknown].
    #[inline]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Value::Unknown)
    }

    /// Checks if the [Value] is a number type.
    #[inline]
    pub fn is_number(&self) -> bool {
        matches!(
            self,
            Value::I8(_)
                | Value::U8(_)
                | Value::I16(_)
                | Value::U16(_)
                | Value::I32(_)
                | Value::U32(_)
                | Value::I64(_)
                | Value::U64(_)
                | Value::I128(_)
                | Value::U128(_)
                | Value::ISize(_)
                | Value::USize(_)
                | Value::F32(_)
                | Value::F64(_)
        )
    }

    /// Checks if the [Value] is an integer type.
    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Value::I8(_)
                | Value::U8(_)
                | Value::I16(_)
                | Value::U16(_)
                | Value::I32(_)
                | Value::U32(_)
                | Value::I64(_)
                | Value::U64(_)
                | Value::I128(_)
                | Value::U128(_)
                | Value::ISize(_)
                | Value::USize(_)
        )
    }

    #[inline]
    pub fn is_comparable(&self) -> bool {
        matches!(
            self,
            Value::I8(_)
                | Value::U8(_)
                | Value::I16(_)
                | Value::U16(_)
                | Value::I32(_)
                | Value::U32(_)
                | Value::I64(_)
                | Value::U64(_)
                | Value::I128(_)
                | Value::U128(_)
                | Value::ISize(_)
                | Value::USize(_)
                | Value::F32(_)
                | Value::F64(_)
                | Value::Char(_)
                | Value::String(_)
                | Value::NitriteId(_)
                | Value::Bool(_)
                | Value::Null
        )
    }

    /// Checks if the [Value] is a decimal type.
    #[inline]
    pub fn is_decimal(&self) -> bool {
        matches!(self, Value::F32(_) | Value::F64(_))
    }

    /// Takes the value, replacing it with [Value::Null].
    ///
    /// # Returns
    /// The original value, leaving `Value::Null` in its place.
    ///
    /// # Behavior
    /// Consumes the value and replaces self with Null using `std::mem::replace`.
    /// Useful for extracting a value from mutable reference while leaving placeholder behind.
    /// Avoids cloning when moving a value out of a document field.
    pub fn take(&mut self) -> Value {
        std::mem::replace(self, Value::Null)
    }

    pub(crate) fn to_pretty_json(&self, indent: usize) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(v) => v.to_string(),
            Value::I8(v) => v.to_string(),
            Value::U8(v) => v.to_string(),
            Value::I16(v) => v.to_string(),
            Value::U16(v) => v.to_string(),
            Value::I32(v) => v.to_string(),
            Value::U32(v) => v.to_string(),
            Value::I64(v) => v.to_string(),
            Value::U64(v) => v.to_string(),
            Value::I128(v) => v.to_string(),
            Value::U128(v) => v.to_string(),
            Value::ISize(v) => v.to_string(),
            Value::USize(v) => v.to_string(),
            Value::F32(v) => v.to_string(),
            Value::F64(v) => v.to_string(),
            Value::Char(v) => format!("\"{}\"", v),
            Value::String(v) => format!("\"{}\"", v),
            Value::Document(v) => {
                let doc = v.clone();
                doc.to_pretty_json(indent)
            }
            Value::Array(v) => {
                if v.is_empty() {
                    return "[]".to_string();
                }

                let mut json_str = String::new();
                json_str.push_str("[\n");
                let indent_str = " ".repeat(indent + 2);
                for value in v {
                    json_str.push_str(&format!(
                        "{}{},\n",
                        indent_str,
                        value.to_pretty_json(indent + 2)
                    ));
                }
                json_str.pop(); // remove last comma
                json_str.pop(); // remove last newline
                json_str.push_str(&format!("\n{}]", " ".repeat(indent)));
                json_str
            }
            Value::Map(v) => {
                if v.is_empty() {
                    return "{}".to_string();
                }

                let mut json_str = String::new();
                json_str.push_str("{\n");
                let indent_str = " ".repeat(indent + 2);
                for (key, value) in v {
                    json_str.push_str(&format!(
                        "{}{}: {},\n",
                        indent_str,
                        key.to_pretty_json(indent + 2),
                        value.to_pretty_json(indent + 2)
                    ));
                }
                json_str.pop(); // remove last comma
                json_str.pop(); // remove last newline
                json_str.push_str(&format!("\n{}]", " ".repeat(indent)));
                json_str
            }
            Value::NitriteId(v) => format!("\"{}\"", v),
            Value::Bytes(v) => {
                if v.is_empty() {
                    return "[]".to_string();
                }

                let mut json_str = String::new();
                json_str.push_str("[\n");
                let indent_str = " ".repeat(indent + 2);
                for value in v {
                    json_str.push_str(&format!("{}{},\n", indent_str, value));
                }
                json_str.pop(); // remove last comma
                json_str.pop(); // remove last newline
                json_str.push_str(&format!("\n{}]", " ".repeat(indent)));
                json_str
            }
            Value::Unknown => "unknown".to_string(),
        }
    }

    pub(crate) fn to_debug_string(&self, indent: usize) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(v) => format!("bool({})", v),
            Value::I8(v) => format!("i8({})", v),
            Value::U8(v) => format!("u8({})", v),
            Value::I16(v) => format!("i16({})", v),
            Value::U16(v) => format!("u16({})", v),
            Value::I32(v) => format!("i32({})", v),
            Value::U32(v) => format!("u32({})", v),
            Value::I64(v) => format!("i64({})", v),
            Value::U64(v) => format!("u64({})", v),
            Value::I128(v) => format!("i128({})", v),
            Value::U128(v) => format!("u128({})", v),
            Value::ISize(v) => format!("isize({})", v),
            Value::USize(v) => format!("usize({})", v),
            Value::F32(v) => format!("f32({})", v),
            Value::F64(v) => format!("f64({})", v),
            Value::Char(v) => format!("char(\"{}\")", v),
            Value::String(v) => format!("string(\"{}\")", v),
            Value::Document(v) => {
                let doc = v.clone();
                format!("object({})", doc.to_debug_string(indent))
            }
            Value::Array(v) => {
                if v.is_empty() {
                    return "array([])".to_string();
                }

                let mut debug_str = String::new();
                debug_str.push_str("array([\n");
                let indent_str = " ".repeat(indent + 2);
                for value in v {
                    debug_str.push_str(&format!(
                        "{}{},\n",
                        indent_str,
                        value.to_debug_string(indent + 2)
                    ));
                }
                debug_str.pop(); // remove last comma
                debug_str.pop(); // remove last newline
                debug_str.push_str(&format!("\n{}])", " ".repeat(indent)));
                debug_str
            }
            Value::Map(v) => {
                if v.is_empty() {
                    return "map({})".to_string();
                }

                let mut debug_str = String::new();
                debug_str.push_str("map({\n");
                let indent_str = " ".repeat(indent + 2);
                for (key, value) in v {
                    debug_str.push_str(&format!(
                        "{}{}: {},\n",
                        indent_str,
                        key.to_debug_string(indent + 2),
                        value.to_debug_string(indent + 2)
                    ));
                }
                debug_str.pop(); // remove last comma
                debug_str.pop(); // remove last newline
                debug_str.push_str(&format!("\n{}])", " ".repeat(indent)));
                debug_str
            }
            Value::NitriteId(v) => format!("nitrite_id(\"{}\")", v),
            Value::Bytes(v) => {
                if v.is_empty() {
                    return "bytes([])".to_string();
                }

                let mut debug_str = String::new();
                debug_str.push_str("bytes([\n");
                let indent_str = " ".repeat(indent + 2);
                for value in v {
                    debug_str.push_str(&format!("{}{},\n", indent_str, value));
                }
                debug_str.pop(); // remove last comma
                debug_str.pop(); // remove last newline
                debug_str.push_str(&format!("\n{}])", " ".repeat(indent)));
                debug_str
            }
            Value::Unknown => "unknown".to_string(),
        }
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<i8> for Value {
    #[inline]
    fn from(value: i8) -> Self {
        Value::I8(value)
    }
}

impl From<u8> for Value {
    #[inline]
    fn from(value: u8) -> Self {
        Value::U8(value)
    }
}

impl From<i16> for Value {
    #[inline]
    fn from(value: i16) -> Self {
        Value::I16(value)
    }
}

impl From<u16> for Value {
    #[inline]
    fn from(value: u16) -> Self {
        Value::U16(value)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(value: i32) -> Self {
        Value::I32(value)
    }
}

impl From<u32> for Value {
    #[inline]
    fn from(value: u32) -> Self {
        Value::U32(value)
    }
}

impl From<i64> for Value {
    #[inline]
    fn from(value: i64) -> Self {
        Value::I64(value)
    }
}

impl From<u64> for Value {
    #[inline]
    fn from(value: u64) -> Self {
        Value::U64(value)
    }
}

impl From<i128> for Value {
    #[inline]
    fn from(value: i128) -> Self {
        Value::I128(value)
    }
}

impl From<u128> for Value {
    #[inline]
    fn from(value: u128) -> Self {
        Value::U128(value)
    }
}

impl From<isize> for Value {
    #[inline]
    fn from(value: isize) -> Self {
        Value::ISize(value)
    }
}

impl From<usize> for Value {
    #[inline]
    fn from(value: usize) -> Self {
        Value::USize(value)
    }
}

impl From<f32> for Value {
    #[inline]
    fn from(value: f32) -> Self {
        Value::F32(value)
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(value: f64) -> Self {
        Value::F64(value)
    }
}

impl From<char> for Value {
    fn from(value: char) -> Self {
        Value::Char(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<Document> for Value {
    fn from(value: Document) -> Self {
        Value::Document(value)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value> + 'static,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value> + 'static,
{
    fn from(value: Vec<T>) -> Self {
        if TypeId::of::<T>() == TypeId::of::<u8>() {
            let len = value.len();
            let cap = value.capacity();
            let ptr = value.as_ptr() as *mut u8;

            // SAFETY: We verified T is u8 via TypeId check above
            // Safe to reconstruct Vec<u8> with same memory layout
            let vec_u8 = unsafe {
                // Forget the original Vec to prevent double-free
                std::mem::forget(value);
                Vec::from_raw_parts(ptr, len, cap)
            };
            return Value::Bytes(vec_u8);
        }
        Value::Array(value.into_iter().map(|v| v.into()).collect())
    }
}

impl From<BTreeMap<Value, Value>> for Value {
    fn from(value: BTreeMap<Value, Value>) -> Self {
        Value::Map(value)
    }
}

impl From<NitriteId> for Value {
    fn from(value: NitriteId) -> Self {
        Value::NitriteId(value)
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null
    }
}

/// A macro to create a `Value` from a given expression.
///
/// This macro simplifies the creation of `Value` instances by automatically
/// converting the provided expression into a `Value` using the `From` trait.
///
/// # Examples
///
/// ```rust
/// use nitrite::common::Value;
/// use nitrite::val;
///
/// let int_value = val!(42);
/// assert_eq!(int_value, Value::I32(42));
///
/// let string_value = val!("hello");
/// assert_eq!(string_value, Value::String("hello".to_string()));
///
/// let bool_value = val!(true);
/// assert_eq!(bool_value, Value::Bool(true));
/// ```
#[macro_export]
macro_rules! val {
    ($value:expr) => {
        $crate::common::Value::from($value)
    };
}

#[macro_export]
macro_rules! key {
    ($value:expr) => {
        $crate::common::Key::from($value)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::util::document_from_map;
    use std::collections::BTreeMap;

    mod num_comparison_tests {
        use super::*;
        use std::cmp::Ordering;

        // =====================================================================
        // num_eq_int tests
        // =====================================================================

        #[test]
        fn test_num_eq_int_equal_values() {
            assert!(num_eq_int(0, 0));
            assert!(num_eq_int(1, 1));
            assert!(num_eq_int(100, 100));
            assert!(num_eq_int(u128::MAX, u128::MAX));
        }

        #[test]
        fn test_num_eq_int_different_values() {
            assert!(!num_eq_int(0, 1));
            assert!(!num_eq_int(1, 0));
            assert!(!num_eq_int(100, 200));
            assert!(!num_eq_int(u128::MAX, 0));
        }

        #[test]
        fn test_num_eq_int_boundary_values() {
            assert!(num_eq_int(0, 0));
            assert!(num_eq_int(u128::MAX, u128::MAX));
            assert!(!num_eq_int(u128::MAX, u128::MAX - 1));
            assert!(!num_eq_int(0, u128::MAX));
        }

        // =====================================================================
        // num_eq_float tests
        // =====================================================================

        #[test]
        fn test_num_eq_float_equal_values() {
            assert!(num_eq_float(0.0, 0.0));
            assert!(num_eq_float(1.0, 1.0));
            assert!(num_eq_float(-1.0, -1.0));
            assert!(num_eq_float(3.14159, 3.14159));
            assert!(num_eq_float(f64::MAX, f64::MAX));
            assert!(num_eq_float(f64::MIN, f64::MIN));
        }

        #[test]
        fn test_num_eq_float_different_values() {
            assert!(!num_eq_float(0.0, 1.0));
            assert!(!num_eq_float(1.0, -1.0));
            assert!(!num_eq_float(3.14, 3.15));
            assert!(!num_eq_float(f64::MAX, f64::MIN));
        }

        #[test]
        fn test_num_eq_float_nan_handling() {
            // Two NaNs should be equal (unlike standard float comparison)
            assert!(num_eq_float(f64::NAN, f64::NAN));

            // NaN should not equal any regular number
            assert!(!num_eq_float(f64::NAN, 0.0));
            assert!(!num_eq_float(0.0, f64::NAN));
            assert!(!num_eq_float(f64::NAN, 1.0));
            assert!(!num_eq_float(f64::NAN, f64::INFINITY));
        }

        #[test]
        fn test_num_eq_float_infinity_handling() {
            assert!(num_eq_float(f64::INFINITY, f64::INFINITY));
            assert!(num_eq_float(f64::NEG_INFINITY, f64::NEG_INFINITY));
            assert!(!num_eq_float(f64::INFINITY, f64::NEG_INFINITY));
            assert!(!num_eq_float(f64::INFINITY, 0.0));
        }

        #[test]
        fn test_num_eq_float_negative_zero() {
            // -0.0 and 0.0 should be equal
            assert!(num_eq_float(-0.0, 0.0));
            assert!(num_eq_float(0.0, -0.0));
        }

        // =====================================================================
        // num_cmp_int tests
        // =====================================================================

        #[test]
        fn test_num_cmp_int_equal() {
            assert_eq!(num_cmp_int(0, 0), Ordering::Equal);
            assert_eq!(num_cmp_int(100, 100), Ordering::Equal);
            assert_eq!(num_cmp_int(u128::MAX, u128::MAX), Ordering::Equal);
        }

        #[test]
        fn test_num_cmp_int_less() {
            assert_eq!(num_cmp_int(0, 1), Ordering::Less);
            assert_eq!(num_cmp_int(99, 100), Ordering::Less);
            assert_eq!(num_cmp_int(0, u128::MAX), Ordering::Less);
        }

        #[test]
        fn test_num_cmp_int_greater() {
            assert_eq!(num_cmp_int(1, 0), Ordering::Greater);
            assert_eq!(num_cmp_int(100, 99), Ordering::Greater);
            assert_eq!(num_cmp_int(u128::MAX, 0), Ordering::Greater);
        }

        #[test]
        fn test_num_cmp_int_boundary_values() {
            assert_eq!(num_cmp_int(u128::MAX - 1, u128::MAX), Ordering::Less);
            assert_eq!(num_cmp_int(u128::MAX, u128::MAX - 1), Ordering::Greater);
            assert_eq!(num_cmp_int(1, 0), Ordering::Greater);
        }

        // =====================================================================
        // num_cmp_float tests
        // =====================================================================

        #[test]
        fn test_num_cmp_float_equal() {
            assert_eq!(num_cmp_float(0.0, 0.0), Ordering::Equal);
            assert_eq!(num_cmp_float(1.0, 1.0), Ordering::Equal);
            assert_eq!(num_cmp_float(-1.0, -1.0), Ordering::Equal);
            assert_eq!(num_cmp_float(3.14159, 3.14159), Ordering::Equal);
        }

        #[test]
        fn test_num_cmp_float_less() {
            assert_eq!(num_cmp_float(0.0, 1.0), Ordering::Less);
            assert_eq!(num_cmp_float(-1.0, 0.0), Ordering::Less);
            assert_eq!(num_cmp_float(-1.0, 1.0), Ordering::Less);
            assert_eq!(num_cmp_float(f64::MIN, f64::MAX), Ordering::Less);
        }

        #[test]
        fn test_num_cmp_float_greater() {
            assert_eq!(num_cmp_float(1.0, 0.0), Ordering::Greater);
            assert_eq!(num_cmp_float(0.0, -1.0), Ordering::Greater);
            assert_eq!(num_cmp_float(1.0, -1.0), Ordering::Greater);
            assert_eq!(num_cmp_float(f64::MAX, f64::MIN), Ordering::Greater);
        }

        #[test]
        fn test_num_cmp_float_nan_handling() {
            // Two NaNs should be equal
            assert_eq!(num_cmp_float(f64::NAN, f64::NAN), Ordering::Equal);

            // NaN should be greater than any regular number
            assert_eq!(num_cmp_float(f64::NAN, 0.0), Ordering::Greater);
            assert_eq!(num_cmp_float(f64::NAN, f64::MAX), Ordering::Greater);
            assert_eq!(num_cmp_float(f64::NAN, f64::INFINITY), Ordering::Greater);

            // Any regular number should be less than NaN
            assert_eq!(num_cmp_float(0.0, f64::NAN), Ordering::Less);
            assert_eq!(num_cmp_float(f64::MAX, f64::NAN), Ordering::Less);
            assert_eq!(num_cmp_float(f64::INFINITY, f64::NAN), Ordering::Less);
        }

        #[test]
        fn test_num_cmp_float_infinity_handling() {
            // Positive infinity comparisons
            assert_eq!(num_cmp_float(f64::INFINITY, f64::INFINITY), Ordering::Equal);
            assert_eq!(num_cmp_float(f64::INFINITY, 0.0), Ordering::Greater);
            assert_eq!(num_cmp_float(0.0, f64::INFINITY), Ordering::Less);

            // Negative infinity comparisons
            assert_eq!(
                num_cmp_float(f64::NEG_INFINITY, f64::NEG_INFINITY),
                Ordering::Equal
            );
            assert_eq!(num_cmp_float(f64::NEG_INFINITY, 0.0), Ordering::Less);
            assert_eq!(num_cmp_float(0.0, f64::NEG_INFINITY), Ordering::Greater);

            // Positive vs negative infinity
            assert_eq!(
                num_cmp_float(f64::INFINITY, f64::NEG_INFINITY),
                Ordering::Greater
            );
            assert_eq!(
                num_cmp_float(f64::NEG_INFINITY, f64::INFINITY),
                Ordering::Less
            );
        }

        #[test]
        fn test_num_cmp_float_negative_zero() {
            // -0.0 and 0.0 should be equal
            assert_eq!(num_cmp_float(-0.0, 0.0), Ordering::Equal);
            assert_eq!(num_cmp_float(0.0, -0.0), Ordering::Equal);
        }

        #[test]
        fn test_num_cmp_float_small_differences() {
            // Very small positive differences
            assert_eq!(num_cmp_float(1.0, 1.0 + f64::EPSILON), Ordering::Less);
            assert_eq!(num_cmp_float(1.0 + f64::EPSILON, 1.0), Ordering::Greater);
        }
    }

    // =========================================================================
    // Integration tests for Value comparison using the inline functions
    // =========================================================================

    mod value_comparison_tests {
        use super::*;
        use std::cmp::Ordering;

        #[test]
        fn test_value_integer_equality() {
            // Same type
            assert_eq!(Value::I32(42), Value::I32(42));
            assert_eq!(Value::I64(100), Value::I64(100));

            // Cross-type comparison via as_integer
            assert_eq!(Value::I32(42), Value::I64(42));
            assert_eq!(Value::U8(255), Value::U64(255));
        }

        #[test]
        fn test_value_integer_inequality() {
            assert_ne!(Value::I32(42), Value::I32(43));
            assert_ne!(Value::I32(42), Value::I64(43));
        }

        #[test]
        fn test_value_float_equality() {
            assert_eq!(Value::F32(3.14), Value::F32(3.14));
            assert_eq!(Value::F64(3.14159), Value::F64(3.14159));
        }

        #[test]
        fn test_value_float_nan_equality() {
            // NaN values should be equal in our implementation
            assert_eq!(Value::F64(f64::NAN), Value::F64(f64::NAN));
            assert_eq!(Value::F32(f32::NAN), Value::F32(f32::NAN));
        }

        #[test]
        fn test_value_integer_ordering() {
            assert!(Value::I32(1) < Value::I32(2));
            assert!(Value::I64(100) > Value::I64(50));
            assert!(Value::I32(42) <= Value::I32(42));
            assert!(Value::I32(42) >= Value::I32(42));
        }

        #[test]
        fn test_value_float_ordering() {
            assert!(Value::F64(1.0) < Value::F64(2.0));
            assert!(Value::F64(100.0) > Value::F64(50.0));
            assert!(Value::F64(-1.0) < Value::F64(0.0));
        }

        #[test]
        fn test_value_float_nan_ordering() {
            // NaN should be greater than any number
            assert!(Value::F64(f64::NAN) > Value::F64(f64::MAX));
            assert!(Value::F64(f64::NAN) > Value::F64(f64::INFINITY));

            // Two NaNs should be equal
            assert_eq!(
                Value::F64(f64::NAN).cmp(&Value::F64(f64::NAN)),
                Ordering::Equal
            );
        }

        #[test]
        fn test_value_cross_integer_type_ordering() {
            // Different integer types should compare correctly
            assert!(Value::I8(10) < Value::I32(20));
            assert!(Value::U64(100) > Value::I8(50));
        }
    }

    fn create_test_objet() -> Document {
        let mut map = BTreeMap::new();
        map.insert("key1".to_string(), Value::I32(42));
        map.insert("key2".to_string(), Value::String("value".to_string()));
        document_from_map(&map).unwrap()
    }

    fn create_test_array() -> Vec<Value> {
        vec![Value::I32(42), Value::String("value".to_string())]
    }

    #[test]
    fn value_from_i8() {
        assert_eq!(Value::from(42i8), Value::I8(42));
    }

    #[test]
    fn value_from_u8() {
        assert_eq!(Value::from(42u8), Value::U8(42));
    }

    #[test]
    fn value_from_i16() {
        assert_eq!(Value::from(42i16), Value::I16(42));
    }

    #[test]
    fn value_from_u16() {
        assert_eq!(Value::from(42u16), Value::U16(42));
    }

    #[test]
    fn value_from_i32() {
        assert_eq!(Value::from(42i32), Value::I32(42));
    }

    #[test]
    fn value_from_u32() {
        assert_eq!(Value::from(42u32), Value::U32(42));
    }

    #[test]
    fn value_from_i64() {
        assert_eq!(Value::from(42i64), Value::I64(42));
    }

    #[test]
    fn value_from_u64() {
        assert_eq!(Value::from(42u64), Value::U64(42));
    }

    #[test]
    fn value_from_i128() {
        assert_eq!(Value::from(42i128), Value::I128(42));
    }

    #[test]
    fn value_from_u128() {
        assert_eq!(Value::from(42u128), Value::U128(42));
    }

    #[test]
    fn value_from_isize() {
        assert_eq!(Value::from(42isize), Value::ISize(42));
    }

    #[test]
    fn value_from_usize() {
        assert_eq!(Value::from(42usize), Value::USize(42));
    }

    #[test]
    fn value_from_f32() {
        assert_eq!(Value::from(42.0f32), Value::F32(42.0));
    }

    #[test]
    fn value_from_f64() {
        assert_eq!(Value::from(42.0f64), Value::F64(42.0));
    }

    #[test]
    fn value_from_string() {
        assert_eq!(Value::from("value"), Value::String("value".to_string()));
    }

    #[test]
    fn value_from_str() {
        assert_eq!(Value::from("value"), Value::String("value".to_string()));
    }

    #[test]
    fn value_from_document() {
        let map = create_test_objet();
        assert_eq!(Value::from(map.clone()), Value::Document(map));
    }

    #[test]
    fn value_from_vec() {
        let array = create_test_array();
        assert_eq!(Value::from(array.clone()), Value::Array(array));
    }

    #[test]
    fn value_from_bool() {
        assert_eq!(Value::from(true), Value::Bool(true));
    }

    #[test]
    fn value_from_nitrite_id() {
        let id = NitriteId::create_id(1234567890123456789).unwrap();
        assert_eq!(Value::from(id.clone()), Value::NitriteId(id));
    }

    #[test]
    fn value_from_bytes() {
        let bytes = vec![1, 2, 3];
        assert_eq!(Value::from(bytes.clone()), Value::Bytes(bytes));
    }

    #[test]
    fn value_from_unit() {
        assert_eq!(Value::from(()), Value::Null);
    }

    #[test]
    fn value_from_option_some() {
        assert_eq!(Value::from_option(Some(42i32)), Value::I32(42));
    }

    #[test]
    fn value_from_option_none() {
        assert_eq!(Value::from_option(None::<i32>), Value::Null);
    }

    #[test]
    fn value_is_null() {
        assert_eq!(Value::Null.is_null(), true);
    }

    #[test]
    fn value_is_bool() {
        assert_eq!(Value::Bool(true).is_bool(), true);
    }

    #[test]
    fn value_is_byte() {
        assert_eq!(Value::I8(42).is_i8(), true);
    }

    #[test]
    fn value_is_unsigned_byte() {
        assert_eq!(Value::U8(42).is_u8(), true);
    }

    #[test]
    fn value_is_short() {
        assert_eq!(Value::I16(42).is_i16(), true);
    }

    #[test]
    fn value_is_unsigned_short() {
        assert_eq!(Value::U16(42).is_u16(), true);
    }

    #[test]
    fn value_is_int() {
        assert_eq!(Value::I32(42).is_i32(), true);
    }

    #[test]
    fn value_is_unsigned_int() {
        assert_eq!(Value::U32(42).is_u32(), true);
    }

    #[test]
    fn value_is_long() {
        assert_eq!(Value::I64(42).is_i64(), true);
    }

    #[test]
    fn value_is_unsigned_long() {
        assert_eq!(Value::U64(42).is_u64(), true);
    }

    #[test]
    fn value_is_big_int() {
        assert_eq!(Value::I128(42).is_i128(), true);
    }

    #[test]
    fn value_is_unsigned_big_int() {
        assert_eq!(Value::U128(42).is_u128(), true);
    }

    #[test]
    fn value_is_size() {
        assert_eq!(Value::ISize(42).is_isize(), true);
    }

    #[test]
    fn value_is_unsigned_size() {
        assert_eq!(Value::USize(42).is_usize(), true);
    }

    #[test]
    fn value_is_float() {
        assert_eq!(Value::F32(42.0).is_f32(), true);
    }

    #[test]
    fn value_is_double() {
        assert_eq!(Value::F64(42.0).is_f64(), true);
    }

    #[test]
    fn value_is_string() {
        assert_eq!(Value::String("value".to_string()).is_string(), true);
    }

    #[test]
    fn value_is_object() {
        let map = create_test_objet();
        assert_eq!(Value::Document(map).is_document(), true);
    }

    #[test]
    fn value_is_array() {
        let array = create_test_array();
        assert_eq!(Value::Array(array).is_array(), true);
    }

    #[test]
    fn value_is_nitrite_id() {
        let id = NitriteId::new();
        assert_eq!(Value::NitriteId(id).is_nitrite_id(), true);
    }

    #[test]
    fn value_is_bytes() {
        let bytes = vec![1, 2, 3];
        assert_eq!(Value::Bytes(bytes).is_bytes(), true);
    }

    #[test]
    fn value_is_unknown() {
        assert_eq!(Value::Unknown.is_unknown(), true);
    }

    #[test]
    fn value_is_number() {
        assert_eq!(Value::I32(42).is_number(), true);
        assert_eq!(Value::F32(42.0).is_integer(), false);
    }

    #[test]
    fn value_is_integer() {
        assert_eq!(Value::I32(42).is_integer(), true);
        assert_eq!(Value::I64(42).is_integer(), true);
    }

    #[test]
    fn value_is_decimal() {
        assert_eq!(Value::F32(42.0).is_decimal(), true);
        assert_eq!(Value::F64(42.0).is_decimal(), true);
    }

    #[test]
    fn value_take() {
        let mut value = Value::I32(42);
        let taken = value.take();
        assert_eq!(value, Value::Null);
        assert_eq!(taken, Value::I32(42));
    }

    #[test]
    fn value_as_bool() {
        assert_eq!(Value::Bool(true).as_bool(), Some(&true));
        assert_eq!(Value::I32(42).as_bool(), None);
    }

    #[test]
    fn value_as_byte() {
        assert_eq!(Value::I8(42).as_i8(), Some(&42));
        assert_eq!(Value::I32(42).as_i8(), None);
    }

    #[test]
    fn value_as_unsigned_byte() {
        assert_eq!(Value::U8(42).as_u8(), Some(&42));
        assert_eq!(Value::I32(42).as_u8(), None);
    }

    #[test]
    fn value_as_short() {
        assert_eq!(Value::I16(42).as_i16(), Some(&42));
        assert_eq!(Value::I32(42).as_i16(), None);
    }

    #[test]
    fn value_as_unsigned_short() {
        assert_eq!(Value::U16(42).as_u16(), Some(&42));
        assert_eq!(Value::I32(42).as_u16(), None);
    }

    #[test]
    fn value_as_int() {
        assert_eq!(Value::I32(42).as_i32(), Some(&42));
        assert_eq!(Value::Bool(true).as_i32(), None);
    }

    #[test]
    fn value_as_unsigned_int() {
        assert_eq!(Value::U32(42).as_u32(), Some(&42));
        assert_eq!(Value::I32(42).as_u32(), None);
    }

    #[test]
    fn value_as_long() {
        assert_eq!(Value::I64(42).as_i64(), Some(&42));
        assert_eq!(Value::I32(42).as_i64(), None);
    }

    #[test]
    fn value_as_unsigned_long() {
        assert_eq!(Value::U64(42).as_u64(), Some(&42));
        assert_eq!(Value::I32(42).as_u64(), None);
    }

    #[test]
    fn value_as_big_int() {
        assert_eq!(Value::I128(42).as_i128(), Some(&42));
        assert_eq!(Value::I32(42).as_i128(), None);
    }

    #[test]
    fn value_as_unsigned_big_int() {
        assert_eq!(Value::U128(42).as_u128(), Some(&42));
        assert_eq!(Value::I32(42).as_u128(), None);
    }

    #[test]
    fn value_as_size() {
        assert_eq!(Value::ISize(42).as_isize(), Some(&42));
        assert_eq!(Value::I32(42).as_isize(), None);
    }

    #[test]
    fn value_as_unsigned_size() {
        assert_eq!(Value::USize(42).as_usize(), Some(&42));
        assert_eq!(Value::I32(42).as_usize(), None);
    }

    #[test]
    fn value_as_float() {
        assert_eq!(Value::F32(42.0).as_f32(), Some(&42.0));
        assert_eq!(Value::I32(42).as_f32(), None);
    }

    #[test]
    fn value_as_double() {
        assert_eq!(Value::F64(42.0).as_f64(), Some(&42.0));
        assert_eq!(Value::I32(42).as_f64(), None);
    }

    #[test]
    fn value_as_string() {
        assert_eq!(
            Value::String("value".to_string()).as_string(),
            Some(&"value".to_string())
        );
        assert_eq!(Value::I32(42).as_string(), None);
    }

    #[test]
    fn value_as_object() {
        let map = create_test_objet();
        assert_eq!(Value::Document(map.clone()).as_document(), Some(&map));
        assert_eq!(Value::I32(42).as_document(), None);
    }

    #[test]
    fn value_as_object_mut() {
        let mut map = create_test_objet();
        assert_eq!(
            Value::Document(map.clone()).as_document_mut(),
            Some(&mut map)
        );
        assert_eq!(Value::I32(42).as_document_mut(), None);
    }

    #[test]
    fn value_as_array() {
        let array = create_test_array();
        assert_eq!(Value::Array(array.clone()).as_array(), Some(&array));
        assert_eq!(Value::I32(42).as_array(), None);
    }

    #[test]
    fn value_as_array_mut() {
        let mut array = create_test_array();
        assert_eq!(Value::Array(array.clone()).as_array_mut(), Some(&mut array));
        assert_eq!(Value::I32(42).as_array_mut(), None);
    }

    #[test]
    fn value_as_nitrite_id() {
        let id = NitriteId::new();
        assert_eq!(Value::NitriteId(id.clone()).as_nitrite_id(), Some(&id));
        assert_eq!(Value::I32(42).as_nitrite_id(), None);
    }

    #[test]
    fn value_as_bytes() {
        let bytes = vec![1, 2, 3];
        assert_eq!(Value::Bytes(bytes.clone()).as_bytes(), Some(&bytes));
        assert_eq!(Value::I32(42).as_bytes(), None);
    }

    #[test]
    fn value_to_pretty_json() {
        let value = Value::I32(42);
        assert_eq!(value.to_pretty_json(0), "42");
    }

    #[test]
    fn value_to_debug_string() {
        let value = Value::I32(42);
        assert_eq!(value.to_debug_string(0), "i32(42)");
    }

    #[test]
    fn value_eq() {
        assert_eq!(Value::I32(42), Value::I32(42));
        assert_ne!(Value::I32(42), Value::I32(43));
    }

    #[test]
    fn value_from_option() {
        assert_eq!(Value::from_option(Some(42i32)), Value::I32(42));
        assert_eq!(Value::from_option(None::<i32>), Value::Null);
    }

    #[test]
    fn value_from_vec_of_values() {
        let array = vec![Value::I32(42), Value::I32(12)];
        assert_eq!(Value::from_vec(array.clone()), Value::Array(array));
    }

    #[test]
    fn test_val_macro_with_bool() {
        let value = val!(true);
        assert_eq!(value, Value::Bool(true));
    }

    #[test]
    fn test_val_macro_with_i8() {
        let value = val!(42i8);
        assert_eq!(value, Value::I8(42));
    }

    #[test]
    fn test_val_macro_with_u8() {
        let value = val!(42u8);
        assert_eq!(value, Value::U8(42));
    }

    #[test]
    fn test_val_macro_with_i16() {
        let value = val!(42i16);
        assert_eq!(value, Value::I16(42));
    }

    #[test]
    fn test_val_macro_with_u16() {
        let value = val!(42u16);
        assert_eq!(value, Value::U16(42));
    }

    #[test]
    fn test_val_macro_with_i32() {
        let value = val!(42i32);
        assert_eq!(value, Value::I32(42));
    }

    #[test]
    fn test_val_macro_with_u32() {
        let value = val!(42u32);
        assert_eq!(value, Value::U32(42));
    }

    #[test]
    fn test_val_macro_with_i64() {
        let value = val!(42i64);
        assert_eq!(value, Value::I64(42));
    }

    #[test]
    fn test_val_macro_with_u64() {
        let value = val!(42u64);
        assert_eq!(value, Value::U64(42));
    }

    #[test]
    fn test_val_macro_with_i128() {
        let value = val!(42i128);
        assert_eq!(value, Value::I128(42));
    }

    #[test]
    fn test_val_macro_with_u128() {
        let value = val!(42u128);
        assert_eq!(value, Value::U128(42));
    }

    #[test]
    fn test_val_macro_with_isize() {
        let value = val!(42isize);
        assert_eq!(value, Value::ISize(42));
    }

    #[test]
    fn test_val_macro_with_usize() {
        let value = val!(42usize);
        assert_eq!(value, Value::USize(42));
    }

    #[test]
    fn test_val_macro_with_f32() {
        let value = val!(42.0f32);
        assert_eq!(value, Value::F32(42.0));
    }

    #[test]
    fn test_val_macro_with_f64() {
        let value = val!(42.0f64);
        assert_eq!(value, Value::F64(42.0));
    }

    #[test]
    fn test_val_macro_with_string() {
        let value = val!("value".to_string());
        assert_eq!(value, Value::String("value".to_string()));
    }

    #[test]
    fn test_val_macro_with_str() {
        let value = val!("value");
        assert_eq!(value, Value::String("value".to_string()));
    }

    #[test]
    fn test_val_macro_with_btree_map() {
        let mut map = BTreeMap::new();
        map.insert("key1".to_string(), Value::I32(42));
        map.insert("key2".to_string(), Value::String("value".to_string()));
        let doc = document_from_map(&map).unwrap();
        let value = val!(doc);
        assert_eq!(value.as_document().unwrap().to_map(), map);
    }

    #[test]
    fn test_val_macro_with_vec() {
        let array = vec![Value::I32(42), Value::String("value".to_string())];
        let value = val!(array.clone());
        assert_eq!(value, Value::Array(array));
    }

    #[test]
    fn test_val_macro_with_nitrite_id() {
        let id = NitriteId::new();
        let value = val!(id.clone());
        assert_eq!(value, Value::NitriteId(id));
    }

    #[test]
    fn test_val_macro_with_bytes() {
        let bytes = vec![1, 2, 3];
        let value = val!(bytes.clone());
        assert_eq!(value, Value::Bytes(bytes));
    }

    #[test]
    fn test_val_macro_with_unit() {
        let value = val!(());
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn test_is_comparable() {
        assert_eq!(Value::I32(42).is_comparable(), true);
        assert_eq!(Value::F32(42.0).is_comparable(), true);
        assert_eq!(Value::Char('c').is_comparable(), true);
        assert_eq!(Value::String("value".to_string()).is_comparable(), true);
        assert_eq!(Value::NitriteId(NitriteId::new()).is_comparable(), true);
        assert_eq!(Value::Bool(true).is_comparable(), true);
        assert_eq!(Value::Array(vec![]).is_comparable(), false);
        assert_eq!(Value::Document(Document::new()).is_comparable(), false);
        assert_eq!(Value::Map(BTreeMap::new()).is_comparable(), false);
    }

    #[test]
    fn test_value_from_vec_of_i32() {
        let vec = vec![1, 2, 3];
        let value = Value::from(vec.clone());
        assert_eq!(
            value,
            Value::Array(vec.into_iter().map(Value::from).collect())
        );
    }

    #[test]
    fn test_value_from_vec_of_strings() {
        let vec = vec!["one".to_string(), "two".to_string(), "three".to_string()];
        let value = Value::from(vec.clone());
        assert_eq!(
            value,
            Value::Array(vec.into_iter().map(Value::from).collect())
        );
    }

    #[test]
    fn test_value_from_vec_of_bytes() {
        let vec = vec![1u8, 2u8, 3u8];
        let value = Value::from(vec.clone());
        assert_eq!(value, Value::Bytes(vec));
    }

    #[test]
    fn test_value_from_vec_of_values() {
        let vec = vec![Value::I32(1), Value::I32(2), Value::I32(3)];
        let value = Value::from(vec.clone());
        assert_eq!(value, Value::Array(vec));
    }

    #[test]
    fn test_integer_comparison_no_unwrap_panic() {
        // Test that integer comparison doesn't panic on unwrap failures
        let val1 = Value::I32(42);
        let val2 = Value::I32(100);

        let ordering = val1.cmp(&val2);
        assert_eq!(ordering, std::cmp::Ordering::Less);

        let ordering = val2.cmp(&val1);
        assert_eq!(ordering, std::cmp::Ordering::Greater);

        let ordering = val1.cmp(&val1);
        assert_eq!(ordering, std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_decimal_comparison_no_unwrap_panic() {
        // Test that decimal comparison doesn't panic on unwrap failures
        let val1 = Value::F64(3.14);
        let val2 = Value::F64(2.71);

        let ordering = val1.cmp(&val2);
        assert_eq!(ordering, std::cmp::Ordering::Greater);

        let ordering = val2.cmp(&val1);
        assert_eq!(ordering, std::cmp::Ordering::Less);

        let ordering = val1.cmp(&val1);
        assert_eq!(ordering, std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_mixed_numeric_types_comparison() {
        // Test comparison between different numeric types
        let int_val = Value::I32(42);
        let float_val = Value::F64(42.0);

        // Should not panic and produce a consistent ordering
        let _ = int_val.cmp(&float_val);
        let _ = float_val.cmp(&int_val);
    }

    #[test]
    fn test_integer_i64_comparison() {
        let val1 = Value::I64(9223372036854775807i64); // i64::MAX
        let val2 = Value::I64(0i64);

        let ordering = val1.cmp(&val2);
        assert_eq!(ordering, std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_null_comparison() {
        let val1 = Value::Null;
        let val2 = Value::Null;

        let ordering = val1.cmp(&val2);
        assert_eq!(ordering, std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_string_comparison() {
        let val1 = Value::String("abc".to_string());
        let val2 = Value::String("def".to_string());

        let ordering = val1.cmp(&val2);
        assert_eq!(ordering, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_bool_comparison() {
        let val1 = Value::Bool(false);
        let val2 = Value::Bool(true);

        let ordering = val1.cmp(&val2);
        assert_eq!(ordering, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_vec_u8_safe_transmute_to_bytes() {
        let vec_bytes: Vec<u8> = vec![1, 2, 3, 4, 5];
        let value = Value::from(vec_bytes.clone());

        match value {
            Value::Bytes(b) => {
                assert_eq!(b, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected Value::Bytes"),
        }
    }

    #[test]
    fn test_vec_generic_to_array() {
        let vec_i32: Vec<i32> = vec![1, 2, 3];
        let value = Value::from(vec_i32);

        match value {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert!(matches!(arr[0], Value::I32(1)));
                assert!(matches!(arr[1], Value::I32(2)));
                assert!(matches!(arr[2], Value::I32(3)));
            }
            _ => panic!("Expected Value::Array for i32 vector"),
        }
    }

    #[test]
    fn test_vec_string_to_array() {
        let vec_string: Vec<String> = vec!["hello".to_string(), "world".to_string()];
        let value = Value::from(vec_string);

        match value {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(arr[0], Value::String(ref s) if s == "hello"));
                assert!(matches!(arr[1], Value::String(ref s) if s == "world"));
            }
            _ => panic!("Expected Value::Array for String vector"),
        }
    }

    #[test]
    fn test_empty_vec_u8_to_bytes() {
        let empty_vec: Vec<u8> = vec![];
        let value = Value::from(empty_vec);

        match value {
            Value::Bytes(b) => {
                assert_eq!(b.len(), 0);
            }
            _ => panic!("Expected Value::Bytes"),
        }
    }

    #[test]
    fn test_large_vec_u8_to_bytes() {
        let large_vec: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let value = Value::from(large_vec.clone());

        match value {
            Value::Bytes(b) => {
                assert_eq!(b.len(), 1000);
                assert_eq!(b, large_vec);
            }
            _ => panic!("Expected Value::Bytes"),
        }
    }

    #[test]
    fn bench_value_type_checks() {
        let values = vec![
            Value::I32(42),
            Value::String("test".to_string()),
            Value::Bool(true),
            Value::F64(3.14),
            Value::Null,
        ];

        for _ in 0..1000 {
            for val in &values {
                let _ = val.is_number();
                let _ = val.is_string();
                let _ = val.is_integer();
                let _ = val.is_decimal();
            }
        }
    }

    #[test]
    fn bench_value_conversions() {
        for i in 0..500 {
            let _ = Value::from(i as i32);
            let _ = Value::from(i as f64);
            let _ = Value::from(i % 2 == 0);
            let _ = Value::from(format!("value_{}", i));
        }
    }

    #[test]
    fn bench_value_as_accessor() {
        let values = vec![
            Value::I32(42),
            Value::String("test".to_string()),
            Value::F64(3.14),
            Value::Bool(true),
        ];

        for _ in 0..200 {
            for val in &values {
                let _ = val.as_i32();
                let _ = val.as_string();
                let _ = val.as_f64();
                let _ = val.as_bool();
            }
        }
    }
}
