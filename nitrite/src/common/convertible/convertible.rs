#![allow(non_snake_case)]

use crate::collection::{Document, NitriteId};
use crate::common::{ReadExecutor, Value};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::{atomic, document_from_map, Atomic};
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::str::FromStr;

pub trait Convertible {
    type Output;

    fn to_value(&self) -> NitriteResult<Value>;
    fn from_value(value: &Value) -> NitriteResult<Self::Output>;
}

impl Convertible for i8 {
    type Output = i8;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::I8(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::I8(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not an i8", value);
                Err(NitriteError::new(
                    "Value is not an i8",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for i16 {
    type Output = i16;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::I16(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::I16(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not an i16", value);
                Err(NitriteError::new(
                    "Value is not an i16",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for i32 {
    type Output = i32;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::I32(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::I32(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not an i32", value);
                Err(NitriteError::new(
                    "Value is not an i32",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for i64 {
    type Output = i64;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::I64(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::I64(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not an i64", value);
                Err(NitriteError::new(
                    "Value is not an i64",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for i128 {
    type Output = i128;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::I128(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::I128(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not an i128", value);
                Err(NitriteError::new(
                    "Value is not an i128",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for u8 {
    type Output = u8;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::U8(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::U8(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a u8", value);
                Err(NitriteError::new(
                    "Value is not a u8",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for u16 {
    type Output = u16;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::U16(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::U16(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a u16", value);
                Err(NitriteError::new(
                    "Value is not a u16",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for u32 {
    type Output = u32;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::U32(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::U32(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a u32", value);
                Err(NitriteError::new(
                    "Value is not a u32",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for u64 {
    type Output = u64;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::U64(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::U64(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a u64", value);
                Err(NitriteError::new(
                    "Value is not a u64",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for u128 {
    type Output = u128;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::U128(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::U128(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a u128", value);
                Err(NitriteError::new(
                    "Value is not a u128",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for f32 {
    type Output = f32;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::F32(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::F32(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a f32", value);
                Err(NitriteError::new(
                    "Value is not a f32",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for f64 {
    type Output = f64;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::F64(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::F64(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a f64", value);
                Err(NitriteError::new(
                    "Value is not a f64",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for char {
    type Output = char;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::Char(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::Char(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a char", value);
                Err(NitriteError::new(
                    "Value is not a char",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for String {
    type Output = String;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::String(self.clone()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::String(i) => Ok(i.clone()),
            _ => {
                log::error!("Value {} is not a string", value);
                Err(NitriteError::new(
                    "Value is not a string",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for &str {
    type Output = String;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::String(self.to_string()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::String(i) => Ok(i.to_owned()),
            _ => {
                log::error!("Value {} is not a string", value);
                Err(NitriteError::new(
                    "Value is not a string",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for bool {
    type Output = bool;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::Bool(*self))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::Bool(i) => Ok(*i),
            _ => {
                log::error!("Value {} is not a bool", value);
                Err(NitriteError::new(
                    "Value is not a bool",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for () {
    type Output = ();

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::Null)
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::Null => Ok(()),
            _ => {
                log::error!("Value {} is not a null", value);
                Err(NitriteError::new(
                    "Value is not a null",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for NitriteId {
    type Output = NitriteId;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::NitriteId(self.clone()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::NitriteId(i) => Ok(i.clone()),
            _ => {
                log::error!("Value {} is not a nitrite id", value);
                Err(NitriteError::new(
                    "Value is not a nitrite id",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for Document {
    type Output = Document;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::Document(self.clone()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::Document(i) => Ok(i.clone()),
            _ => {
                log::error!("Value {} is not a document", value);
                Err(NitriteError::new(
                    "Value is not a document",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for BTreeMap<Value, Value> {
    type Output = BTreeMap<Value, Value>;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::Map(self.clone()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        match value {
            Value::Map(i) => Ok(i.clone()),
            _ => {
                log::error!("Value {} is not a map", value);
                Err(NitriteError::new(
                    "Value is not a map",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl<T> Convertible for Option<T>
where
    T: Convertible,
{
    type Output = Option<T::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        match self {
            Some(v) => v.to_value(),
            None => Ok(Value::Null),
        }
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Null => Ok(None),
            _ => Ok(Some(T::from_value(value)?)),
        }
    }
}

impl<T> Convertible for Box<T>
where
    T: Convertible,
{
    type Output = Box<T::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        self.as_ref().to_value()
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        Ok(Box::new(T::from_value(value)?))
    }
}

impl<T> Convertible for Atomic<T>
where
    T: Convertible,
{
    type Output = Atomic<T::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        // self.read().to_value()
        self.read_with(|it| it.to_value())
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        Ok(atomic(T::from_value(value)?))
    }
}

impl<T> Convertible for Vec<T>
where
    T: Convertible + Any,
{
    type Output = Vec<T::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        if TypeId::of::<T>() == TypeId::of::<u8>() {
            let mut arr = Vec::new();
            for item in self {
                let raw = item.to_value()?;
                if let Value::U8(item) = raw {
                    arr.push(item);
                } else {
                    log::error!("Value {} is not a u8", raw);
                    return Err(NitriteError::new(
                        "Value is not a u8",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            }
            return Ok(Value::Bytes(arr));
        }

        let mut arr = Vec::new();
        for item in self {
            arr.push(item.to_value()?);
        }
        Ok(Value::Array(arr))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Bytes(arr) => {
                let mut vec = Vec::new();
                for item in arr {
                    vec.push(T::from_value(&Value::U8(*item))?);
                }
                Ok(vec)
            }
            Value::Array(arr) => {
                let mut vec = Vec::new();
                for item in arr {
                    vec.push(T::from_value(item)?);
                }
                Ok(vec)
            }
            _ => {
                log::error!("Value {} is not an array", value);
                Err(NitriteError::new(
                    "Value is not an array",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl<K, V> Convertible for BTreeMap<K, V>
where
    K: ToString + FromStr + Ord,
    V: Convertible,
{
    type Output = BTreeMap<K, V::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        let mut map = BTreeMap::new();
        for (k, v) in self {
            map.insert(k.to_string(), v.to_value()?);
        }
        Ok(Value::Document(document_from_map(&map)?))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Document(doc) => {
                let mut result = BTreeMap::new();
                for (k, v) in doc.iter() {
                    match K::from_str(k.as_str()) {
                        Ok(key) => {
                            result.insert(key, V::from_value(&v)?);
                        }
                        Err(_) => {
                            log::error!("Failed to convert key {} to type", k);
                            return Err(NitriteError::new(
                                &format!("Failed to convert key {} to type", k),
                                ErrorKind::ObjectMappingError,
                            ));
                        }
                    }
                }
                Ok(result)
            }
            _ => {
                log::error!("Value is not a document");
                Err(NitriteError::new(
                    "Value is not a document",
                    ErrorKind::ObjectMappingError,
                ))
            }
        }
    }
}

impl<K, V> Convertible for HashMap<K, V>
where
    K: ToString + FromStr + Eq + Hash,
    V: Convertible,
{
    type Output = HashMap<K, V::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        let mut map = BTreeMap::new();
        for (k, v) in self {
            map.insert(k.to_string(), v.to_value()?);
        }
        Ok(Value::Document(document_from_map(&map)?))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Document(doc) => {
                let mut map = HashMap::new();
                for (k, v) in doc.iter() {
                    let key = K::from_str(&k);
                    match key {
                        Ok(k) => {
                            map.insert(k, V::from_value(&v)?);
                        }
                        Err(_) => {
                            log::error!("Key is not a valid string: {}", k);
                            return Err(NitriteError::new(
                                "Key is not a valid string",
                                ErrorKind::ObjectMappingError,
                            ));
                        }
                    }
                }
                Ok(map)
            }
            _ => {
                log::error!("Value {} is not a document", value);
                Err(NitriteError::new(
                    "Value is not a document",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl<V> Convertible for HashSet<V>
where
    V: Convertible,
    V::Output: Eq + Hash,
{
    type Output = HashSet<V::Output>;

    fn to_value(&self) -> NitriteResult<Value> {
        let mut array = Vec::new();
        for item in self {
            array.push(item.to_value()?);
        }
        Ok(Value::Array(array))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Array(arr) => {
                let mut set = HashSet::new();
                for item in arr {
                    set.insert(V::from_value(item)?);
                }
                Ok(set)
            }
            _ => {
                log::error!("Value {} is not an array", value);
                Err(NitriteError::new(
                    "Value is not an array",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

impl Convertible for Value {
    type Output = Value;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(self.clone())
    }

    fn from_value(value: &Value) -> NitriteResult<Self> {
        Ok(value.clone())
    }
}

macro_rules! impl_convertible_for_tuples {
    ($(($($T:ident),+)),+) => {
        $(
            impl<$($T),+> Convertible for ($($T),+) where $($T: Convertible),+ {
                type Output = ($($T::Output),+);

                fn to_value(&self) -> NitriteResult<Value> {
                    let ($($T),+) = self;
                    Ok(Value::Array(vec![$($T.to_value()?),+]))
                }

                fn from_value(value: &Value) -> NitriteResult<Self::Output> {
                    match value {
                        Value::Array(arr) => {
                            if arr.len() != count_idents!($($T),+) {
                                log::error!("Value is not a tuple");
                                return Err(NitriteError::new(
                                    "Value is not a tuple",
                                    ErrorKind::ObjectMappingError,
                                ));
                            }
                            let mut iter = arr.iter();
                            Ok(($($T::from_value(iter.next().ok_or_else(|| NitriteError::new(
                                "Tuple element missing",
                                ErrorKind::ObjectMappingError,
                            ))?)?),+))
                        }
                        _ => { 
                            log::error!("Value {} is not an array", value);
                            Err(NitriteError::new(
                                "Value is not an array",
                                ErrorKind::ObjectMappingError,
                            ))
                        }
                    }
                }
            }
        )+
    };
}

macro_rules! count_idents {
    ($($idents:ident),*) => {
        <[()]>::len(&[$(count_idents!(@sub $idents)),*])
    };
    (@sub $idents:ident) => { () };
}

impl_convertible_for_tuples! {
    (T1, T2),
    (T1, T2, T3),
    (T1, T2, T3, T4),
    (T1, T2, T3, T4, T5),
    (T1, T2, T3, T4, T5, T6),
    (T1, T2, T3, T4, T5, T6, T7),
    (T1, T2, T3, T4, T5, T6, T7, T8),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
}

pub fn from_value<T>(value: &Value) -> NitriteResult<T::Output>
where
    T: Convertible,
{
    T::from_value(value)
}

pub fn to_value<T>(data: &T) -> NitriteResult<Value>
where
    T: Convertible,
{
    data.to_value()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Value;
    use std::collections::{BTreeMap, HashMap, HashSet};

    fn test_convertible<T>(value: T, expected: Value)
    where
        T: Convertible<Output = T> + PartialEq + std::fmt::Debug,
        T::Output: PartialEq + std::fmt::Debug,
    {
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = T::from_value(&expected).unwrap();
        assert_eq!(from_value, value);
    }

    fn test_convertible_error<T>(value: &Value, expected_error: &str)
    where
        T: Convertible, <T as Convertible>::Output: std::fmt::Debug
    {
        let result = T::from_value(value);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), expected_error);
    }

    #[test]
    fn test_i8() {
        test_convertible(42_i8, Value::I8(42));
        test_convertible_error::<i8>(&Value::I16(42), "Value is not an i8");
    }

    #[test]
    fn test_i16() {
        test_convertible(42_i16, Value::I16(42));
        test_convertible_error::<i16>(&Value::I32(42), "Value is not an i16");
    }

    #[test]
    fn test_i32() {
        test_convertible(42_i32, Value::I32(42));
        test_convertible_error::<i32>(&Value::I64(42), "Value is not an i32");
    }

    #[test]
    fn test_i64() {
        test_convertible(42_i64, Value::I64(42));
        test_convertible_error::<i64>(&Value::I128(42), "Value is not an i64");
    }

    #[test]
    fn test_i128() {
        test_convertible(42_i128, Value::I128(42));
        test_convertible_error::<i128>(&Value::U8(42), "Value is not an i128");
    }

    #[test]
    fn test_u8() {
        test_convertible(42_u8, Value::U8(42));
        test_convertible_error::<u8>(&Value::U16(42), "Value is not a u8");
    }

    #[test]
    fn test_u16() {
        test_convertible(42_u16, Value::U16(42));
        test_convertible_error::<u16>(&Value::U32(42), "Value is not a u16");
    }

    #[test]
    fn test_u32() {
        test_convertible(42_u32, Value::U32(42));
        test_convertible_error::<u32>(&Value::U64(42), "Value is not a u32");
    }

    #[test]
    fn test_u64() {
        test_convertible(42_u64, Value::U64(42));
        test_convertible_error::<u64>(&Value::U128(42), "Value is not a u64");
    }

    #[test]
    fn test_u128() {
        test_convertible(42_u128, Value::U128(42));
        test_convertible_error::<u128>(&Value::F32(42.0), "Value is not a u128");
    }

    #[test]
    fn test_f32() {
        test_convertible(42.0_f32, Value::F32(42.0));
        test_convertible_error::<f32>(&Value::F64(42.0), "Value is not a f32");
    }

    #[test]
    fn test_f64() {
        test_convertible(42.0_f64, Value::F64(42.0));
        test_convertible_error::<f64>(&Value::Char('a'), "Value is not a f64");
    }

    #[test]
    fn test_char() {
        test_convertible('a', Value::Char('a'));
        test_convertible_error::<char>(&Value::String("a".to_string()), "Value is not a char");
    }

    #[test]
    fn test_string() {
        test_convertible("hello".to_string(), Value::String("hello".to_string()));
        test_convertible_error::<String>(&Value::Bool(true), "Value is not a string");
    }

    #[test]
    fn test_str() {
        let value: &str = "hello";
        let expected = Value::String("hello".to_string());
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = <&str>::from_value(&expected).unwrap();
        assert_eq!(from_value, "hello".to_string());

        test_convertible_error::<&str>(&Value::Bool(true), "Value is not a string");
    }

    #[test]
    fn test_bool() {
        test_convertible(true, Value::Bool(true));
        test_convertible_error::<bool>(&Value::I32(42), "Value is not a bool");
    }

    #[test]
    fn test_unit() {
        test_convertible((), Value::Null);
        test_convertible_error::<()>(&Value::I32(42), "Value is not a null");
    }

    #[test]
    fn test_option() {
        let value: Option<i32> = Some(42);
        let expected = Value::I32(42);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = Option::<i32>::from_value(&expected).unwrap();
        assert_eq!(from_value, Some(42));

        let value: Option<i32> = None;
        let expected = Value::Null;
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = Option::<i32>::from_value(&expected).unwrap();
        assert_eq!(from_value, None);

        test_convertible_error::<Option<i32>>(&Value::Bool(true), "Value is not an i32");
    }

    #[test]
    fn test_box() {
        let value: Box<i32> = Box::new(42);
        let expected = Value::I32(42);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = Box::<i32>::from_value(&expected).unwrap();
        assert_eq!(from_value, Box::new(42));

        test_convertible_error::<Box<i32>>(&Value::Bool(true), "Value is not an i32");
    }

    #[test]
    fn test_vec() {
        let value: Vec<i32> = vec![1, 2, 3];
        let expected = Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = Vec::<i32>::from_value(&expected).unwrap();
        assert_eq!(from_value, vec![1, 2, 3]);

        test_convertible_error::<Vec<i32>>(&Value::Bool(true), "Value is not an array");
    }

    #[test]
    fn test_btreemap() {
        let mut value = BTreeMap::new();
        value.insert("key".to_string(), 42);
        let mut expected = BTreeMap::new();
        expected.insert("key".to_string(), Value::I32(42));
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, Value::Document(document_from_map(&expected).unwrap()));

        let from_value = BTreeMap::<String, i32>::from_value(&to_value).unwrap();
        assert_eq!(from_value, value);

        test_convertible_error::<BTreeMap<String, i32>>(&Value::Bool(true), "Value is not a document");
    }

    #[test]
    fn test_hashmap() {
        let mut value = HashMap::new();
        value.insert("key".to_string(), 42);
        let mut expected = BTreeMap::new();
        expected.insert("key".to_string(), Value::I32(42));
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, Value::Document(document_from_map(&expected).unwrap()));

        let from_value = HashMap::<String, i32>::from_value(&to_value).unwrap();
        assert_eq!(from_value, value);

        test_convertible_error::<HashMap<String, i32>>(&Value::Bool(true), "Value is not a document");
    }

    #[test]
    fn test_hashset() {
        let mut value = HashSet::new();
        value.insert(42);
        let expected = Value::Array(vec![Value::I32(42)]);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = HashSet::<i32>::from_value(&expected).unwrap();
        assert_eq!(from_value, value);

        test_convertible_error::<HashSet<i32>>(&Value::Bool(true), "Value is not an array");
    }

    #[test]
    fn test_value() {
        let value = Value::I32(42);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, value);

        let from_value = Value::from_value(&value).unwrap();
        assert_eq!(from_value, value);
    }

    #[test]
    fn test_tuple() {
        let value = (1, "hello".to_string());
        let expected = Value::Array(vec![Value::I32(1), Value::String("hello".to_string())]);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = <(i32, String)>::from_value(&expected).unwrap();
        assert_eq!(from_value, value);

        let value = (1, "hello".to_string(), 3.14);
        let expected = Value::Array(vec![Value::I32(1), Value::String("hello".to_string()), Value::F64(3.14)]);
        let to_value = value.to_value().unwrap();
        assert_eq!(to_value, expected);

        let from_value = <(i32, String, f64)>::from_value(&expected).unwrap();
        assert_eq!(from_value, value);

        test_convertible_error::<(i32, String)>(&Value::Bool(true), "Value is not an array");
    }

    #[test]
    fn test_tuple_with_length_mismatch() {
        // Test tuple deserialization with array too short (missing element)
        let short_array = Value::Array(vec![Value::I32(1)]);
        let result = <(i32, String)>::from_value(&short_array);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Value is not a tuple");
    }

    #[test]
    fn test_tuple_with_length_mismatch_too_many_elements() {
        // Test tuple deserialization with array too long
        let long_array = Value::Array(vec![
            Value::I32(1),
            Value::String("hello".to_string()),
            Value::F64(3.14),
        ]);
        let result = <(i32, String)>::from_value(&long_array);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Value is not a tuple");
    }

    #[test]
    fn test_tuple_with_type_conversion_error() {
        // Test tuple with correct length but wrong types
        let wrong_type_array = Value::Array(vec![
            Value::String("not_an_int".to_string()), // Wrong type, should be i32
            Value::String("hello".to_string()),
        ]);
        let result = <(i32, String)>::from_value(&wrong_type_array);
        assert!(result.is_err());
    }

    #[test]
    fn test_tuple_three_elements_with_length_mismatch() {
        // Test 3-element tuple with only 2 elements in array
        let short_array = Value::Array(vec![Value::I32(1), Value::String("hello".to_string())]);
        let result = <(i32, String, f64)>::from_value(&short_array);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Value is not a tuple");
    }

    #[test]
    fn test_tuple_three_elements_with_wrong_element_type() {
        // Test 3-element tuple with correct length but wrong element type
        let wrong_type_array = Value::Array(vec![
            Value::I32(1),
            Value::String("hello".to_string()),
            Value::Bool(true), // Wrong type, should be f64
        ]);
        let result = <(i32, String, f64)>::from_value(&wrong_type_array);
        assert!(result.is_err());
    }

    #[test]
    fn test_tuple_roundtrip_multiple_types() {
        // Integration test: tuple serialization and deserialization roundtrip
        let original = (42_i32, "test".to_string(), 3.14_f64, true);
        let serialized = original.to_value().unwrap();
        let deserialized = <(i32, String, f64, bool)>::from_value(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_tuple_nested_in_array() {
        // Test tuple values nested inside arrays
        let tuple_values = vec![(1_i32, "a".to_string()), (2_i32, "b".to_string())];
        let expected = Value::Array(vec![
            Value::Array(vec![Value::I32(1), Value::String("a".to_string())]),
            Value::Array(vec![Value::I32(2), Value::String("b".to_string())]),
        ]);
        let serialized = tuple_values.to_value().unwrap();
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_tuple_empty_array_conversion() {
        // Test that empty array fails for non-zero-tuple types
        let empty_array = Value::Array(vec![]);
        let result = <(i32, String)>::from_value(&empty_array);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Value is not a tuple");
    }
}