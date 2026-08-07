//! Nitrite's `Value` on its way to the wire.
//!
//! The core is engine-neutral and so knows nothing of `Value` or `Document`;
//! this is the one place that translates. Total by construction: an unexpected
//! variant degrades to its string form rather than failing a page, because an
//! adapter that throws mid-page shows the developer nothing at all.

use std::collections::BTreeMap;

use dbinspect_bridge::CellValue;
use nitrite::common::Value;

pub fn encode(value: &Value) -> CellValue {
    match value {
        Value::Null | Value::Unknown => CellValue::Null,
        Value::Bool(flag) => CellValue::Bool(*flag),

        Value::I8(number) => CellValue::Int(*number as i64),
        Value::U8(number) => CellValue::Int(*number as i64),
        Value::I16(number) => CellValue::Int(*number as i64),
        Value::U16(number) => CellValue::Int(*number as i64),
        Value::I32(number) => CellValue::Int(*number as i64),
        Value::U32(number) => CellValue::Int(*number as i64),
        Value::I64(number) => CellValue::Int(*number),
        Value::ISize(number) => CellValue::Int(*number as i64),

        // JSON has no 64-bit integer, and a client that reads a document id as a
        // double gets a different id back. Anything that does not fit exactly
        // goes over as its decimal string, which renders and compares correctly
        // even though it no longer sorts as a number.
        Value::U64(number) => int_or_text(i64::try_from(*number), || number.to_string()),
        Value::I128(number) => int_or_text(i64::try_from(*number), || number.to_string()),
        Value::U128(number) => int_or_text(i64::try_from(*number), || number.to_string()),
        Value::USize(number) => int_or_text(i64::try_from(*number), || number.to_string()),

        Value::F32(number) => CellValue::Double(*number as f64),
        Value::F64(number) => CellValue::Double(*number),

        Value::Char(character) => CellValue::Text(character.to_string()),
        Value::String(text) => CellValue::Text(text.clone()),

        // Only actual binary becomes a blob, and the core truncates it at 64 KB
        // with the real length beside it.
        Value::Bytes(bytes) => CellValue::Blob(bytes.clone()),

        Value::NitriteId(id) => CellValue::Text(id.to_string()),

        Value::Array(values) => CellValue::List(values.iter().map(encode).collect()),

        // A nested document is unwrapped here rather than degrading to its
        // `Display`, which would render a whole sub-object as one string. A
        // repository row with a nested entity in it is the common case.
        Value::Document(document) => CellValue::Map(
            document
                .iter()
                .map(|(field, value)| (field, encode(&value)))
                .collect(),
        ),

        Value::Map(entries) => CellValue::Map(
            entries
                .iter()
                .map(|(key, value)| (key_to_string(key), encode(value)))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

fn int_or_text(
    narrowed: Result<i64, impl std::fmt::Debug>,
    wide: impl Fn() -> String,
) -> CellValue {
    match narrowed {
        Ok(number) => CellValue::Int(number),
        Err(_) => CellValue::Text(wide()),
    }
}

/// A map key is a `Value` in Nitrite and a string on the wire; a non-string key
/// renders rather than being dropped.
fn key_to_string(key: &Value) -> String {
    match key {
        Value::String(text) => text.clone(),
        Value::Char(character) => character.to_string(),
        other => other.to_string(),
    }
}

/// The rendering the client shows for a column, from the values a sample carried.
///
/// A closed set, so a client has a known set of renderings rather than whatever
/// `Debug` prints. `None` means this document said nothing about the field.
pub fn type_of(value: &Value) -> Option<&'static str> {
    Some(match value {
        Value::Null | Value::Unknown => return None,
        Value::Bool(_) => "bool",
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
        | Value::USize(_) => "int",
        Value::F32(_) | Value::F64(_) => "real",
        Value::Char(_) | Value::String(_) => "text",
        Value::Bytes(_) => "blob",
        Value::NitriteId(_) => "id",
        Value::Array(_) => "list",
        Value::Document(_) | Value::Map(_) => "document",
    })
}
