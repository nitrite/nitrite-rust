//! Order-preserving key codec.
//!
//! Fjall is an LSM store: it keeps keys sorted by their raw bytes and answers range / seek
//! queries (`ceiling_key`, `higher_key`, `range(..)`) by walking that byte order. A plain
//! `bincode` encoding is **not** order-preserving — integers are little-endian, arrays are
//! length-prefixed — so range scans and sorted index walks over numeric (and varying-length)
//! keys were wrong. See the regression that motivated this: an integer `between`/`gte` over an
//! index returned the wrong rows because `I32(255)` (`..,255,0`) sorted *after* `I32(256)`
//! (`..,0,1`).
//!
//! This module encodes a [`Value`] used as a **key** into bytes whose lexicographic order is a
//! total order that refines [`Value::cmp`]:
//! - all numbers (int + float) order by numeric magnitude, integers before floats on ties —
//!   matching `Value`'s own numeric comparison — and integers additionally break magnitude
//!   ties by their exact value, so large integers (e.g. nanosecond timestamps beyond `2^53`)
//!   order *exactly*;
//! - strings / bytes order by their natural byte order, with a self-delimiting terminator so
//!   `"ab" < "abc"`;
//! - arrays (the composite `(value, id)` index keys) order element-by-element.
//!
//! Values are stored separately with ordinary `bincode` (they never need ordering); only keys
//! use this codec.

use nitrite::collection::NitriteId;
use nitrite::common::Value;

// Type-group tags. Ordered so the byte order across (non-numeric) types is stable and total.
// Within a single map keys are homogeneous in practice (a data partition is all `NitriteId`,
// an index is all one field type), so the exact cross-type order only needs to be *consistent*.
const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x10;
const TAG_NUMBER: u8 = 0x20;
const TAG_CHAR: u8 = 0x30;
const TAG_STRING: u8 = 0x40;
const TAG_NITRITE_ID: u8 = 0x50;
const TAG_BYTES: u8 = 0x60;
const TAG_ARRAY: u8 = 0x70;
// Fallback for types that are never used as comparable keys (Document/Map/Unknown). Encoded
// with bincode after the tag — not order-preserving, but these never participate in range scans.
const TAG_OTHER: u8 = 0x80;

const CLASS_INT: u8 = 0x00;
const CLASS_FLOAT: u8 = 0x01;

const FLOAT_F32: u8 = 0x00;
const FLOAT_F64: u8 = 0x01;

// Array element framing: each element is prefixed with CONTINUE; the array ends with END.
// END < CONTINUE so `[a]` sorts before `[a, b]` (a shorter tuple is the smaller key), matching
// `Vec`'s lexicographic ordering.
const ELEM_END: u8 = 0x00;
const ELEM_CONTINUE: u8 = 0x01;

/// Maps an `f64` to a `u64` whose big-endian bytes sort in the same order as the floats
/// (the standard monotonic transform: flip the sign bit for positives, flip all bits for
/// negatives).
#[inline]
fn ordered_f64_bits(f: f64) -> u64 {
    let bits = f.to_bits();
    let mask = ((bits >> 63) as u64).wrapping_neg() | (1u64 << 63);
    bits ^ mask
}

/// Order-preserving big-endian encoding of an `i128` (flip the sign bit so negatives sort
/// below positives).
#[inline]
fn ordered_i128(v: i128) -> [u8; 16] {
    ((v as u128) ^ (1u128 << 127)).to_be_bytes()
}

#[inline]
fn unordered_i128(bytes: [u8; 16]) -> i128 {
    (u128::from_be_bytes(bytes) ^ (1u128 << 127)) as i128
}

/// Returns `(exact_i128, magnitude_f64)` for an integer `Value`, or `None` if it is not an
/// integer variant. Unsigned 128-bit values are mapped through `as i128` (wrapping), matching
/// the adapter's long-standing numeric normalization.
fn integer_parts(value: &Value) -> Option<(i128, f64)> {
    let v: i128 = match value {
        Value::I8(v) => *v as i128,
        Value::U8(v) => *v as i128,
        Value::I16(v) => *v as i128,
        Value::U16(v) => *v as i128,
        Value::I32(v) => *v as i128,
        Value::U32(v) => *v as i128,
        Value::I64(v) => *v as i128,
        Value::U64(v) => *v as i128,
        Value::I128(v) => *v,
        Value::U128(v) => *v as i128,
        Value::ISize(v) => *v as i128,
        Value::USize(v) => *v as i128,
        _ => return None,
    };
    Some((v, v as f64))
}

/// Encodes a single value (no leading framing) into `out`.
fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Bool(b) => {
            out.push(TAG_BOOL);
            out.push(if *b { 1 } else { 0 });
        }
        Value::Char(c) => {
            out.push(TAG_CHAR);
            out.extend_from_slice(&(*c as u32).to_be_bytes());
        }
        Value::String(s) => {
            out.push(TAG_STRING);
            encode_ordered_bytes(s.as_bytes(), out);
        }
        Value::NitriteId(id) => {
            out.push(TAG_NITRITE_ID);
            // ids are unsigned and big-endian sorts them in numeric order.
            out.extend_from_slice(&id.id_value().to_be_bytes());
        }
        Value::Bytes(bytes) => {
            out.push(TAG_BYTES);
            encode_ordered_bytes(bytes, out);
        }
        Value::Array(items) => {
            out.push(TAG_ARRAY);
            for item in items {
                out.push(ELEM_CONTINUE);
                encode_into(item, out);
            }
            out.push(ELEM_END);
        }
        // Floats.
        Value::F32(_) | Value::F64(_) => {
            let f = match value {
                Value::F32(v) => *v as f64,
                Value::F64(v) => *v,
                _ => unreachable!(),
            };
            out.push(TAG_NUMBER);
            out.extend_from_slice(&ordered_f64_bits(f).to_be_bytes());
            out.push(CLASS_FLOAT);
            match value {
                Value::F32(v) => {
                    out.push(FLOAT_F32);
                    out.extend_from_slice(&v.to_bits().to_be_bytes());
                }
                Value::F64(v) => {
                    out.push(FLOAT_F64);
                    out.extend_from_slice(&v.to_bits().to_be_bytes());
                }
                _ => unreachable!(),
            }
        }
        // Integers.
        _ if integer_parts(value).is_some() => {
            let (exact, mag) = integer_parts(value).expect("checked is_some");
            out.push(TAG_NUMBER);
            out.extend_from_slice(&ordered_f64_bits(mag).to_be_bytes());
            out.push(CLASS_INT);
            out.extend_from_slice(&ordered_i128(exact));
        }
        // Document / Map / Unknown — never comparable keys; keep them encodable for safety.
        other => {
            out.push(TAG_OTHER);
            let bytes = bincode::serde::encode_to_vec(other, bincode::config::legacy())
                .unwrap_or_default();
            out.extend_from_slice(&bytes);
        }
    }
}

/// Order-preserving, self-delimiting byte-string encoding: `0x00` is escaped as `0x00 0x01`
/// and the field is terminated by a bare `0x00`, so a shorter string sorts before a longer one
/// that extends it.
fn encode_ordered_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0x01);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

fn decode_ordered_bytes(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    loop {
        let b = *buf.get(*pos).ok_or("ordered key: truncated byte string")?;
        *pos += 1;
        if b == 0x00 {
            let next = *buf.get(*pos).ok_or("ordered key: truncated escape")?;
            *pos += 1;
            match next {
                0x00 => break,       // terminator
                0x01 => bytes.push(0x00), // escaped null
                _ => return Err("ordered key: invalid byte-string escape".to_string()),
            }
        } else {
            bytes.push(b);
        }
    }
    Ok(bytes)
}

fn take<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], String> {
    let end = *pos + n;
    let slice = buf.get(*pos..end).ok_or("ordered key: unexpected end of input")?;
    *pos = end;
    Ok(slice)
}

fn decode_from(buf: &[u8], pos: &mut usize) -> Result<Value, String> {
    let tag = *buf.get(*pos).ok_or("ordered key: empty input")?;
    *pos += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_BOOL => {
            let b = *buf.get(*pos).ok_or("ordered key: truncated bool")?;
            *pos += 1;
            Ok(Value::Bool(b != 0))
        }
        TAG_CHAR => {
            let bytes = take(buf, pos, 4)?;
            let code = u32::from_be_bytes(bytes.try_into().unwrap());
            Ok(Value::Char(
                char::from_u32(code).ok_or("ordered key: invalid char")?,
            ))
        }
        TAG_STRING => {
            let bytes = decode_ordered_bytes(buf, pos)?;
            Ok(Value::String(
                String::from_utf8(bytes).map_err(|e| e.to_string())?,
            ))
        }
        TAG_NITRITE_ID => {
            let bytes = take(buf, pos, 8)?;
            let id = u64::from_be_bytes(bytes.try_into().unwrap());
            Ok(Value::NitriteId(
                NitriteId::create_id(id).map_err(|e| e.to_string())?,
            ))
        }
        TAG_BYTES => Ok(Value::Bytes(decode_ordered_bytes(buf, pos)?)),
        TAG_ARRAY => {
            let mut items = Vec::new();
            loop {
                let marker = *buf.get(*pos).ok_or("ordered key: truncated array")?;
                *pos += 1;
                match marker {
                    ELEM_END => break,
                    ELEM_CONTINUE => items.push(decode_from(buf, pos)?),
                    _ => return Err("ordered key: invalid array framing".to_string()),
                }
            }
            Ok(Value::Array(items))
        }
        TAG_NUMBER => {
            // Skip the 8-byte ordering prefix; the exact payload after the class byte is canonical.
            let _ = take(buf, pos, 8)?;
            let class = *buf.get(*pos).ok_or("ordered key: truncated number class")?;
            *pos += 1;
            match class {
                CLASS_INT => {
                    let bytes = take(buf, pos, 16)?;
                    let v = unordered_i128(bytes.try_into().unwrap());
                    // Return the narrowest natural integer that preserves the value.
                    if let Ok(v64) = i64::try_from(v) {
                        Ok(Value::I64(v64))
                    } else {
                        Ok(Value::I128(v))
                    }
                }
                CLASS_FLOAT => {
                    let sub = *buf.get(*pos).ok_or("ordered key: truncated float kind")?;
                    *pos += 1;
                    match sub {
                        FLOAT_F32 => {
                            let bytes = take(buf, pos, 4)?;
                            Ok(Value::F32(f32::from_bits(u32::from_be_bytes(
                                bytes.try_into().unwrap(),
                            ))))
                        }
                        FLOAT_F64 => {
                            let bytes = take(buf, pos, 8)?;
                            Ok(Value::F64(f64::from_bits(u64::from_be_bytes(
                                bytes.try_into().unwrap(),
                            ))))
                        }
                        _ => Err("ordered key: invalid float kind".to_string()),
                    }
                }
                _ => Err("ordered key: invalid number class".to_string()),
            }
        }
        TAG_OTHER => {
            let (value, _) =
                bincode::serde::decode_from_slice(&buf[*pos..], bincode::config::legacy())
                    .map_err(|e| e.to_string())?;
            *pos = buf.len();
            Ok(value)
        }
        _ => Err(format!("ordered key: unknown type tag {tag:#x}")),
    }
}

/// Encodes a key value into its order-preserving byte representation.
pub fn encode_key(value: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    encode_into(value, &mut out);
    out
}

/// Decodes an order-preserving key back into a semantically-equal `Value`.
pub fn decode_key(buf: &[u8]) -> Result<Value, String> {
    let mut pos = 0;
    let value = decode_from(buf, &mut pos)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn roundtrip(v: Value) {
        let enc = encode_key(&v);
        let dec = decode_key(&enc).expect("decode");
        assert_eq!(v, dec, "round-trip mismatch for {v:?}");
    }

    #[test]
    fn roundtrips_all_scalar_kinds() {
        roundtrip(Value::Null);
        roundtrip(Value::Bool(true));
        roundtrip(Value::Bool(false));
        roundtrip(Value::Char('a'));
        roundtrip(Value::Char('λ'));
        roundtrip(Value::String("hello".into()));
        roundtrip(Value::String(String::new()));
        roundtrip(Value::String("with\0null".into()));
        roundtrip(Value::F32(3.5));
        roundtrip(Value::F64(-2.25));
        roundtrip(Value::Bytes(vec![1, 0, 2, 0, 0, 3]));
        roundtrip(Value::NitriteId(NitriteId::create_id(1_500_000_000_000_000_001).unwrap()));
    }

    #[test]
    fn integers_roundtrip_to_equal_value() {
        for v in [
            Value::I64(0),
            Value::I64(-1),
            Value::I64(i64::MAX),
            Value::I64(i64::MIN),
            Value::I32(42),
        ] {
            let dec = decode_key(&encode_key(&v)).unwrap();
            assert_eq!(v, dec, "int round-trip {v:?}");
        }
        // Width-collapsed but semantically equal.
        assert_eq!(decode_key(&encode_key(&Value::I32(7))).unwrap(), Value::I64(7));
    }

    /// The crucial property: byte order matches `Value`'s ordering for homogeneous keys.
    fn assert_sorted_matches(mut values: Vec<Value>) {
        values.sort();
        let mut encoded: Vec<Vec<u8>> = values.iter().map(encode_key).collect();
        let original = encoded.clone();
        encoded.sort();
        assert_eq!(
            original, encoded,
            "byte order diverged from Value::Ord for {values:?}"
        );
    }

    #[test]
    fn integer_byte_order_matches_value_order() {
        assert_sorted_matches(vec![
            Value::I64(256),
            Value::I64(255),
            Value::I64(0),
            Value::I64(-1),
            Value::I64(-256),
            Value::I64(1_000_000),
            Value::I64(i64::MAX),
            Value::I64(i64::MIN),
        ]);
    }

    #[test]
    fn large_integers_order_exactly_beyond_f64_precision() {
        let base = 1i64 << 53;
        assert_sorted_matches(vec![
            Value::I64(base),
            Value::I64(base + 1),
            Value::I64(base + 2),
            Value::I64(base + 3),
            Value::I64(9_000_000_000_000_000_001),
            Value::I64(9_000_000_000_000_000_002),
        ]);
    }

    #[test]
    fn float_byte_order_matches_value_order() {
        assert_sorted_matches(vec![
            Value::F64(-100.5),
            Value::F64(-1.0),
            Value::F64(0.0),
            Value::F64(1.5),
            Value::F64(2.0),
            Value::F64(1e9),
        ]);
    }

    #[test]
    fn string_byte_order_matches_value_order() {
        assert_sorted_matches(vec![
            Value::String("a".into()),
            Value::String("ab".into()),
            Value::String("abc".into()),
            Value::String("b".into()),
            Value::String("".into()),
            Value::String("aa".into()),
        ]);
    }

    #[test]
    fn nitrite_id_byte_order_matches_value_order() {
        assert_sorted_matches(vec![
            Value::NitriteId(NitriteId::create_id(1_000_000_000_000_000_001).unwrap()),
            Value::NitriteId(NitriteId::create_id(1_000_000_000_000_000_002).unwrap()),
            Value::NitriteId(NitriteId::create_id(9_000_000_000_000_000_000).unwrap()),
        ]);
    }

    #[test]
    fn composite_array_keys_order_by_value_then_id() {
        // [value, id] tuples: order by value first, then id — exactly what the non-unique
        // index range scan relies on.
        let mk = |v: i64, id: u64| {
            Value::Array(vec![
                Value::I64(v),
                Value::NitriteId(NitriteId::create_id(id).unwrap()),
            ])
        };
        assert_sorted_matches(vec![
            mk(5, 1_000_000_000_000_000_002),
            mk(5, 1_000_000_000_000_000_001),
            mk(10, 1_000_000_000_000_000_000),
            mk(255, 1_000_000_000_000_000_000),
            mk(256, 1_000_000_000_000_000_000),
            mk(-1, 1_000_000_000_000_000_000),
        ]);
    }

    #[test]
    fn shorter_array_sorts_before_extension() {
        // Homogeneous element types (as real composite keys are): a shorter tuple sorts before
        // a longer one that extends it.
        assert_sorted_matches(vec![
            Value::Array(vec![Value::I64(1)]),
            Value::Array(vec![Value::I64(1), Value::I64(2)]),
            Value::Array(vec![Value::I64(1), Value::I64(3)]),
            Value::Array(vec![Value::I64(2)]),
        ]);
    }

    #[test]
    fn int_before_float_on_equal_magnitude() {
        // Matches Value::Ord: integers sort before floats of equal value.
        let i = encode_key(&Value::I64(5));
        let f = encode_key(&Value::F64(5.0));
        assert!(i < f);
        // And ordering across the boundary is by magnitude.
        assert!(encode_key(&Value::I64(4)) < encode_key(&Value::F64(4.5)));
        assert!(encode_key(&Value::F64(4.5)) < encode_key(&Value::I64(5)));
    }

    #[test]
    fn document_key_roundtrips_via_fallback() {
        let mut m = BTreeMap::new();
        m.insert(Value::String("k".into()), Value::I64(1));
        let v = Value::Map(m);
        roundtrip(v);
    }
}
