//! Vector search filter.
//!
//! [`VectorNearestFilter`] carries a kNN query (field, query vector, `k`, and
//! optional `ef` / `min_score`). It declares itself index-only for the
//! `"vector"` index type, so the query optimizer routes it into the
//! `IndexScanFilter` of the find plan, where [`crate::indexer::VectorIndexer`]
//! recovers it via `as_any().downcast_ref()`.

use std::any::Any;
use std::fmt::{self, Display};
use std::sync::{Arc, OnceLock};

use nitrite::collection::Document;
use nitrite::common::Value;
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::filter::{Filter, FilterProvider};

/// The index type name for vector (HNSW) indexes.
pub const VECTOR_INDEX: &str = "vector";

/// A k-nearest-neighbor filter over a vector field.
#[derive(Clone)]
pub struct VectorNearestFilter {
    inner: Arc<VectorNearestInner>,
}

struct VectorNearestInner {
    field: OnceLock<String>,
    query: Vec<f32>,
    k: usize,
    ef: Option<usize>,
    min_score: Option<f32>,
}

impl VectorNearestFilter {
    /// Creates a new kNN filter.
    pub fn new(field: impl Into<String>, query: Vec<f32>, k: usize) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field.into());
        VectorNearestFilter {
            inner: Arc::new(VectorNearestInner {
                field: name,
                query,
                k,
                ef: None,
                min_score: None,
            }),
        }
    }

    /// Sets the query-time `ef` (search width). Larger = higher recall, slower.
    pub fn with_ef(self, ef: usize) -> Self {
        let inner = VectorNearestInner {
            field: clone_once(&self.inner.field),
            query: self.inner.query.clone(),
            k: self.inner.k,
            ef: Some(ef),
            min_score: self.inner.min_score,
        };
        VectorNearestFilter { inner: Arc::new(inner) }
    }

    /// Sets a minimum similarity score cutoff (metric-dependent; see
    /// [`crate::distance::Metric::score`]).
    pub fn with_min_score(self, min_score: f32) -> Self {
        let inner = VectorNearestInner {
            field: clone_once(&self.inner.field),
            query: self.inner.query.clone(),
            k: self.inner.k,
            ef: self.inner.ef,
            min_score: Some(min_score),
        };
        VectorNearestFilter { inner: Arc::new(inner) }
    }

    /// The query vector.
    pub fn query(&self) -> &[f32] {
        &self.inner.query
    }

    /// The number of neighbors requested.
    pub fn k(&self) -> usize {
        self.inner.k
    }

    /// The optional query-time `ef`.
    pub fn ef(&self) -> Option<usize> {
        self.inner.ef
    }

    /// The optional minimum score cutoff.
    pub fn min_score(&self) -> Option<f32> {
        self.inner.min_score
    }
}

fn clone_once(field: &OnceLock<String>) -> OnceLock<String> {
    let out = OnceLock::new();
    if let Some(v) = field.get() {
        let _ = out.set(v.clone());
    }
    out
}

impl FilterProvider for VectorNearestFilter {
    /// A per-document predicate cannot perform kNN ranking, so evaluating this
    /// filter outside the index path is always a mistake (it would silently
    /// match every document carrying a right-shaped vector). The query planner
    /// already rejects index-only filters without an index; this error is the
    /// defense-in-depth for any other route into `apply`.
    fn apply(&self, _entry: &Document) -> NitriteResult<bool> {
        Err(NitriteError::new(
            &format!(
                "kNN filter on '{}' requires a vector index; create one with \
                 collection.create_index([field], &vector_index_options())",
                self.field_name().unwrap_or_else(|_| "<unknown>".into())
            ),
            ErrorKind::FilterError,
        ))
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        self.field_name()
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.inner.field.get_or_init(|| field_name);
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(VECTOR_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if other.as_any().downcast_ref::<VectorNearestFilter>().is_some() {
            Ok(self.field_name()? == other.get_field_name()?)
        } else {
            Ok(false)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl VectorNearestFilter {
    fn field_name(&self) -> NitriteResult<String> {
        self.inner
            .field
            .get()
            .cloned()
            .ok_or_else(|| NitriteError::new("Field name not set", ErrorKind::InvalidOperation))
    }
}

impl Display for VectorNearestFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The query vector is part of this filter's identity: the collection's
        // query optimizer caches find plans keyed by `filter.to_string()`, so
        // two different query vectors MUST produce different strings or the
        // wrong (cached) query is reused. Embed a compact digest of the query
        // and the search parameters rather than the raw (possibly huge) vector.
        use std::hash::{Hash, Hasher};
        let field = self.inner.field.get().map(String::as_str).unwrap_or("<unknown>");
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for x in &self.inner.query {
            x.to_bits().hash(&mut hasher);
        }
        self.inner.k.hash(&mut hasher);
        self.inner.ef.hash(&mut hasher);
        self.inner.min_score.map(f32::to_bits).hash(&mut hasher);
        write!(
            f,
            "({} nearest k={} dim={} q={:016x})",
            field,
            self.inner.k,
            self.inner.query.len(),
            hasher.finish()
        )
    }
}

/// Converts a document field value into a dense `f32` vector.
///
/// Accepts a `Value::Array` of numeric values (ints, `F32`, `F64`). Returns
/// `None` for non-array or non-numeric contents.
pub fn value_to_vector(value: &Value) -> Option<Vec<f32>> {
    let arr = value.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        out.push(value_to_f32(v)?);
    }
    Some(out)
}

fn value_to_f32(v: &Value) -> Option<f32> {
    match v {
        Value::F32(x) => Some(*x),
        Value::F64(x) => Some(*x as f32),
        Value::I8(x) => Some(*x as f32),
        Value::I16(x) => Some(*x as f32),
        Value::I32(x) => Some(*x as f32),
        Value::I64(x) => Some(*x as f32),
        Value::U8(x) => Some(*x as f32),
        Value::U16(x) => Some(*x as f32),
        Value::U32(x) => Some(*x as f32),
        Value::U64(x) => Some(*x as f32),
        _ => None,
    }
}

/// Builds a `Value::Array` of `F32` from a vector (for storing in documents).
pub fn vector_to_value(vector: &[f32]) -> Value {
    Value::Array(vector.iter().map(|x| Value::F32(*x)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_vector_value() {
        let v = vec![0.1f32, 0.2, 0.3];
        let value = vector_to_value(&v);
        assert_eq!(value_to_vector(&value), Some(v));
    }

    #[test]
    fn parses_mixed_numeric_array() {
        let value = Value::Array(vec![Value::I32(1), Value::F64(2.5), Value::U8(3)]);
        assert_eq!(value_to_vector(&value), Some(vec![1.0, 2.5, 3.0]));
    }

    #[test]
    fn rejects_non_array() {
        assert_eq!(value_to_vector(&Value::String("x".into())), None);
    }

    #[test]
    fn filter_exposes_query_params() {
        let f = VectorNearestFilter::new("emb", vec![1.0, 2.0], 5)
            .with_ef(100)
            .with_min_score(0.8);
        assert_eq!(f.k(), 5);
        assert_eq!(f.ef(), Some(100));
        assert_eq!(f.min_score(), Some(0.8));
        assert_eq!(f.query(), &[1.0, 2.0]);
        assert_eq!(f.get_field_name().unwrap(), "emb");
        assert!(f.is_index_only_filter());
        assert_eq!(f.supported_index_type().unwrap(), VECTOR_INDEX);
    }
}
