//! Serializable records for the HNSW graph.
//!
//! A [`NodeRecord`] is used both as the in-memory graph node and as the
//! on-disk representation (bincode-encoded into a `Value::Bytes` in the
//! index's `NitriteMap`), so there is a single source of truth for a node's
//! vector and its per-level neighbor lists.

use serde::{Deserialize, Serialize};

use crate::distance::Metric;

/// A single node in the HNSW graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The document id this node indexes (`NitriteId::id_value()`).
    pub id: u64,
    /// The (metric-prepared) vector. For cosine indexes this is L2-normalized.
    pub vector: Vec<f32>,
    /// Neighbor ids per level, `neighbors[level]`. `neighbors.len() - 1` is the
    /// node's top level.
    pub neighbors: Vec<Vec<u64>>,
}

impl NodeRecord {
    /// The top level this node participates in.
    #[inline]
    pub fn top_level(&self) -> usize {
        self.neighbors.len() - 1
    }
}

/// The per-index header, persisted under a reserved key so index parameters and
/// the entry point survive restarts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswHeader {
    pub dim: usize,
    pub metric: Metric,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub entry_point: Option<u64>,
    pub max_level: usize,
}

/// Encodes a serializable value with the standard bincode config.
pub(crate) fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
}

/// Decodes a value previously produced by [`to_bytes`].
pub(crate) fn from_bytes<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
) -> Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map(|(v, _)| v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_record_round_trips() {
        let node = NodeRecord {
            id: 42,
            vector: vec![0.1, 0.2, 0.3],
            neighbors: vec![vec![1, 2, 3], vec![4, 5], vec![]],
        };
        let bytes = to_bytes(&node).unwrap();
        let back: NodeRecord = from_bytes(&bytes).unwrap();
        assert_eq!(node, back);
        assert_eq!(back.top_level(), 2);
    }

    #[test]
    fn header_round_trips() {
        let header = HnswHeader {
            dim: 128,
            metric: Metric::Cosine,
            m: 16,
            ef_construction: 200,
            ef_search: 64,
            entry_point: Some(7),
            max_level: 3,
        };
        let bytes = to_bytes(&header).unwrap();
        let back: HnswHeader = from_bytes(&bytes).unwrap();
        assert_eq!(header, back);
    }
}
