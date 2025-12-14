//! Core types and data structures for disk-based R-Tree implementation.
//!
//! This module defines the fundamental types used throughout the R-Tree:
//! - Error types and result types
//! - Node types (Leaf and Internal)
//! - Statistics structures
//! - File header and serialization types

use nitrite::errors::NitriteError;
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur in spatial indexing operations
#[derive(Debug, Error)]
pub enum SpatialError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Tree is closed")]
    Closed,
}

impl From<SpatialError> for NitriteError {
    fn from(err: SpatialError) -> Self {
        match err {
            SpatialError::Io(io_err) => NitriteError::new(
                &format!("Spatial I/O error: {}", io_err),
                nitrite::errors::ErrorKind::IOError,
            ),
            SpatialError::Closed => NitriteError::new(
                "Spatial index is closed",
                nitrite::errors::ErrorKind::StoreAlreadyClosed,
            ),
            SpatialError::Serialization(msg) => {
                NitriteError::new(&msg, nitrite::errors::ErrorKind::EncodingError)
            }
            SpatialError::InvalidOperation(msg) => {
                NitriteError::new(&msg, nitrite::errors::ErrorKind::ValidationError)
            }
        }
    }
}

impl From<NitriteError> for SpatialError {
    fn from(err: NitriteError) -> Self {
        use nitrite::errors::ErrorKind;

        match err.kind() {
            ErrorKind::IOError
            | ErrorKind::FileNotFound
            | ErrorKind::PermissionDenied
            | ErrorKind::DiskFull
            | ErrorKind::FileCorrupted
            | ErrorKind::FileAccessError => SpatialError::Io(io::Error::new(
                io::ErrorKind::Other,
                err.message().to_string(),
            )),
            ErrorKind::EncodingError | ErrorKind::ObjectMappingError => {
                SpatialError::Serialization(err.message().to_string())
            }
            ErrorKind::StoreAlreadyClosed => SpatialError::Closed,
            _ => SpatialError::InvalidOperation(err.message().to_string()),
        }
    }
}

/// Result type for spatial operations
pub type SpatialResult<T> = Result<T, SpatialError>;

/// Type alias for Nitrite document IDs (64-bit unsigned integer)
pub type NitriteIdValue = u64;

/// Page ID - unique identifier for a node/page on disk
pub type PageId = u64;

// ============================================================================
// Statistics
// ============================================================================

/// Statistics about R-Tree operations
#[derive(Debug, Clone, Default)]
pub struct RTreeStats {
    pub total_entries: u64,
    pub cached_pages: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub disk_reads: u64,
    pub disk_writes: u64,
    pub tree_height: u32,
}

/// Statistics about index rebuild/optimization operations
#[derive(Debug, Clone)]
pub struct RebuildStats {
    /// Total entries reindexed
    pub entries_reindexed: u64,
    /// Number of pages before rebuild
    pub pages_before: u64,
    /// Number of pages after rebuild
    pub pages_after: u64,
    /// Tree height before rebuild
    pub height_before: u32,
    /// Tree height after rebuild
    pub height_after: u32,
    /// Fill factor improvement (0-100, higher is better)
    pub fill_factor_improvement: f64,
}

/// Metrics for detecting index fragmentation
#[derive(Debug, Clone)]
pub struct FragmentationMetrics {
    /// Percentage of wasted space in pages (0-100)
    pub wasted_space_percent: f64,
    /// Ratio of cache misses to total cache accesses
    pub cache_miss_ratio: f64,
    /// Average tree height vs optimal height ratio
    pub tree_balance_ratio: f64,
    /// Number of cached pages currently in use
    pub active_pages: u64,
    /// Total disk operations (reads + writes)
    pub disk_operations: u64,
    /// Is fragmentation above recommended rebuild threshold?
    pub should_rebuild: bool,
    /// Fragmentation severity: "None", "Low", "Medium", "High"
    pub severity: String,
}

impl FragmentationMetrics {
    /// Calculate fragmentation metrics from current tree state
    ///
    /// Fragmentation is considered high when:
    /// - Wasted space exceeds 30%
    /// - Cache miss ratio exceeds 0.5 (50%)
    /// - Tree is imbalanced (height ratio > 1.3)
    pub fn calculate(stats: &RTreeStats, entries: u64) -> Self {
        // Calculate wasted space percentage
        // Assuming ideal utilization would be 75% fill factor
        let ideal_entries_per_page = 50; // approximate based on geometry size
        let optimal_pages =
            ((entries + ideal_entries_per_page - 1) / ideal_entries_per_page) as f64;
        let actual_pages = stats.cached_pages as f64;
        let wasted_space_percent = if actual_pages > 0.0 {
            ((actual_pages - optimal_pages) / actual_pages * 100.0).max(0.0)
        } else {
            0.0
        };

        // Calculate cache miss ratio
        let total_cache_accesses = stats.cache_hits.saturating_add(stats.cache_misses);
        let cache_miss_ratio = if total_cache_accesses > 0 {
            stats.cache_misses as f64 / total_cache_accesses as f64
        } else {
            0.0
        };

        // Calculate tree balance ratio
        // Optimal height = log(entries) with branching factor ~50
        let branching_factor = 50.0;
        let optimal_height = if entries > 0 {
            (entries as f64).log(branching_factor).ceil() as u32
        } else {
            0
        };
        let tree_balance_ratio = if optimal_height > 0 {
            stats.tree_height as f64 / optimal_height.max(1) as f64
        } else {
            1.0
        };

        // Determine severity and rebuild flag
        let severity_score = (wasted_space_percent / 30.0) * 0.4
            + (cache_miss_ratio * 100.0 / 50.0) * 0.3
            + ((tree_balance_ratio - 1.0).max(0.0) / 0.3) * 0.3;

        let (severity, should_rebuild) = match severity_score {
            s if s < 0.3 => ("None".to_string(), false),
            s if s < 0.6 => ("Low".to_string(), false),
            s if s < 0.85 => ("Medium".to_string(), false),
            _ => ("High".to_string(), true),
        };

        Self {
            wasted_space_percent: wasted_space_percent.min(100.0),
            cache_miss_ratio,
            tree_balance_ratio,
            active_pages: stats.cached_pages,
            disk_operations: stats.disk_reads.saturating_add(stats.disk_writes),
            should_rebuild,
            severity,
        }
    }
}

// ============================================================================
// Internal Types for Serialization
// ============================================================================

/// Internal bounding box for serialization (avoid trait issues)
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct InternalBBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl InternalBBox {
    pub fn from_bbox(b: &crate::bounding_box::BoundingBox) -> Self {
        Self {
            min_x: b.min_x,
            min_y: b.min_y,
            max_x: b.max_x,
            max_y: b.max_y,
        }
    }

    #[allow(dead_code)]
    pub fn to_bbox(&self) -> crate::bounding_box::BoundingBox {
        crate::bounding_box::BoundingBox::new(self.min_x, self.min_y, self.max_x, self.max_y)
    }

    pub fn intersects(&self, other: &InternalBBox) -> bool {
        !(self.max_x < other.min_x
            || self.min_x > other.max_x
            || self.max_y < other.min_y
            || self.min_y > other.max_y)
    }

    pub fn contains(&self, other: &InternalBBox) -> bool {
        self.min_x <= other.min_x
            && self.min_y <= other.min_y
            && self.max_x >= other.max_x
            && self.max_y >= other.max_y
    }

    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    pub fn enlargement(&self, other: &InternalBBox) -> f64 {
        self.merge(other).area() - self.area()
    }

    pub fn merge(&self, other: &InternalBBox) -> InternalBBox {
        InternalBBox {
            min_x: self.min_x.min(other.min_x),
            min_y: self.min_y.min(other.min_y),
            max_x: self.max_x.max(other.max_x),
            max_y: self.max_y.max(other.max_y),
        }
    }

    pub fn expand(&mut self, other: &InternalBBox) {
        self.min_x = self.min_x.min(other.min_x);
        self.min_y = self.min_y.min(other.min_y);
        self.max_x = self.max_x.max(other.max_x);
        self.max_y = self.max_y.max(other.max_y);
    }

    pub fn empty() -> Self {
        Self {
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.min_x > self.max_x || self.min_y > self.max_y
    }
}

// ============================================================================
// Node Types
// ============================================================================

/// An entry in a leaf node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafEntry {
    pub bbox: InternalBBox,
    pub id: NitriteIdValue,
}

/// A child reference in an internal node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildRef {
    pub bbox: InternalBBox,
    pub page_id: PageId,
}

/// Node types in the R-Tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    /// Leaf node containing actual entries
    Leaf { entries: Vec<LeafEntry> },
    /// Internal node containing child references
    Internal {
        children: Vec<ChildRef>,
        level: u32, // Height from leaf level (leaves are 0)
    },
}

impl Node {
    /// Get the bounding box enclosing all children/entries
    pub fn compute_bbox(&self) -> InternalBBox {
        match self {
            Node::Leaf { entries } => {
                let mut bbox = InternalBBox::empty();
                for e in entries {
                    bbox.expand(&e.bbox);
                }
                bbox
            }
            Node::Internal { children, .. } => {
                let mut bbox = InternalBBox::empty();
                for c in children {
                    bbox.expand(&c.bbox);
                }
                bbox
            }
        }
    }

    #[allow(dead_code)]
    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf { .. })
    }

    pub fn len(&self) -> usize {
        match self {
            Node::Leaf { entries } => entries.len(),
            Node::Internal { children, .. } => children.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(dead_code)]
    pub fn is_full(&self) -> bool {
        match self {
            Node::Leaf { entries } => entries.len() >= 64, // MAX_LEAF_ENTRIES
            Node::Internal { children, .. } => children.len() >= 64, // MAX_INTERNAL_CHILDREN
        }
    }

    pub fn is_underfull(&self) -> bool {
        match self {
            Node::Leaf { entries } => entries.len() < 25, // MIN_LEAF_ENTRIES
            Node::Internal { children, .. } => children.len() < 25, // MIN_INTERNAL_CHILDREN
        }
    }
}

// ============================================================================
// Free List Page
// ============================================================================

/// A free page in the free list chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreePage {
    /// Next free page in the chain (0 = end of chain)
    pub next_free: PageId,
}

// ============================================================================
// Page with Checksum
// ============================================================================

/// A page wrapped with CRC32 checksum for corruption detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageWithChecksum {
    /// CRC32 checksum of the node data
    pub checksum: u32,
    /// The actual node data
    pub node: Node,
}

impl PageWithChecksum {
    pub fn new(node: Node) -> Self {
        let checksum = Self::calculate_checksum(&node);
        Self { checksum, node }
    }

    /// Calculate CRC32 checksum of node data
    pub fn calculate_checksum(node: &Node) -> u32 {
        let serialized =
            bincode::serde::encode_to_vec(node, bincode::config::legacy()).unwrap_or_default();
        Self::crc32(&serialized)
    }

    /// CRC32-MPEG2 implementation (matching common checksums)
    fn crc32(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFFFFFF;
        const POLY: u32 = 0x04C11DB7;

        for &byte in data {
            crc ^= (byte as u32) << 24;
            for _ in 0..8 {
                crc = if crc & 0x80000000 != 0 {
                    (crc << 1) ^ POLY
                } else {
                    crc << 1
                };
            }
        }

        crc ^ 0xFFFFFFFF
    }

    /// Verify checksum and return node if valid
    pub fn verify(&self) -> SpatialResult<&Node> {
        let expected = Self::calculate_checksum(&self.node);
        if self.checksum != expected {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Page checksum mismatch - possible corruption (expected: {:x}, got: {:x})",
                    expected, self.checksum
                ),
            )));
        }
        Ok(&self.node)
    }

    /// Verify checksum and consume self to return node
    pub fn into_node(self) -> SpatialResult<Node> {
        let expected = Self::calculate_checksum(&self.node);
        if self.checksum != expected {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Page checksum mismatch - possible corruption (expected: {:x}, got: {:x})",
                    expected, self.checksum
                ),
            )));
        }
        Ok(self.node)
    }
}

// ============================================================================
// File Header
// ============================================================================

/// File header stored at the beginning of the R-Tree file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub root_page: PageId,
    pub next_page_id: PageId,
    pub entry_count: u64,
    pub height: u32,
    pub free_list_head: PageId,
    pub checksum_enabled: bool,
    pub free_page_count: u64,
}

impl FileHeader {
    pub fn new() -> Self {
        Self {
            magic: 0x4E525452, // MAGIC
            version: 1,        // VERSION
            page_size: 16384,  // PAGE_SIZE
            root_page: 0,
            next_page_id: 1,
            entry_count: 0,
            height: 0,
            free_list_head: 0,
            checksum_enabled: true,
            free_page_count: 0,
        }
    }

    pub fn validate(&self) -> SpatialResult<()> {
        if self.magic != 0x4E525452 {
            return Err(SpatialError::InvalidOperation(
                "Invalid file format (bad magic)".into(),
            ));
        }
        if self.version != 1 {
            return Err(SpatialError::InvalidOperation(
                "Unsupported file format version".into(),
            ));
        }
        Ok(())
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}
