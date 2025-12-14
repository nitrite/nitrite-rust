//! Constants for disk-based R-Tree implementation.

/// Default page size (16KB) - balances I/O efficiency and memory usage
pub const PAGE_SIZE: usize = 16384;

/// Maximum number of entries per leaf node
pub const MAX_LEAF_ENTRIES: usize = 64;

/// Minimum entries before underflow (typically 40% of max)
pub const MIN_LEAF_ENTRIES: usize = 25;

/// Maximum children per internal node  
pub const MAX_INTERNAL_CHILDREN: usize = 64;

/// Minimum children before underflow
pub const MIN_INTERNAL_CHILDREN: usize = 25;

/// Default cache size in number of pages (16MB with 16KB pages)
pub const DEFAULT_CACHE_PAGES: usize = 1024;

/// Magic number for file format identification
pub const MAGIC: u32 = 0x4E525452; // "NRTR" - Nitrite R-Tree

/// File format version
pub const VERSION: u32 = 1;
