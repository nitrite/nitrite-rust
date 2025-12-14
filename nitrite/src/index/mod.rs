//! Indexing support for optimized querying.
//!
//! This module provides indexing capabilities to accelerate document queries.
//! Indexes map field values to document IDs, enabling fast lookups and range scans.
//!
//! # Index Types
//!
//! - **Unique Index**: Ensures field values are unique across all documents
//! - **Non-Unique Index**: Allows duplicate field values, maps to multiple documents
//! - **Text Index**: Full-text search index for substring and text matching
//! - **Compound Index**: Index on multiple fields for multi-field queries
//!
//! # Creating Indexes
//!
//! ```rust,ignore
//! use nitrite::index::unique_index;
//!
//! let collection = db.collection("users")?;
//! collection.create_index(vec!["email"], &unique_index())?;
//! collection.create_index(vec!["name", "age"], &non_unique_index())?;
//! ```
//!
//! # Index Management
//!
//! - **Automatic indexing**: Indexes are automatically used when applicable
//! - **Index hints**: Filters support specifying preferred indexes
//! - **Index rebuilding**: Rebuild corrupted or fragmented indexes
//! - **Index dropping**: Remove indexes to save space
//!
//! # Performance Considerations
//!
//! - Indexes speed up `find()` and filtering operations
//! - Indexes slow down insert/update/delete operations (index maintenance cost)
//! - Create indexes on frequently queried fields
//! - Unique indexes prevent duplicate values automatically

mod descriptor;
mod nitrite_indexer;
mod index_map;
pub mod index_meta;
mod nitrite_index;
mod compound_index;
pub mod text;
pub mod index_scanner;
mod options;
mod text_index;
mod simple_index;
pub mod text_indexer;
pub mod unique_indexer;
pub mod non_unique_indexer;

pub use descriptor::*;
pub use index_map::*;
pub use nitrite_indexer::*;
pub use options::*;
