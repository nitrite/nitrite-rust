//! # Nitrite Tantivy FTS - Full-Text Search Indexing for Nitrite Database
//!
//! This crate provides full-text search indexing capabilities for the Nitrite database,
//! using Tantivy as the underlying search engine.
//!
//! ## Features
//!
//! - **Full-Text Search**: Index and search text fields with BM25 scoring
//! - **Phrase Queries**: Match exact phrases in documents
//! - **Fuzzy Search**: Find documents with approximate term matches
//! - **Prefix Search**: Match terms by prefix
//! - **Persistent**: Index data survives process restarts
//! - **Thread Safe**: Concurrent read/write support
//! - **Configurable**: Tune memory usage and performance
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use nitrite_tantivy_fts::{TantivyFtsModule, fts_index, fts_field};
//! use nitrite::nitrite_builder::NitriteBuilder;
//! use nitrite::common::PersistentCollection;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Register FTS module with default configuration
//! let db = Nitrite::builder()
//!     .load_module(TantivyFtsModule::default())
//!     .open_or_create(None, None)?;
//!
//! // Or with custom configuration
//! let db = Nitrite::builder()
//!     .load_module(
//!         TantivyFtsModule::with_config()
//!             .index_writer_heap_size(100 * 1024 * 1024)  // 100 MB
//!             .num_threads(4)
//!             .build()
//!     )
//!     .open_or_create(None, None)?;
//!
//! // Create FTS index on a field
//! let collection = db.collection("articles")?;
//! collection.create_index(vec!["content"], &fts_index())?;
//!
//! // Query with full-text search
//! let filter = fts_field("content").matches("search terms");
//! let results = collection.find(filter, None)?;
//! # Ok(())
//! # }
//! ```

// Core modules
pub mod config;
pub mod filter;
pub mod fluent;
pub mod fts_module;
pub mod index;
pub mod indexer;

// Re-export config types
pub use config::FtsConfig;

// Re-export filter types
pub use filter::{FtsFilter, PhraseFilter, TextSearchFilter, FTS_INDEX};

// Re-export fluent API
pub use fluent::{fts_field, FtsFluentFilter};

// Re-export indexer types
pub use indexer::FtsIndexer;

// Re-export module
pub use fts_module::{TantivyFtsModule, TantivyFtsModuleBuilder};

/// Creates index options for a full-text search index.
///
/// This is a convenience function to create `IndexOptions` for FTS indexes.
///
/// # Example
///
/// ```rust,no_run
/// # use nitrite_tantivy_fts::fts_index;
/// let options = fts_index();
/// // Use with collection.create_index(vec!["content"], &options)?;
/// ```
pub fn fts_index() -> nitrite::index::IndexOptions {
    nitrite::index::IndexOptions::new(FTS_INDEX)
}
