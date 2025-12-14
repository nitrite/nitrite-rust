//! Collections and documents for schemaless data storage.
//!
//! This module provides the core document storage abstraction in Nitrite.
//! Collections store unstructured documents and support flexible querying, indexing, and updates.
//!
//! # Documents
//!
//! A `Document` is a key-value map where keys are strings and values are `Value` objects.
//! Documents support nested fields using a configurable separator (default: ".").
//!
//! ```rust,ignore
//! use nitrite::collection::Document;
//!
//! let mut doc = Document::new();
//! doc.put("name", "Alice")?;
//! doc.put("address.city", "New York")?;
//! doc.put("age", 30i64)?;
//! ```
//!
//! # Collections
//!
//! A `NitriteCollection` manages documents with the same logical type. Collections support:
//! - Insert, update, remove operations
//! - Flexible querying with filters
//! - Automatic and manual indexing
//! - Event listeners for change notifications
//!
//! ```rust,ignore
//! use nitrite::collection::Document;
//! use nitrite::filter::field;
//!
//! let mut users = db.collection("users")?;
//!
//! // Insert
//! let mut doc = Document::new();
//! doc.put("name", "Alice")?;
//! let result = users.insert(doc)?;
//!
//! // Query
//! let filter = field("age").eq(30);
//! let results = users.find(filter)?;
//! ```
//!
//! # Document IDs
//!
//! Each document has a unique `_id` field containing a `NitriteId`. The ID is automatically
//! generated using a Snowflake algorithm if not provided during insertion.
//!
//! # Reserved Fields
//!
//! The following fields are reserved and managed by Nitrite:
//! - `_id` - Document ID
//! - `_revision` - Revision number
//! - `_source` - Document source
//! - `_modified` - Last modification timestamp

mod document;
mod event;
mod nitrite_id;
mod find_plan;
pub(crate) mod snowflake;
pub(crate) mod operation;
mod find_options;
mod update_options;
mod nitrite_collection;
mod default_nitrite_collection;
mod collection_factory;

pub(crate) use collection_factory::*;
pub use document::*;
pub use event::*;
pub use find_options::*;
pub use find_plan::*;
pub use nitrite_collection::*;
pub use nitrite_id::NitriteId;
pub use update_options::*;