//! Query filters for selecting documents from collections.
//!
//! This module provides a comprehensive filtering API for querying documents
//! in Nitrite. Filters can be combined using logical operators and support
//! various comparison operations.
//!
//! # Creating Filters
//!
//! Filters are created using the fluent API:
//! - `field("age").gt(30)` - comparison operators
//! - `field("name").eq("Alice")` - equality checks
//! - `all()` - match all documents
//! - `by_id(id)` - match by document ID
//! - `field("name").and(field("age").gt(30))` - logical AND
//!
//! # Examples
//!
//! ```rust,ignore
//! use nitrite::filter::{field, all};
//!
//! // Simple filters using fluent API
//! let age_filter = field("age").gt(30);
//! let price_filter = field("price").gt(100.0);
//! let email_filter = field("email").regex(".*@example\\.com");
//!
//! // Fluent API with logical combinations
//! let filter = field("age").gt(30).and(field("status").eq("active"));
//!
//! // Using logical operators
//! let filter = field("age").eq(30);
//! let other = field("country").eq("USA");
//! let combined = filter.and(other);
//!
//! // Using filters with collections
//! let results = collection.find(filter)?;
//! ```
//!
//! # Supported Operators
//!
//! - **Equality**: `eq`, `ne`
//! - **Comparison**: `gt`, `gte`, `lt`, `lte`
//! - **Pattern**: `regex`, `text`
//! - **Array**: `in`, `nin`, `elemMatch`
//! - **Logical**: `and`, `or`, `not`
//! - **Special**: `all` (match all), `by_id` (match by ID)

mod filter;
mod fluent;

// New modular filter implementations
mod basic_filters;
mod logical_filters;
mod range_filters;
mod pattern_filters;

pub use basic_filters::*;
pub use filter::*;
pub use fluent::*;
pub use logical_filters::*;
pub use pattern_filters::*;
pub use range_filters::*;