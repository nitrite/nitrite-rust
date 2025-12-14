use crate::{FULL_TEXT_INDEX, NON_UNIQUE_INDEX, UNIQUE_INDEX};

/// Specifies configuration options for creating database indexes.
///
/// IndexOptions encapsulates the index type selection for collection operations.
/// It is used to configure how fields are indexed when creating new indexes
/// on document collections.
///
/// # Purpose
/// IndexOptions provides a type-safe way to specify which indexing strategy
/// should be used for specific fields. Different index types enforce different
/// constraints and have different performance characteristics.
///
/// # Characteristics
/// - **Cloneable**: Inexpensive to clone (wraps String)
/// - **Type identifier**: Stores index type as string (UNIQUE, NON_UNIQUE, FULL_TEXT)
/// - **Immutable**: Once created, index type cannot be changed
/// - **Default behavior**: Defaults to UNIQUE_INDEX if not explicitly set
///
/// # Index Types
/// - **UNIQUE**: Each field value must be unique across documents. Duplicate values rejected.
/// - **NON_UNIQUE**: Multiple documents can have the same field value. Allows duplicates.
/// - **FULL_TEXT**: Full-text search indexing for text fields.
///
/// # Usage
///
/// IndexOptions is passed to collection.create_index() when creating new indexes:
/// ```ignore
/// // Create unique index on user_id field
/// collection.create_index(vec!["user_id"], &unique_index())?;
///
/// // Create non-unique index on department field
/// collection.create_index(vec!["department"], &non_unique_index())?;
///
/// // Create custom index with explicit type
/// collection.create_index(vec!["name"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
/// ```
///
/// # Responsibilities
/// - **Index type specification**: Determines indexing strategy
/// - **Configuration encapsulation**: Bundles all index configuration in one place
/// - **Type validation**: Stores valid index type identifiers
#[derive(Clone)]
pub struct IndexOptions {
    index_type: String,
}

impl IndexOptions {
    /// Creates a new IndexOptions with the specified index type.
    ///
    /// # Arguments
    /// * `index_type` - String identifier for the index type (e.g., "UNIQUE", "NON_UNIQUE", "FULL_TEXT")
    ///
    /// # Returns
    /// A new IndexOptions instance with the given index type.
    ///
    /// # Behavior
    /// Converts the string reference to owned String for storage.
    /// No validation is performed on the index_type value.
    ///
    /// # Usage
    /// Direct construction with custom or predefined index type constants:
    /// ```ignore
    /// let opts = IndexOptions::new(NON_UNIQUE_INDEX);
    /// ```
    pub fn new(index_type: &str) -> IndexOptions {
        IndexOptions { index_type: index_type.to_string() }
    }

    /// Retrieves the index type identifier.
    ///
    /// # Returns
    /// A String containing the index type identifier (e.g., "UNIQUE", "NON_UNIQUE", "FULL_TEXT").
    ///
    /// # Behavior
    /// Returns a clone of the internally stored index type string.
    /// This is an inexpensive operation suitable for checking index configuration.
    ///
    /// # Usage
    /// Determine which indexing strategy is configured:
    /// ```ignore
    /// let opts = unique_index();
    /// assert_eq!(opts.index_type(), UNIQUE_INDEX);
    /// ```
    pub fn index_type(&self) -> String {
        self.index_type.clone()
    }
}

impl Default for IndexOptions {
    /// Provides default IndexOptions using UNIQUE_INDEX strategy.
    ///
    /// # Returns
    /// IndexOptions configured with UNIQUE_INDEX type.
    ///
    /// # Behavior
    /// When no explicit index options are provided, creates a unique index.
    /// This is the most restrictive index type, ensuring field value uniqueness.
    ///
    /// # Usage
    /// Used when default index configuration is suitable:
    /// ```ignore
    /// let opts = IndexOptions::default();
    /// assert_eq!(opts.index_type(), UNIQUE_INDEX);
    /// ```
    fn default() -> Self {
        IndexOptions::new(UNIQUE_INDEX)
    }
}

/// Creates IndexOptions for a unique index.
///
/// # Returns
/// IndexOptions configured for unique indexing strategy.
///
/// # Behavior
/// Convenience function equivalent to `IndexOptions::new(UNIQUE_INDEX)`.
/// Creates an index where all field values must be unique across documents.
/// Attempting to insert duplicate values will fail with constraint violation error.
///
/// # Characteristics
/// - Enforces uniqueness constraint on indexed fields
/// - Most restrictive index type
/// - Optimal for fields like user IDs, email addresses, usernames
/// - Default index type if none specified
///
/// # Usage
/// Create unique index on identifier fields:
/// ```ignore
/// collection.create_index(vec!["user_id"], &unique_index())?;
/// collection.create_index(vec!["email"], &unique_index())?;
/// ```
pub fn unique_index() -> IndexOptions {
    IndexOptions::new(UNIQUE_INDEX)
}

/// Creates IndexOptions for a non-unique index.
///
/// # Returns
/// IndexOptions configured for non-unique indexing strategy.
///
/// # Behavior
/// Convenience function equivalent to `IndexOptions::new(NON_UNIQUE_INDEX)`.
/// Creates an index where multiple documents can have the same field value.
/// Allows duplicates and bulk operations without uniqueness constraints.
///
/// # Characteristics
/// - Permits duplicate field values across documents
/// - Less restrictive than unique indexes
/// - Better for high-cardinality or categorical fields
/// - Used for most query optimization scenarios
///
/// # Usage
/// Create non-unique index on categorical or searchable fields:
/// ```ignore
/// collection.create_index(vec!["department"], &non_unique_index())?;
/// collection.create_index(vec!["status"], &non_unique_index())?;
/// collection.create_index(vec!["age"], &non_unique_index())?;
/// ```
pub fn non_unique_index() -> IndexOptions {
    IndexOptions::new(NON_UNIQUE_INDEX)
}

/// Creates IndexOptions for a full-text search index.
///
/// # Returns
/// IndexOptions configured for full-text search indexing strategy.
///
/// # Behavior
/// Convenience function equivalent to `IndexOptions::new(FULL_TEXT_INDEX)`.
/// Creates an index optimized for full-text search operations on text fields.
/// Enables phrase search, tokenization, and text-based query optimization.
///
/// # Characteristics
/// - Specialized for textual content indexing
/// - Supports full-text search queries
/// - Handles natural language text fields
/// - Enables text tokenization and phrase matching
///
/// # Usage
/// Create full-text index on text content fields:
/// ```ignore
/// collection.create_index(vec!["description"], &full_text_index())?;
/// collection.create_index(vec!["content"], &full_text_index())?;
/// ```
pub fn full_text_index() -> IndexOptions {
    IndexOptions::new(FULL_TEXT_INDEX)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_options_new() {
        let index_options = IndexOptions::new("test_index");
        assert_eq!(index_options.index_type(), "test_index");
    }

    #[test]
    fn test_index_options_default() {
        let index_options = IndexOptions::default();
        assert_eq!(index_options.index_type(), UNIQUE_INDEX);
    }

    #[test]
    fn test_unique_index() {
        let index_options = unique_index();
        assert_eq!(index_options.index_type(), UNIQUE_INDEX);
    }

    #[test]
    fn test_non_unique_index() {
        let index_options = non_unique_index();
        assert_eq!(index_options.index_type(), NON_UNIQUE_INDEX);
    }

    #[test]
    fn test_full_text_index() {
        let index_options = full_text_index();
        assert_eq!(index_options.index_type(), FULL_TEXT_INDEX);
    }
}