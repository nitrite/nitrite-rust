//! Fluent API for building vector search filters.
//!
//! ```rust
//! use nitrite_vector::vector_field;
//!
//! // 10 nearest neighbours of the query, higher recall, score cutoff.
//! let filter = vector_field("embedding")
//!     .nearest(vec![0.1, 0.2, 0.3], 10)
//!     .ef(128)
//!     .min_score(0.75)
//!     .build();
//! ```

use nitrite::filter::Filter;

use crate::filter::VectorNearestFilter;

/// Entry point for the vector fluent filter builder.
pub fn vector_field(field: impl Into<String>) -> VectorFluentFilter {
    VectorFluentFilter { field: field.into() }
}

/// Fluent builder anchored to a vector field.
pub struct VectorFluentFilter {
    field: String,
}

impl VectorFluentFilter {
    /// Begins a k-nearest-neighbor query with the given query vector.
    pub fn nearest(self, query: Vec<f32>, k: usize) -> VectorNearestBuilder {
        VectorNearestBuilder {
            filter: VectorNearestFilter::new(self.field, query, k),
        }
    }
}

/// Builder for a kNN query, allowing optional `ef` / `min_score` refinement.
pub struct VectorNearestBuilder {
    filter: VectorNearestFilter,
}

impl VectorNearestBuilder {
    /// Sets the query-time `ef` (search width; larger = higher recall, slower).
    pub fn ef(mut self, ef: usize) -> Self {
        self.filter = self.filter.with_ef(ef);
        self
    }

    /// Sets a minimum similarity score cutoff.
    pub fn min_score(mut self, min_score: f32) -> Self {
        self.filter = self.filter.with_min_score(min_score);
        self
    }

    /// Finalizes the builder into a [`Filter`].
    pub fn build(self) -> Filter {
        Filter::new(self.filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_filter_with_field() {
        let filter = vector_field("embedding").nearest(vec![1.0, 2.0], 5).build();
        let s = format!("{filter}");
        assert!(s.contains("embedding"));
        assert!(s.contains("nearest"));
    }

    #[test]
    fn builder_chains_ef_and_min_score() {
        let filter = vector_field("emb")
            .nearest(vec![0.0, 1.0], 3)
            .ef(64)
            .min_score(0.5)
            .build();
        assert!(format!("{filter}").contains("emb"));
    }
}
