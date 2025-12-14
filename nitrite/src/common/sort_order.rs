/// Specifies the direction for sorting documents.
///
/// # Purpose
/// Defines whether documents should be sorted in ascending (low to high) or descending
/// (high to low) order. Used in query options to control result ordering.
///
/// # Variants
/// - `Ascending`: Sort from smallest to largest value (A to Z, 0 to 9, oldest to newest)
/// - `Descending`: Sort from largest to smallest value (Z to A, 9 to 0, newest to oldest)
///
/// # Usage
/// Used with `order_by()` helper function when querying collections:
/// ```text
/// let options = order_by("age", SortOrder::Ascending);
/// let cursor = collection.find_with_options(filter, &options)?;
/// ```
///
/// # Characteristics
/// - **Copy**: Can be copied instead of cloned
/// - **Comparable**: Can be compared for equality
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    /// Sort in ascending order (smallest to largest, A-Z, oldest to newest)
    Ascending,
    /// Sort in descending order (largest to smallest, Z-A, newest to oldest)
    Descending,
}
