use crate::{SortOrder, SortableFields};
use icu_collator::options::CollatorOptions;
use icu_collator::CollatorPreferences;

/// Options for controlling find operations on documents.
///
/// `FindOptions` allows you to specify sorting, pagination, and distinctness
/// for query results. It supports method chaining for convenient configuration.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::collection::FindOptions;
/// use nitrite::SortOrder;
///
/// // Create options with sorting, skip, and limit
/// let options = FindOptions::new()
///     .sort_by("age", SortOrder::Descending)
///     .skip(10)
///     .limit(20);
///
/// // Use convenience functions
/// let options = order_by("name", SortOrder::Ascending);
/// let options = skip_by(5);
/// let options = limit_to(100);
/// let options = distinct();
/// ```
pub struct FindOptions {
    pub(crate) sort_by: Option<SortableFields>,
    pub(crate) skip: Option<u64>,
    pub(crate) limit: Option<u64>,
    pub(crate) distinct: bool,
    pub(crate) collator_options: Option<CollatorOptions>,
    pub(crate) collator_preferences: Option<CollatorPreferences>,
}

/// Creates `FindOptions` with sorting by a field.
///
/// # Arguments
///
/// * `field_name` - The field to sort by
/// * `sort_order` - The sort order (Ascending or Descending)
///
/// # Returns
///
/// A new `FindOptions` with sorting configured
pub fn order_by(field_name: &str, sort_order: SortOrder) -> FindOptions {
    let fields = SortableFields::new();
    let fields = fields.add_sorted_field(field_name.to_string(), sort_order);

    FindOptions {
        sort_by: Some(fields),
        skip: None,
        limit: None,
        distinct: false,
        collator_options: None,
        collator_preferences: None,
    }
}

/// Creates `FindOptions` that skips a number of results.
///
/// Useful for pagination: skip the first N results and process the remaining.
///
/// # Arguments
///
/// * `skip` - Number of documents to skip
///
/// # Returns
///
/// A new `FindOptions` with skip configured
pub fn skip_by(skip: u64) -> FindOptions {
    FindOptions {
        sort_by: None,
        skip: Some(skip),
        limit: None,
        distinct: false,
        collator_options: None,
        collator_preferences: None,
    }
}

/// Creates `FindOptions` that limits the number of results.
///
/// Combined with skip for pagination: skip(10).limit(20) returns results 11-30.
///
/// # Arguments
///
/// * `limit` - Maximum number of documents to return
///
/// # Returns
///
/// A new `FindOptions` with limit configured
pub fn limit_to(limit: u64) -> FindOptions {
    FindOptions {
        sort_by: None,
        skip: None,
        limit: Some(limit),
        distinct: false,
        collator_options: None,
        collator_preferences: None,
    }
}

/// Creates `FindOptions` that returns only distinct documents.
///
/// This removes duplicate documents from the result set based on their content.
pub fn distinct() -> FindOptions {
    FindOptions {
        sort_by: None,
        skip: None,
        limit: None,
        distinct: true,
        collator_options: None,
        collator_preferences: None,
    }
}

impl FindOptions {
    /// Creates a new `FindOptions` with default settings.
    pub fn new() -> FindOptions {
        FindOptions {
            sort_by: None,
            skip: None,
            limit: None,
            distinct: false,
            collator_options: Some(CollatorOptions::default()),
            collator_preferences: Some(CollatorPreferences::default()),
        }
    }

    /// Sets the number of documents to skip.
    ///
    /// # Arguments
    ///
    /// * `skip` - Number of documents to skip from the beginning
    pub fn skip(mut self, skip: u64) -> FindOptions {
        self.skip = Some(skip);
        self
    }

    /// Sets the maximum number of documents to return.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of documents to return
    pub fn limit(mut self, limit: u64) -> FindOptions {
        self.limit = Some(limit);
        self
    }

    pub fn sort_by(mut self, field_name: String, sort_order: SortOrder) -> FindOptions {
        let fields = self.sort_by.unwrap_or_else(|| SortableFields::new());

        let fields = fields.add_sorted_field(field_name, sort_order);
        self.sort_by = Some(fields);
        self
    }

    pub fn distinct(mut self) -> FindOptions {
        self.distinct = true;
        self
    }

    pub fn collator_options(mut self, collator: CollatorOptions) -> FindOptions {
        self.collator_options = Some(collator);
        self
    }
    
    pub fn collator_preferences(mut self, collator: CollatorPreferences) -> FindOptions {
        self.collator_preferences = Some(collator);
        self
    }
}

impl Default for FindOptions {
    fn default() -> Self {
        FindOptions::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SortOrder;

    #[test]
    fn test_order_by() {
        let field_name = "name";
        let sort_order = SortOrder::Ascending;
        let options = order_by(field_name, sort_order);

        assert!(options.sort_by.is_some());
        let fields = options.sort_by.unwrap();
        assert_eq!(fields.sorting_order().len(), 1);
        assert_eq!(fields.sorting_order()[0].0, field_name);
        assert_eq!(fields.sorting_order()[0].1, sort_order);
    }

    #[test]
    fn test_skip_by() {
        let skip = 10;
        let options = skip_by(skip);

        assert_eq!(options.skip, Some(skip));
        assert!(options.sort_by.is_none());
        assert!(options.limit.is_none());
        assert!(!options.distinct);
        assert!(options.collator_options.is_none());
    }

    #[test]
    fn test_limit_to() {
        let limit = 5;
        let options = limit_to(limit);

        assert_eq!(options.limit, Some(limit));
        assert!(options.sort_by.is_none());
        assert!(options.skip.is_none());
        assert!(!options.distinct);
        assert!(options.collator_options.is_none());
    }

    #[test]
    fn test_distinct() {
        let options = distinct();

        assert!(options.distinct);
        assert!(options.sort_by.is_none());
        assert!(options.skip.is_none());
        assert!(options.limit.is_none());
        assert!(options.collator_options.is_none());
    }

    #[test]
    fn test_find_options_new() {
        let options = FindOptions::new();

        assert!(options.sort_by.is_none());
        assert!(options.skip.is_none());
        assert!(options.limit.is_none());
        assert!(!options.distinct);
        assert!(options.collator_options.is_some());
    }

    #[test]
    fn test_find_options_skip() {
        let skip = 10;
        let options = FindOptions::new().skip(skip);

        assert_eq!(options.skip, Some(skip));
    }

    #[test]
    fn test_find_options_limit() {
        let limit = 5;
        let options = FindOptions::new().limit(limit);

        assert_eq!(options.limit, Some(limit));
    }

    #[test]
    fn test_find_options_sort_by() {
        let field_name = "name".to_string();
        let sort_order = SortOrder::Ascending;
        let options = FindOptions::new().sort_by(field_name.clone(), sort_order);

        assert!(options.sort_by.is_some());
        let fields = options.sort_by.unwrap();
        assert_eq!(fields.sorting_order().len(), 1);
        assert_eq!(fields.sorting_order()[0].0, field_name);
        assert_eq!(fields.sorting_order()[0].1, sort_order);
    }

    #[test]
    fn test_find_options_distinct() {
        let options = FindOptions::new().distinct();

        assert!(options.distinct);
    }

    #[test]
    fn test_find_options_collator_options() {
        let collator = CollatorOptions::default();
        let options = FindOptions::new().collator_options(collator.clone());

        assert_eq!(options.collator_options.unwrap().alternate_handling, collator.alternate_handling);
    }
    
    #[test]
    fn test_find_options_collator_preferences() {
        let collator = CollatorPreferences::default();
        let options = FindOptions::new().collator_preferences(collator.clone());

        assert_eq!(options.collator_preferences.unwrap().case_first, collator.case_first);
    }

    #[test]
    fn test_find_options_default() {
        let options = FindOptions::default();

        assert!(options.sort_by.is_none());
        assert!(options.skip.is_none());
        assert!(options.limit.is_none());
        assert!(!options.distinct);
        assert!(options.collator_options.is_some());
        assert!(options.collator_preferences.is_some());
    }
}