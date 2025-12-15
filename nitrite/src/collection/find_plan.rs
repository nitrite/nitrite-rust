use crate::{
    filter::{Filter, IndexScanFilter},
    index::IndexDescriptor,
    SortOrder,
};
use icu_collator::options::CollatorOptions;
use icu_collator::CollatorPreferences;
use std::collections::HashMap;
use std::sync::Arc;

/// Represents an execution plan for a database query.
///
/// A `FindPlan` describes how a query should be executed, including:
///
/// * **Index Usage** - Whether to scan an index and which one
/// * **Filtering** - What filters to apply (by ID, index scan, or full table scan)
/// * **Sorting** - Blocking sort requirements after filtering
/// * **Pagination** - Skip and limit for result pagination
/// * **Distinct** - Whether to deduplicate results
/// * **Sub-Plans** - Nested plans for composite queries
/// * **Collation** - Text comparison options for sorting
///
/// The plan is typically created by the query optimizer and used internally
/// by index implementations to efficiently retrieve matching documents.
#[derive(Clone)]
pub struct FindPlan {
    /// Arc-wrapped implementation pointer (opaque to users)
    inner: Arc<FindPlanInner>,
}

impl FindPlan {
    /// Creates a new empty `FindPlan`.
    ///
    /// The plan starts with all options unset. Individual fields can be set
    /// by the query optimizer as needed.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.by_id_filter().is_none());
    /// assert!(!plan.distinct());
    /// ```
    pub(crate) fn new() -> Self {
        FindPlan {
            inner: Arc::new(FindPlanInner::new()),
        }
    }

    /// Returns the by-ID filter if present.
    ///
    /// When a query directly filters by document ID (e.g., `find_by_id()`),
    /// this filter is set instead of other filters for maximum efficiency.
    ///
    /// # Returns
    ///
    /// `Some(Filter)` if the plan uses ID-based lookup, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.by_id_filter().is_none());
    /// ```
    pub fn by_id_filter(&self) -> Option<Filter> {
        self.inner.by_id_filter.clone()
    }

    /// Returns the index scan filter if present.
    ///
    /// This filter describes which index to scan and what range/bounds to apply.
    /// Used when the optimizer determines an index provides the most efficient access.
    ///
    /// # Returns
    ///
    /// `Some(IndexScanFilter)` if the plan uses index scanning, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.index_scan_filter().is_none());
    /// ```
    pub fn index_scan_filter(&self) -> Option<IndexScanFilter> {
        self.inner.index_scan_filter.clone()
    }

    /// Returns the full table scan filter if present.
    ///
    /// This filter is applied when no index can be used and a full scan is necessary.
    /// All documents in the collection must be checked against this filter.
    ///
    /// # Returns
    ///
    /// `Some(Filter)` if the plan uses full table scan, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.full_scan_filter().is_none());
    /// ```
    pub fn full_scan_filter(&self) -> Option<Filter> {
        self.inner.full_scan_filter.clone()
    }

    /// Returns the index descriptor if an index is to be used.
    ///
    /// Describes which index (field name, type, etc.) should be scanned.
    /// Present only when `index_scan_filter()` is `Some`.
    ///
    /// # Returns
    ///
    /// `Some(IndexDescriptor)` if the plan uses an index, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.index_descriptor().is_none());
    /// ```
    pub fn index_descriptor(&self) -> Option<IndexDescriptor> {
        self.inner.index_descriptor.clone()
    }

    /// Returns the index scan order mapping if present.
    ///
    /// Maps field names to boolean values indicating sort order:
    /// - `true` = ascending order
    /// - `false` = descending order
    ///
    /// This describes the natural order of the index being scanned.
    ///
    /// # Returns
    ///
    /// `Some(HashMap)` if the index has specific sort order, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.index_scan_order().is_none());
    /// ```
    pub fn index_scan_order(&self) -> Option<HashMap<String, bool>> {
        self.inner.index_scan_order.clone()
    }

    /// Returns the blocking sort order if results need sorting.
    ///
    /// A "blocking sort" means the results from the filter/scan phase must be
    /// sorted before being returned. This is necessary when:
    /// - Results come from a full scan (not naturally ordered)
    /// - The desired sort order differs from the index order
    /// - Multiple sort keys are needed
    ///
    /// # Returns
    ///
    /// `Some(Vec)` containing (field_name, SortOrder) pairs if sorting is needed,
    /// `None` if results are already in the correct order.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.blocking_sort_order().is_none());
    /// ```
    pub fn blocking_sort_order(&self) -> Option<Vec<(String, SortOrder)>> {
        self.inner.blocking_sort_order.clone()
    }

    /// Returns the number of results to skip.
    ///
    /// Used for pagination. Results are skipped before the limit is applied.
    ///
    /// # Returns
    ///
    /// `Some(skip_count)` if pagination is applied, `None` if all results are wanted.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.skip().is_none());
    /// ```
    pub fn skip(&self) -> Option<u64> {
        self.inner.skip
    }

    /// Returns the maximum number of results to return.
    ///
    /// Used for pagination. Applied after skip.
    ///
    /// # Returns
    ///
    /// `Some(limit)` if results are limited, `None` for unlimited results.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.limit().is_none());
    /// ```
    pub fn limit(&self) -> Option<u64> {
        self.inner.limit
    }

    /// Returns whether the query should return distinct/unique results only.
    ///
    /// When true, duplicate documents are removed from results.
    ///
    /// # Returns
    ///
    /// `true` if distinct results are required, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(!plan.distinct());
    /// ```
    pub fn distinct(&self) -> bool {
        self.inner.distinct
    }

    /// Returns the collator options for text comparison if specified.
    ///
    /// ICU Collator options control how strings are compared during sorting,
    /// including case sensitivity, accent sensitivity, etc.
    ///
    /// # Returns
    ///
    /// `Some(CollatorOptions)` if custom collation is needed, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.collator_options().is_none());
    /// ```
    pub fn collator_options(&self) -> Option<CollatorOptions> {
        self.inner.collator_options
    }

    /// Returns the collator preferences for text comparison if specified.
    ///
    /// ICU Collator preferences (like locale) control regional sorting rules.
    ///
    /// # Returns
    ///
    /// `Some(CollatorPreferences)` if custom collation is needed, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.collator_preferences().is_none());
    /// ```
    pub fn collator_preferences(&self) -> Option<CollatorPreferences> {
        self.inner.collator_preferences
    }

    /// Returns any sub-plans attached to this plan.
    ///
    /// Sub-plans are used for composite queries or complex filtering scenarios
    /// where the query is broken into multiple execution stages.
    ///
    /// # Returns
    ///
    /// `Some(Vec<FindPlan>)` if sub-plans exist, `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let plan = FindPlan::new();
    /// assert!(plan.sub_plans().is_none());
    /// ```
    pub fn sub_plans(&self) -> Option<Vec<FindPlan>> {
        self.inner.sub_plans.clone()
    }

    /// Adds a sub-plan to this plan.
    ///
    /// Sub-plans are executed as part of a composite query strategy.
    /// This method initializes the sub-plans vector if needed.
    ///
    /// # Arguments
    ///
    /// * `sub_plan` - The sub-plan to add
    ///
    /// # Panics
    ///
    /// This method requires mutable access to FindPlanInner via Arc.
    /// In the PIMPL pattern, mutation is handled specially - this will
    /// only work during plan construction before the plan is shared.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut plan = FindPlan::new();
    /// let sub_plan = FindPlan::new();
    /// plan.add_sub_plan(sub_plan);
    /// assert!(plan.sub_plans().is_some());
    /// ```
    pub fn add_sub_plan(&mut self, sub_plan: FindPlan) {
        // Safety: We use Arc::get_mut which requires exclusive access
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.sub_plans.get_or_insert_with(Vec::new).push(sub_plan);
        }
    }

    // Internal setter methods for query optimizer use
    // These use Arc::get_mut to safely mutate only when the Arc is not shared

    pub(crate) fn set_by_id_filter(&mut self, filter: Filter) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.by_id_filter = Some(filter);
        }
    }

    pub(crate) fn set_index_scan_filter(&mut self, filter: IndexScanFilter) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.index_scan_filter = Some(filter);
        }
    }

    pub(crate) fn set_full_scan_filter(&mut self, filter: Filter) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.full_scan_filter = Some(filter);
        }
    }

    pub(crate) fn set_index_descriptor(&mut self, descriptor: IndexDescriptor) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.index_descriptor = Some(descriptor);
        }
    }

    pub(crate) fn set_index_scan_order(&mut self, order: HashMap<String, bool>) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.index_scan_order = Some(order);
        }
    }

    pub(crate) fn set_blocking_sort_order(&mut self, order: Vec<(String, SortOrder)>) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.blocking_sort_order = Some(order);
        }
    }

    pub(crate) fn set_skip(&mut self, skip: u64) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.skip = Some(skip);
        }
    }

    pub(crate) fn set_limit(&mut self, limit: u64) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.limit = Some(limit);
        }
    }

    pub(crate) fn set_distinct(&mut self, distinct: bool) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.distinct = distinct;
        }
    }

    pub(crate) fn set_collator_options(&mut self, options: CollatorOptions) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.collator_options = Some(options);
        }
    }

    pub(crate) fn set_collator_preferences(&mut self, preferences: CollatorPreferences) {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.collator_preferences = Some(preferences);
        }
    }
}

/// Opaque implementation details of FindPlan.
/// This struct is part of the PIMPL pattern and should not be accessed directly.
pub(crate) struct FindPlanInner {
    pub(crate) by_id_filter: Option<Filter>,
    pub(crate) index_scan_filter: Option<IndexScanFilter>,
    pub(crate) full_scan_filter: Option<Filter>,
    pub(crate) index_descriptor: Option<IndexDescriptor>,
    pub(crate) index_scan_order: Option<HashMap<String, bool>>,
    pub(crate) blocking_sort_order: Option<Vec<(String, SortOrder)>>,
    pub(crate) skip: Option<u64>,
    pub(crate) limit: Option<u64>,
    pub(crate) distinct: bool,
    pub(crate) collator_options: Option<CollatorOptions>,
    pub(crate) collator_preferences: Option<CollatorPreferences>,
    pub(crate) sub_plans: Option<Vec<FindPlan>>,
}

impl FindPlanInner {
    fn new() -> Self {
        FindPlanInner {
            by_id_filter: None,
            index_scan_filter: None,
            full_scan_filter: None,
            index_descriptor: None,
            index_scan_order: None,
            blocking_sort_order: None,
            skip: None,
            limit: None,
            distinct: false,
            collator_options: None,
            collator_preferences: None,
            sub_plans: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_single_sub_plan() {
        let mut plan = FindPlan::new();
        assert!(plan.sub_plans().is_none());

        let sub_plan = FindPlan::new();
        plan.add_sub_plan(sub_plan);

        assert!(plan.sub_plans().is_some());
        let plans = plan.sub_plans().unwrap();
        assert_eq!(plans.len(), 1);
    }

    #[test]
    fn test_add_multiple_sub_plans() {
        let mut plan = FindPlan::new();

        for i in 0..5 {
            let mut sub_plan = FindPlan::new();
            sub_plan.set_skip(i as u64);
            plan.add_sub_plan(sub_plan);
        }

        let plans = plan.sub_plans().unwrap();
        assert_eq!(plans.len(), 5);
        for (i, sub_plan) in plans.iter().enumerate() {
            assert_eq!(sub_plan.skip(), Some(i as u64));
        }
    }

    #[test]
    fn test_add_sub_plan_idempotent_initialization() {
        let mut plan1 = FindPlan::new();
        let mut plan2 = FindPlan::new();

        let sub_plan1 = FindPlan::new();
        let sub_plan2 = FindPlan::new();

        plan1.add_sub_plan(sub_plan1);
        plan1.add_sub_plan(sub_plan2);

        plan2.add_sub_plan(FindPlan::new());

        assert_eq!(plan1.sub_plans().unwrap().len(), 2);
        assert_eq!(plan2.sub_plans().unwrap().len(), 1);
    }

    #[test]
    fn test_add_sub_plan_preserves_existing_data() {
        let mut parent_plan = FindPlan::new();
        parent_plan.set_skip(10);
        parent_plan.set_limit(20);

        let mut sub_plan = FindPlan::new();
        sub_plan.set_distinct(true);

        parent_plan.add_sub_plan(sub_plan);

        // Verify parent data is preserved
        assert_eq!(parent_plan.skip(), Some(10));
        assert_eq!(parent_plan.limit(), Some(20));

        // Verify sub-plan is added
        let plans = parent_plan.sub_plans().unwrap();
        assert_eq!(plans.len(), 1);
        assert!(plans[0].distinct());
    }

    #[test]
    fn test_add_sub_plan_thread_safety_simulation() {
        let mut plans_vec = vec![FindPlan::new(); 3];

        // Simulate adding sub-plans without race conditions
        for parent_plan in &mut plans_vec {
            for _ in 0..10 {
                let sub_plan = FindPlan::new();
                parent_plan.add_sub_plan(sub_plan);
            }
        }

        // Verify all plans have correct number of sub-plans
        // Create immutable references separately to avoid borrow conflicts
        let verification_plans = plans_vec.clone();
        for plan in &verification_plans {
            let sub_plans = plan.sub_plans();
            if let Some(plans) = sub_plans {
                assert_eq!(plans.len(), 10);
            }
        }
    }
}
