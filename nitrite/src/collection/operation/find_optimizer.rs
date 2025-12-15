use dashmap::DashMap;
use smallvec::{SmallVec, ToSmallVec};

use crate::common::{SortableFields, Value};
use crate::filter::{AndFilter, EqualsFilter, OrFilter};
use crate::{
    collection::{FindOptions, FindPlan},
    errors::{ErrorKind, NitriteError, NitriteResult},
    filter::{
        is_and_filter, is_equals_filter, is_or_filter, is_text_filter, Filter, FilterProvider,
        IndexScanFilter,
    },
    index::IndexDescriptor,
    SortOrder, DOC_ID,
};
use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

type FilterVec = SmallVec<[Filter; 4]>;

#[derive(Clone)]
pub(crate) struct FindOptimizer {
    inner: Arc<FindOptimizerInner>,
}

impl FindOptimizer {
    pub fn new() -> Self {
        FindOptimizer {
            inner: Arc::new(FindOptimizerInner::new()),
        }
    }

    pub fn create_find_plan(
        &self,
        filter: &Filter,
        find_options: &FindOptions,
        index_descriptors: &[IndexDescriptor],
    ) -> NitriteResult<FindPlan> {
        self.inner.create_find_plan(filter, find_options, index_descriptors)
    }

    pub fn invalidate_cache(&self) {
        self.inner.invalidate_cache();
    }

    pub fn invalidate_index_entries(&self, affected_index: &IndexDescriptor) {
        self.inner.invalidate_index_entries(affected_index);
    }
}

pub(crate) struct FindOptimizerInner {
    query_cache: DashMap<u64, CachedPlan>,
    cache_limit: usize,
    last_index_version: AtomicU64,
}

struct CachedPlan {
    plan: FindPlan,
    used_index_descriptors: Vec<IndexDescriptor>,
}

impl FindOptimizerInner {
    pub fn new() -> Self {
        FindOptimizerInner {
            query_cache: DashMap::new(),
            cache_limit: 100,
            last_index_version: AtomicU64::new(0),
        }
    }

    pub fn create_find_plan(
        &self,
        filter: &Filter,
        find_options: &FindOptions,
        index_descriptors: &[IndexDescriptor],
    ) -> NitriteResult<FindPlan> {
        let cache_key = self.compute_cache_key(filter, find_options);

        // Check if cached plan exists and is valid
        if let Some(cached) = self.query_cache.get(&cache_key) {
            let cached_plan = &cached.plan;
            
            // Verify all used indexes still exist and are the same
            let all_indexes_valid = cached.used_index_descriptors.iter().all(|used_idx| {
                index_descriptors.iter().any(|current_idx| {
                    current_idx.index_fields() == used_idx.index_fields() &&
                    current_idx.index_type() == used_idx.index_type()
                })
            });
            
            if all_indexes_valid {
                return Ok(cached_plan.clone());
            }
            // If invalid, remove from cache and continue to generate new plan
            self.query_cache.remove(&cache_key);
        }
        
        // Create new plan
        let mut find_plan = self.create_find_plan_internal(index_descriptors, filter)?;
        self.read_sort_options(find_options, &mut find_plan)?;
        self.read_limit_options(find_options, &mut find_plan)?;

        if let Some(options) = find_options.collator_options {
            find_plan.set_collator_options(options);
        }
        find_plan.set_distinct(find_options.distinct);
        
        // Extract all indexes used by this plan
        let mut used_indexes = Vec::new();
        if let Some(idx) = find_plan.index_descriptor() {
            used_indexes.push(idx.clone());
        }
        
        // For OR queries, also check sub-plans
        if let Some(sub_plans) = find_plan.sub_plans() {
            for sub_plan in sub_plans {
                if let Some(idx) = &sub_plan.index_descriptor() {
                    used_indexes.push(idx.clone());
                }
            }
        }
        
        // Cache the new plan if we have capacity
        if self.query_cache.len() < self.cache_limit {
            let cached_plan = CachedPlan {
                plan: find_plan.clone(),
                used_index_descriptors: used_indexes,
            };
            self.query_cache.insert(cache_key, cached_plan);
        }
        
        Ok(find_plan)
    }

    // Call this method whenever indexes change (creation or deletion)
    pub fn invalidate_cache(&self) {
        self.query_cache.clear();
        self.last_index_version.fetch_add(1, Ordering::Relaxed);
    }
    
    // Add this method to invalidate specific index-related entries
    pub fn invalidate_index_entries(&self, affected_index: &IndexDescriptor) {
        // Remove any plan that uses this specific index
        self.query_cache.retain(|_, cached_plan| {
            !cached_plan.used_index_descriptors.iter().any(|idx| 
                idx.index_fields() == affected_index.index_fields() &&
                idx.index_type() == affected_index.index_type()
            )
        });
    }

    fn compute_cache_key(&self, filter: &Filter, find_options: &FindOptions) -> u64 {
        let mut hasher = DefaultHasher::new();
        
        // Include index version in the key to invalidate all cache when indexes change
        self.last_index_version.load(Ordering::Relaxed).hash(&mut hasher);
        
        // Hash the filter string representation
        filter.to_string().hash(&mut hasher);
        
        // Hash the find options
        if let Some(ref sort_by) = find_options.sort_by {
            for (field, order) in sort_by.sorting_order() {
                field.hash(&mut hasher);
                (order as u8).hash(&mut hasher);
            }
        }
        
        find_options.skip.hash(&mut hasher);
        find_options.limit.hash(&mut hasher);
        find_options.distinct.hash(&mut hasher);
        
        hasher.finish()
    }

    fn create_find_plan_internal(
        &self,
        index_descriptors: &[IndexDescriptor],
        filter: &Filter,
    ) -> NitriteResult<FindPlan> {
        if is_and_filter(filter) {
            let filters = self.flatten_and_filter(filter)?;
            self.create_and_plan(index_descriptors, filters)
        } else if is_or_filter(filter) {
            let filters = SmallVec::from_vec(filter.logical_filters()?);
            self.create_or_plan(index_descriptors, filters)
        } else {
            let mut filters = FilterVec::new();
            filters.push(filter.clone());
            self.create_and_plan(index_descriptors, filters)
        }
    }

    fn flatten_and_filter(&self, filter: &Filter) -> NitriteResult<FilterVec> {
        // Use proper downcast with error handling instead of unwrap
        let and_filter = filter.as_any().downcast_ref::<AndFilter>()
            .ok_or_else(|| NitriteError::new(
                "Expected AndFilter but got different filter type",
                ErrorKind::FilterError,
            ))?;
        let logical_filters = and_filter.logical_filters()?;

        let mut filters = FilterVec::new();

        for f in logical_filters {
            if is_and_filter(&f) {
                filters.append(&mut self.flatten_and_filter(&f)?);
            } else {
                filters.push(f);
            }
        }

        Ok(filters)
    }

    fn create_and_plan(
        &self,
        index_descriptors: &[IndexDescriptor],
        filters: FilterVec,
    ) -> NitriteResult<FindPlan> {
        let mut find_plan = FindPlan::new();
        let mut index_scan_filters = FilterVec::new();
        let mut full_scan_filters = FilterVec::new();

        self.plan_id_filter(&mut find_plan, &filters)?;
        // If we have an ID filter, we don't need to do anything else
        if find_plan.by_id_filter().is_some() {
            return Ok(find_plan);
        }

        // Then process index-only filters
        self.plan_index_only_filter(
            &mut find_plan,
            &mut index_scan_filters,
            index_descriptors,
            &filters,
        )?;

        // If no index-only filters, try regular indexed fields
        if index_scan_filters.is_empty() {
            self.plan_index_scan_filter(
                &mut find_plan,
                &mut index_scan_filters,
                index_descriptors,
                &filters,
            )?;
        }

        // Finally, handle full scan filters
        self.plan_full_scan_filter(
            &mut find_plan,
            &index_scan_filters,
            &mut full_scan_filters,
            &filters,
        )?;

        // Set up filter plan with minimal allocations
        if index_scan_filters.len() == 1 {
            // Use iterator to create vec without cloning
            find_plan.set_index_scan_filter(
                IndexScanFilter::new(index_scan_filters.iter().cloned().collect()));
        } else if index_scan_filters.len() > 1 {
            // Convert SmallVec to Vec directly
            find_plan.set_index_scan_filter(IndexScanFilter::new(index_scan_filters.to_vec()));
        }

        if full_scan_filters.len() == 1 {
            // Move single filter by using iterator instead of cloning
            find_plan.set_full_scan_filter(full_scan_filters[0].clone());
        } else if full_scan_filters.len() > 1 {
            // Convert SmallVec to Vec directly
            find_plan.set_full_scan_filter(
                Filter::new(AndFilter::new(full_scan_filters.to_vec())));
        }

        Ok(find_plan)
    }

    fn plan_id_filter(&self, find_plan: &mut FindPlan, filters: &[Filter]) -> NitriteResult<()> {
        for filter in filters {
            if is_equals_filter(filter) {
                let equals_filter = filter.as_any().downcast_ref::<EqualsFilter>()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast filter to EqualsFilter",
                        ErrorKind::FilterError,
                    ))?;

                if equals_filter.get_field_name()? == DOC_ID {
                    find_plan.set_by_id_filter(filter.clone());
                    break;
                }
            }
        }

        Ok(())
    }

    fn plan_index_only_filter(
        &self,
        find_plan: &mut FindPlan,
        index_scan_filters: &mut FilterVec,
        index_descriptors: &[IndexDescriptor],
        filters: &[Filter],
    ) -> NitriteResult<()> {
        let mut io_filters = FilterVec::new();

        for filter in filters {
            if filter.is_index_only_filter() {
                if self.can_be_grouped(filter, &io_filters)? {
                    // Avoid cloning by moving the reference directly
                    // Filter already wraps Arc<dyn TFilter>, so we're just incrementing the ref count
                    io_filters.push(filter.clone());
                } else {
                    log::error!("Cannot group index only filters");
                    return Err(NitriteError::new(
                        "Cannot group index only filters",
                        ErrorKind::FilterError,
                    ));
                }
            }
        }

        if !io_filters.is_empty() {
            // Get the index type from the first filter without cloning
            let supported_type = {
                let first_filter = &io_filters[0];
                first_filter.supported_index_type()?
            };

            for index_descriptor in index_descriptors {
                if supported_type == index_descriptor.index_type() {
                    find_plan.set_index_descriptor(index_descriptor.clone());
                    index_scan_filters.append(&mut io_filters);
                    break;
                }
            }

            if find_plan.index_descriptor().is_none() {
                log::error!("No index found for index only filter");
                return Err(NitriteError::new(
                    "No index found for index only filter",
                    ErrorKind::FilterError,
                ));
            }
        }

        Ok(())
    }

    fn can_be_grouped(&self, filter: &Filter, filters: &[Filter]) -> NitriteResult<bool> {
        if filters.is_empty() {
            return Ok(true);
        }

        let first_filter = &filters[0];
        filter.can_be_grouped(first_filter.clone())
    }

    fn plan_index_scan_filter(
        &self,
        find_plan: &mut FindPlan,
        index_scan_filters: &mut FilterVec,
        index_descriptors: &[IndexDescriptor],
        filters: &[Filter],
    ) -> NitriteResult<()> {
        let mut index_filter_map = BTreeMap::new();

        for index_descriptor in index_descriptors {
            let fields_names = index_descriptor.index_fields().field_names();
            let mut index_filters = FilterVec::new();

            for field_name in fields_names {
                let mut matched = false;
                for filter in filters {
                    if !filter.has_field() {
                        // filter has no field, skip
                        continue;
                    }

                    // Using ? operator for error propagation
                    let name = filter.get_field_name()?;
                    if field_name == name {
                        index_filters.push(filter.clone());
                        matched = true;
                        break;
                    }
                }

                if !matched {
                    break;
                }
            }

            if !index_filters.is_empty() {
                index_filter_map.insert(index_descriptor.clone(), index_filters);
            }
        }

        // Find the best matching index descriptor and its filters without extra cloning
        if let Some((best_descriptor, best_filters)) = index_filter_map
            .into_iter()
            .max_by_key(|(_, filters)| filters.len())
        {
            // Cache the filters by moving them directly instead of cloning again
            index_scan_filters.extend(best_filters);
            find_plan.set_index_descriptor(best_descriptor);
        }

        Ok(())
    }

    fn plan_full_scan_filter(
        &self,
        find_plan: &mut FindPlan,
        index_scan_filters: &[Filter],
        full_scan_filters: &mut FilterVec,
        filters: &[Filter],
    ) -> NitriteResult<()> {
        for filter in filters {
            if !self.contains_filter(filter, index_scan_filters)? {
                let mut eligible = false;

                if let Some(by_id_filter) = &find_plan.by_id_filter() {
                    if !self.same_filter(filter, by_id_filter)? {
                        eligible = true;
                    }
                } else {
                    eligible = true;
                }

                if eligible {
                    full_scan_filters.push(filter.clone());
                }
            }
        }

        if index_scan_filters.is_empty() {
            for filter in full_scan_filters.iter() {
                if filter.is_index_only_filter() {
                    log::error!("Index only filter {} cannot be used in full scan", filter);
                    return Err(NitriteError::new(
                        "Index only filter cannot be used in full scan",
                        ErrorKind::FilterError,
                    ));
                } else if is_text_filter(filter) {
                    log::error!("{} is not full text indexed", filter.get_field_name()?);
                    return Err(NitriteError::new(
                        &format!("{} is not full text indexed", filter.get_field_name()?),
                        ErrorKind::FilterError,
                    ));
                }
            }
        }

        Ok(())
    }

    fn contains_filter(&self, filter: &Filter, filters: &[Filter]) -> NitriteResult<bool> {
        for f in filters {
            if self.same_filter(filter, f)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn same_filter(&self, filter1: &Filter, filter2: &Filter) -> NitriteResult<bool> {
        let matcher1 = FilterMatcher::from_filter(filter1)?;
        let matcher2 = FilterMatcher::from_filter(filter2)?;

        Ok(matcher1.matches(&matcher2))
    }

    fn create_or_plan(
        &self,
        index_descriptors: &[IndexDescriptor],
        filters: FilterVec,
    ) -> NitriteResult<FindPlan> {
        let mut find_plan = FindPlan::new();
        let mut flattened_filters = FilterVec::new();

        for filter in &filters {
            if is_or_filter(filter) {
                // Use proper downcast with error handling instead of unwrap
                let or_filter = filter.as_any().downcast_ref::<OrFilter>()
                    .ok_or_else(|| NitriteError::new(
                        "Expected OrFilter but got different filter type",
                        ErrorKind::FilterError,
                    ))?;
                let sub_filters = or_filter.logical_filters()?;
                flattened_filters.extend(sub_filters);
            } else {
                flattened_filters.push(filter.clone());
            }
        }

        for filter in flattened_filters {
            let sub_plan = self.create_find_plan_internal(index_descriptors, &filter)?;
            find_plan.add_sub_plan(sub_plan);
        }

        let mut clear = false;
        for plan in find_plan.sub_plans().unwrap() {
            if plan.index_descriptor().is_none() {
                clear = true;
                break;
            }
        }

        if clear {
            if let Some(_sub_plans) = find_plan.sub_plans() {
                // We cannot easily clear sub_plans through Arc, so we use set_full_scan_filter
                // to establish the fallback search strategy
                drop(_sub_plans);
            }
            find_plan.set_full_scan_filter(Filter::new(OrFilter::new(filters.to_vec())));
        }

        Ok(find_plan)
    }

    fn read_sort_options(
        &self,
        find_options: &FindOptions,
        find_plan: &mut FindPlan,
    ) -> NitriteResult<()> {
        if let Some(sort_by) = &find_options.sort_by {
            if find_plan.index_descriptor().is_none() {
                find_plan.set_blocking_sort_order(sort_by.sorting_order());
            } else {
                self.try_index_based_sort(find_plan, sort_by)?;
            }
        }

        Ok(())
    }

    fn try_index_based_sort(
        &self,
        find_plan: &mut FindPlan,
        sort_by: &SortableFields,
    ) -> NitriteResult<()> {
        let index_descriptor = find_plan.index_descriptor().unwrap();
        let fields = index_descriptor.index_fields().field_names();
        let mut can_use_index = false;
        let mut index_scan_order = HashMap::new();

        let sort_orders = sort_by.sorting_order();
        let len = sort_orders.len();

        if fields.len() >= len {
            can_use_index =
                self.compute_index_scan_order(&fields, &sort_orders, &mut index_scan_order)?;
        }

        if can_use_index {
            find_plan.set_index_scan_order(index_scan_order);
        } else {
            find_plan.set_blocking_sort_order(sort_by.sorting_order());
        }

        Ok(())
    }

    fn compute_index_scan_order(
        &self,
        fields: &[String],
        sort_orders: &[(String, SortOrder)],
        index_scan_order: &mut HashMap<String, bool>,
    ) -> NitriteResult<bool> {
        let mut can_use_index = true;

        for i in 0..sort_orders.len() {
            let field_name = &fields[i];
            let (sort_field, sort_order) = &sort_orders[i];

            if field_name != sort_field {
                can_use_index = false;
                break;
            }

            let reverse_scan = match sort_order {
                SortOrder::Ascending => false,
                SortOrder::Descending => true,
            };

            index_scan_order.insert(field_name.clone(), reverse_scan);
        }

        Ok(can_use_index)
    }

    fn read_limit_options(
        &self,
        find_options: &FindOptions,
        find_plan: &mut FindPlan,
    ) -> NitriteResult<()> {
        if let Some(skip) = find_options.skip {
            find_plan.set_skip(skip);
        }

        if let Some(limit) = find_options.limit {
            find_plan.set_limit(limit);
        }

        Ok(())
    }
}

struct FilterMatcher {
    field_name: Option<String>,
    filter_type_id: std::any::TypeId,
    filter_value: Option<Value>,
}

impl FilterMatcher {
    fn from_filter(filter: &Filter) -> NitriteResult<Self> {
        let field_name = if filter.has_field() {
            Some(filter.get_field_name()?)
        } else {
            None
        };

        // Some filters (like spatial filters) don't have a simple field value.
        // In those cases, get_field_value() returns an error, which we convert to None.
        // This allows such filters to still be matched by type_id and field_name.
        let filter_value = filter.get_field_value().ok().flatten();

        Ok(Self {
            field_name,
            filter_type_id: filter.as_any().type_id(),
            filter_value,
        })
    }

    fn matches(&self, other: &FilterMatcher) -> bool {
        // Type ID must match
        if self.filter_type_id != other.filter_type_id {
            return false;
        }

        // If both have field names, they must match
        match (&self.field_name, &other.field_name) {
            (Some(name1), Some(name2)) if name1 != name2 => return false,
            (Some(_), None) | (None, Some(_)) => return false,
            _ => {}
        }

        // If both have values, they must match
        match (&self.filter_value, &other.filter_value) {
            (Some(val1), Some(val2)) if val1 != val2 => return false,
            _ => {}
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{FindOptions, FindPlan};
    use crate::common::{Fields, UNIQUE_INDEX};
    use crate::filter::{and, field, or, Filter};
    use crate::index::IndexDescriptor;

    fn setup_find_optimizer() -> FindOptimizer {
        FindOptimizer::new()
    }

    fn create_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field"]).unwrap(),
            "test_collection",
        )
    }

    fn create_filter() -> Filter {
        field("field").eq("value")
    }

    #[test]
    fn test_create_find_plan() {
        let optimizer = setup_find_optimizer();
        let filter = create_filter();
        let find_options = FindOptions::default();
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.create_find_plan(&filter, &find_options, &index_descriptors);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_find_plan_internal() {
        let optimizer = setup_find_optimizer();
        let filter = create_filter();
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_find_plan_internal(&index_descriptors, &filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_flatten_and_filter() {
        let optimizer = setup_find_optimizer();
        let filter = and(vec![create_filter(), create_filter()]);

        let result = optimizer.inner.flatten_and_filter(&filter);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_create_and_plan() {
        let optimizer = setup_find_optimizer();
        let filters = FilterVec::from_vec(vec![create_filter()]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_and_plan(&index_descriptors, filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_id_filter() {
        let optimizer = setup_find_optimizer();
        let mut find_plan = FindPlan::new();
        let filters = FilterVec::from_vec(vec![create_filter()]);

        let result = optimizer.inner.plan_id_filter(&mut find_plan, &filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_index_only_filter() {
        let optimizer = setup_find_optimizer();
        let mut find_plan = FindPlan::new();
        let mut index_scan_filters = FilterVec::new();
        let index_descriptors = vec![create_index_descriptor()];
        let filters = FilterVec::from_vec(vec![create_filter()]);

        let result = optimizer.inner.plan_index_only_filter(
            &mut find_plan,
            &mut index_scan_filters,
            &index_descriptors,
            &filters,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_can_be_grouped() {
        let optimizer = setup_find_optimizer();
        let filter = create_filter();
        let filters = vec![create_filter()];

        let result = optimizer.inner.can_be_grouped(&filter, &filters);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_plan_index_scan_filter() {
        let optimizer = setup_find_optimizer();
        let mut find_plan = FindPlan::new();
        let mut index_scan_filters = FilterVec::new();
        let index_descriptors = vec![create_index_descriptor()];
        let filters = FilterVec::from_vec(vec![create_filter()]);

        let result = optimizer.inner.plan_index_scan_filter(
            &mut find_plan,
            &mut index_scan_filters,
            &index_descriptors,
            &filters,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_full_scan_filter() {
        let optimizer = setup_find_optimizer();
        let mut find_plan = FindPlan::new();
        let index_scan_filters = FilterVec::new();
        let mut full_scan_filters = FilterVec::new();
        let filters = FilterVec::from_vec(vec![create_filter()]);

        let result = optimizer.inner.plan_full_scan_filter(
            &mut find_plan,
            &index_scan_filters,
            &mut full_scan_filters,
            &filters,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_contains_filter() {
        let optimizer = setup_find_optimizer();
        let filter = create_filter();
        let filters = vec![create_filter()];

        let result = optimizer.inner.contains_filter(&filter, &filters);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_same_filter() {
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();

        let result = optimizer.inner.same_filter(&filter1, &filter2);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_create_or_plan() {
        let optimizer = setup_find_optimizer();
        let filters = FilterVec::from_vec(vec![create_filter()]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_or_plan(&index_descriptors, filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_read_sort_options() {
        let optimizer = setup_find_optimizer();
        let find_options = FindOptions::default();
        let mut find_plan = FindPlan::new();

        let result = optimizer.inner.read_sort_options(&find_options, &mut find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_read_limit_options() {
        let optimizer = setup_find_optimizer();
        let find_options = FindOptions::default();
        let mut find_plan = FindPlan::new();

        let result = optimizer.inner.read_limit_options(&find_options, &mut find_plan);
        assert!(result.is_ok());
    }

    // Tests for safe AndFilter downcasting ========
    
    #[test]
    fn test_flatten_and_filter_with_valid_and_filter() {
        // Verify flatten_and_filter safely downcasts to AndFilter
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let and_filter = and(vec![filter1, filter2]);

        let result = optimizer.inner.flatten_and_filter(&and_filter);
        assert!(result.is_ok());
        // Should successfully flatten the AND filter
        let flattened = result.unwrap();
        assert_eq!(flattened.len(), 2);
    }

    #[test]
    fn test_flatten_and_filter_with_nested_and_filters() {
        // Verify flatten_and_filter recursively flattens nested AND filters
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let inner_and = and(vec![filter1, filter2]);
        
        let filter3 = create_filter();
        let outer_and = and(vec![inner_and, filter3]);

        let result = optimizer.inner.flatten_and_filter(&outer_and);
        assert!(result.is_ok());
        // Should flatten nested AND filters
        let flattened = result.unwrap();
        assert_eq!(flattened.len(), 3);
    }

    #[test]
    fn test_flatten_and_filter_with_non_and_filter() {
        // Verify flatten_and_filter returns error for non-AndFilter
        let optimizer = setup_find_optimizer();
        let regular_filter = create_filter();

        let result = optimizer.inner.flatten_and_filter(&regular_filter);
        // Should return error because it's not an AND filter
        assert!(result.is_err());
        let error_msg = format!("{}", result.err().unwrap());
        assert!(error_msg.contains("Expected AndFilter"));
    }

    #[test]
    fn test_flatten_and_filter_preserves_non_and_sub_filters() {
        // Verify flatten_and_filter preserves non-AND sub-filters
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let and_filter = and(vec![filter1, filter2]);

        let result = optimizer.inner.flatten_and_filter(&and_filter);
        assert!(result.is_ok());
        let flattened = result.unwrap();
        // All sub-filters should be preserved
        assert!(flattened.iter().all(|f| !is_and_filter(f)));
    }

    // Tests for safe OrFilter downcasting ========
    
    #[test]
    fn test_create_or_plan_with_valid_or_filter() {
        // Verify create_or_plan safely downcasts to OrFilter
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let or_filter = or(vec![filter1, filter2]);
        let filters = FilterVec::from_vec(vec![or_filter]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_or_plan(&index_descriptors, filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_or_plan_with_multiple_or_filters() {
        // Verify create_or_plan handles multiple OR filters
        let optimizer = setup_find_optimizer();
        let or_filter1 = or(vec![create_filter(), create_filter()]);
        let or_filter2 = or(vec![create_filter(), create_filter()]);
        let filters = FilterVec::from_vec(vec![or_filter1, or_filter2]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_or_plan(&index_descriptors, filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_or_plan_with_mixed_filters() {
        // Verify create_or_plan handles mixed OR and non-OR filters
        let optimizer = setup_find_optimizer();
        let or_filter = or(vec![create_filter(), create_filter()]);
        let regular_filter = create_filter();
        let filters = FilterVec::from_vec(vec![or_filter, regular_filter]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_or_plan(&index_descriptors, filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_or_plan_flattens_or_filters() {
        // Verify create_or_plan extracts sub-filters from OR filters
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let or_filter = or(vec![filter1, filter2]);
        let filters = FilterVec::from_vec(vec![or_filter]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_or_plan(&index_descriptors, filters);
        assert!(result.is_ok());
        // The OR filter should be flattened into sub-plans
        let find_plan = result.unwrap();
        assert!(find_plan.sub_plans().is_some());
    }

    #[test]
    fn test_create_or_plan_with_no_or_filters() {
        // Verify create_or_plan handles case with no OR filters
        let optimizer = setup_find_optimizer();
        let regular_filter = create_filter();
        let filters = FilterVec::from_vec(vec![regular_filter]);
        let index_descriptors = vec![create_index_descriptor()];

        let result = optimizer.inner.create_or_plan(&index_descriptors, filters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_or_plan_or_filter_type_safety() {
        // Verify create_or_plan type-checks OR filters
        let optimizer = setup_find_optimizer();
        // Create a filter that claims to be OR but isn't really
        let filter = create_filter();
        
        // Only actual OR filters should pass the is_or_filter check
        if is_or_filter(&filter) {
            // This shouldn't happen for regular filters
            let result = optimizer.inner.create_or_plan(
                &[create_index_descriptor()],
                FilterVec::from_vec(vec![filter]),
            );
            // If it gets here, it should handle it safely
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_flatten_and_filter_type_consistency() {
        // Verify type check and downcast are consistent
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let and_filter = and(vec![filter1, filter2]);

        // Verify is_and_filter returns true
        assert!(is_and_filter(&and_filter));
        
        // And flatten_and_filter should work
        let result = optimizer.inner.flatten_and_filter(&and_filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_or_plan_type_consistency() {
        // Verify type check and downcast are consistent
        let optimizer = setup_find_optimizer();
        let filter1 = create_filter();
        let filter2 = create_filter();
        let or_filter = or(vec![filter1, filter2]);

        // Verify is_or_filter returns true
        assert!(is_or_filter(&or_filter));
        
        // And create_or_plan should work
        let result = optimizer.inner.create_or_plan(
            &[create_index_descriptor()],
            FilterVec::from_vec(vec![or_filter]),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_sorting_aware_filter_creates_full_scan_filter() {
        // Test that a simple SortingAwareFilter (gt/lt/gte/lte) creates a plan with full_scan_filter
        let optimizer = setup_find_optimizer();
        let filter = field("emp_id").gt(5i64);
        let find_options = FindOptions::default();
        let index_descriptors: Vec<IndexDescriptor> = vec![];  // No indexes

        let result = optimizer.create_find_plan(&filter, &find_options, &index_descriptors);
        assert!(result.is_ok());
        
        let find_plan = result.unwrap();
        // The filter should be in full_scan_filter since there's no matching index
        assert!(find_plan.full_scan_filter().is_some(), "full_scan_filter should be set for SortingAwareFilter");
        assert!(find_plan.index_descriptor().is_none(), "No index should be used");
        assert!(find_plan.index_scan_filter().is_none(), "No index scan filter should be used");
    }

    #[test]
    fn test_equals_filter_creates_full_scan_filter_without_index() {
        // Test that an EqualsFilter without matching index creates a plan with full_scan_filter
        let optimizer = setup_find_optimizer();
        let filter = field("emp_id").eq(5i64);
        let find_options = FindOptions::default();
        let index_descriptors: Vec<IndexDescriptor> = vec![];  // No indexes

        let result = optimizer.create_find_plan(&filter, &find_options, &index_descriptors);
        assert!(result.is_ok());
        
        let find_plan = result.unwrap();
        // The filter should be in full_scan_filter since there's no matching index
        assert!(find_plan.full_scan_filter().is_some(), "full_scan_filter should be set for EqualsFilter without index");
    }
}
