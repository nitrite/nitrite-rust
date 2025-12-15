use std::{any::Any, fmt::Display, sync::{atomic::AtomicBool, OnceLock}};

use crate::{
    collection::Document,
    errors::{ErrorKind, NitriteError, NitriteResult},
    index::IndexMap,
    Value,
};

use super::{Filter, FilterProvider};

/// Represents inclusive/exclusive bounds for range-based filtering.
///
/// This internal struct defines the lower and upper boundaries for range filters,
/// with separate control over whether each boundary is inclusive or exclusive.
/// Created through `between()`, `between_inclusive()`, and `between_optional_inclusive()`
/// methods on FluentFilter.
///
/// # Responsibilities
/// - **Boundary Definition**: Stores lower and upper bound values for range queries
/// - **Inclusion Control**: Tracks separate inclusive/exclusive flags for each boundary
/// - **Range Validation**: Ensures bounds are properly configured for range operations
pub(crate) struct Bound {
    upper_bound: Value,
    lower_bound: Value,
    upper_inclusive: bool,
    lower_inclusive: bool,
}

impl Bound {
    pub(crate) fn inclusive(lower_bound: Value, upper_bound: Value) -> Self {
        Bound {
            upper_bound,
            lower_bound,
            upper_inclusive: true,
            lower_inclusive: true,
        }
    }

    pub(crate) fn optional_inclusive(
        lower_bound: Value,
        upper_bound: Value,
        inclusive: bool,
    ) -> Self {
        Bound {
            upper_bound,
            lower_bound,
            upper_inclusive: inclusive,
            lower_inclusive: inclusive,
        }
    }

    pub(crate) fn new(
        lower_bound: Value,
        upper_bound: Value,
        lower_inclusive: bool,
        upper_inclusive: bool,
    ) -> Self {
        Bound {
            upper_bound,
            lower_bound,
            upper_inclusive,
            lower_inclusive,
        }
    }
}

/// Comparison modes for range-based field comparisons.
///
/// This enum specifies the type of comparison operation to perform when filtering
/// by numeric or comparable field values. Created through comparison methods:
/// - `Greater` from `gt()` method
/// - `GreaterEqual` from `gte()` method
/// - `Lesser` from `lt()` method
/// - `LesserEqual` from `lte()` method
///
/// # Responsibilities
/// - **Operation Selection**: Determines the comparison operator (>, >=, <, <=)
/// - **Index Optimization**: Enables efficient index-accelerated comparisons
/// - **Sort Direction Control**: Supports reverse-scan optimization for index traversal
pub(crate) enum ComparisonMode {
    Greater,
    GreaterEqual,
    Lesser,
    LesserEqual,
}

/// Evaluates documents using numeric comparison operations with index optimization.
///
/// This internal filter performs greater-than, greater-than-or-equal, less-than,
/// and less-than-or-equal comparisons on document fields. It supports index-accelerated
/// queries that traverse indexes in either forward or reverse scan direction depending
/// on the comparison mode.
///
/// Created internally by FluentFilter methods: `gt()`, `gte()`, `lt()`, `lte()`.
///
/// # Responsibilities
/// - **Comparison Evaluation**: Applies numeric comparison operators to field values
/// - **Index Acceleration**: Uses index ranges and directional traversal for efficiency
/// - **Reverse-Scan Support**: Optimizes backward iteration through sorted indexes
/// - **Field Value Storage**: Maintains field name and comparison value with OnceLock
pub(crate) struct SortingAwareFilter {
    field_name: OnceLock<String>,
    field_value: OnceLock<Value>,
    reverse_scan: AtomicBool,
    comparison_mode: ComparisonMode,
    collection_name: OnceLock<String>,
}

impl SortingAwareFilter {
    /// Creates a new comparison filter for numeric range queries.
    ///
    /// # Arguments
    /// * `field_name` - The field to compare, stored in OnceLock
    /// * `field_value` - The comparison value, stored in OnceLock
    /// * `comparison_mode` - The comparison operator (>, >=, <, or <=)
    ///
    /// # Behavior
    /// Immediately initializes OnceLock fields for thread-safe access. The filter is ready
    /// to be applied to documents or indexes immediately after construction.
    #[inline]
    pub(crate) fn new(
        field_name: String,
        field_value: Value,
        comparison_mode: ComparisonMode,
    ) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let value = OnceLock::new();
        let _ = value.set(field_value);

        SortingAwareFilter {
            field_name: name,
            field_value: value,
            comparison_mode,
            reverse_scan: AtomicBool::new(false),
            collection_name: OnceLock::new(),
        }
    }

    fn compare_greater(
        &self,
        index_map: &IndexMap,
        sub_map: &mut Vec<Value>,
        nitrite_ids: &mut Vec<Value>,
    ) -> NitriteResult<()> {
        let field_value = self.field_value.get()
            .ok_or_else(|| NitriteError::new(
                "Range filter error: comparison field value not set for greater-than filter",
                ErrorKind::InvalidFieldName
            ))?;
        
        if self.get_reverse_scan()? {
            // if reverse scan then get the last key
            let mut last_key = index_map.last_key()?;
            while let Some(ref key) = last_key {
                if key > field_value {
                    let value = index_map.get(key)?;
                    self.process_index_value(value, sub_map, nitrite_ids);
                    last_key = index_map.lower_key(key)?;
                } else {
                    break;
                }
            }
        } else {
            // if forward scan then get the higher key
            let mut higher_key = index_map.higher_key(field_value)?;
            while let Some(ref key) = higher_key {
                let value = index_map.get(key)?;
                self.process_index_value(value, sub_map, nitrite_ids);
                higher_key = index_map.higher_key(key)?;
            }
        }
        Ok(())
    }

    fn compare_greater_equal(
        &self,
        index_map: &IndexMap,
        sub_map: &mut Vec<Value>,
        nitrite_ids: &mut Vec<Value>,
    ) -> NitriteResult<()> {
        let field_value = self.field_value.get()
            .ok_or_else(|| NitriteError::new(
                "Range filter error: comparison field value not set for greater-than-or-equal filter",
                ErrorKind::InvalidFieldName
            ))?;
        
        if self.get_reverse_scan()? {
            // if reverse scan then get the last key
            let mut last_key = index_map.last_key()?;
            while let Some(ref key) = last_key {
                if key >= field_value {
                    let value = index_map.get(key)?;
                    self.process_index_value(value, sub_map, nitrite_ids);
                    last_key = index_map.lower_key(key)?;
                } else {
                    break;
                }
            }
        } else {
            // if forward scan then get the ceiling key
            let mut ceiling_key = index_map.ceiling_key(field_value)?;
            while let Some(ref key) = ceiling_key {
                let value = index_map.get(key)?;
                self.process_index_value(value, sub_map, nitrite_ids);
                ceiling_key = index_map.higher_key(key)?;
            }
        }
        Ok(())
    }

    fn compare_lesser(
        &self,
        index_map: &IndexMap,
        sub_map: &mut Vec<Value>,
        nitrite_ids: &mut Vec<Value>,
    ) -> NitriteResult<()> {
        let field_value = self.field_value.get()
            .ok_or_else(|| NitriteError::new(
                "Range filter error: comparison field value not set for less-than filter",
                ErrorKind::InvalidFieldName
            ))?;
        
        if self.get_reverse_scan()? {
            // if reverse scan then get the lower key
            let mut lower_key = index_map.lower_key(field_value)?;
            while let Some(ref key) = lower_key {
                let value = index_map.get(key)?;
                self.process_index_value(value, sub_map, nitrite_ids);
                lower_key = index_map.lower_key(key)?;
            }
        } else {
            // if forward scan then get the first key
            let mut first_key = index_map.first_key()?;
            while let Some(ref key) = first_key {
                if key < field_value {
                    let value = index_map.get(key)?;
                    self.process_index_value(value, sub_map, nitrite_ids);
                    first_key = index_map.higher_key(key)?;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    fn compare_lesser_equal(
        &self,
        index_map: &IndexMap,
        sub_map: &mut Vec<Value>,
        nitrite_ids: &mut Vec<Value>,
    ) -> NitriteResult<()> {
        let field_value = self.field_value.get()
            .ok_or_else(|| NitriteError::new(
                "Range filter error: comparison field value not set for less-than-or-equal filter",
                ErrorKind::InvalidFieldName
            ))?;
        
        if self.get_reverse_scan()? {
            // if reverse scan then get the floor key
            let mut floor_key = index_map.floor_key(field_value)?;
            while let Some(ref key) = floor_key {
                let value = index_map.get(key)?;
                self.process_index_value(value, sub_map, nitrite_ids);
                floor_key = index_map.lower_key(key)?;
            }
        } else {
            // if forward scan then get the first key
            let mut first_key = index_map.first_key()?;
            while let Some(ref key) = first_key {
                if key <= field_value {
                    let value = index_map.get(key)?;
                    self.process_index_value(value, sub_map, nitrite_ids);
                    first_key = index_map.higher_key(key)?;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
}

impl Display for SortingAwareFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operator = match self.comparison_mode {
            ComparisonMode::Greater => ">",
            ComparisonMode::GreaterEqual => ">=",
            ComparisonMode::Lesser => "<",
            ComparisonMode::LesserEqual => "<=",
        };
        write!(
            f,
            "({} {} {})",
            self.field_name.get().expect("field_name not initialized"),
            operator,
            self.field_value.get().expect("field_value not initialized")
        )
    }
}

impl FilterProvider for SortingAwareFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let value = entry.get(self.field_name.get().expect("field_name not initialized"))?;
        let field_value = self.field_value.get().expect("field_value not initialized");
        
        match self.comparison_mode {
            ComparisonMode::Greater => Ok(&value > field_value),
            ComparisonMode::GreaterEqual => Ok(&value >= field_value),
            ComparisonMode::Lesser => Ok(&value < field_value),
            ComparisonMode::LesserEqual => Ok(&value <= field_value),
        }
    }

    fn apply_on_index(&self, index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        let mut sub_map = Vec::new();
        let mut nitrite_ids = Vec::new();

        match self.comparison_mode {
            ComparisonMode::Greater => self.compare_greater(index_map, &mut sub_map, &mut nitrite_ids)?,
            ComparisonMode::GreaterEqual => self.compare_greater_equal(index_map, &mut sub_map, &mut nitrite_ids)?,
            ComparisonMode::Lesser => self.compare_lesser(index_map, &mut sub_map, &mut nitrite_ids)?,
            ComparisonMode::LesserEqual => self.compare_lesser_equal(index_map, &mut sub_map, &mut nitrite_ids)?,
        }

        if sub_map.is_empty() {
            Ok(nitrite_ids)
        } else {
            Ok(sub_map)
        }
    }

    fn get_collection_name(&self) -> NitriteResult<String> {
        if self.collection_name.get().is_none() {
            log::error!("Collection name is not set for filter {}", self);
            Err(NitriteError::new(
                "Collection name is not set",
                ErrorKind::CollectionNotFound,
            ))
        } else {
            Ok(self.collection_name.get().unwrap().clone())
        }
    }

    fn set_collection_name(&self, collection_name: String) -> NitriteResult<()> {
        self.collection_name.get_or_init(|| collection_name);
        Ok(())
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        Ok(self.field_name.get().expect("field_name not initialized").clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        Ok(self.field_value.get().cloned())
    }

    fn set_field_value(&self, field_value: Value) -> NitriteResult<()> {
        self.field_value.get_or_init(|| field_value);
        Ok(())
    }

    fn get_reverse_scan(&self) -> NitriteResult<bool> {
        Ok(self.reverse_scan.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn set_reverse_scan(&self, reverse_scan: bool) -> NitriteResult<()> {
        self.reverse_scan.store(reverse_scan, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn is_reverse_scan_supported(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Evaluates documents where a field value falls within a specified range.
///
/// This internal filter matches documents if the field value is between (inclusive or
/// exclusive) lower and upper bounds. It is implemented as a conjunction of two
/// comparison filters to reuse comparison logic and index optimization.
///
/// Created internally by FluentFilter methods:
/// - `between(lower, upper, lower_inclusive, upper_inclusive)`
/// - `between_inclusive(lower, upper)`
/// - `between_optional_inclusive(lower, upper, inclusive)`
///
/// # Responsibilities
/// - **Range Matching**: Evaluates if field value is within specified bounds
/// - **Boundary Control**: Supports inclusive/exclusive bounds independently
/// - **Index Acceleration**: Uses two SortingAwareFilter objects for efficient range scans
/// - **Short-Circuit Evaluation**: Returns false immediately if either bound fails
pub(crate) struct BetweenFilter {
    filters: Vec<Filter>,
}

impl BetweenFilter {
    /// Creates a new range filter with specified bounds.
    ///
    /// # Arguments
    /// * `field_name` - The field to filter on
    /// * `bound` - A Bound structure defining lower/upper values and their inclusivity
    ///
    /// # Behavior
    /// Creates two SortingAwareFilter objects internally: one for the lower bound and
    /// one for the upper bound. Both must match for a document to satisfy the range filter.
    pub(crate) fn new(field_name: String, bound: Bound) -> Self {
        let rhs = Self::rhs_filter(&field_name, &bound);
        let lhs = Self::lhs_filter(&field_name, &bound);

        BetweenFilter {
            filters: vec![rhs, lhs],
        }
    }

    fn rhs_filter(field_name: &str, bound: &Bound) -> Filter {
        let field_value = bound.upper_bound.clone();
        let inclusive = bound.upper_inclusive;

        if inclusive {
            Filter::new(SortingAwareFilter::new(
                field_name.to_string(),
                field_value.clone(),
                ComparisonMode::LesserEqual,
            ))
        } else {
            Filter::new(SortingAwareFilter::new(
                field_name.to_string(),
                field_value.clone(),
                ComparisonMode::Lesser,
            ))
        }
    }

    fn lhs_filter(field_name: &str, bound: &Bound) -> Filter {
        let field_value = bound.lower_bound.clone();
        let inclusive = bound.lower_inclusive;

        if inclusive {
            Filter::new(SortingAwareFilter::new(
                field_name.to_string(),
                field_value.clone(),
                ComparisonMode::GreaterEqual,
            ))
        } else {
            Filter::new(SortingAwareFilter::new(
                field_name.to_string(),
                field_value.clone(),
                ComparisonMode::Greater,
            ))
        }
    }
}

impl Display for BetweenFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} && {})", self.filters[0], self.filters[1])
    }
}

impl FilterProvider for BetweenFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        for filter in &self.filters {
            if !filter.apply(entry)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn logical_filters(&self) -> NitriteResult<Vec<Filter>> {
        Ok(self.filters.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Evaluates documents where a field value matches one of multiple allowed values.
///
/// This internal filter performs membership testing, matching documents if the field value
/// equals any of the provided values. Supports index-accelerated queries that perform
/// multiple single-key lookups for efficient set membership testing.
///
/// Created internally by FluentFilter method: `in_array(values)`
///
/// # Responsibilities
/// - **Set Membership Testing**: Checks if field value is in the provided set
/// - **Index Acceleration**: Uses direct key lookups in indexes for efficiency
/// - **Value Storage**: Maintains field name and list of allowed values with OnceLock
/// - **Collection Context**: Tracks collection name for index operations
pub(crate) struct InFilter {
    field_name: OnceLock<String>,
    field_values: OnceLock<Vec<Value>>,
    collection_name: OnceLock<String>,
}

impl InFilter {
    /// Creates a new membership filter for set membership testing.
    ///
    /// # Arguments
    /// * `field_name` - The field to check membership on
    /// * `field_values` - Vector of allowed values; document matches if field equals any value
    ///
    /// # Behavior
    /// Immediately initializes OnceLock fields for thread-safe access. The filter performs
    /// equality testing against each value in the set.
    #[inline]
    pub(crate) fn new(field_name: String, field_values: Vec<Value>) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let values = OnceLock::new();
        let _ = values.set(field_values);

        InFilter {
            field_name: name,
            field_values: values,
            collection_name: OnceLock::new(),
        }
    }
}

impl Display for InFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut values = String::new();
        for value in self.field_values.get().expect("field_values not initialized") {
            values.push_str(&format!("{}, ", value));
        }
        write!(f, "({} in [{}])", self.field_name.get().expect("field_name not initialized"), values.trim_end_matches(", "))
    }
}

impl FilterProvider for InFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let value = entry.get(self.field_name.get().expect("field_name not initialized"))?;
        for field_value in self.field_values.get().expect("field_values not initialized") {
            if &value == field_value {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn apply_on_index(&self, index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        let mut sub_map = Vec::new();
        let mut nitrite_ids = Vec::new();

        for field_value in self.field_values.get().expect("field_values not initialized") {
            let value = index_map.get(field_value)?;
            self.process_index_value(value, &mut sub_map, &mut nitrite_ids);
        }

        if sub_map.is_empty() {
            // it is filtering on either single field index,
            // or it is a terminal filter on compound index, return only nitrite-ids
            Ok(nitrite_ids)
        } else {
            // if sub-map is populated then filtering on compound index, return sub-map
            Ok(sub_map)
        }
    }

    fn get_collection_name(&self) -> NitriteResult<String> {
        if self.collection_name.get().is_none() {
            log::error!("Collection name is not set for filter {}", self);
            Err(NitriteError::new(
                "Collection name is not set",
                ErrorKind::CollectionNotFound,
            ))
        } else {
            Ok(self.collection_name.get().expect("collection_name not initialized").clone())
        }
    }

    fn set_collection_name(&self, collection_name: String) -> NitriteResult<()> {
        self.collection_name.get_or_init(|| collection_name);
        Ok(())
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        Ok(self.field_name.get().expect("field_name not initialized").clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        Ok(Some(Value::Array(self.field_values.get().expect("field_values not initialized").clone())))
    }

    fn set_field_value(&self, field_values: Value) -> NitriteResult<()> {
        if let Value::Array(array) = field_values {
            self.field_values.get_or_init(|| array);
            Ok(())
        } else {
            log::error!("Field value is not an array for filter {}", self);
            Err(NitriteError::new(
                "Field value is not an array",
                ErrorKind::InvalidDataType,
            ))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Evaluates documents where a field value does not match any of multiple excluded values.
///
/// This internal filter performs negative membership testing, matching documents if the field
/// value differs from all provided values. Supports index-accelerated queries that enumerate
/// all index entries except those in the exclusion set.
///
/// Created internally by FluentFilter method: `not_in_array(values)`
///
/// # Responsibilities
/// - **Negative Set Membership Testing**: Checks if field value is not in the exclusion set
/// - **Index Acceleration**: Enumerates index entries excluding matched values
/// - **Value Storage**: Maintains field name and list of excluded values with OnceLock
/// - **Collection Context**: Tracks collection name for index operations
pub(crate) struct NotInFilter {
    field_name: OnceLock<String>,
    field_values: OnceLock<Vec<Value>>,
    collection_name: OnceLock<String>,
}

impl NotInFilter {
    /// Creates a new exclusion filter for negative membership testing.
    ///
    /// # Arguments
    /// * `field_name` - The field to check membership on
    /// * `field_values` - Vector of excluded values; document matches if field differs from all values
    ///
    /// # Behavior
    /// Immediately initializes OnceLock fields for thread-safe access. The filter performs
    /// inequality testing against each value in the exclusion set.
    #[inline]
    pub(crate) fn new(field_name: String, field_values: Vec<Value>) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let values = OnceLock::new();
        let _ = values.set(field_values);

        NotInFilter {
            field_name: name,
            field_values: values,
            collection_name: OnceLock::new(),
        }
    }
}

impl Display for NotInFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut values = String::new();
        for value in self.field_values.get().expect("field_values not initialized") {
            values.push_str(&format!("{}, ", value));
        }
        write!(f, "({} not in [{}])", self.field_name.get().expect("field_name not initialized"), values.trim_end_matches(", "))
    }
}

impl FilterProvider for NotInFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let value = entry.get(self.field_name.get().expect("field_name not initialized"))?;
        for field_value in self.field_values.get().expect("field_values not initialized") {
            if &value == field_value {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn apply_on_index(&self, index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        let mut sub_map = Vec::new();
        let mut nitrite_ids = Vec::new();

        let entries = index_map.entries()?;
        for result in entries {
            let (key, value) = result?;
            if !self.field_values.get().expect("field_values not initialized").contains(&key) {
                self.process_index_value(Some(value), &mut sub_map, &mut nitrite_ids);
            }
        }

        if sub_map.is_empty() {
            // it is filtering on either single field index,
            // or it is a terminal filter on compound index, return only nitrite-ids
            Ok(nitrite_ids)
        } else {
            // if sub-map is populated then filtering on compound index, return sub-map
            Ok(sub_map)
        }
    }

    fn get_collection_name(&self) -> NitriteResult<String> {
        if self.collection_name.get().is_none() {
            log::error!("Collection name is not set for filter {}", self);
            Err(NitriteError::new(
                "Collection name is not set",
                ErrorKind::CollectionNotFound,
            ))
        } else {
            Ok(self.collection_name.get().expect("collection_name not initialized").clone())
        }
    }

    fn set_collection_name(&self, collection_name: String) -> NitriteResult<()> {
        self.collection_name.get_or_init(|| collection_name);
        Ok(())
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        Ok(self.field_name.get().expect("field_name not initialized").clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        Ok(Some(Value::Array(self.field_values.get().unwrap().clone())))
    }

    fn set_field_value(&self, field_values: Value) -> NitriteResult<()> {
        if let Value::Array(array) = field_values {
            self.field_values.get_or_init(|| array);
            Ok(())
        } else {
            log::error!("Field value is not an array for filter {}", self);
            Err(NitriteError::new(
                "Field value is not an array",
                ErrorKind::InvalidDataType,
            ))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;

    #[test]
    fn test_between_filter_apply() {
        let bound = Bound::inclusive(Value::I32(10), Value::I32(20));
        let filter = BetweenFilter::new("field".to_string(), bound);
        let mut doc = Document::new();
        doc.put("field", Value::I32(15)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_between_filter_apply_negative() {
        let bound = Bound::inclusive(Value::I32(10), Value::I32(20));
        let filter = BetweenFilter::new("field".to_string(), bound);
        let mut doc = Document::new();
        doc.put("field", Value::I32(25)).unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_in_filter_apply() {
        let filter = InFilter::new(
            "field".to_string(),
            vec![Value::I32(1), Value::I32(2), Value::I32(3)],
        );
        let mut doc = Document::new();
        doc.put("field", Value::I32(2)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_in_filter_apply_negative() {
        let filter = InFilter::new(
            "field".to_string(),
            vec![Value::I32(1), Value::I32(2), Value::I32(3)],
        );
        let mut doc = Document::new();
        doc.put("field", Value::I32(4)).unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_not_in_filter_apply() {
        let filter = NotInFilter::new(
            "field".to_string(),
            vec![Value::I32(1), Value::I32(2), Value::I32(3)],
        );
        let mut doc = Document::new();
        doc.put("field", Value::I32(4)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_not_in_filter_apply_negative() {
        let filter = NotInFilter::new(
            "field".to_string(),
            vec![Value::I32(1), Value::I32(2), Value::I32(3)],
        );
        let mut doc = Document::new();
        doc.put("field", Value::I32(2)).unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }


    #[test]
    fn test_sorting_aware_filter_apply() {
        let filter =
            SortingAwareFilter::new("field".to_string(), Value::I32(42), ComparisonMode::Greater);
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    // Tests: Range Filter Unwraps - Verify error messages
    
    #[test]
    fn test_sorting_aware_filter_get_field_name() {
        let filter =
            SortingAwareFilter::new("test_field".to_string(), Value::I32(10), ComparisonMode::Greater);
        let field_name = filter.get_field_name();
        assert!(field_name.is_ok());
        assert_eq!(field_name.unwrap(), "test_field");
    }

    #[test]
    fn test_in_filter_display_with_multiple_values() {
        let filter = InFilter::new(
            "status".to_string(),
            vec![Value::I32(1), Value::I32(2), Value::I32(3)],
        );
        let display_string = format!("{}", filter);
        assert!(display_string.contains("status"));
        assert!(display_string.contains("in"));
    }

    #[test]
    fn test_not_in_filter_display_with_multiple_values() {
        let filter = NotInFilter::new(
            "priority".to_string(),
            vec![Value::I32(0), Value::I32(-1)],
        );
        let display_string = format!("{}", filter);
        assert!(display_string.contains("priority"));
        assert!(display_string.contains("not in"));
    }

    #[test]
    fn test_sorting_aware_filter_comparison_modes() {
        let test_value = Value::I32(50);
        
        // Test Greater
        let greater_filter = SortingAwareFilter::new("val".to_string(), test_value.clone(), ComparisonMode::Greater);
        let mut doc = Document::new();
        doc.put("val", Value::I32(51)).unwrap();
        assert!(greater_filter.apply(&doc).unwrap());
        
        // Test GreaterEqual
        let ge_filter = SortingAwareFilter::new("val".to_string(), test_value.clone(), ComparisonMode::GreaterEqual);
        let mut doc2 = Document::new();
        doc2.put("val", Value::I32(50)).unwrap();
        assert!(ge_filter.apply(&doc2).unwrap());
        
        // Test Lesser
        let less_filter = SortingAwareFilter::new("val".to_string(), test_value.clone(), ComparisonMode::Lesser);
        let mut doc3 = Document::new();
        doc3.put("val", Value::I32(49)).unwrap();
        assert!(less_filter.apply(&doc3).unwrap());
        
        // Test LesserEqual
        let le_filter = SortingAwareFilter::new("val".to_string(), test_value.clone(), ComparisonMode::LesserEqual);
        let mut doc4 = Document::new();
        doc4.put("val", Value::I32(50)).unwrap();
        assert!(le_filter.apply(&doc4).unwrap());
    }

    #[test]
    fn test_in_filter_get_field_name() {
        let filter = InFilter::new(
            "category".to_string(),
            vec![Value::I32(1), Value::I32(2)],
        );
        let field_name = filter.get_field_name();
        assert!(field_name.is_ok());
        assert_eq!(field_name.unwrap(), "category");
    }

    #[test]
    fn test_not_in_filter_get_field_name() {
        let filter = NotInFilter::new(
            "blocked".to_string(),
            vec![Value::I32(0)],
        );
        let field_name = filter.get_field_name();
        assert!(field_name.is_ok());
        assert_eq!(field_name.unwrap(), "blocked");
    }

    #[test]
    fn test_between_filter_multiple_bounds() {
        // Test exclusive bounds
        let bound_exclusive = Bound::new(
            Value::I32(10),
            Value::I32(20),
            false,
            false,
        );
        let filter = BetweenFilter::new("range".to_string(), bound_exclusive);
        let mut doc = Document::new();
        doc.put("range", Value::I32(10)).unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }
    
    #[test]
    fn test_sorting_aware_filter_greater_initializes_field_value() {
        // Verify field_value OnceLock is properly initialized
        let filter = SortingAwareFilter::new(
            "score".to_string(),
            Value::I32(100),
            ComparisonMode::Greater,
        );
        let field_value = filter.get_field_value();
        assert!(field_value.is_ok());
        assert_eq!(field_value.unwrap(), Some(Value::I32(100)));
    }

    #[test]
    fn test_sorting_aware_filter_greater_equal_initializes_field_value() {
        // Verify field_value OnceLock is properly initialized for GreaterEqual
        let filter = SortingAwareFilter::new(
            "age".to_string(),
            Value::I32(18),
            ComparisonMode::GreaterEqual,
        );
        let field_value = filter.get_field_value();
        assert!(field_value.is_ok());
        assert_eq!(field_value.unwrap(), Some(Value::I32(18)));
    }

    #[test]
    fn test_sorting_aware_filter_lesser_initializes_field_value() {
        // Verify field_value OnceLock is properly initialized for Lesser
        let filter = SortingAwareFilter::new(
            "count".to_string(),
            Value::I32(5),
            ComparisonMode::Lesser,
        );
        let field_value = filter.get_field_value();
        assert!(field_value.is_ok());
        assert_eq!(field_value.unwrap(), Some(Value::I32(5)));
    }

    #[test]
    fn test_sorting_aware_filter_lesser_equal_initializes_field_value() {
        // Verify field_value OnceLock is properly initialized for LesserEqual
        let filter = SortingAwareFilter::new(
            "limit".to_string(),
            Value::I32(1000),
            ComparisonMode::LesserEqual,
        );
        let field_value = filter.get_field_value();
        assert!(field_value.is_ok());
        assert_eq!(field_value.unwrap(), Some(Value::I32(1000)));
    }

    #[test]
    fn test_in_filter_initializes_field_name_and_values() {
        // Verify InFilter properly initializes all OnceLock fields
        let filter = InFilter::new(
            "status".to_string(),
            vec![Value::I32(1), Value::I32(2), Value::I32(3)],
        );
        assert!(filter.get_field_name().is_ok());
        assert!(filter.get_field_value().is_ok());
    }

    #[test]
    fn test_not_in_filter_initializes_field_name_and_values() {
        // Verify NotInFilter properly initializes all OnceLock fields
        let filter = NotInFilter::new(
            "excluded".to_string(),
            vec![Value::I32(0), Value::I32(-1)],
        );
        assert!(filter.get_field_name().is_ok());
        assert!(filter.get_field_value().is_ok());
    }

    #[test]
    fn test_sorting_aware_filter_set_collection_name() {
        // Verify collection_name OnceLock can be set
        let filter = SortingAwareFilter::new(
            "field".to_string(),
            Value::I32(42),
            ComparisonMode::Greater,
        );
        let result = filter.set_collection_name("test_collection".to_string());
        assert!(result.is_ok());
        let collection_name = filter.get_collection_name();
        assert!(collection_name.is_ok());
        assert_eq!(collection_name.unwrap(), "test_collection");
    }

    #[test]
    fn test_in_filter_set_collection_name() {
        // Verify InFilter collection_name OnceLock works properly
        let filter = InFilter::new(
            "type".to_string(),
            vec![Value::I32(1)],
        );
        let result = filter.set_collection_name("my_collection".to_string());
        assert!(result.is_ok());
        let collection_name = filter.get_collection_name();
        assert!(collection_name.is_ok());
        assert_eq!(collection_name.unwrap(), "my_collection");
    }

    #[test]
    fn test_sorting_aware_filter_display_with_initialized_values() {
        // Verify Display trait works with properly initialized OnceLock values
        let filter = SortingAwareFilter::new(
            "price".to_string(),
            Value::I32(99),
            ComparisonMode::Greater,
        );
        filter.set_collection_name("products".to_string()).ok();
        let display_str = format!("{}", filter);
        assert!(display_str.contains("price"));
        assert!(display_str.contains(">"));
    }

    #[test]
    fn test_sorting_aware_filter_field_name_safe_access() {
        // Verify field_name can be safely accessed multiple times
        let filter = SortingAwareFilter::new(
            "value".to_string(),
            Value::I32(50),
            ComparisonMode::Greater,
        );
        let first_access = filter.get_field_name();
        let second_access = filter.get_field_name();
        assert!(first_access.is_ok());
        assert!(second_access.is_ok());
        assert_eq!(first_access.unwrap(), second_access.unwrap());
    }
}
