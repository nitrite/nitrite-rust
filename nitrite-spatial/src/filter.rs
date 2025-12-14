//! Spatial filters for querying geometries in Nitrite collections.
//!
//! This module provides filter types for spatial queries:
//! - `IntersectsFilter` - finds geometries that intersect with a given geometry
//! - `WithinFilter` - finds geometries contained within a given geometry  
//! - `NearFilter` - finds geometries within a distance of a point
//! - `GeoNearFilter` - finds geometries within a geodesic distance of a geographic point
//!
//! ## Two-Phase Query Execution
//!
//! Spatial queries use a two-phase approach for accuracy and performance:
//! 1. **Phase 1 (R-tree scan)**: Fast bounding box search using the R-tree index.
//!    This may return false positives due to bbox approximation.
//! 2. **Phase 2 (Geometry refinement)**: Precise geometric operations to eliminate
//!    false positives and return accurate results.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::sync::{Arc, OnceLock};

use nitrite::collection::Document;
use nitrite::common::Value;
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::filter::{Filter, FilterProvider};

use crate::geometry::{create_geodesic_circle, GeoPoint, Geometry, Point};
use crate::SpatialError;

/// The index type name for spatial indexes.
pub const SPATIAL_INDEX: &str = "Spatial";

/// Base trait for spatial filters.
pub trait SpatialFilterOps {

    /// Gets the search geometry.
    fn geometry(&self) -> &Geometry;
    
    /// Performs the precise geometry match (Phase 2).
    /// Returns true if the stored geometry matches the filter criteria.
    fn matches_geometry(&self, stored: &Geometry) -> bool;
}

/// Filter that finds geometries intersecting with a given geometry.
///
/// Two geometries intersect if they share any portion of space.
/// This includes overlapping, touching, or one containing the other.
#[derive(Clone)]
pub struct IntersectsFilter {
    inner: Arc<IntersectsFilterInner>,
}

struct IntersectsFilterInner {
    field: OnceLock<String>,
    geometry: Geometry,
}

impl IntersectsFilter {
    /// Creates a new intersects filter.
    pub fn new(field: impl Into<String>, geometry: Geometry) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field.into());

        Self {
            inner: Arc::new(IntersectsFilterInner {
                field: name,
                geometry,
            }),
        }
    }
}

impl SpatialFilterOps for IntersectsFilter {    
    fn geometry(&self) -> &Geometry {
        &self.inner.geometry
    }
    
    fn matches_geometry(&self, stored: &Geometry) -> bool {
        self.inner.geometry.intersects(stored)
    }
}

impl FilterProvider for IntersectsFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InvalidOperation)
        })?;
        
        match entry.get(&*field) {
            Ok(value) => {
                if let Some(stored_geom) = value_to_geometry(&value) {
                    Ok(self.inner.geometry.intersects(&stored_geom))
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InvalidOperation)
        })?;
        Ok(field.clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.inner.field.get_or_init(|| field_name);
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(SPATIAL_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if let Some(_) = other.as_any().downcast_ref::<IntersectsFilter>() {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else if let Some(_) = other.as_any().downcast_ref::<WithinFilter>() {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else if let Some(_) = other.as_any().downcast_ref::<NearFilter>() {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else if let Some(_) = other.as_any().downcast_ref::<GeoNearFilter>() {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else {
            Ok(false)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for IntersectsFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.get().ok_or_else(|| NitriteError::new("Field name not set", ErrorKind::InvalidOperation));
        match field {
            Ok(field_name) => write!(f, "({} intersects {})", field_name, self.inner.geometry),
            Err(_) => write!(f, "(<unknown field> intersects {})", self.inner.geometry),
        }
    }
}

/// Filter that finds geometries contained within a given geometry.
///
/// A stored geometry matches if it is completely contained within
/// the search geometry.
#[derive(Clone)]
pub struct WithinFilter {
    inner: Arc<WithinFilterInner>,
}

struct WithinFilterInner {
    field: OnceLock<String>,
    geometry: Geometry,
}

impl WithinFilter {
    /// Creates a new within filter.
    pub fn new(field: impl Into<String>, geometry: Geometry) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field.into());

        Self {
            inner: Arc::new(WithinFilterInner {
                field: name,
                geometry,
            }),
        }
    }
}

impl SpatialFilterOps for WithinFilter {
    fn geometry(&self) -> &Geometry {
        &self.inner.geometry
    }
    
    fn matches_geometry(&self, stored: &Geometry) -> bool {
        self.inner.geometry.contains(stored)
    }
}

impl FilterProvider for WithinFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InvalidOperation)
        })?;
        
        match entry.get(&*field) {
            Ok(value) => {
                if let Some(stored_geom) = value_to_geometry(&value) {
                    Ok(self.inner.geometry.contains(&stored_geom))
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InvalidOperation)
        })?;
        Ok(field.clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.inner.field.get_or_init(|| field_name);
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(SPATIAL_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if other.as_any().downcast_ref::<IntersectsFilter>().is_some()
            || other.as_any().downcast_ref::<WithinFilter>().is_some()
            || other.as_any().downcast_ref::<NearFilter>().is_some()
            || other.as_any().downcast_ref::<GeoNearFilter>().is_some()
        {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else {
            Ok(false)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for WithinFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.get().ok_or_else(|| NitriteError::new("Field name not set", ErrorKind::InvalidOperation));
        match field {
            Ok(field_name) => write!(f, "({} within {})", field_name, self.inner.geometry),
            Err(_) => write!(f, "(<unknown field> within {})", self.inner.geometry),
        }
    }
}

/// Filter that finds geometries near a point within a specified distance.
///
/// This filter uses Euclidean distance for Cartesian coordinates.
/// For geographic coordinates, use `GeoNearFilter` instead.
#[derive(Clone)]
pub struct NearFilter {
    inner: Arc<NearFilterInner>,
}

struct NearFilterInner {
    field: OnceLock<String>,
    center: Point,
    distance: f64,
    geometry: Geometry,
}

impl NearFilter {
    /// Creates a new near filter.
    ///
    /// # Arguments
    /// * `field` - The field containing the geometry
    /// * `center` - The center point
    /// * `distance` - The maximum distance (in coordinate units)
    pub fn new(field: impl Into<String>, center: Point, distance: f64) -> Self {
        let geometry = Geometry::circle(center.x(), center.y(), distance);
        let name = OnceLock::new();
        let _ = name.set(field.into());

        Self {
            inner: Arc::new(NearFilterInner {
                field: name,
                center,
                distance,
                geometry,
            }),
        }
    }

    /// Creates a near filter from coordinates.
    pub fn from_coords(field: impl Into<String>, x: f64, y: f64, distance: f64) -> Self {
        Self::new(field, Point::new(x, y), distance)
    }
}

impl SpatialFilterOps for NearFilter {
    fn geometry(&self) -> &Geometry {
        &self.inner.geometry
    }
    
    fn matches_geometry(&self, stored: &Geometry) -> bool {
        // Check if any part of the stored geometry is within distance
        match stored {
            Geometry::Point(p) => {
                let dist = self.inner.center.coordinate().distance(p.coordinate());
                dist <= self.inner.distance
            }
            _ => {
                // For non-points, check if the search circle contains/intersects
                self.inner.geometry.contains(stored) || self.inner.geometry.intersects(stored)
            }
        }
    }
}

impl FilterProvider for NearFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InvalidOperation)
        })?;
        
        match entry.get(&*field) {
            Ok(value) => {
                if let Some(stored_geom) = value_to_geometry(&value) {
                    Ok(self.matches_geometry(&stored_geom))
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InvalidOperation)
        })?;
        Ok(field.clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.inner.field.get_or_init(|| field_name);
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(SPATIAL_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if other.as_any().downcast_ref::<IntersectsFilter>().is_some()
            || other.as_any().downcast_ref::<WithinFilter>().is_some()
            || other.as_any().downcast_ref::<NearFilter>().is_some()
            || other.as_any().downcast_ref::<GeoNearFilter>().is_some()
        {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else {
            Ok(false)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for NearFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.get().ok_or_else(|| NitriteError::new("Field name not set", ErrorKind::InvalidOperation));
        match field {
            Ok(field_name) => write!(f, "({} near ({}, {}) within {})", field_name, self.inner.center.x(), self.inner.center.y(), self.inner.distance),
            Err(_) => write!(f, "(<unknown field> near ({}, {}) within {})", self.inner.center.x(), self.inner.center.y(), self.inner.distance),
        }
    }
}

/// Filter that finds geometries near a geographic point using geodesic distance.
///
/// This filter is specifically for geographic coordinates (latitude/longitude)
/// and uses the Haversine formula for accurate distance calculations on Earth's
/// surface.
///
/// ## Example
///
/// ```rust,no_run
/// use nitrite_spatial::{GeoNearFilter, GeoPoint};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Find locations within 5km of Minneapolis
/// let minneapolis = GeoPoint::new(45.0, -93.265)?;
/// let filter = GeoNearFilter::new("location", minneapolis, 5000.0)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct GeoNearFilter {
    inner: Arc<GeoNearFilterInner>,
}

struct GeoNearFilterInner {
    field: OnceLock<String>,
    center: GeoPoint,
    distance_meters: f64,
    geometry: Geometry,
}

impl GeoNearFilter {
    /// Creates a new geographic near filter.
    ///
    /// # Arguments
    /// * `field` - The field containing the geometry/GeoPoint
    /// * `center` - The center geographic point
    /// * `distance_meters` - The maximum distance in meters
    pub fn new(field: impl Into<String>, center: GeoPoint, distance_meters: f64) -> Result<Self, SpatialError> {
        // Create a search geometry (circle in degrees) for the R-tree query
        let geometry = create_geodesic_circle(&center, distance_meters, 64);
        let name = OnceLock::new();
        let _ = name.set(field.into());
        
        Ok(Self {
            inner: Arc::new(GeoNearFilterInner {
                field: name,
                center,
                distance_meters,
                geometry,
            }),
        })
    }

    /// Creates a GeoNearFilter from latitude/longitude coordinates.
    pub fn from_coords(
        field: impl Into<String>,
        latitude: f64,
        longitude: f64,
        distance_meters: f64,
    ) -> Result<Self, SpatialError> {
        let center = GeoPoint::new(latitude, longitude)?;
        Self::new(field, center, distance_meters)
    }
}

impl SpatialFilterOps for GeoNearFilter {
    fn geometry(&self) -> &Geometry {
        &self.inner.geometry
    }
    
    fn matches_geometry(&self, stored: &Geometry) -> bool {
        // For geographic queries, we need to do precise distance calculation
        match stored {
            Geometry::Point(p) => {
                // Convert point to GeoPoint and calculate geodesic distance
                if let Ok(stored_geo) = GeoPoint::new(p.y(), p.x()) {
                    let dist = self.inner.center.distance_meters(&stored_geo);
                    dist <= self.inner.distance_meters
                } else {
                    false
                }
            }
            _ => {
                // For non-points, fall back to geometry containment check
                self.inner.geometry.contains(stored)
            }
        }
    }
}

impl FilterProvider for GeoNearFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InternalError)
        })?;
        
        match entry.get(&*field) {
            Ok(value) => {
                if let Some(stored_geom) = value_to_geometry(&value) {
                    Ok(self.matches_geometry(&stored_geom))
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InternalError)
        })?;
        Ok(field.clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.inner.field.get_or_init(|| field_name);
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(SPATIAL_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if other.as_any().downcast_ref::<IntersectsFilter>().is_some()
            || other.as_any().downcast_ref::<WithinFilter>().is_some()
            || other.as_any().downcast_ref::<NearFilter>().is_some()
            || other.as_any().downcast_ref::<GeoNearFilter>().is_some()
        {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else {
            Ok(false)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for GeoNearFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.get().ok_or_else(|| NitriteError::new("Field name not set", ErrorKind::InternalError));
        match field {
            Ok(field_name) => write!(f, "({} geo_near ({}, {}) within {} meters)", field_name, self.inner.center.latitude(), self.inner.center.longitude(), self.inner.distance_meters),
            Err(_) => write!(f, "(<unknown field> geo_near ({}, {}) within {} meters)", self.inner.center.latitude(), self.inner.center.longitude(), self.inner.distance_meters),
        }
    }
}

/// Filter that finds the K nearest geometries to a given point.
///
/// This filter returns the K nearest entries, optionally filtered by a maximum distance.
/// Uses Euclidean distance for Cartesian coordinates.
///
/// # Example
///
/// ```rust
/// use nitrite_spatial::filter::KNearestFilter;
/// use nitrite_spatial::Point;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Find the 10 nearest points to (0, 0)
/// let filter = KNearestFilter::new("location", Point::new(0.0, 0.0), 10)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KNearestFilter {
    inner: Arc<KNearestFilterInner>,
}

struct KNearestFilterInner {
    field: OnceLock<String>,
    center: Point,
    k: usize,
    max_distance: Option<f64>,
    geometry: Geometry,
}

impl KNearestFilter {
    /// Creates a new K-nearest filter.
    ///
    /// # Arguments
    /// * `field` - The field containing the geometry
    /// * `center` - The center point
    /// * `k` - Number of nearest entries to return
    pub fn new(field: impl Into<String>, center: Point, k: usize) -> Result<Self, SpatialError> {
        if k == 0 {
            return Err(SpatialError::InvalidOperation("k must be greater than 0".to_string()));
        }
        
        let geometry = Geometry::point(center.x(), center.y());
        let name = OnceLock::new();
        let _ = name.set(field.into());
        
        Ok(Self {
            inner: Arc::new(KNearestFilterInner {
                field: name,
                center,
                k,
                max_distance: None,
                geometry,
            }),
        })
    }

    /// Creates a K-nearest filter with a maximum distance constraint.
    ///
    /// # Arguments
    /// * `field` - The field containing the geometry
    /// * `center` - The center point
    /// * `k` - Number of nearest entries to return
    /// * `max_distance` - Maximum distance to consider (in coordinate units)
    pub fn with_max_distance(
        field: impl Into<String>,
        center: Point,
        k: usize,
        max_distance: f64,
    ) -> Result<Self, SpatialError> {
        if k == 0 {
            return Err(SpatialError::InvalidOperation("k must be greater than 0".to_string()));
        }
        if max_distance < 0.0 {
            return Err(SpatialError::InvalidOperation("max_distance must be non-negative".to_string()));
        }
        
        let geometry = Geometry::point(center.x(), center.y());
        let name = OnceLock::new();
        let _ = name.set(field.into());
        
        Ok(Self {
            inner: Arc::new(KNearestFilterInner {
                field: name,
                center,
                k,
                max_distance: Some(max_distance),
                geometry,
            }),
        })
    }

    /// Creates a K-nearest filter from coordinates.
    pub fn from_coords(field: impl Into<String>, x: f64, y: f64, k: usize) -> Result<Self, SpatialError> {
        Self::new(field, Point::new(x, y), k)
    }

    /// Gets the center point.
    pub fn center(&self) -> &Point {
        &self.inner.center
    }

    /// Gets the k value.
    pub fn k(&self) -> usize {
        self.inner.k
    }

    /// Gets the maximum distance, if set.
    pub fn max_distance(&self) -> Option<f64> {
        self.inner.max_distance
    }
}

impl SpatialFilterOps for KNearestFilter {
    fn geometry(&self) -> &Geometry {
        &self.inner.geometry
    }
    
    fn matches_geometry(&self, stored: &Geometry) -> bool {
        // KNN filtering is special - the actual K selection happens in the indexer
        // This method just checks if the geometry is within the max_distance if set
        if let Some(max_dist) = self.inner.max_distance {
            match stored {
                Geometry::Point(p) => {
                    let dist = self.inner.center.coordinate().distance(p.coordinate());
                    dist <= max_dist
                }
                _ => true, // Accept non-points for now, refinement happens elsewhere
            }
        } else {
            true // Unbounded KNN accepts all geometries for the refinement phase
        }
    }
}

impl FilterProvider for KNearestFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InternalError)
        })?;
        
        match entry.get(&*field) {
            Ok(value) => {
                if let Some(stored_geom) = value_to_geometry(&value) {
                    Ok(self.matches_geometry(&stored_geom))
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        let field = self.inner.field.get().ok_or_else(|| {
            NitriteError::new("Field name not set", ErrorKind::InternalError)
        })?;
        Ok(field.clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.inner.field.get_or_init(|| field_name);
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(SPATIAL_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if other.as_any().downcast_ref::<IntersectsFilter>().is_some()
            || other.as_any().downcast_ref::<WithinFilter>().is_some()
            || other.as_any().downcast_ref::<NearFilter>().is_some()
            || other.as_any().downcast_ref::<GeoNearFilter>().is_some()
            || other.as_any().downcast_ref::<KNearestFilter>().is_some()
        {
            let self_field = self.get_field_name()?;
            let other_field = other.get_field_name()?;
            Ok(self_field == other_field)
        } else {
            Ok(false)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for KNearestFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.get().ok_or_else(|| fmt::Error)?;
        if let Some(max_dist) = self.inner.max_distance {
            write!(
                f,
                "({} nearest {} to ({}, {}) within {})",
                field,
                self.inner.k,
                self.inner.center.x(),
                self.inner.center.y(),
                max_dist
            )
        } else {
            write!(
                f,
                "({} nearest {} to ({}, {}))",
                field,
                self.inner.k,
                self.inner.center.x(),
                self.inner.center.y()
            )
        }
    }
}

/// Converts a Nitrite Value to a Geometry, if possible.
///
/// Supports:
/// - Map with "x" and "y" fields (Point)
/// - Map with "latitude" and "longitude" fields (GeoPoint)
/// - Map with "min_x", "min_y", "max_x", "max_y" fields (BoundingBox)
/// - Document with the same field structures (converted to map internally)
pub fn value_to_geometry(value: &Value) -> Option<Geometry> {
    // Helper function to process map-like data
    fn process_map(map: &BTreeMap<Value, Value>) -> Option<Geometry> {
        let x_key = Value::String("x".to_string());
        let y_key = Value::String("y".to_string());
        let lat_key = Value::String("latitude".to_string());
        let lon_key = Value::String("longitude".to_string());
        let min_x_key = Value::String("min_x".to_string());
        let min_y_key = Value::String("min_y".to_string());
        let max_x_key = Value::String("max_x".to_string());
        let max_y_key = Value::String("max_y".to_string());
        
        // Try to parse as a point (x, y)
        if let (Some(x_val), Some(y_val)) = (map.get(&x_key), map.get(&y_key)) {
            let x = value_to_f64(x_val)?;
            let y = value_to_f64(y_val)?;
            return Some(Geometry::point(x, y));
        }
        
        // Try to parse as GeoPoint (latitude, longitude)
        if let (Some(lat), Some(lon)) = (map.get(&lat_key), map.get(&lon_key)) {
            let lat_f = value_to_f64(lat)?;
            let lon_f = value_to_f64(lon)?;
            // GeoPoint stores lat/lon, but Geometry uses x=lon, y=lat
            return Some(Geometry::point(lon_f, lat_f));
        }
        
        // Try to parse as BoundingBox
        if let (Some(min_x), Some(min_y), Some(max_x), Some(max_y)) = (
            map.get(&min_x_key),
            map.get(&min_y_key),
            map.get(&max_x_key),
            map.get(&max_y_key),
        ) {
            let min_x = value_to_f64(min_x)?;
            let min_y = value_to_f64(min_y)?;
            let max_x = value_to_f64(max_x)?;
            let max_y = value_to_f64(max_y)?;
            return Some(Geometry::envelope(min_x, min_y, max_x, max_y));
        }
        
        None
    }
    
    // Helper function to process document (string keys)
    fn process_document(doc: &Document) -> Option<Geometry> {
        // Try to parse as a point (x, y)
        if let (Ok(x_val), Ok(y_val)) = (doc.get("x"), doc.get("y")) {
            if !x_val.is_null() && !y_val.is_null() {
                let x = value_to_f64(&x_val)?;
                let y = value_to_f64(&y_val)?;
                return Some(Geometry::point(x, y));
            }
        }
        
        // Try to parse as GeoPoint (latitude, longitude)
        if let (Ok(lat), Ok(lon)) = (doc.get("latitude"), doc.get("longitude")) {
            if !lat.is_null() && !lon.is_null() {
                let lat_f = value_to_f64(&lat)?;
                let lon_f = value_to_f64(&lon)?;
                // GeoPoint stores lat/lon, but Geometry uses x=lon, y=lat
                return Some(Geometry::point(lon_f, lat_f));
            }
        }
        
        // Try to parse as BoundingBox
        if let (Ok(min_x), Ok(min_y), Ok(max_x), Ok(max_y)) = (
            doc.get("min_x"),
            doc.get("min_y"),
            doc.get("max_x"),
            doc.get("max_y"),
        ) {
            if !min_x.is_null() && !min_y.is_null() && !max_x.is_null() && !max_y.is_null() {
                let min_x = value_to_f64(&min_x)?;
                let min_y = value_to_f64(&min_y)?;
                let max_x = value_to_f64(&max_x)?;
                let max_y = value_to_f64(&max_y)?;
                return Some(Geometry::envelope(min_x, min_y, max_x, max_y));
            }
        }
        
        None
    }
    
    match value {
        Value::Map(map) => process_map(map),
        Value::Document(doc) => process_document(doc),
        Value::F64(_) | Value::I64(_) | Value::I32(_) | Value::U64(_) => {
            // Single number can't be a geometry
            None
        }
        _ => None,
    }
}

/// Helper to convert a Value to f64.
fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::F64(f) => Some(*f),
        Value::I64(i) => Some(*i as f64),
        Value::I32(i) => Some(*i as f64),
        Value::U64(u) => Some(*u as f64),
        _ => None,
    }
}

/// Checks if a filter is a spatial filter.
pub fn is_spatial_filter(filter: &Filter) -> bool {
    filter.as_any().downcast_ref::<IntersectsFilter>().is_some()
        || filter.as_any().downcast_ref::<WithinFilter>().is_some()
        || filter.as_any().downcast_ref::<NearFilter>().is_some()
        || filter.as_any().downcast_ref::<GeoNearFilter>().is_some()
        || filter.as_any().downcast_ref::<KNearestFilter>().is_some()
}

/// Gets the spatial filter from a Filter if it is one.
pub fn as_spatial_filter(filter: &Filter) -> Option<&dyn SpatialFilterOps> {
    if let Some(f) = filter.as_any().downcast_ref::<IntersectsFilter>() {
        Some(f as &dyn SpatialFilterOps)
    } else if let Some(f) = filter.as_any().downcast_ref::<WithinFilter>() {
        Some(f as &dyn SpatialFilterOps)
    } else if let Some(f) = filter.as_any().downcast_ref::<NearFilter>() {
        Some(f as &dyn SpatialFilterOps)
    } else if let Some(f) = filter.as_any().downcast_ref::<GeoNearFilter>() {
        Some(f as &dyn SpatialFilterOps)
    } else if let Some(f) = filter.as_any().downcast_ref::<KNearestFilter>() {
        Some(f as &dyn SpatialFilterOps)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_intersects_filter_display() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        let display = format!("{}", filter);
        assert!(display.contains("intersects"));
    }

    #[test]
    fn test_within_filter_display() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        let display = format!("{}", filter);
        assert!(display.contains("within"));
    }

    #[test]
    fn test_near_filter_display() {
        let filter = NearFilter::from_coords("location", 10.0, 20.0, 5.0);
        let display = format!("{}", filter);
        assert!(display.contains("near"));
    }

    #[test]
    fn test_geo_near_filter_display() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.0, 5000.0).unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("geo_near"));
    }

    #[test]
    fn test_value_to_geometry_point() {
        let mut map = BTreeMap::new();
        map.insert(Value::String("x".to_string()), Value::F64(10.0));
        map.insert(Value::String("y".to_string()), Value::F64(20.0));
        let value = Value::Map(map);
        
        let geom = value_to_geometry(&value);
        assert!(geom.is_some());
        if let Some(Geometry::Point(p)) = geom {
            assert_eq!(p.x(), 10.0);
            assert_eq!(p.y(), 20.0);
        } else {
            panic!("Expected Point geometry");
        }
    }

    #[test]
    fn test_value_to_geometry_geopoint() {
        let mut map = BTreeMap::new();
        map.insert(Value::String("latitude".to_string()), Value::F64(45.0));
        map.insert(Value::String("longitude".to_string()), Value::F64(-93.0));
        let value = Value::Map(map);
        
        let geom = value_to_geometry(&value);
        assert!(geom.is_some());
        if let Some(Geometry::Point(p)) = geom {
            // Geometry uses x=lon, y=lat
            assert_eq!(p.x(), -93.0);
            assert_eq!(p.y(), 45.0);
        } else {
            panic!("Expected Point geometry");
        }
    }

    #[test]
    fn test_near_filter_matches() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 10.0);
        
        // Point inside
        let inside = Geometry::point(3.0, 4.0); // distance = 5
        assert!(filter.matches_geometry(&inside));
        
        // Point outside
        let outside = Geometry::point(10.0, 10.0); // distance ≈ 14.14
        assert!(!filter.matches_geometry(&outside));
    }

    #[test]
    fn test_geo_near_filter_matches() {
        // Minneapolis
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.265, 10000.0).unwrap();
        
        // Point ~5km away (should match)
        let nearby = Geometry::point(-93.265, 45.045); // ~5km north
        assert!(filter.matches_geometry(&nearby));
        
        // Point very far away (should not match)
        let far = Geometry::point(-118.0, 34.0); // Los Angeles
        assert!(!filter.matches_geometry(&far));
    }

    // =========================================================================
    // Additional Comprehensive Tests
    // =========================================================================

    #[test]
    fn test_intersects_filter_get_field_name() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "location");
    }

    #[test]
    fn test_intersects_filter_has_field() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        assert!(filter.has_field());
    }

    #[test]
    fn test_intersects_filter_is_index_only() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_intersects_filter_supported_index_type() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        let index_type = filter.supported_index_type().unwrap();
        assert_eq!(index_type, SPATIAL_INDEX);
    }

    #[test]
    fn test_within_filter_get_field_name() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "location");
    }

    #[test]
    fn test_within_filter_is_index_only() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_within_filter_supported_index_type() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        let index_type = filter.supported_index_type().unwrap();
        assert_eq!(index_type, SPATIAL_INDEX);
    }

    #[test]
    fn test_near_filter_get_field_name() {
        let filter = NearFilter::from_coords("location", 10.0, 20.0, 5.0);
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "location");
    }

    #[test]
    fn test_near_filter_is_index_only() {
        let filter = NearFilter::from_coords("location", 10.0, 20.0, 5.0);
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_geo_near_filter_get_field_name() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.0, 5000.0).unwrap();
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "location");
    }

    #[test]
    fn test_geo_near_filter_is_index_only() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.0, 5000.0).unwrap();
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_geo_near_filter_invalid_latitude() {
        let result = GeoNearFilter::from_coords("location", 91.0, 0.0, 1000.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_geo_near_filter_invalid_longitude() {
        let result = GeoNearFilter::from_coords("location", 0.0, 181.0, 1000.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_intersects_filter_matches_geometry_point_point() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        
        // Same point
        let same = Geometry::point(10.0, 20.0);
        assert!(filter.matches_geometry(&same));
        
        // Different point
        let different = Geometry::point(15.0, 25.0);
        assert!(!filter.matches_geometry(&different));
    }

    #[test]
    fn test_intersects_filter_matches_geometry_point_circle() {
        let filter = IntersectsFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        
        // Point inside circle
        let inside = Geometry::point(3.0, 4.0);
        assert!(filter.matches_geometry(&inside));
        
        // Point on edge
        let edge = Geometry::point(10.0, 0.0);
        assert!(filter.matches_geometry(&edge));
        
        // Point outside
        let outside = Geometry::point(20.0, 20.0);
        assert!(!filter.matches_geometry(&outside));
    }

    #[test]
    fn test_intersects_filter_matches_geometry_circle_circle() {
        let filter = IntersectsFilter::new("location", Geometry::circle(0.0, 0.0, 5.0));
        
        // Overlapping circles
        let overlapping = Geometry::circle(8.0, 0.0, 5.0);
        assert!(filter.matches_geometry(&overlapping));
        
        // Non-overlapping circles
        let separate = Geometry::circle(20.0, 0.0, 5.0);
        assert!(!filter.matches_geometry(&separate));
    }

    #[test]
    fn test_intersects_filter_matches_geometry_envelope() {
        let filter = IntersectsFilter::new("location", Geometry::envelope(0.0, 0.0, 10.0, 10.0));
        
        // Point inside
        let inside = Geometry::point(5.0, 5.0);
        assert!(filter.matches_geometry(&inside));
        
        // Point outside
        let outside = Geometry::point(20.0, 20.0);
        assert!(!filter.matches_geometry(&outside));
    }

    #[test]
    fn test_within_filter_matches_geometry() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        
        // Point inside
        let inside = Geometry::point(3.0, 4.0);
        assert!(filter.matches_geometry(&inside));
        
        // Point outside
        let outside = Geometry::point(20.0, 20.0);
        assert!(!filter.matches_geometry(&outside));
    }

    #[test]
    fn test_value_to_geometry_envelope() {
        let mut map = BTreeMap::new();
        map.insert(Value::String("min_x".to_string()), Value::F64(0.0));
        map.insert(Value::String("min_y".to_string()), Value::F64(0.0));
        map.insert(Value::String("max_x".to_string()), Value::F64(10.0));
        map.insert(Value::String("max_y".to_string()), Value::F64(10.0));
        let value = Value::Map(map);
        
        let geom = value_to_geometry(&value);
        assert!(geom.is_some());
        if let Some(Geometry::Envelope(bbox)) = geom {
            assert_eq!(bbox.min_x, 0.0);
            assert_eq!(bbox.min_y, 0.0);
            assert_eq!(bbox.max_x, 10.0);
            assert_eq!(bbox.max_y, 10.0);
        } else {
            panic!("Expected Envelope geometry");
        }
    }

    #[test]
    fn test_value_to_geometry_integer_values() {
        let mut map = BTreeMap::new();
        map.insert(Value::String("x".to_string()), Value::I64(10));
        map.insert(Value::String("y".to_string()), Value::I64(20));
        let value = Value::Map(map);
        
        let geom = value_to_geometry(&value);
        assert!(geom.is_some());
        if let Some(Geometry::Point(p)) = geom {
            assert_eq!(p.x(), 10.0);
            assert_eq!(p.y(), 20.0);
        } else {
            panic!("Expected Point geometry");
        }
    }

    #[test]
    fn test_value_to_geometry_invalid() {
        // Empty map
        let empty_map = Value::Map(BTreeMap::new());
        assert!(value_to_geometry(&empty_map).is_none());
        
        // Single number
        let number = Value::F64(42.0);
        assert!(value_to_geometry(&number).is_none());
        
        // String
        let string = Value::String("not a geometry".to_string());
        assert!(value_to_geometry(&string).is_none());
    }

    #[test]
    fn test_is_spatial_filter() {
        let intersects = Filter::new(IntersectsFilter::new("location", Geometry::point(0.0, 0.0)));
        assert!(is_spatial_filter(&intersects));
        
        let within = Filter::new(WithinFilter::new("location", Geometry::circle(0.0, 0.0, 5.0)));
        assert!(is_spatial_filter(&within));
        
        let near = Filter::new(NearFilter::from_coords("location", 0.0, 0.0, 5.0));
        assert!(is_spatial_filter(&near));
    }

    #[test]
    fn test_as_spatial_filter() {
        let intersects = Filter::new(IntersectsFilter::new("location", Geometry::point(0.0, 0.0)));
        let spatial = as_spatial_filter(&intersects);
        assert!(spatial.is_some());
        
        // Check that we can access geometry
        let geometry = spatial.unwrap().geometry();
        if let Geometry::Point(p) = geometry {
            assert_eq!(p.x(), 0.0);
            assert_eq!(p.y(), 0.0);
        } else {
            panic!("Expected Point geometry");
        }
    }

    #[test]
    fn test_near_filter_geometry() {
        let filter = NearFilter::from_coords("location", 10.0, 20.0, 5.0);
        let geom = filter.geometry();
        
        // Near filter creates a circle geometry
        if let Geometry::Circle { center, radius } = geom {
            assert_eq!(center.x(), 10.0);
            assert_eq!(center.y(), 20.0);
            assert_eq!(*radius, 5.0);
        } else {
            panic!("Expected Circle geometry");
        }
    }

    #[test]
    fn test_geo_near_filter_geometry() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.265, 10000.0).unwrap();
        let geom = filter.geometry();
        
        // GeoNear filter creates a circle geometry with center at (lon, lat)
        if let Geometry::Circle { center, radius } = geom {
            assert_eq!(center.x(), -93.265);
            assert_eq!(center.y(), 45.0);
            assert!(*radius > 0.0);
        } else {
            panic!("Expected Circle geometry");
        }
    }

    #[test]
    fn test_knearest_filter_creation() {
        let filter = KNearestFilter::from_coords("location", 10.0, 20.0, 5).unwrap();
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "location");
        assert_eq!(filter.k(), 5);
    }

    #[test]
    fn test_knearest_filter_with_max_distance() {
        let center = Point::new(10.0, 20.0);
        let filter = KNearestFilter::with_max_distance("location", center, 5, 100.0).unwrap();
        assert_eq!(filter.k(), 5);
        assert_eq!(filter.max_distance(), Some(100.0));
    }

    #[test]
    fn test_knearest_filter_zero_k() {
        let result = KNearestFilter::from_coords("location", 0.0, 0.0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_knearest_filter_display() {
        let filter = KNearestFilter::from_coords("location", 10.0, 20.0, 5).unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
        assert!(display.contains("5"));
    }

    #[test]
    fn test_knearest_filter_is_index_only() {
        let filter = KNearestFilter::from_coords("location", 10.0, 20.0, 5).unwrap();
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_knearest_filter_supported_index_type() {
        let filter = KNearestFilter::from_coords("location", 10.0, 20.0, 5).unwrap();
        let index_type = filter.supported_index_type().unwrap();
        assert_eq!(index_type, SPATIAL_INDEX);
    }

    #[test]
    fn test_filter_clone() {
        let filter = IntersectsFilter::new("location", Geometry::point(10.0, 20.0));
        let cloned = filter.clone();
        assert_eq!(filter.get_field_name().unwrap(), cloned.get_field_name().unwrap());
    }

    #[test]
    fn test_near_filter_matches_nearby_point() {
        let filter = NearFilter::from_coords("location", 10.0, 20.0, 5.0);
        // A point within distance should match
        let nearby_point = Geometry::point(12.0, 21.0); // Distance ~sqrt(5) < 5
        assert!(filter.matches_geometry(&nearby_point));
    }

    #[test]
    fn test_near_filter_rejects_far_point() {
        let filter = NearFilter::from_coords("location", 10.0, 20.0, 5.0);
        // A point outside distance should not match
        let far_point = Geometry::point(20.0, 30.0); // Distance ~sqrt(200) > 5
        assert!(!filter.matches_geometry(&far_point));
    }

    #[test]
    fn test_geo_near_filter_matches_nearby_point() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.265, 50000.0).unwrap(); // 50km
        // A point within distance should match
        // Point at ~45.1, -93.265 is about 11km away
        let nearby_point = Geometry::point(-93.265, 45.1);
        assert!(filter.matches_geometry(&nearby_point));
    }

    #[test]
    fn test_geo_near_filter_rejects_far_point() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.265, 1000.0).unwrap(); // 1km
        // A point outside distance should not match
        // Point at 46.0, -93.265 is about 111km away
        let far_point = Geometry::point(-93.265, 46.0);
        assert!(!filter.matches_geometry(&far_point));
    }
}

// ADDITIONAL TESTS FOR COVERAGE
#[cfg(test)]
mod tests_additional_filter {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_within_filter_has_field() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        assert!(filter.has_field());
    }

    #[test]
    fn test_within_filter_can_be_grouped_with_intersects() {
        let within = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        let intersects = Filter::new(IntersectsFilter::new("location", Geometry::point(0.0, 0.0)));
        let can_group = within.can_be_grouped(intersects).unwrap();
        assert!(can_group);
    }

    #[test]
    fn test_within_filter_can_be_grouped_different_field() {
        let within = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        let intersects = Filter::new(IntersectsFilter::new("other_field", Geometry::point(0.0, 0.0)));
        let can_group = within.can_be_grouped(intersects).unwrap();
        assert!(!can_group);
    }

    // =========================================================================
    // POSITIVE TEST CASES - Expected successful filter operations
    // =========================================================================

    #[test]
    fn test_intersects_filter_with_envelope() {
        let filter = IntersectsFilter::new("location", Geometry::envelope(0.0, 0.0, 10.0, 10.0));
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "location");
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_within_filter_with_envelope() {
        let filter = WithinFilter::new("bounds", Geometry::envelope(0.0, 0.0, 100.0, 100.0));
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "bounds");
        assert!(filter.has_field());
    }

    #[test]
    fn test_near_filter_point_exactly_at_distance() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 5.0);
        // Point exactly at distance
        let at_distance = Geometry::point(5.0, 0.0);
        assert!(filter.matches_geometry(&at_distance));
    }

    #[test]
    fn test_near_filter_circle_intersection() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 10.0);
        // Circle that intersects search circle
        let intersecting = Geometry::circle(15.0, 0.0, 10.0);
        assert!(filter.matches_geometry(&intersecting));
    }

    #[test]
    fn test_knearest_filter_center_property() {
        let filter = KNearestFilter::from_coords("location", 5.5, 7.3, 10).unwrap();
        assert_eq!(filter.center().x(), 5.5);
        assert_eq!(filter.center().y(), 7.3);
    }

    #[test]
    fn test_intersects_filter_can_be_grouped_with_within() {
        let intersects = IntersectsFilter::new("geo", Geometry::point(0.0, 0.0));
        let within = Filter::new(WithinFilter::new("geo", Geometry::circle(0.0, 0.0, 10.0)));
        let can_group = intersects.can_be_grouped(within).unwrap();
        assert!(can_group);
    }

    #[test]
    fn test_intersects_filter_can_be_grouped_with_near() {
        let intersects = IntersectsFilter::new("geo", Geometry::point(0.0, 0.0));
        let near = Filter::new(NearFilter::from_coords("geo", 0.0, 0.0, 5.0));
        let can_group = intersects.can_be_grouped(near).unwrap();
        assert!(can_group);
    }

    #[test]
    fn test_within_filter_can_be_grouped_with_near() {
        let within = WithinFilter::new("geo", Geometry::circle(0.0, 0.0, 10.0));
        let near = Filter::new(NearFilter::from_coords("geo", 0.0, 0.0, 5.0));
        let can_group = within.can_be_grouped(near).unwrap();
        assert!(can_group);
    }

    #[test]
    fn test_near_filter_can_be_grouped_with_geo_near() {
        let near = NearFilter::from_coords("geo", 0.0, 0.0, 5.0);
        let geo_near = Filter::new(GeoNearFilter::from_coords("geo", 45.0, -93.0, 5000.0).unwrap());
        let can_group = near.can_be_grouped(geo_near).unwrap();
        assert!(can_group);
    }

    #[test]
    fn test_knearest_filter_can_be_grouped_with_intersects() {
        let knearest = KNearestFilter::from_coords("geo", 0.0, 0.0, 5).unwrap();
        let intersects = Filter::new(IntersectsFilter::new("geo", Geometry::point(0.0, 0.0)));
        let can_group = knearest.can_be_grouped(intersects).unwrap();
        assert!(can_group);
    }

    // =========================================================================
    // NEGATIVE TEST CASES - Error conditions and invalid inputs
    // =========================================================================

    #[test]
    fn test_knearest_filter_negative_max_distance() {
        let center = Point::new(0.0, 0.0);
        let result = KNearestFilter::with_max_distance("location", center, 5, -1.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_intersects_filter_can_be_grouped_different_field_with_within() {
        let intersects = IntersectsFilter::new("geo1", Geometry::point(0.0, 0.0));
        let within = Filter::new(WithinFilter::new("geo2", Geometry::circle(0.0, 0.0, 10.0)));
        let can_group = intersects.can_be_grouped(within).unwrap();
        assert!(!can_group);
    }

    #[test]
    fn test_near_filter_can_be_grouped_different_field() {
        let near = NearFilter::from_coords("location", 0.0, 0.0, 5.0);
        let intersects = Filter::new(IntersectsFilter::new("other", Geometry::point(0.0, 0.0)));
        let can_group = near.can_be_grouped(intersects).unwrap();
        assert!(!can_group);
    }

    #[test]
    fn test_intersects_filter_matches_non_matching_geometry() {
        let filter = IntersectsFilter::new("location", Geometry::point(0.0, 0.0));
        // Point far away
        let far = Geometry::point(1000.0, 1000.0);
        assert!(!filter.matches_geometry(&far));
    }

    #[test]
    fn test_within_filter_matches_non_contained_geometry() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 5.0));
        // Point outside circle
        let outside = Geometry::point(100.0, 100.0);
        assert!(!filter.matches_geometry(&outside));
    }

    #[test]
    fn test_near_filter_envelope_not_contained() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 10.0);
        // Envelope completely outside search circle
        let far_envelope = Geometry::envelope(100.0, 100.0, 110.0, 110.0);
        assert!(!filter.matches_geometry(&far_envelope));
    }

    #[test]
    fn test_geo_near_filter_point_far_away() {
        let filter = GeoNearFilter::from_coords("location", 0.0, 0.0, 1000.0).unwrap(); // 1km
        // Point very far away (about 10km at equator per degree)
        let far = Geometry::point(0.0, 1.0);
        assert!(!filter.matches_geometry(&far));
    }

    // =========================================================================
    // EDGE CASES - Boundary conditions and special scenarios
    // =========================================================================

    #[test]
    fn test_intersects_filter_point_at_origin() {
        let filter = IntersectsFilter::new("location", Geometry::point(0.0, 0.0));
        let same = Geometry::point(0.0, 0.0);
        assert!(filter.matches_geometry(&same));
    }

    #[test]
    fn test_intersects_filter_negative_coordinates() {
        let filter = IntersectsFilter::new("location", Geometry::point(-10.0, -20.0));
        let same = Geometry::point(-10.0, -20.0);
        assert!(filter.matches_geometry(&same));
    }

    #[test]
    fn test_intersects_filter_very_large_coordinates() {
        let filter = IntersectsFilter::new("location", Geometry::point(1_000_000.0, 2_000_000.0));
        let same = Geometry::point(1_000_000.0, 2_000_000.0);
        assert!(filter.matches_geometry(&same));
    }

    #[test]
    fn test_intersects_filter_tiny_coordinates() {
        let filter = IntersectsFilter::new("location", Geometry::point(0.00001, 0.00002));
        let same = Geometry::point(0.00001, 0.00002);
        assert!(filter.matches_geometry(&same));
    }

    #[test]
    fn test_within_filter_point_on_boundary() {
        let filter = WithinFilter::new("location", Geometry::circle(0.0, 0.0, 10.0));
        // Point exactly on the circle boundary
        let boundary = Geometry::point(10.0, 0.0);
        assert!(filter.matches_geometry(&boundary));
    }

    #[test]
    fn test_near_filter_zero_distance() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 0.0);
        // Only the exact center point should match
        let center = Geometry::point(0.0, 0.0);
        assert!(filter.matches_geometry(&center));
    }

    #[test]
    fn test_near_filter_very_large_distance() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 1_000_000.0);
        // Point should match even if very far
        let far = Geometry::point(1000.0, 1000.0);
        assert!(filter.matches_geometry(&far));
    }

    #[test]
    fn test_near_filter_envelope_at_edge() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 5.0);
        // Envelope just touching the search circle boundary
        let touching = Geometry::envelope(4.99, 0.0, 5.01, 0.01);
        assert!(filter.matches_geometry(&touching));
    }

    #[test]
    fn test_near_filter_large_envelope() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 5.0);
        // Large envelope that contains search point
        let large = Geometry::envelope(-100.0, -100.0, 100.0, 100.0);
        assert!(filter.matches_geometry(&large));
    }

    #[test]
    fn test_near_filter_envelope_partially_overlapping() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 5.0);
        // Envelope partially overlapping search circle
        let partial = Geometry::envelope(3.0, 3.0, 7.0, 7.0);
        assert!(filter.matches_geometry(&partial));
    }

    #[test]
    fn test_geo_near_filter_zero_distance() {
        let filter = GeoNearFilter::from_coords("location", 45.0, -93.0, 0.0).unwrap();
        // Only point at exact location should match
        let exact = Geometry::point(-93.0, 45.0);
        assert!(filter.matches_geometry(&exact));
    }

    #[test]
    fn test_geo_near_filter_very_large_distance() {
        let filter = GeoNearFilter::from_coords("location", 0.0, 0.0, 5_000_000.0).unwrap(); // 5,000 km
        // Should match points within this very large radius
        let nearby = Geometry::point(10.0, 10.0); // ~1,500km away
        assert!(filter.matches_geometry(&nearby));
    }

    #[test]
    fn test_knearest_filter_max_distance_zero() {
        let filter = KNearestFilter::with_max_distance(
            "location",
            Point::new(0.0, 0.0),
            5,
            0.0,
        ).unwrap();
        // Only exact point should match
        let exact = Geometry::point(0.0, 0.0);
        assert!(filter.matches_geometry(&exact));
    }

    #[test]
    fn test_knearest_filter_without_max_distance() {
        let filter = KNearestFilter::from_coords("location", 0.0, 0.0, 5).unwrap();
        // Any geometry should match when no max_distance
        let any_point = Geometry::point(1000.0, 1000.0);
        assert!(filter.matches_geometry(&any_point));
    }

    #[test]
    fn test_near_filter_circle_at_boundary() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 10.0);
        // Circle at the boundary of search circle
        let at_boundary = Geometry::circle(10.0, 0.0, 0.1);
        assert!(filter.matches_geometry(&at_boundary));
    }

    #[test]
    fn test_intersects_filter_very_small_circle() {
        let filter = IntersectsFilter::new("location", Geometry::circle(0.0, 0.0, 0.001));
        // Very close point should intersect
        let close = Geometry::point(0.0005, 0.0005);
        assert!(filter.matches_geometry(&close));
    }

    #[test]
    fn test_within_filter_very_small_envelope() {
        let filter = WithinFilter::new("location", Geometry::envelope(0.0, 0.0, 0.001, 0.001));
        // Point at edge of tiny envelope
        let point = Geometry::point(0.0005, 0.0005);
        assert!(filter.matches_geometry(&point));
    }

    #[test]
    fn test_value_to_geometry_with_mixed_integer_float() {
        let mut map = BTreeMap::new();
        map.insert(Value::String("x".to_string()), Value::I32(10));
        map.insert(Value::String("y".to_string()), Value::F64(20.5));
        let value = Value::Map(map);
        
        let geom = value_to_geometry(&value);
        assert!(geom.is_some());
        if let Some(Geometry::Point(p)) = geom {
            assert_eq!(p.x(), 10.0);
            assert_eq!(p.y(), 20.5);
        } else {
            panic!("Expected Point geometry");
        }
    }

    #[test]
    fn test_value_to_geometry_with_u64() {
        let mut map = BTreeMap::new();
        map.insert(Value::String("x".to_string()), Value::U64(100));
        map.insert(Value::String("y".to_string()), Value::U64(200));
        let value = Value::Map(map);
        
        let geom = value_to_geometry(&value);
        assert!(geom.is_some());
        if let Some(Geometry::Point(p)) = geom {
            assert_eq!(p.x(), 100.0);
            assert_eq!(p.y(), 200.0);
        } else {
            panic!("Expected Point geometry");
        }
    }

    #[test]
    fn test_knearest_filter_very_large_k() {
        let filter = KNearestFilter::from_coords("location", 0.0, 0.0, 10000).unwrap();
        assert_eq!(filter.k(), 10000);
    }

    #[test]
    fn test_near_filter_matches_touching_circles() {
        let filter = NearFilter::from_coords("location", 0.0, 0.0, 5.0);
        // Circle just touching the search circle
        let touching = Geometry::circle(5.0, 0.0, 0.0);
        assert!(filter.matches_geometry(&touching));
    }

    #[test]
    fn test_intersects_filter_set_field_name() {
        let filter = IntersectsFilter::new("original", Geometry::point(0.0, 0.0));
        filter.set_field_name("updated".to_string()).unwrap();
        let field = filter.get_field_name().unwrap();
        assert_eq!(field, "original"); // OnceLock prevents change
    }

    #[test]
    fn test_knearest_filter_with_envelope() {
        let filter = KNearestFilter::from_coords("location", 0.0, 0.0, 5).unwrap();
        // Envelope containing center should match
        let envelope = Geometry::envelope(-10.0, -10.0, 10.0, 10.0);
        assert!(filter.matches_geometry(&envelope));
    }

    #[test]
    fn test_knearest_filter_with_circle() {
        let filter = KNearestFilter::from_coords("location", 0.0, 0.0, 5).unwrap();
        // Circle containing center should match
        let circle = Geometry::circle(0.0, 0.0, 10.0);
        assert!(filter.matches_geometry(&circle));
    }

    #[test]
    fn test_geo_near_filter_equator_query() {
        let filter = GeoNearFilter::from_coords("location", 0.0, 0.0, 100_000.0).unwrap(); // 100km
        // Points near equator in same area
        let nearby = Geometry::point(0.1, 0.0);
        assert!(filter.matches_geometry(&nearby));
    }

    #[test]
    fn test_geo_near_filter_far_apart_poles() {
        let filter = GeoNearFilter::from_coords("location", 89.0, 0.0, 1000.0).unwrap(); // 1km
        // Point at opposite pole should not match
        let opposite = Geometry::point(0.0, -89.0);
        assert!(!filter.matches_geometry(&opposite));
    }

    #[test]
    fn test_filter_as_any_returns_concrete_type() {
        let filter = IntersectsFilter::new("location", Geometry::point(0.0, 0.0));
        let as_any = filter.as_any();
        assert!(as_any.downcast_ref::<IntersectsFilter>().is_some());
    }

    #[test]
    fn test_knearest_filter_geometry_is_point() {
        let filter = KNearestFilter::from_coords("location", 5.0, 10.0, 5).unwrap();
        let geom = filter.geometry();
        
        if let Geometry::Point(p) = geom {
            assert_eq!(p.x(), 5.0);
            assert_eq!(p.y(), 10.0);
        } else {
            panic!("Expected Point geometry");
        }
    }

}
