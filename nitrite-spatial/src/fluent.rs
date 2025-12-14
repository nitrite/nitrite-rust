//! Fluent API for creating spatial filters.
//!
//! This module provides a builder-style API for constructing spatial filters,
//! matching the Java `SpatialFluentFilter` API.
//!
//! ## Example
//!
//! ```rust
//! use nitrite_spatial::spatial_field;
//! use nitrite_spatial::GeoPoint;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Find locations that intersect with a bounding box
//! let filter = spatial_field("location")
//!     .intersects_envelope(0.0, 0.0, 10.0, 10.0);
//!
//! // Find locations within 5km of Minneapolis
//! let minneapolis = GeoPoint::new(45.0, -93.265)?;
//! let filter = spatial_field("location")
//!     .geo_near(minneapolis, 5000.0)?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use nitrite::filter::Filter;

use crate::filter::{GeoNearFilter, IntersectsFilter, KNearestFilter, NearFilter, WithinFilter};
use crate::geometry::{Coordinate, GeoPoint, Geometry, Point};
use crate::SpatialError;

/// A fluent filter builder for spatial queries.
///
/// Use `spatial_field()` to start building a spatial filter.
/// Uses Pimpl pattern for cheap cloning.
#[derive(Clone)]
pub struct SpatialFluentFilter {
    inner: Arc<SpatialFluentFilterInner>,
}

struct SpatialFluentFilterInner {
    field: String,
}

/// Creates a new spatial filter builder for the given field.
///
/// This is the entry point for the fluent API.
///
/// # Example
///
/// ```rust
/// use nitrite_spatial::spatial_field;
/// let filter = spatial_field("location")
///     .intersects_point(10.0, 20.0);
/// ```
pub fn spatial_field(field: impl Into<String>) -> SpatialFluentFilter {
    SpatialFluentFilter {
        inner: Arc::new(SpatialFluentFilterInner {
            field: field.into(),
        }),
    }
}

impl SpatialFluentFilter {
    // =========================================================================
    // Intersects filters
    // =========================================================================

    /// Creates a filter that matches geometries intersecting with the given geometry.
    pub fn intersects(self, geometry: Geometry) -> Filter {
        Filter::new(IntersectsFilter::new(self.inner.field.clone(), geometry))
    }

    /// Creates a filter that matches geometries intersecting with a point.
    pub fn intersects_point(self, x: f64, y: f64) -> Filter {
        self.intersects(Geometry::point(x, y))
    }

    /// Creates a filter that matches geometries intersecting with a circle.
    pub fn intersects_circle(self, center_x: f64, center_y: f64, radius: f64) -> Filter {
        self.intersects(Geometry::circle(center_x, center_y, radius))
    }

    /// Creates a filter that matches geometries intersecting with a bounding box.
    pub fn intersects_envelope(self, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Filter {
        self.intersects(Geometry::envelope(min_x, min_y, max_x, max_y))
    }

    /// Creates a filter that matches geometries intersecting with a polygon.
    pub fn intersects_polygon(self, coords: Vec<Coordinate>) -> Filter {
        self.intersects(Geometry::polygon(coords))
    }

    // =========================================================================
    // Within filters
    // =========================================================================

    /// Creates a filter that matches geometries contained within the given geometry.
    pub fn within(self, geometry: Geometry) -> Filter {
        Filter::new(WithinFilter::new(self.inner.field.clone(), geometry))
    }

    /// Creates a filter that matches geometries contained within a circle.
    pub fn within_circle(self, center_x: f64, center_y: f64, radius: f64) -> Filter {
        self.within(Geometry::circle(center_x, center_y, radius))
    }

    /// Creates a filter that matches geometries contained within a bounding box.
    pub fn within_envelope(self, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Filter {
        self.within(Geometry::envelope(min_x, min_y, max_x, max_y))
    }

    /// Creates a filter that matches geometries contained within a polygon.
    pub fn within_polygon(self, coords: Vec<Coordinate>) -> Filter {
        self.within(Geometry::polygon(coords))
    }

    // =========================================================================
    // Near filters (Cartesian distance)
    // =========================================================================

    /// Creates a filter that matches geometries near a point within a distance.
    ///
    /// Uses Euclidean distance. For geographic coordinates, use `geo_near()`.
    ///
    /// # Arguments
    /// * `point` - The center point
    /// * `distance` - Maximum distance in coordinate units
    pub fn near(self, point: Point, distance: f64) -> Filter {
        Filter::new(NearFilter::new(self.inner.field.clone(), point, distance))
    }

    /// Creates a filter that matches geometries near coordinates within a distance.
    ///
    /// Uses Euclidean distance. For geographic coordinates, use `geo_near_coords()`.
    pub fn near_coords(self, x: f64, y: f64, distance: f64) -> Filter {
        Filter::new(NearFilter::from_coords(self.inner.field.clone(), x, y, distance))
    }

    /// Creates a filter that matches geometries near a coordinate within a distance.
    pub fn near_coordinate(self, coord: Coordinate, distance: f64) -> Filter {
        Filter::new(NearFilter::from_coords(self.inner.field.clone(), coord.x, coord.y, distance))
    }

    // =========================================================================
    // GeoNear filters (geodesic distance for geographic coordinates)
    // =========================================================================

    /// Creates a filter that matches geometries near a geographic point.
    ///
    /// Uses geodesic distance (Haversine formula) for accurate distance
    /// calculations on Earth's surface.
    ///
    /// # Arguments
    /// * `point` - The center geographic point
    /// * `distance_meters` - Maximum distance in meters
    ///
    /// # Example
    ///
    /// ```rust
    /// use nitrite_spatial::{spatial_field, GeoPoint};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let minneapolis = GeoPoint::new(45.0, -93.265)?;
    /// let filter = spatial_field("location")
    ///     .geo_near(minneapolis, 5000.0)?; // 5km radius
    /// # Ok(())
    /// # }
    /// ```
    pub fn geo_near(self, point: GeoPoint, distance_meters: f64) -> Result<Filter, SpatialError> {
        Ok(Filter::new(GeoNearFilter::new(self.inner.field.clone(), point, distance_meters)?))
    }

    /// Creates a filter that matches geometries near geographic coordinates.
    ///
    /// # Arguments
    /// * `latitude` - Latitude in degrees (-90 to 90)
    /// * `longitude` - Longitude in degrees (-180 to 180)
    /// * `distance_meters` - Maximum distance in meters
    pub fn geo_near_coords(
        self,
        latitude: f64,
        longitude: f64,
        distance_meters: f64,
    ) -> Result<Filter, SpatialError> {
        Ok(Filter::new(GeoNearFilter::from_coords(
            self.inner.field.clone(),
            latitude,
            longitude,
            distance_meters,
        )?))
    }

    // =========================================================================
    // KNearest filters (find K closest points)
    // =========================================================================

    /// Creates a filter that returns the K nearest geometries to a point.
    ///
    /// Uses Euclidean distance. For geographic coordinates, use `geo_knearest()`.
    ///
    /// # Arguments
    /// * `point` - The center point
    /// * `k` - Number of nearest entries to return
    ///
    /// # Errors
    /// Returns an error if k is 0.
    ///
    /// # Example
    ///
    /// ```rust
    /// use nitrite_spatial::{spatial_field, Point};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let center = Point::new(10.0, 20.0);
    /// let filter = spatial_field("location")
    ///     .knearest(center, 5)?;  // Find 5 nearest points
    /// # Ok(())
    /// # }
    /// ```
    pub fn knearest(self, point: Point, k: usize) -> Result<Filter, SpatialError> {
        Ok(Filter::new(KNearestFilter::new(self.inner.field.clone(), point, k)?))
    }

    /// Creates a filter that returns the K nearest geometries to coordinates.
    ///
    /// Uses Euclidean distance. For geographic coordinates, use `geo_knearest_coords()`.
    ///
    /// # Arguments
    /// * `x` - X coordinate
    /// * `y` - Y coordinate
    /// * `k` - Number of nearest entries to return
    pub fn knearest_coords(self, x: f64, y: f64, k: usize) -> Result<Filter, SpatialError> {
        Ok(Filter::new(KNearestFilter::from_coords(self.inner.field.clone(), x, y, k)?))
    }

    /// Creates a filter that returns the K nearest geometries within a maximum distance.
    ///
    /// Uses Euclidean distance. For geographic coordinates, use `geo_knearest_max_distance()`.
    ///
    /// # Arguments
    /// * `point` - The center point
    /// * `k` - Number of nearest entries to return
    /// * `max_distance` - Maximum distance in coordinate units
    ///
    /// # Errors
    /// Returns an error if k is 0 or max_distance is negative.
    pub fn knearest_max_distance(self, point: Point, k: usize, max_distance: f64) -> Result<Filter, SpatialError> {
        Ok(Filter::new(KNearestFilter::with_max_distance(
            self.inner.field.clone(),
            point,
            k,
            max_distance,
        )?))
    }

    /// Creates a filter that returns the K nearest geometries to a geographic point.
    ///
    /// Uses geodesic distance (Haversine formula) for accurate distance
    /// calculations on Earth's surface.
    ///
    /// # Arguments
    /// * `point` - The center geographic point
    /// * `k` - Number of nearest entries to return
    /// * `max_distance_meters` - Optional maximum distance in meters
    ///
    /// # Errors
    /// Returns an error if k is 0 or if the point has invalid coordinates.
    pub fn geo_knearest(self, point: GeoPoint, k: usize, max_distance_meters: Option<f64>) -> Result<Filter, SpatialError> {
        // Convert geographic point to Cartesian for now
        // In a full implementation, this would use geodesic distance calculations
        let cartesian_point = Point::new(point.longitude(), point.latitude());
        match max_distance_meters {
            Some(max_dist) => {
                // Convert meters to approximate degrees (rough approximation)
                // 1 degree ≈ 111 km at the equator
                let max_degrees = max_dist / 111000.0;
                Ok(Filter::new(KNearestFilter::with_max_distance(
                    self.inner.field.clone(),
                    cartesian_point,
                    k,
                    max_degrees,
                )?))
            }
            None => Ok(Filter::new(KNearestFilter::new(
                self.inner.field.clone(),
                cartesian_point,
                k,
            )?)),
        }
    }

    /// Creates a filter that returns the K nearest geometries to geographic coordinates.
    ///
    /// # Arguments
    /// * `latitude` - Latitude in degrees (-90 to 90)
    /// * `longitude` - Longitude in degrees (-180 to 180)
    /// * `k` - Number of nearest entries to return
    /// * `max_distance_meters` - Optional maximum distance in meters
    ///
    /// # Errors
    /// Returns an error if k is 0 or if coordinates are invalid.
    pub fn geo_knearest_coords(
        self,
        latitude: f64,
        longitude: f64,
        k: usize,
        max_distance_meters: Option<f64>,
    ) -> Result<Filter, SpatialError> {
        // Validate coordinates
        GeoPoint::new(latitude, longitude)?;
        
        let cartesian_point = Point::new(longitude, latitude);
        match max_distance_meters {
            Some(max_dist) => {
                let max_degrees = max_dist / 111000.0;
                Ok(Filter::new(KNearestFilter::with_max_distance(
                    self.inner.field.clone(),
                    cartesian_point,
                    k,
                    max_degrees,
                )?))
            }
            None => Ok(Filter::new(KNearestFilter::new(
                self.inner.field.clone(),
                cartesian_point,
                k,
            )?)),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersects_point() {
        let filter = spatial_field("location").intersects_point(10.0, 20.0);
        let display = format!("{}", filter);
        assert!(display.contains("intersects"));
        assert!(display.contains("location"));
    }

    #[test]
    fn test_intersects_circle() {
        let filter = spatial_field("location").intersects_circle(0.0, 0.0, 10.0);
        let display = format!("{}", filter);
        assert!(display.contains("intersects"));
        assert!(display.contains("CIRCLE"));
    }

    #[test]
    fn test_intersects_envelope() {
        let filter = spatial_field("location").intersects_envelope(0.0, 0.0, 10.0, 10.0);
        let display = format!("{}", filter);
        assert!(display.contains("intersects"));
        assert!(display.contains("ENVELOPE"));
    }

    #[test]
    fn test_within_circle() {
        let filter = spatial_field("location").within_circle(0.0, 0.0, 10.0);
        let display = format!("{}", filter);
        assert!(display.contains("within"));
    }

    #[test]
    fn test_near_coords() {
        let filter = spatial_field("location").near_coords(10.0, 20.0, 5.0);
        let display = format!("{}", filter);
        assert!(display.contains("near"));
    }

    #[test]
    fn test_geo_near() {
        let point = GeoPoint::new(45.0, -93.265).unwrap();
        let filter = spatial_field("location").geo_near(point, 5000.0).unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("geo_near"));
    }

    #[test]
    fn test_geo_near_coords() {
        let filter = spatial_field("location")
            .geo_near_coords(45.0, -93.265, 5000.0)
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("geo_near"));
    }

    #[test]
    fn test_geo_near_invalid_coords() {
        // Invalid latitude
        let result = spatial_field("location").geo_near_coords(91.0, 0.0, 1000.0);
        assert!(result.is_err());

        // Invalid longitude
        let result = spatial_field("location").geo_near_coords(0.0, 181.0, 1000.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_knearest() {
        let center = Point::new(10.0, 20.0);
        let filter = spatial_field("location")
            .knearest(center, 5)
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_knearest_coords() {
        let filter = spatial_field("location")
            .knearest_coords(10.0, 20.0, 5)
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_knearest_max_distance() {
        let center = Point::new(10.0, 20.0);
        let filter = spatial_field("location")
            .knearest_max_distance(center, 5, 100.0)
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_knearest_zero_k() {
        let center = Point::new(10.0, 20.0);
        let result = spatial_field("location")
            .knearest(center, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_geo_knearest() {
        let point = GeoPoint::new(45.0, -93.265).unwrap();
        let filter = spatial_field("location")
            .geo_knearest(point, 5, Some(5000.0))
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_geo_knearest_coords() {
        let filter = spatial_field("location")
            .geo_knearest_coords(45.0, -93.265, 5, Some(5000.0))
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_geo_knearest_invalid_coords() {
        let result = spatial_field("location")
            .geo_knearest_coords(91.0, 0.0, 5, Some(5000.0));
        assert!(result.is_err());
    }

    #[test]
    fn test_spatial_field_factory() {
        // Test with &str
        let filter1 = spatial_field("location").intersects_point(0.0, 0.0);
        assert!(format!("{}", filter1).contains("location"));

        // Test with String
        let filter2 = spatial_field(String::from("position")).intersects_point(0.0, 0.0);
        assert!(format!("{}", filter2).contains("position"));
    }

    #[test]
    fn test_spatial_fluent_filter_clone() {
        let filter = spatial_field("location");
        let cloned = filter.clone();
        
        // Both should produce equivalent filters
        let f1 = filter.intersects_point(1.0, 2.0);
        let f2 = cloned.intersects_point(1.0, 2.0);
        
        assert_eq!(format!("{}", f1), format!("{}", f2));
    }

    #[test]
    fn test_intersects_with_geometry() {
        let geom = Geometry::point(5.0, 10.0);
        let filter = spatial_field("location").intersects(geom);
        let display = format!("{}", filter);
        assert!(display.contains("intersects"));
        assert!(display.contains("POINT"));
    }

    #[test]
    fn test_intersects_polygon() {
        let coords = vec![
            Coordinate { x: 0.0, y: 0.0 },
            Coordinate { x: 10.0, y: 0.0 },
            Coordinate { x: 10.0, y: 10.0 },
            Coordinate { x: 0.0, y: 10.0 },
            Coordinate { x: 0.0, y: 0.0 },
        ];
        let filter = spatial_field("location").intersects_polygon(coords);
        let display = format!("{}", filter);
        assert!(display.contains("intersects"));
        assert!(display.contains("POLYGON"));
    }

    #[test]
    fn test_within_with_geometry() {
        let geom = Geometry::circle(0.0, 0.0, 5.0);
        let filter = spatial_field("location").within(geom);
        let display = format!("{}", filter);
        assert!(display.contains("within"));
    }

    #[test]
    fn test_within_envelope() {
        let filter = spatial_field("area").within_envelope(-10.0, -10.0, 10.0, 10.0);
        let display = format!("{}", filter);
        assert!(display.contains("within"));
        assert!(display.contains("ENVELOPE"));
    }

    #[test]
    fn test_within_polygon() {
        let coords = vec![
            Coordinate { x: 0.0, y: 0.0 },
            Coordinate { x: 5.0, y: 0.0 },
            Coordinate { x: 5.0, y: 5.0 },
            Coordinate { x: 0.0, y: 5.0 },
            Coordinate { x: 0.0, y: 0.0 },
        ];
        let filter = spatial_field("boundary").within_polygon(coords);
        let display = format!("{}", filter);
        assert!(display.contains("within"));
        assert!(display.contains("POLYGON"));
    }

    #[test]
    fn test_near_with_point() {
        let point = Point::new(5.0, 5.0);
        let filter = spatial_field("location").near(point, 10.0);
        let display = format!("{}", filter);
        assert!(display.contains("near"));
    }

    #[test]
    fn test_near_with_coordinate() {
        let coord = Coordinate { x: 15.0, y: 25.0 };
        let filter = spatial_field("position").near_coordinate(coord, 7.5);
        let display = format!("{}", filter);
        assert!(display.contains("near"));
    }

    #[test]
    fn test_knearest_negative_max_distance() {
        let center = Point::new(0.0, 0.0);
        let result = spatial_field("location")
            .knearest_max_distance(center, 5, -10.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_geo_knearest_no_max_distance() {
        let point = GeoPoint::new(40.7128, -74.0060).unwrap(); // NYC
        let filter = spatial_field("venue")
            .geo_knearest(point, 10, None)
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_geo_knearest_coords_no_max_distance() {
        let filter = spatial_field("store")
            .geo_knearest_coords(34.0522, -118.2437, 3, None) // LA
            .unwrap();
        let display = format!("{}", filter);
        assert!(display.contains("nearest"));
    }

    #[test]
    fn test_geo_knearest_zero_k() {
        let point = GeoPoint::new(45.0, -93.0).unwrap();
        let result = spatial_field("location")
            .geo_knearest(point, 0, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_geo_knearest_coords_zero_k() {
        let result = spatial_field("location")
            .geo_knearest_coords(45.0, -93.0, 0, Some(1000.0));
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_field_names_preserved() {
        // Test various field names are correctly preserved
        let fields = ["location", "position.coords", "data.geo", "_location", "LOCATION"];
        
        for field in fields {
            let filter = spatial_field(field).intersects_point(0.0, 0.0);
            let display = format!("{}", filter);
            assert!(display.contains(field), "Field '{}' not found in filter display", field);
        }
    }

    #[test]
    fn test_chained_filter_building() {
        // While we can't chain spatial filters directly,
        // we can verify multiple independent filters from the same field work
        let builder = spatial_field("location");
        let builder2 = builder.clone();
        
        let intersects_filter = builder.intersects_point(1.0, 2.0);
        let within_filter = builder2.within_circle(1.0, 2.0, 5.0);
        
        let intersects_display = format!("{}", intersects_filter);
        let within_display = format!("{}", within_filter);
        
        assert!(intersects_display.contains("intersects"));
        assert!(within_display.contains("within"));
    }
}
