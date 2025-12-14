//! Geometry types for spatial indexing.
//!
//! This module provides geometry types that can be stored and queried
//! in Nitrite's spatial index. It supports:
//! - Points (2D coordinates)
//! - GeoPoints (geographic coordinates with validation)
//! - Polygons and other shapes via WKT parsing
//!
//! ## Design Philosophy
//!
//! Unlike the Java implementation which uses JTS for all geometry operations,
//! the Rust implementation uses lightweight custom types optimized for the
//! common use cases in Nitrite (points and bounding box queries).

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::sync::Arc;

use crate::bounding_box::BoundingBox;
use crate::SpatialError;

/// A 2D coordinate (x, y).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Coordinate {
    pub x: f64,
    pub y: f64,
}

impl Coordinate {
    /// Creates a new coordinate.
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Calculates the Euclidean distance to another coordinate.
    pub fn distance(&self, other: &Coordinate) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }
}

impl Display for Coordinate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.x, self.y)
    }
}

/// A 2D point geometry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    coordinate: Coordinate,
}

impl Point {
    /// Creates a new point at the given coordinates.
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            coordinate: Coordinate::new(x, y),
        }
    }

    /// Creates a point from a coordinate.
    pub fn from_coordinate(coord: Coordinate) -> Self {
        Self { coordinate: coord }
    }

    /// Gets the x coordinate.
    pub fn x(&self) -> f64 {
        self.coordinate.x
    }

    /// Gets the y coordinate.
    pub fn y(&self) -> f64 {
        self.coordinate.y
    }

    /// Gets the coordinate.
    pub fn coordinate(&self) -> &Coordinate {
        &self.coordinate
    }
}

impl Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POINT({} {})", self.coordinate.x, self.coordinate.y)
    }
}

/// A geographic point with validated latitude and longitude coordinates.
///
/// This type provides explicit type safety for geographic coordinates,
/// validating that:
/// - Latitude is between -90 and 90 degrees
/// - Longitude is between -180 and 180 degrees
///
/// ## Coordinate Order
///
/// The constructor takes `(latitude, longitude)` which is the natural
/// order for geographic coordinates, unlike the (x, y) convention used
/// in Cartesian systems where x=longitude, y=latitude.
///
/// ## Example
///
/// ```rust
/// use nitrite_spatial::GeoPoint;
///
/// // Create a point for Minneapolis
/// let minneapolis = GeoPoint::new(45.0, -93.265).unwrap();
/// assert_eq!(minneapolis.latitude(), 45.0);
/// assert_eq!(minneapolis.longitude(), -93.265);
/// ```
#[derive(Debug, Clone)]
pub struct GeoPoint {
    inner: Arc<GeoPointInner>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GeoPointInner {
    latitude: f64,
    longitude: f64,
}

impl PartialEq for GeoPoint {
    fn eq(&self, other: &Self) -> bool {
        self.inner.latitude == other.inner.latitude
            && self.inner.longitude == other.inner.longitude
    }
}

impl Serialize for GeoPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GeoPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = GeoPointInner::deserialize(deserializer)?;
        Ok(GeoPoint {
            inner: Arc::new(inner),
        })
    }
}

impl GeoPoint {
    /// Creates a new GeoPoint with validated geographic coordinates.
    ///
    /// # Arguments
    /// * `latitude` - Latitude in degrees (-90 to 90)
    /// * `longitude` - Longitude in degrees (-180 to 180)
    ///
    /// # Errors
    /// Returns an error if coordinates are out of valid range.
    pub fn new(latitude: f64, longitude: f64) -> Result<Self, SpatialError> {
        Self::validate_coordinates(latitude, longitude)?;
        Ok(Self {
            inner: Arc::new(GeoPointInner { latitude, longitude }),
        })
    }

    /// Creates a GeoPoint from a Coordinate.
    /// The coordinate's x value is treated as longitude, y as latitude.
    pub fn from_coordinate(coord: &Coordinate) -> Result<Self, SpatialError> {
        Self::new(coord.y, coord.x)
    }

    fn validate_coordinates(latitude: f64, longitude: f64) -> Result<(), SpatialError> {
        if !(-90.0..=90.0).contains(&latitude) {
            return Err(SpatialError::InvalidOperation(format!(
                "Latitude must be between -90 and 90 degrees, got: {}",
                latitude
            )));
        }
        if !(-180.0..=180.0).contains(&longitude) {
            return Err(SpatialError::InvalidOperation(format!(
                "Longitude must be between -180 and 180 degrees, got: {}",
                longitude
            )));
        }
        Ok(())
    }

    /// Gets the latitude in degrees.
    pub fn latitude(&self) -> f64 {
        self.inner.latitude
    }

    /// Gets the longitude in degrees.
    pub fn longitude(&self) -> f64 {
        self.inner.longitude
    }

    /// Converts to a Coordinate (x=longitude, y=latitude).
    pub fn to_coordinate(&self) -> Coordinate {
        Coordinate::new(self.inner.longitude, self.inner.latitude)
    }

    /// Converts to a Point.
    pub fn to_point(&self) -> Point {
        Point::new(self.inner.longitude, self.inner.latitude)
    }

    /// Calculates the geodesic distance to another GeoPoint in meters.
    /// Uses the Haversine formula which is accurate for most purposes.
    pub fn distance_meters(&self, other: &GeoPoint) -> f64 {
        haversine_distance(
            self.inner.latitude,
            self.inner.longitude,
            other.inner.latitude,
            other.inner.longitude,
        )
    }
}

impl Display for GeoPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GeoPoint(lat={:.6}, lon={:.6})",
            self.inner.latitude, self.inner.longitude
        )
    }
}

/// Earth's mean radius in meters (WGS84)
const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

/// Calculates the great-circle distance between two points using the Haversine formula.
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_METERS * c
}

/// Represents a geometry that can be spatially indexed.
///
/// This enum supports various geometry types for spatial queries.
/// The most common type is `Point` for location-based queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Geometry {
    /// A single point.
    Point(Point),
    /// A circle defined by center and radius (for query purposes).
    Circle { center: Point, radius: f64 },
    /// A polygon defined by its exterior ring coordinates.
    Polygon(Vec<Coordinate>),
    /// A bounding box (rectangle).
    Envelope(BoundingBox),
}

impl Geometry {
    /// Creates a point geometry.
    pub fn point(x: f64, y: f64) -> Self {
        Geometry::Point(Point::new(x, y))
    }

    /// Creates a circle geometry.
    pub fn circle(center_x: f64, center_y: f64, radius: f64) -> Self {
        Geometry::Circle {
            center: Point::new(center_x, center_y),
            radius,
        }
    }

    /// Creates a polygon from coordinates.
    pub fn polygon(coords: Vec<Coordinate>) -> Self {
        Geometry::Polygon(coords)
    }

    /// Creates an envelope (bounding box) geometry.
    pub fn envelope(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Geometry::Envelope(BoundingBox::new(min_x, min_y, max_x, max_y))
    }

    /// Gets the bounding box of this geometry.
    pub fn bounding_box(&self) -> BoundingBox {
        match self {
            Geometry::Point(p) => BoundingBox::new(p.x(), p.y(), p.x(), p.y()),
            Geometry::Circle { center, radius } => BoundingBox::new(
                center.x() - radius,
                center.y() - radius,
                center.x() + radius,
                center.y() + radius,
            ),
            Geometry::Polygon(coords) => {
                if coords.is_empty() {
                    return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
                }
                let mut min_x = f64::MAX;
                let mut min_y = f64::MAX;
                let mut max_x = f64::MIN;
                let mut max_y = f64::MIN;
                for c in coords {
                    min_x = min_x.min(c.x);
                    min_y = min_y.min(c.y);
                    max_x = max_x.max(c.x);
                    max_y = max_y.max(c.y);
                }
                BoundingBox::new(min_x, min_y, max_x, max_y)
            }
            Geometry::Envelope(bbox) => bbox.clone(),
        }
    }

    /// Checks if this geometry intersects with another geometry.
    pub fn intersects(&self, other: &Geometry) -> bool {
        // Use bounding box intersection as a first pass
        let bbox1 = self.bounding_box();
        let bbox2 = other.bounding_box();
        
        if !bboxes_intersect(&bbox1, &bbox2) {
            return false;
        }

        // Refine based on actual geometry types
        match (self, other) {
            (Geometry::Point(p1), Geometry::Point(p2)) => {
                p1.x() == p2.x() && p1.y() == p2.y()
            }
            (Geometry::Point(p), Geometry::Circle { center, radius })
            | (Geometry::Circle { center, radius }, Geometry::Point(p)) => {
                let dist = center.coordinate().distance(p.coordinate());
                dist <= *radius
            }
            (Geometry::Circle { center: c1, radius: r1 }, Geometry::Circle { center: c2, radius: r2 }) => {
                let dist = c1.coordinate().distance(c2.coordinate());
                dist <= r1 + r2
            }
            (Geometry::Point(p), Geometry::Envelope(bbox))
            | (Geometry::Envelope(bbox), Geometry::Point(p)) => {
                p.x() >= bbox.min_x && p.x() <= bbox.max_x
                    && p.y() >= bbox.min_y && p.y() <= bbox.max_y
            }
            (Geometry::Point(p), Geometry::Polygon(coords))
            | (Geometry::Polygon(coords), Geometry::Point(p)) => {
                point_in_polygon(p.coordinate(), coords)
            }
            (Geometry::Circle { center, radius }, Geometry::Envelope(bbox))
            | (Geometry::Envelope(bbox), Geometry::Circle { center, radius }) => {
                // Check if circle intersects rectangle
                let closest_x = center.x().clamp(bbox.min_x, bbox.max_x);
                let closest_y = center.y().clamp(bbox.min_y, bbox.max_y);
                let dist = center.coordinate().distance(&Coordinate::new(closest_x, closest_y));
                dist <= *radius
            }
            _ => {
                // For other combinations, fall back to bounding box intersection
                true
            }
        }
    }

    /// Checks if this geometry contains another geometry.
    pub fn contains(&self, other: &Geometry) -> bool {
        match (self, other) {
            (Geometry::Circle { center, radius }, Geometry::Point(p)) => {
                let dist = center.coordinate().distance(p.coordinate());
                dist <= *radius
            }
            (Geometry::Envelope(bbox), Geometry::Point(p)) => {
                p.x() >= bbox.min_x && p.x() <= bbox.max_x
                    && p.y() >= bbox.min_y && p.y() <= bbox.max_y
            }
            (Geometry::Polygon(coords), Geometry::Point(p)) => {
                point_in_polygon(p.coordinate(), coords)
            }
            (Geometry::Circle { center: c1, radius: r1 }, Geometry::Circle { center: c2, radius: r2 }) => {
                let dist = c1.coordinate().distance(c2.coordinate());
                dist + r2 <= *r1
            }
            (Geometry::Envelope(outer), Geometry::Envelope(inner)) => {
                inner.min_x >= outer.min_x && inner.max_x <= outer.max_x
                    && inner.min_y >= outer.min_y && inner.max_y <= outer.max_y
            }
            _ => {
                // For other combinations, check if bbox of other is contained in bbox of self
                let bbox_self = self.bounding_box();
                let bbox_other = other.bounding_box();
                bbox_other.min_x >= bbox_self.min_x && bbox_other.max_x <= bbox_self.max_x
                    && bbox_other.min_y >= bbox_self.min_y && bbox_other.max_y <= bbox_self.max_y
            }
        }
    }
}

impl Display for Geometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Geometry::Point(p) => write!(f, "{}", p),
            Geometry::Circle { center, radius } => {
                write!(f, "CIRCLE({} {}, {})", center.x(), center.y(), radius)
            }
            Geometry::Polygon(coords) => {
                write!(f, "POLYGON((")?;
                for (i, c) in coords.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", c.x, c.y)?;
                }
                write!(f, "))")
            }
            Geometry::Envelope(bbox) => {
                write!(f, "ENVELOPE({}, {}, {}, {})", bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y)
            }
        }
    }
}

/// Check if two bounding boxes intersect.
fn bboxes_intersect(a: &BoundingBox, b: &BoundingBox) -> bool {
    a.min_x <= b.max_x && a.max_x >= b.min_x && a.min_y <= b.max_y && a.max_y >= b.min_y
}

/// Ray casting algorithm to determine if a point is inside a polygon.
fn point_in_polygon(point: &Coordinate, polygon: &[Coordinate]) -> bool {
    if polygon.len() < 3 {
        return false;
    }

    let mut inside = false;
    let n = polygon.len();
    let mut j = n - 1;

    for i in 0..n {
        let xi = polygon[i].x;
        let yi = polygon[i].y;
        let xj = polygon[j].x;
        let yj = polygon[j].y;

        if ((yi > point.y) != (yj > point.y))
            && (point.x < (xj - xi) * (point.y - yi) / (yj - yi) + xi)
        {
            inside = !inside;
        }
        j = i;
    }

    inside
}

/// Converts meters to approximate degrees at a given latitude.
/// 
/// This is used for creating circular search regions in geographic coordinates.
/// The conversion accounts for the fact that longitude degrees vary with latitude.
pub fn meters_to_degrees(meters: f64, latitude: f64) -> f64 {
    // At the equator, 1 degree of latitude ≈ 111,320 meters
    // 1 degree of longitude varies with latitude: cos(lat) * 111,320 meters
    let meters_per_degree_lat = 111_320.0;
    let meters_per_degree_lon = 111_320.0 * latitude.to_radians().cos();
    
    // Use the average of lat and lon conversion for a circle
    // This is approximate but good enough for search bounding boxes
    let avg_meters_per_degree = (meters_per_degree_lat + meters_per_degree_lon) / 2.0;
    
    if avg_meters_per_degree > 0.0 {
        meters / avg_meters_per_degree
    } else {
        // At the poles, use latitude conversion only
        meters / meters_per_degree_lat
    }
}

/// Creates a geodesic circle (approximated as a polygon) around a geographic point.
///
/// This function creates a circular search region in geographic coordinates,
/// accounting for Earth's curvature.
///
/// # Arguments
/// * `center` - The center point (longitude, latitude)
/// * `radius_meters` - The radius in meters
/// * `_num_points` - Number of points to use for the circle approximation (reserved for future use)
pub fn create_geodesic_circle(center: &GeoPoint, radius_meters: f64, _num_points: usize) -> Geometry {
    let radius_degrees = meters_to_degrees(radius_meters, center.latitude());
    
    // For query purposes, we use a bounding circle in degrees
    // The actual containment check will use precise distance calculations
    Geometry::Circle {
        center: center.to_point(),
        radius: radius_degrees,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinate() {
        let c1 = Coordinate::new(0.0, 0.0);
        let c2 = Coordinate::new(3.0, 4.0);
        assert!((c1.distance(&c2) - 5.0).abs() < 0.0001);
    }

    #[test]
    fn test_point() {
        let p = Point::new(10.0, 20.0);
        assert_eq!(p.x(), 10.0);
        assert_eq!(p.y(), 20.0);
        assert_eq!(format!("{}", p), "POINT(10 20)");
    }

    #[test]
    fn test_geopoint_valid() {
        let gp = GeoPoint::new(45.0, -93.265).unwrap();
        assert_eq!(gp.latitude(), 45.0);
        assert_eq!(gp.longitude(), -93.265);
    }

    #[test]
    fn test_geopoint_invalid_latitude() {
        let result = GeoPoint::new(91.0, 0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_geopoint_invalid_longitude() {
        let result = GeoPoint::new(0.0, 181.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_geopoint_distance() {
        // Distance from New York to Los Angeles is approximately 3,940 km
        let nyc = GeoPoint::new(40.7128, -74.0060).unwrap();
        let la = GeoPoint::new(34.0522, -118.2437).unwrap();
        let distance = nyc.distance_meters(&la);
        
        // Should be approximately 3,940 km (allow 5% error for Haversine approximation)
        assert!(distance > 3_700_000.0 && distance < 4_200_000.0);
    }

    #[test]
    fn test_geometry_bounding_box() {
        let point = Geometry::point(10.0, 20.0);
        let bbox = point.bounding_box();
        assert_eq!(bbox.min_x, 10.0);
        assert_eq!(bbox.max_x, 10.0);
        
        let circle = Geometry::circle(0.0, 0.0, 5.0);
        let bbox = circle.bounding_box();
        assert_eq!(bbox.min_x, -5.0);
        assert_eq!(bbox.max_x, 5.0);
    }

    #[test]
    fn test_geometry_intersects() {
        let point = Geometry::point(5.0, 5.0);
        let circle = Geometry::circle(0.0, 0.0, 10.0);
        assert!(circle.intersects(&point));
        
        let far_point = Geometry::point(100.0, 100.0);
        assert!(!circle.intersects(&far_point));
    }

    #[test]
    fn test_geometry_contains() {
        let circle = Geometry::circle(0.0, 0.0, 10.0);
        let point = Geometry::point(3.0, 4.0); // distance = 5, inside circle
        assert!(circle.contains(&point));
        
        let far_point = Geometry::point(8.0, 8.0); // distance ≈ 11.3, outside
        assert!(!circle.contains(&far_point));
    }

    #[test]
    fn test_point_in_polygon() {
        let polygon = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
        ];
        
        let inside = Coordinate::new(5.0, 5.0);
        let outside = Coordinate::new(15.0, 5.0);
        
        assert!(point_in_polygon(&inside, &polygon));
        assert!(!point_in_polygon(&outside, &polygon));
    }

    #[test]
    fn test_meters_to_degrees() {
        // At the equator, 111,320 meters ≈ 1 degree
        let deg = meters_to_degrees(111_320.0, 0.0);
        assert!((deg - 1.0).abs() < 0.1);
    }
}

// ADDITIONAL TESTS FOR COVERAGE
#[cfg(test)]
mod tests_additional {
    use super::*;

    #[test]
    fn test_geopoint_from_coordinate() {
        let coord = Coordinate::new(-93.265, 45.0);
        let gp = GeoPoint::from_coordinate(&coord).unwrap();
        assert_eq!(gp.latitude(), 45.0);
    }

    #[test]
    fn test_point_from_coordinate() {
        let coord = Coordinate::new(10.0, 20.0);
        let pt = Point::from_coordinate(coord);
        assert_eq!(pt.x(), 10.0);
    }

    #[test]
    fn test_geometry_polygon_bbox() {
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
        ];
        let polygon = Geometry::polygon(coords);
        let bbox = polygon.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
    }

    #[test]
    fn test_geometry_empty_polygon() {
        let polygon = Geometry::polygon(vec![]);
        let bbox = polygon.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
    }

    #[test]
    fn test_geometry_intersects_circle_envelope() {
        let circle = Geometry::circle(5.0, 5.0, 3.0);
        let envelope = Geometry::envelope(0.0, 0.0, 10.0, 10.0);
        assert!(circle.intersects(&envelope));
    }

    #[test]
    fn test_coordinate_equality() {
        let c1 = Coordinate::new(10.0, 20.0);
        let c2 = Coordinate::new(10.0, 20.0);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_haversine_same_point() {
        let pt1 = GeoPoint::new(45.0, -93.265).unwrap();
        let pt2 = GeoPoint::new(45.0, -93.265).unwrap();
        let distance = pt1.distance_meters(&pt2);
        assert!(distance < 1.0);
    }

    #[test]
    fn test_coordinate_distance_zero() {
        let c1 = Coordinate::new(5.0, 10.0);
        let distance = c1.distance(&c1);
        assert!(distance < 0.0001);
    }
}
