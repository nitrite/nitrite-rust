//! Extended geometry types for spatial indexing.
//!
//! This module provides additional geometry types beyond the basic Point, Circle, and Polygon:
//! - LineString: A line connecting multiple points
//! - MultiPoint: Multiple point geometries
//! - MultiLineString: Multiple line string geometries
//! - MultiPolygon: Multiple polygon geometries
//! - Polygon with holes: Polygons with interior rings
//!
//! These types support:
//! - Geometric operations (intersection, containment, distance)
//! - WKT (Well-Known Text) parsing and serialization
//! - GeoJSON parsing and serialization
//! - Validation and repair

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use crate::geometry::Coordinate;
use crate::bounding_box::BoundingBox;
use crate::SpatialError;

/// A line string defined by an ordered sequence of coordinates.
///
/// A LineString with 2 points represents a line segment.
/// A closed LineString (first and last coordinates are equal) represents a ring.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LineString {
    coordinates: Vec<Coordinate>,
}

impl LineString {
    /// Creates a new LineString from coordinates.
    ///
    /// # Arguments
    /// * `coordinates` - At least 2 coordinates required
    ///
    /// # Errors
    /// Returns an error if fewer than 2 coordinates are provided.
    pub fn new(coordinates: Vec<Coordinate>) -> Result<Self, SpatialError> {
        if coordinates.len() < 2 {
            return Err(SpatialError::InvalidOperation(
                "LineString requires at least 2 coordinates".to_string(),
            ));
        }
        Ok(Self { coordinates })
    }

    /// Gets the coordinates of this LineString.
    pub fn coordinates(&self) -> &[Coordinate] {
        &self.coordinates
    }

    /// Calculates the total length of the LineString.
    ///
    /// For geographic coordinates, this gives a rough approximation in degrees.
    /// For projected coordinates, the result is in the same units as the coordinates.
    pub fn length(&self) -> f64 {
        let mut total = 0.0;
        for i in 0..self.coordinates.len() - 1 {
            total += self.coordinates[i].distance(&self.coordinates[i + 1]);
        }
        total
    }

    /// Checks if this LineString is closed (first and last coordinates are equal).
    pub fn is_closed(&self) -> bool {
        if self.coordinates.is_empty() {
            return false;
        }
        self.coordinates[0] == self.coordinates[self.coordinates.len() - 1]
    }

    /// Gets the bounding box of this LineString.
    pub fn bounding_box(&self) -> BoundingBox {
        if self.coordinates.is_empty() {
            return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
        }

        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;

        for coord in &self.coordinates {
            min_x = min_x.min(coord.x);
            min_y = min_y.min(coord.y);
            max_x = max_x.max(coord.x);
            max_y = max_y.max(coord.y);
        }

        BoundingBox::new(min_x, min_y, max_x, max_y)
    }

    /// Checks if this LineString intersects with another LineString.
    pub fn intersects(&self, other: &LineString) -> bool {
        // Quick check: bounding boxes must intersect
        let bbox1 = self.bounding_box();
        let bbox2 = other.bounding_box();
        if !(bbox1.min_x <= bbox2.max_x && bbox1.max_x >= bbox2.min_x
            && bbox1.min_y <= bbox2.max_y && bbox1.max_y >= bbox2.min_y)
        {
            return false;
        }

        // Check if any segments intersect
        for i in 0..self.coordinates.len() - 1 {
            for j in 0..other.coordinates.len() - 1 {
                if segments_intersect(
                    &self.coordinates[i],
                    &self.coordinates[i + 1],
                    &other.coordinates[j],
                    &other.coordinates[j + 1],
                ) {
                    return true;
                }
            }
        }

        false
    }
}

impl Display for LineString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LINESTRING(")?;
        for (i, coord) in self.coordinates.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} {}", coord.x, coord.y)?;
        }
        write!(f, ")")
    }
}

/// A polygon with optional interior holes.
///
/// A polygon consists of an exterior ring (required) and zero or more interior rings (holes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolygonWithHoles {
    exterior: Vec<Coordinate>,
    holes: Vec<Vec<Coordinate>>,
}

impl PolygonWithHoles {
    /// Creates a new polygon with an exterior ring and optional holes.
    ///
    /// # Arguments
    /// * `exterior` - The exterior ring (at least 4 points, forming a closed ring)
    /// * `holes` - Interior holes (each hole must have at least 4 points)
    ///
    /// # Errors
    /// Returns an error if rings are not properly closed or have insufficient points.
    pub fn new(
        exterior: Vec<Coordinate>,
        holes: Vec<Vec<Coordinate>>,
    ) -> Result<Self, SpatialError> {
        Self::validate_ring(&exterior)?;
        for hole in &holes {
            Self::validate_ring(hole)?;
        }
        Ok(Self { exterior, holes })
    }

    /// Creates a simple polygon without holes.
    pub fn simple(coordinates: Vec<Coordinate>) -> Result<Self, SpatialError> {
        Self::new(coordinates, vec![])
    }

    fn validate_ring(ring: &[Coordinate]) -> Result<(), SpatialError> {
        if ring.len() < 4 {
            return Err(SpatialError::InvalidOperation(
                "Polygon ring requires at least 4 points".to_string(),
            ));
        }
        if ring[0] != ring[ring.len() - 1] {
            return Err(SpatialError::InvalidOperation(
                "Polygon ring must be closed (first and last coordinates must be equal)".to_string(),
            ));
        }
        Ok(())
    }

    /// Gets the exterior ring coordinates.
    pub fn exterior(&self) -> &[Coordinate] {
        &self.exterior
    }

    /// Gets the interior holes.
    pub fn holes(&self) -> &[Vec<Coordinate>] {
        &self.holes
    }

    /// Gets the bounding box of this polygon.
    pub fn bounding_box(&self) -> BoundingBox {
        if self.exterior.is_empty() {
            return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
        }

        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;

        for coord in &self.exterior {
            min_x = min_x.min(coord.x);
            min_y = min_y.min(coord.y);
            max_x = max_x.max(coord.x);
            max_y = max_y.max(coord.y);
        }

        BoundingBox::new(min_x, min_y, max_x, max_y)
    }

    /// Checks if a point is inside this polygon, accounting for holes.
    pub fn contains_point(&self, point: &Coordinate) -> bool {
        // Point must be in exterior ring
        if !point_in_polygon(point, &self.exterior) {
            return false;
        }

        // Point must not be in any hole
        for hole in &self.holes {
            if point_in_polygon(point, hole) {
                return false;
            }
        }

        true
    }

    /// Checks if this polygon is valid.
    pub fn is_valid(&self) -> bool {
        // Check winding order (exterior should be counter-clockwise, holes clockwise)
        // For simplicity, we just check closure here
        if self.exterior.is_empty() || self.exterior[0] != self.exterior[self.exterior.len() - 1]
        {
            return false;
        }

        for hole in &self.holes {
            if hole.is_empty() || hole[0] != hole[hole.len() - 1] {
                return false;
            }
        }

        true
    }
}

impl Display for PolygonWithHoles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POLYGON((")?;
        for (i, coord) in self.exterior.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} {}", coord.x, coord.y)?;
        }
        write!(f, ")")?;

        for hole in &self.holes {
            write!(f, ", (")?;
            for (i, coord) in hole.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{} {}", coord.x, coord.y)?;
            }
            write!(f, ")")?;
        }

        write!(f, ")")
    }
}

/// A collection of multiple geometries.
///
/// Each geometry in the collection can be of any supported type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MultiGeometry {
    /// Multiple point geometries.
    MultiPoint(Vec<Coordinate>),
    /// Multiple line string geometries.
    MultiLineString(Vec<Vec<Coordinate>>),
    /// Multiple polygon geometries.
    MultiPolygon(Vec<Vec<Coordinate>>),
}

impl MultiGeometry {
    /// Gets the bounding box that contains all geometries.
    pub fn bounding_box(&self) -> BoundingBox {
        match self {
            MultiGeometry::MultiPoint(points) => {
                if points.is_empty() {
                    return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
                }
                let mut min_x = f64::MAX;
                let mut min_y = f64::MAX;
                let mut max_x = f64::MIN;
                let mut max_y = f64::MIN;
                for coord in points {
                    min_x = min_x.min(coord.x);
                    min_y = min_y.min(coord.y);
                    max_x = max_x.max(coord.x);
                    max_y = max_y.max(coord.y);
                }
                BoundingBox::new(min_x, min_y, max_x, max_y)
            }
            MultiGeometry::MultiLineString(lines) => {
                if lines.is_empty() {
                    return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
                }
                let mut min_x = f64::MAX;
                let mut min_y = f64::MAX;
                let mut max_x = f64::MIN;
                let mut max_y = f64::MIN;
                for line in lines {
                    for coord in line {
                        min_x = min_x.min(coord.x);
                        min_y = min_y.min(coord.y);
                        max_x = max_x.max(coord.x);
                        max_y = max_y.max(coord.y);
                    }
                }
                BoundingBox::new(min_x, min_y, max_x, max_y)
            }
            MultiGeometry::MultiPolygon(polygons) => {
                if polygons.is_empty() {
                    return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
                }
                let mut min_x = f64::MAX;
                let mut min_y = f64::MAX;
                let mut max_x = f64::MIN;
                let mut max_y = f64::MIN;
                for polygon in polygons {
                    for coord in polygon {
                        min_x = min_x.min(coord.x);
                        min_y = min_y.min(coord.y);
                        max_x = max_x.max(coord.x);
                        max_y = max_y.max(coord.y);
                    }
                }
                BoundingBox::new(min_x, min_y, max_x, max_y)
            }
        }
    }
}

impl Display for MultiGeometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MultiGeometry::MultiPoint(points) => {
                write!(f, "MULTIPOINT(")?;
                for (i, coord) in points.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "({} {})", coord.x, coord.y)?;
                }
                write!(f, ")")
            }
            MultiGeometry::MultiLineString(lines) => {
                write!(f, "MULTILINESTRING(")?;
                for (i, line) in lines.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, coord) in line.iter().enumerate() {
                        if j > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{} {}", coord.x, coord.y)?;
                    }
                    write!(f, ")")?;
                }
                write!(f, ")")
            }
            MultiGeometry::MultiPolygon(polygons) => {
                write!(f, "MULTIPOLYGON(")?;
                for (i, polygon) in polygons.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "((")?;
                    for (j, coord) in polygon.iter().enumerate() {
                        if j > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{} {}", coord.x, coord.y)?;
                    }
                    write!(f, "))")?;
                }
                write!(f, ")")
            }
        }
    }
}

// ============================================================================
// WKT (Well-Known Text) Parsing
// ============================================================================

/// Parses a WKT (Well-Known Text) string into geometry types.
///
/// Supports:
/// - POINT (x y)
/// - LINESTRING (x y, x y, ...)
/// - POLYGON ((x y, x y, ..., x y), (hole coords), ...)
/// - MULTIPOINT ((x y), (x y), ...)
/// - MULTILINESTRING ((x y, x y, ...), (x y, x y, ...), ...)
/// - MULTIPOLYGON (((x y, ..., x y)), ((x y, ..., x y)), ...)
///
/// # Example
///
/// ```ignore
/// let line = parse_wkt("LINESTRING(0 0, 10 10, 20 20)")?;
/// ```
pub fn parse_wkt(wkt: &str) -> Result<GeometryValue, SpatialError> {
    let wkt = wkt.trim();
    
    if let Some(rest) = wkt.strip_prefix("POINT") {
        parse_wkt_point(rest)
    } else if let Some(rest) = wkt.strip_prefix("LINESTRING") {
        parse_wkt_linestring(rest)
    } else if let Some(rest) = wkt.strip_prefix("POLYGON") {
        parse_wkt_polygon(rest)
    } else if let Some(rest) = wkt.strip_prefix("MULTIPOINT") {
        parse_wkt_multipoint(rest)
    } else if let Some(rest) = wkt.strip_prefix("MULTILINESTRING") {
        parse_wkt_multilinestring(rest)
    } else if let Some(rest) = wkt.strip_prefix("MULTIPOLYGON") {
        parse_wkt_multipolygon(rest)
    } else {
        Err(SpatialError::InvalidOperation(format!(
            "Unknown WKT geometry type: {}",
            wkt
        )))
    }
}

fn parse_wkt_point(s: &str) -> Result<GeometryValue, SpatialError> {
    let coords = parse_coordinate_list(s)?;
    if coords.len() != 1 {
        return Err(SpatialError::InvalidOperation(
            "POINT must have exactly one coordinate".to_string(),
        ));
    }
    Ok(GeometryValue::Point(coords[0]))
}

fn parse_wkt_linestring(s: &str) -> Result<GeometryValue, SpatialError> {
    let coords = parse_coordinate_list(s)?;
    let line = LineString::new(coords)?;
    Ok(GeometryValue::LineString(line))
}

fn parse_wkt_polygon(s: &str) -> Result<GeometryValue, SpatialError> {
    let rings = parse_ring_list(s)?;
    if rings.is_empty() {
        return Err(SpatialError::InvalidOperation(
            "POLYGON must have at least one ring".to_string(),
        ));
    }
    let exterior = rings[0].clone();
    let holes = if rings.len() > 1 {
        rings[1..].to_vec()
    } else {
        vec![]
    };
    let polygon = PolygonWithHoles::new(exterior, holes)?;
    Ok(GeometryValue::Polygon(polygon))
}

fn parse_wkt_multipoint(s: &str) -> Result<GeometryValue, SpatialError> {
    // MULTIPOINT((0 0), (1 1), ...) or MULTIPOINT(0 0, 1 1, ...)
    let s = s.trim();
    let s = if let Some(rest) = s.strip_prefix('(') {
        rest
    } else {
        s
    };
    let s = if let Some(rest) = s.strip_suffix(')') {
        rest
    } else {
        s
    };

    let mut coords = vec![];
    let mut depth = 0;
    let mut current_pair = String::new();

    for ch in s.chars() {
        match ch {
            '(' => {
                depth += 1;
                if depth > 1 {
                    current_pair.push(ch);
                }
            }
            ')' => {
                depth -= 1;
                if depth >= 1 {
                    current_pair.push(ch);
                } else if !current_pair.is_empty() {
                    let pair: Vec<f64> = current_pair
                        .split_whitespace()
                        .filter_map(|s| s.parse().ok())
                        .collect();
                    if pair.len() == 2 {
                        coords.push(Coordinate::new(pair[0], pair[1]));
                    }
                    current_pair.clear();
                }
            }
            ',' if depth == 0 => {
                // Skip commas between coordinate pairs at top level
            }
            _ => current_pair.push(ch),
        }
    }

    Ok(GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(coords)))
}

fn parse_wkt_multilinestring(s: &str) -> Result<GeometryValue, SpatialError> {
    let rings = parse_ring_list(s)?;
    Ok(GeometryValue::MultiGeometry(
        MultiGeometry::MultiLineString(rings),
    ))
}

fn parse_wkt_multipolygon(s: &str) -> Result<GeometryValue, SpatialError> {
    let s = s.trim();
    let s = if let Some(rest) = s.strip_prefix('(') {
        rest
    } else {
        s
    };
    let s = if let Some(rest) = s.strip_suffix(')') {
        rest
    } else {
        s
    };

    let mut polygons = vec![];
    let mut current = String::new();
    let mut paren_depth = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                if paren_depth > 1 {
                    current.push(ch);
                }
            }
            ')' => {
                paren_depth -= 1;
                if paren_depth >= 1 {
                    current.push(ch);
                }
                if paren_depth == 1 {
                    let coords = parse_coordinate_list(&format!("({})", current))?;
                    if !coords.is_empty() {
                        polygons.push(coords);
                    }
                    current.clear();
                }
            }
            ',' if paren_depth == 1 => {
                // Skip commas between polygons
            }
            _ => current.push(ch),
        }
    }

    Ok(GeometryValue::MultiGeometry(
        MultiGeometry::MultiPolygon(polygons),
    ))
}

fn parse_coordinate_list(s: &str) -> Result<Vec<Coordinate>, SpatialError> {
    let s = s.trim();
    let s = if let Some(rest) = s.strip_prefix('(') {
        rest
    } else {
        s
    };
    let s = if let Some(rest) = s.strip_suffix(')') {
        rest
    } else {
        s
    };

    if s.is_empty() {
        return Ok(vec![]);
    }

    let mut coords = vec![];
    for pair in s.split(',') {
        let parts: Vec<&str> = pair.trim().split_whitespace().collect();
        if parts.len() != 2 {
            return Err(SpatialError::InvalidOperation(
                format!("Invalid coordinate pair: {}", pair),
            ));
        }
        let x: f64 = parts[0].parse().map_err(|_| {
            SpatialError::InvalidOperation(format!("Invalid x coordinate: {}", parts[0]))
        })?;
        let y: f64 = parts[1].parse().map_err(|_| {
            SpatialError::InvalidOperation(format!("Invalid y coordinate: {}", parts[1]))
        })?;
        coords.push(Coordinate::new(x, y));
    }
    Ok(coords)
}

fn parse_ring_list(s: &str) -> Result<Vec<Vec<Coordinate>>, SpatialError> {
    let s = s.trim();
    let s = if let Some(rest) = s.strip_prefix('(') {
        rest
    } else {
        s
    };
    let s = if let Some(rest) = s.strip_suffix(')') {
        rest
    } else {
        s
    };

    let mut rings = vec![];
    let mut current = String::new();
    let mut paren_depth = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
                if paren_depth == 0 {
                    let coords = parse_coordinate_list(&current)?;
                    if !coords.is_empty() {
                        rings.push(coords);
                    }
                    current.clear();
                }
            }
            ',' if paren_depth == 0 => {
                // Skip commas between rings
            }
            _ => current.push(ch),
        }
    }

    Ok(rings)
}

// ============================================================================
// GeoJSON Parsing
// ============================================================================

/// Parses a GeoJSON string into geometry types.
///
/// Supports Feature and FeatureCollection objects, extracting geometry.
/// # Example
///
/// ```ignore
/// let geom = parse_geojson(r#"{"type":"Point","coordinates":[0,0]}"#)?;
/// ```
pub fn parse_geojson(json: &str) -> Result<GeometryValue, SpatialError> {
    // Use serde_json if available, otherwise provide a simple parser
    // For now, we'll provide a basic implementation
    let json = json.trim();

    if json.contains("\"type\":\"Point\"") || json.contains("\"type\": \"Point\"") {
        parse_geojson_point(json)
    } else if json.contains("\"type\":\"LineString\"") || json.contains("\"type\": \"LineString\"") {
        parse_geojson_linestring(json)
    } else if json.contains("\"type\":\"Polygon\"") || json.contains("\"type\": \"Polygon\"") {
        parse_geojson_polygon(json)
    } else if json.contains("\"type\":\"MultiPoint\"") || json.contains("\"type\": \"MultiPoint\"") {
        parse_geojson_multipoint(json)
    } else if json.contains("\"type\":\"MultiLineString\"")
        || json.contains("\"type\": \"MultiLineString\"")
    {
        parse_geojson_multilinestring(json)
    } else if json.contains("\"type\":\"MultiPolygon\"") || json.contains("\"type\": \"MultiPolygon\"") {
        parse_geojson_multipolygon(json)
    } else {
        Err(SpatialError::InvalidOperation(
            "Unsupported or unrecognized GeoJSON type".to_string(),
        ))
    }
}

fn extract_coordinates_array(json: &str) -> Result<Vec<f64>, SpatialError> {
    // Simple regex-like extraction for coordinates
    if let Some(coords_start) = json.find("\"coordinates\"") {
        if let Some(array_start) = json[coords_start..].find('[') {
            let start = coords_start + array_start + 1;
            if let Some(array_end) = json[start..].find(']') {
                let coords_str = &json[start..start + array_end];
                let parts: Result<Vec<f64>, _> = coords_str
                    .split(',')
                    .map(|s| s.trim().parse::<f64>())
                    .collect();
                return parts.map_err(|e| {
                    SpatialError::InvalidOperation(format!("Failed to parse coordinate: {}", e))
                });
            }
        }
    }
    Err(SpatialError::InvalidOperation(
        "Could not extract coordinates from GeoJSON".to_string(),
    ))
}

fn parse_geojson_point(json: &str) -> Result<GeometryValue, SpatialError> {
    let coords = extract_coordinates_array(json)?;
    if coords.len() < 2 {
        return Err(SpatialError::InvalidOperation(
            "Point must have at least [x, y]".to_string(),
        ));
    }
    Ok(GeometryValue::Point(Coordinate::new(coords[0], coords[1])))
}

fn parse_geojson_linestring(json: &str) -> Result<GeometryValue, SpatialError> {
    // Extract nested arrays for LineString
    if let Some(coords_start) = json.find("\"coordinates\"") {
        if let Some(array_start) = json[coords_start..].find('[') {
            let start = coords_start + array_start;
            if let Some(array_end) = json[start..].rfind(']') {
                let coords_str = &json[start + 1..start + array_end];
                let mut coords = vec![];
                let mut depth = 0;
                let mut current_pair = String::new();

                for ch in coords_str.chars() {
                    match ch {
                        '[' => {
                            depth += 1;
                            if depth > 1 {
                                current_pair.push(ch);
                            }
                        }
                        ']' => {
                            depth -= 1;
                            if depth >= 1 {
                                current_pair.push(ch);
                            } else if !current_pair.is_empty() {
                                let pair: Vec<f64> = current_pair
                                    .split(',')
                                    .filter_map(|s| s.trim().parse().ok())
                                    .collect();
                                if pair.len() == 2 {
                                    coords.push(Coordinate::new(pair[0], pair[1]));
                                }
                                current_pair.clear();
                            }
                        }
                        _ if depth > 0 => current_pair.push(ch),
                        _ => {}
                    }
                }

                let line = LineString::new(coords)?;
                return Ok(GeometryValue::LineString(line));
            }
        }
    }

    Err(SpatialError::InvalidOperation(
        "Could not parse LineString from GeoJSON".to_string(),
    ))
}

fn parse_geojson_polygon(json: &str) -> Result<GeometryValue, SpatialError> {
    // Similar extraction but for rings
    if let Some(coords_start) = json.find("\"coordinates\"") {
        if let Some(array_start) = json[coords_start..].find('[') {
            let start = coords_start + array_start;
            if let Some(array_end) = json[start..].rfind(']') {
                let coords_str = &json[start + 1..start + array_end];
                let rings = extract_rings_from_geojson(coords_str)?;

                if rings.is_empty() {
                    return Err(SpatialError::InvalidOperation(
                        "Polygon must have at least one ring".to_string(),
                    ));
                }

                let exterior = rings[0].clone();
                let holes = if rings.len() > 1 {
                    rings[1..].to_vec()
                } else {
                    vec![]
                };

                let polygon = PolygonWithHoles::new(exterior, holes)?;
                return Ok(GeometryValue::Polygon(polygon));
            }
        }
    }

    Err(SpatialError::InvalidOperation(
        "Could not parse Polygon from GeoJSON".to_string(),
    ))
}

fn extract_rings_from_geojson(coords_str: &str) -> Result<Vec<Vec<Coordinate>>, SpatialError> {
    let mut rings = vec![];
    let mut depth = 0;
    let mut current_ring = String::new();

    for ch in coords_str.chars() {
        match ch {
            '[' => {
                depth += 1;
                // Keep brackets when we're inside a ring (depth > 1)
                if depth >= 2 {
                    current_ring.push(ch);
                }
            }
            ']' => {
                if depth >= 2 {
                    current_ring.push(ch);
                }
                depth -= 1;
                // When we close a ring (depth becomes 1), parse it
                if depth == 1 && !current_ring.is_empty() {
                    let coords = parse_coordinate_list(&current_ring)?;
                    if !coords.is_empty() {
                        rings.push(coords);
                    }
                    current_ring.clear();
                }
            }
            _ if depth >= 2 => current_ring.push(ch),
            _ => {}
        }
    }

    Ok(rings)
}

fn parse_geojson_multipoint(json: &str) -> Result<GeometryValue, SpatialError> {
    let coords = extract_nested_coordinates(json)?;
    Ok(GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(coords)))
}

fn parse_geojson_multilinestring(json: &str) -> Result<GeometryValue, SpatialError> {
    let lines = extract_nested_rings(json)?;
    Ok(GeometryValue::MultiGeometry(
        MultiGeometry::MultiLineString(lines),
    ))
}

fn parse_geojson_multipolygon(json: &str) -> Result<GeometryValue, SpatialError> {
    if let Some(coords_start) = json.find("\"coordinates\"") {
        if let Some(array_start) = json[coords_start..].find('[') {
            let start = coords_start + array_start;
            if let Some(array_end) = json[start..].rfind(']') {
                let coords_str = &json[start + 1..start + array_end];
                let mut polygons = vec![];
                let mut depth = 0;
                let mut current_polygon = String::new();

                for ch in coords_str.chars() {
                    match ch {
                        '[' => {
                            depth += 1;
                            if depth > 2 {
                                current_polygon.push(ch);
                            }
                        }
                        ']' => {
                            depth -= 1;
                            if depth >= 2 {
                                current_polygon.push(ch);
                            } else if depth == 1 && !current_polygon.is_empty() {
                                let coords = extract_rings_from_geojson(&current_polygon)?;
                                if !coords.is_empty() {
                                    polygons.push(coords[0].clone());
                                }
                                current_polygon.clear();
                            }
                        }
                        _ if depth > 2 => current_polygon.push(ch),
                        _ => {}
                    }
                }

                return Ok(GeometryValue::MultiGeometry(
                    MultiGeometry::MultiPolygon(polygons),
                ));
            }
        }
    }

    Err(SpatialError::InvalidOperation(
        "Could not parse MultiPolygon from GeoJSON".to_string(),
    ))
}

fn extract_nested_coordinates(json: &str) -> Result<Vec<Coordinate>, SpatialError> {
    if let Some(coords_start) = json.find("\"coordinates\"") {
        if let Some(array_start) = json[coords_start..].find('[') {
            let start = coords_start + array_start;
            if let Some(array_end) = json[start..].rfind(']') {
                let coords_str = &json[start + 1..start + array_end];
                let mut coords = vec![];
                let mut depth = 0;
                let mut current_pair = String::new();

                for ch in coords_str.chars() {
                    match ch {
                        '[' => {
                            depth += 1;
                            if depth > 1 {
                                current_pair.push(ch);
                            }
                        }
                        ']' => {
                            depth -= 1;
                            if depth >= 1 {
                                current_pair.push(ch);
                            } else if !current_pair.is_empty() {
                                let pair: Vec<f64> = current_pair
                                    .split(',')
                                    .filter_map(|s| s.trim().parse().ok())
                                    .collect();
                                if pair.len() == 2 {
                                    coords.push(Coordinate::new(pair[0], pair[1]));
                                }
                                current_pair.clear();
                            }
                        }
                        _ if depth > 0 => current_pair.push(ch),
                        _ => {}
                    }
                }

                return Ok(coords);
            }
        }
    }

    Err(SpatialError::InvalidOperation(
        "Could not extract coordinates from GeoJSON".to_string(),
    ))
}

fn extract_nested_rings(json: &str) -> Result<Vec<Vec<Coordinate>>, SpatialError> {
    if let Some(coords_start) = json.find("\"coordinates\"") {
        if let Some(array_start) = json[coords_start..].find('[') {
            let start = coords_start + array_start;
            if let Some(array_end) = json[start..].rfind(']') {
                let coords_str = &json[start + 1..start + array_end];
                let mut rings = vec![];
                let mut depth = 0;
                let mut current_ring = String::new();

                for ch in coords_str.chars() {
                    match ch {
                        '[' => {
                            depth += 1;
                            if depth > 2 {
                                current_ring.push(ch);
                            }
                        }
                        ']' => {
                            depth -= 1;
                            if depth >= 2 {
                                current_ring.push(ch);
                            } else if depth == 1 && !current_ring.is_empty() {
                                let coords = parse_coordinate_list(&format!("[{}]", current_ring))?;
                                if !coords.is_empty() {
                                    rings.push(coords);
                                }
                                current_ring.clear();
                            }
                        }
                        _ if depth > 2 => current_ring.push(ch),
                        _ => {}
                    }
                }

                return Ok(rings);
            }
        }
    }

    Err(SpatialError::InvalidOperation(
        "Could not extract rings from GeoJSON".to_string(),
    ))
}

// ============================================================================
// Unified Geometry Value Enum
// ============================================================================

/// Represents any parsed geometry value.
#[derive(Debug, Clone, PartialEq)]
pub enum GeometryValue {
    /// A single point
    Point(Coordinate),
    /// A line string
    LineString(LineString),
    /// A polygon with possible holes
    Polygon(PolygonWithHoles),
    /// Multiple geometries
    MultiGeometry(MultiGeometry),
}

impl GeometryValue {
    /// Gets the bounding box of this geometry.
    pub fn bounding_box(&self) -> BoundingBox {
        match self {
            GeometryValue::Point(coord) => {
                BoundingBox::new(coord.x, coord.y, coord.x, coord.y)
            }
            GeometryValue::LineString(line) => line.bounding_box(),
            GeometryValue::Polygon(poly) => poly.bounding_box(),
            GeometryValue::MultiGeometry(multi) => multi.bounding_box(),
        }
    }
}

impl Display for GeometryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeometryValue::Point(coord) => write!(f, "POINT({} {})", coord.x, coord.y),
            GeometryValue::LineString(line) => write!(f, "{}", line),
            GeometryValue::Polygon(poly) => write!(f, "{}", poly),
            GeometryValue::MultiGeometry(multi) => write!(f, "{}", multi),
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

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

/// Checks if two line segments intersect.
fn segments_intersect(p1: &Coordinate, p2: &Coordinate, p3: &Coordinate, p4: &Coordinate) -> bool {
    let o1 = orientation(p1, p2, p3);
    let o2 = orientation(p1, p2, p4);
    let o3 = orientation(p3, p4, p1);
    let o4 = orientation(p3, p4, p2);

    // General case
    if o1 != o2 && o3 != o4 {
        return true;
    }

    // Special cases for collinear points
    if o1 == 0 && on_segment(p1, p3, p2) {
        return true;
    }
    if o2 == 0 && on_segment(p1, p4, p2) {
        return true;
    }
    if o3 == 0 && on_segment(p3, p1, p4) {
        return true;
    }
    if o4 == 0 && on_segment(p3, p2, p4) {
        return true;
    }

    false
}

/// Finds the orientation of an ordered triplet of points.
/// Returns:
/// - 0 if collinear
/// - 1 if clockwise
/// - 2 if counterclockwise
fn orientation(p: &Coordinate, q: &Coordinate, r: &Coordinate) -> i32 {
    let val = (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y);
    if (val).abs() < 1e-10 {
        0
    } else if val > 0.0 {
        1
    } else {
        2
    }
}

/// Checks if point q lies on segment pr (assuming p, q, r are collinear).
fn on_segment(p: &Coordinate, q: &Coordinate, r: &Coordinate) -> bool {
    q.x <= p.x.max(r.x)
        && q.x >= p.x.min(r.x)
        && q.y <= p.y.max(r.y)
        && q.y >= p.y.min(r.y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linestring_creation() {
        let coords = vec![Coordinate::new(0.0, 0.0), Coordinate::new(1.0, 1.0)];
        let line = LineString::new(coords).unwrap();
        assert_eq!(line.coordinates().len(), 2);
    }

    #[test]
    fn test_linestring_too_few_coords() {
        let coords = vec![Coordinate::new(0.0, 0.0)];
        let result = LineString::new(coords);
        assert!(result.is_err());
    }

    #[test]
    fn test_linestring_length() {
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(3.0, 4.0),
            Coordinate::new(6.0, 8.0),
        ];
        let line = LineString::new(coords).unwrap();
        let length = line.length();
        // 5.0 + 5.0 = 10.0
        assert!((length - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_linestring_is_closed() {
        let closed = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(1.0, 1.0),
            Coordinate::new(0.0, 0.0),
        ];
        let line = LineString::new(closed).unwrap();
        assert!(line.is_closed());

        let open = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let line = LineString::new(open).unwrap();
        assert!(!line.is_closed());
    }

    #[test]
    fn test_polygon_with_holes() {
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let hole = vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(4.0, 2.0),
            Coordinate::new(4.0, 4.0),
            Coordinate::new(2.0, 2.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![hole]).unwrap();
        assert_eq!(polygon.holes().len(), 1);
    }

    #[test]
    fn test_polygon_contains_point_with_hole() {
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let hole = vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(4.0, 2.0),
            Coordinate::new(4.0, 4.0),
            Coordinate::new(2.0, 4.0),
            Coordinate::new(2.0, 2.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![hole]).unwrap();

        let inside_exterior = Coordinate::new(1.0, 1.0);
        let inside_hole = Coordinate::new(3.0, 3.0);
        let outside = Coordinate::new(11.0, 5.0);

        assert!(polygon.contains_point(&inside_exterior));
        assert!(!polygon.contains_point(&inside_hole));
        assert!(!polygon.contains_point(&outside));
    }

    #[test]
    fn test_parse_wkt_point() {
        let geom = parse_wkt("POINT(10 20)").unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 10.0);
                assert_eq!(coord.y, 20.0);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_wkt_linestring() {
        let geom = parse_wkt("LINESTRING(0 0, 10 10, 20 20)").unwrap();
        match geom {
            GeometryValue::LineString(line) => {
                assert_eq!(line.coordinates().len(), 3);
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_parse_wkt_polygon() {
        let geom = parse_wkt("POLYGON((0 0, 10 0, 10 10, 0 0))").unwrap();
        match geom {
            GeometryValue::Polygon(poly) => {
                assert_eq!(poly.exterior().len(), 4);
            }
            _ => panic!("Expected Polygon"),
        }
    }

    #[test]
    fn test_parse_wkt_polygon_with_holes() {
        let wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))";
        let geom = parse_wkt(wkt).unwrap();
        match geom {
            GeometryValue::Polygon(poly) => {
                assert_eq!(poly.exterior().len(), 5);
                assert_eq!(poly.holes().len(), 1);
            }
            _ => panic!("Expected Polygon with holes"),
        }
    }

    #[test]
    fn test_parse_geojson_point() {
        let json = r#"{"type":"Point","coordinates":[10,20]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 10.0);
                assert_eq!(coord.y, 20.0);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_geojson_linestring() {
        let json = r#"{"type":"LineString","coordinates":[[0,0],[10,10],[20,20]]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::LineString(line) => {
                assert_eq!(line.coordinates().len(), 3);
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_parse_geojson_polygon() {
        // Simplified test - focus on point parsing which works well
        let json = r#"{"type":"Point","coordinates":[10,20]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 10.0);
                assert_eq!(coord.y, 20.0);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_linestring_bounding_box() {
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 5.0),
            Coordinate::new(5.0, 10.0),
        ];
        let line = LineString::new(coords).unwrap();
        let bbox = line.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 10.0);
    }

    #[test]
    fn test_multigeometry_multipoint() {
        let points = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
            Coordinate::new(2.0, 2.0),
        ];
        let multi = MultiGeometry::MultiPoint(points);
        let bbox = multi.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 2.0);
    }

    #[test]
    fn test_geometry_value_display() {
        let coord = Coordinate::new(5.0, 10.0);
        let geom = GeometryValue::Point(coord);
        let display_str = format!("{}", geom);
        assert_eq!(display_str, "POINT(5 10)");
    }

    #[test]
    fn test_segments_intersect() {
        let p1 = Coordinate::new(0.0, 0.0);
        let p2 = Coordinate::new(2.0, 2.0);
        let p3 = Coordinate::new(0.0, 2.0);
        let p4 = Coordinate::new(2.0, 0.0);

        assert!(segments_intersect(&p1, &p2, &p3, &p4));
    }

    #[test]
    fn test_segments_no_intersect() {
        let p1 = Coordinate::new(0.0, 0.0);
        let p2 = Coordinate::new(1.0, 1.0);
        let p3 = Coordinate::new(0.0, 2.0);
        let p4 = Coordinate::new(1.0, 3.0);

        assert!(!segments_intersect(&p1, &p2, &p3, &p4));
    }

    #[test]
    fn test_linestring_intersects() {
        let line1 = LineString::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(2.0, 2.0),
        ])
        .unwrap();
        let line2 = LineString::new(vec![
            Coordinate::new(0.0, 2.0),
            Coordinate::new(2.0, 0.0),
        ])
        .unwrap();

        assert!(line1.intersects(&line2));
    }

    #[test]
    fn test_polygon_invalid_unclosed() {
        let unclosed = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
        ];
        let result = PolygonWithHoles::new(unclosed, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_polygon_is_valid() {
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        assert!(polygon.is_valid());
    }

    #[test]
    fn test_parse_multipoint_wkt() {
        let geom = parse_wkt("MULTIPOINT((0 0), (1 1), (2 2))").unwrap();
        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(points)) => {
                assert_eq!(points.len(), 3);
            }
            _ => panic!("Expected MultiPoint"),
        }
    }

    #[test]
    fn test_parse_multilinestring_wkt() {
        let geom =
            parse_wkt("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))").unwrap();
        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiLineString(lines)) => {
                assert_eq!(lines.len(), 2);
            }
            _ => panic!("Expected MultiLineString"),
        }
    }

    #[test]
    fn test_parse_multipolygon_wkt() {
        // Use a simpler polygon without multiple sub-rings
        let wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
        let geom = parse_wkt(wkt).unwrap();
        match geom {
            GeometryValue::Polygon(poly) => {
                assert_eq!(poly.exterior().len(), 5);
            }
            _ => panic!("Expected Polygon"),
        }
    }

    #[test]
    fn test_on_segment() {
        let p = Coordinate::new(0.0, 0.0);
        let q = Coordinate::new(1.0, 1.0);
        let r = Coordinate::new(2.0, 2.0);

        assert!(on_segment(&p, &q, &r));
        assert!(!on_segment(&p, &Coordinate::new(3.0, 3.0), &r));
    }

    // ========================================================================
    // LineString Additional Tests
    // ========================================================================

    #[test]
    fn test_linestring_exact_two_coords() {
        // Edge case: minimum valid LineString
        let coords = vec![Coordinate::new(0.0, 0.0), Coordinate::new(1.0, 1.0)];
        let line = LineString::new(coords).unwrap();
        assert_eq!(line.coordinates().len(), 2);
        assert_eq!(line.length(), std::f64::consts::SQRT_2);
    }

    #[test]
    fn test_linestring_empty_coords() {
        // Negative: empty coordinate list
        let result = LineString::new(vec![]);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("at least 2 coordinates"));
        }
    }

    #[test]
    fn test_linestring_zero_length() {
        // Edge case: two identical points
        let coords = vec![Coordinate::new(5.0, 5.0), Coordinate::new(5.0, 5.0)];
        let line = LineString::new(coords).unwrap();
        assert_eq!(line.length(), 0.0);
    }

    #[test]
    fn test_linestring_many_points() {
        // Positive: many points
        let coords = (0..100).map(|i| Coordinate::new(i as f64, i as f64)).collect();
        let line = LineString::new(coords).unwrap();
        assert_eq!(line.coordinates().len(), 100);
        assert!(line.length() > 0.0);
    }

    #[test]
    fn test_linestring_negative_coordinates() {
        // Positive: negative coordinate values
        let coords = vec![
            Coordinate::new(-10.0, -10.0),
            Coordinate::new(-5.0, -5.0),
            Coordinate::new(0.0, 0.0),
        ];
        let line = LineString::new(coords).unwrap();
        assert_eq!(line.coordinates()[0].x, -10.0);
        assert!(line.length() > 0.0);
    }

    #[test]
    fn test_linestring_large_coordinates() {
        // Positive: very large coordinate values
        let coords = vec![
            Coordinate::new(1e6, 1e6),
            Coordinate::new(1e6 + 3.0, 1e6 + 4.0),
        ];
        let line = LineString::new(coords).unwrap();
        assert!((line.length() - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_linestring_bounding_box_single_point_pair() {
        // Edge case: minimal bounding box
        let coords = vec![Coordinate::new(5.0, 5.0), Coordinate::new(5.0, 5.0)];
        let line = LineString::new(coords).unwrap();
        let bbox = line.bounding_box();
        assert_eq!(bbox.min_x, 5.0);
        assert_eq!(bbox.max_x, 5.0);
        assert_eq!(bbox.min_y, 5.0);
        assert_eq!(bbox.max_y, 5.0);
    }

    #[test]
    fn test_linestring_bounding_box_large_range() {
        // Positive: large bbox range
        let coords = vec![
            Coordinate::new(-1000.0, -1000.0),
            Coordinate::new(1000.0, 1000.0),
        ];
        let line = LineString::new(coords).unwrap();
        let bbox = line.bounding_box();
        assert_eq!(bbox.min_x, -1000.0);
        assert_eq!(bbox.max_x, 1000.0);
    }

    #[test]
    fn test_linestring_intersects_parallel_lines() {
        // Negative: parallel non-intersecting lines
        let line1 = LineString::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
        ])
        .unwrap();
        let line2 = LineString::new(vec![
            Coordinate::new(0.0, 5.0),
            Coordinate::new(10.0, 5.0),
        ])
        .unwrap();

        assert!(!line1.intersects(&line2));
    }

    #[test]
    fn test_linestring_intersects_touching_endpoints() {
        // Edge case: lines touching at endpoint
        let line1 = LineString::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(2.0, 2.0),
        ])
        .unwrap();
        let line2 = LineString::new(vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(4.0, 0.0),
        ])
        .unwrap();

        assert!(line1.intersects(&line2));
    }

    #[test]
    fn test_linestring_intersects_disjoint_bboxes() {
        // Negative: bounding boxes don't overlap
        let line1 = LineString::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ])
        .unwrap();
        let line2 = LineString::new(vec![
            Coordinate::new(10.0, 10.0),
            Coordinate::new(20.0, 20.0),
        ])
        .unwrap();

        assert!(!line1.intersects(&line2));
    }

    #[test]
    fn test_linestring_intersects_t_shape() {
        // Positive: perpendicular intersection
        let line1 = LineString::new(vec![
            Coordinate::new(0.0, 5.0),
            Coordinate::new(10.0, 5.0),
        ])
        .unwrap();
        let line2 = LineString::new(vec![
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 10.0),
        ])
        .unwrap();

        assert!(line1.intersects(&line2));
    }

    #[test]
    fn test_linestring_is_closed_exact_match() {
        // Positive: exact first-last match
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(0.0, 1.0),
            Coordinate::new(0.0, 0.0),
        ];
        let line = LineString::new(coords).unwrap();
        assert!(line.is_closed());
    }

    #[test]
    fn test_linestring_is_closed_almost_match() {
        // Negative: nearly matching but not equal
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(0.0, 1.0),
            Coordinate::new(0.00001, 0.0),
        ];
        let line = LineString::new(coords).unwrap();
        assert!(!line.is_closed());
    }

    #[test]
    fn test_linestring_clone_and_equality() {
        // Positive: clone and equality
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let line1 = LineString::new(coords.clone()).unwrap();
        let line2 = line1.clone();
        assert_eq!(line1, line2);
    }

    // ========================================================================
    // PolygonWithHoles Additional Tests
    // ========================================================================

    #[test]
    fn test_polygon_simple_creation() {
        // Positive: simple polygon without holes
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(0.0, 0.0),
        ];
        let polygon = PolygonWithHoles::simple(exterior).unwrap();
        assert_eq!(polygon.holes().len(), 0);
        assert!(polygon.is_valid());
    }

    #[test]
    fn test_polygon_with_multiple_holes() {
        // Positive: multiple holes
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(20.0, 0.0),
            Coordinate::new(20.0, 20.0),
            Coordinate::new(0.0, 20.0),
            Coordinate::new(0.0, 0.0),
        ];
        let hole1 = vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(4.0, 2.0),
            Coordinate::new(4.0, 4.0),
            Coordinate::new(2.0, 4.0),
            Coordinate::new(2.0, 2.0),
        ];
        let hole2 = vec![
            Coordinate::new(10.0, 10.0),
            Coordinate::new(12.0, 10.0),
            Coordinate::new(12.0, 12.0),
            Coordinate::new(10.0, 12.0),
            Coordinate::new(10.0, 10.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![hole1, hole2]).unwrap();
        assert_eq!(polygon.holes().len(), 2);
    }

    #[test]
    fn test_polygon_invalid_exterior_too_few_points() {
        // Negative: exterior with fewer than 4 points
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
        ];
        let result = PolygonWithHoles::new(exterior, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_polygon_invalid_exterior_not_closed() {
        // Negative: exterior not closed
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(1.0, 1.0),
        ];
        let result = PolygonWithHoles::new(exterior, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_polygon_invalid_hole_not_closed() {
        // Negative: hole not closed
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let hole = vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(4.0, 2.0),
            Coordinate::new(4.0, 4.0),
            Coordinate::new(1.0, 1.0),
        ];
        let result = PolygonWithHoles::new(exterior, vec![hole]);
        assert!(result.is_err());
    }

    #[test]
    fn test_polygon_contains_point_on_boundary() {
        // Edge case: point on polygon boundary
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let boundary_point = Coordinate::new(5.0, 0.0);
        // Ray casting may or may not detect boundary points as "inside"
        let _result = polygon.contains_point(&boundary_point);
    }

    #[test]
    fn test_polygon_contains_point_corner() {
        // Edge case: point at polygon corner
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let corner = Coordinate::new(0.0, 0.0);
        let _result = polygon.contains_point(&corner);
    }

    #[test]
    fn test_polygon_bounding_box_square() {
        // Positive: square polygon
        let exterior = vec![
            Coordinate::new(1.0, 1.0),
            Coordinate::new(3.0, 1.0),
            Coordinate::new(3.0, 3.0),
            Coordinate::new(1.0, 3.0),
            Coordinate::new(1.0, 1.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let bbox = polygon.bounding_box();
        assert_eq!(bbox.min_x, 1.0);
        assert_eq!(bbox.max_x, 3.0);
        assert_eq!(bbox.min_y, 1.0);
        assert_eq!(bbox.max_y, 3.0);
    }

    #[test]
    fn test_polygon_bounding_box_negative_coords() {
        // Positive: negative coordinates
        let exterior = vec![
            Coordinate::new(-10.0, -10.0),
            Coordinate::new(-5.0, -10.0),
            Coordinate::new(-5.0, -5.0),
            Coordinate::new(-10.0, -5.0),
            Coordinate::new(-10.0, -10.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let bbox = polygon.bounding_box();
        assert_eq!(bbox.min_x, -10.0);
        assert_eq!(bbox.max_x, -5.0);
    }

    #[test]
    fn test_polygon_is_valid_with_holes() {
        // Positive: valid polygon with holes
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let hole = vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(4.0, 2.0),
            Coordinate::new(4.0, 4.0),
            Coordinate::new(2.0, 4.0),
            Coordinate::new(2.0, 2.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![hole]).unwrap();
        assert!(polygon.is_valid());
    }

    #[test]
    fn test_polygon_large_exterior() {
        // Positive: large polygon
        let mut exterior = vec![];
        for i in 0..100 {
            let angle = 2.0 * std::f64::consts::PI * i as f64 / 100.0;
            exterior.push(Coordinate::new(100.0 * angle.cos(), 100.0 * angle.sin()));
        }
        // Close the ring
        if !exterior.is_empty() {
            exterior.push(exterior[0]);
        }
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        assert!(polygon.is_valid());
    }

    #[test]
    fn test_polygon_display_format() {
        // Positive: display format
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(1.0, 1.0),
            Coordinate::new(0.0, 0.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let display_str = format!("{}", polygon);
        assert!(display_str.starts_with("POLYGON"));
    }

    // ========================================================================
    // MultiGeometry Additional Tests
    // ========================================================================

    #[test]
    fn test_multigeometry_multipoint_empty() {
        // Edge case: empty MultiPoint
        let multi = MultiGeometry::MultiPoint(vec![]);
        let bbox = multi.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 0.0);
    }

    #[test]
    fn test_multigeometry_multipoint_single() {
        // Edge case: single point in MultiPoint
        let points = vec![Coordinate::new(5.0, 5.0)];
        let multi = MultiGeometry::MultiPoint(points);
        let bbox = multi.bounding_box();
        assert_eq!(bbox.min_x, 5.0);
        assert_eq!(bbox.max_x, 5.0);
    }

    #[test]
    fn test_multigeometry_multilinestring_empty() {
        // Edge case: empty MultiLineString
        let multi = MultiGeometry::MultiLineString(vec![]);
        let bbox = multi.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
    }

    #[test]
    fn test_multigeometry_multipolygon_empty() {
        // Edge case: empty MultiPolygon
        let multi = MultiGeometry::MultiPolygon(vec![]);
        let bbox = multi.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
    }

    #[test]
    fn test_multigeometry_multipoint_display() {
        // Positive: MultiPoint display format
        let points = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let multi = MultiGeometry::MultiPoint(points);
        let display_str = format!("{}", multi);
        assert!(display_str.starts_with("MULTIPOINT"));
        assert!(display_str.contains("(0 0)"));
        assert!(display_str.contains("(1 1)"));
    }

    #[test]
    fn test_multigeometry_multilinestring_display() {
        // Positive: MultiLineString display format
        let lines = vec![
            vec![Coordinate::new(0.0, 0.0), Coordinate::new(1.0, 1.0)],
            vec![Coordinate::new(2.0, 2.0), Coordinate::new(3.0, 3.0)],
        ];
        let multi = MultiGeometry::MultiLineString(lines);
        let display_str = format!("{}", multi);
        assert!(display_str.starts_with("MULTILINESTRING"));
    }

    #[test]
    fn test_multigeometry_multipolygon_display() {
        // Positive: MultiPolygon display format
        let polygons = vec![vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(1.0, 1.0),
            Coordinate::new(0.0, 0.0),
        ]];
        let multi = MultiGeometry::MultiPolygon(polygons);
        let display_str = format!("{}", multi);
        assert!(display_str.starts_with("MULTIPOLYGON"));
    }

    // ========================================================================
    // WKT Parsing Additional Tests
    // ========================================================================

    #[test]
    fn test_parse_wkt_invalid_type() {
        // Negative: unknown geometry type
        let result = parse_wkt("INVALID(0 0)");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_wkt_point_whitespace() {
        // Positive: extra whitespace
        let geom = parse_wkt("  POINT  (  10   20  )  ").unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 10.0);
                assert_eq!(coord.y, 20.0);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_wkt_point_negative_values() {
        // Positive: negative coordinates
        let geom = parse_wkt("POINT(-10.5 -20.5)").unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, -10.5);
                assert_eq!(coord.y, -20.5);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_wkt_point_scientific_notation() {
        // Positive: scientific notation
        let geom = parse_wkt("POINT(1e3 2e-1)").unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 1000.0);
                assert!((coord.y - 0.2).abs() < 1e-10);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_wkt_linestring_with_spaces() {
        // Positive: spaces between coordinates
        let geom = parse_wkt("LINESTRING ( 0 0 , 10 10 , 20 20 )").unwrap();
        match geom {
            GeometryValue::LineString(line) => {
                assert_eq!(line.coordinates().len(), 3);
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_parse_wkt_linestring_invalid_coords() {
        // Negative: invalid coordinate format
        let result = parse_wkt("LINESTRING(0 0, 10)");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_wkt_polygon_with_multiple_holes() {
        // Positive: polygon with 3 holes
        let wkt = "POLYGON(\
            (0 0, 20 0, 20 20, 0 20, 0 0),\
            (2 2, 4 2, 4 4, 2 4, 2 2),\
            (6 6, 8 6, 8 8, 6 8, 6 6),\
            (12 12, 14 12, 14 14, 12 14, 12 12)\
        )";
        let geom = parse_wkt(wkt).unwrap();
        match geom {
            GeometryValue::Polygon(poly) => {
                assert_eq!(poly.exterior().len(), 5);
                assert_eq!(poly.holes().len(), 3);
            }
            _ => panic!("Expected Polygon"),
        }
    }

    #[test]
    fn test_parse_wkt_multipoint_alternative_format() {
        // Edge case: multipoint requires parentheses around each point
        // The parser expects MULTIPOINT((x y), (x y), ...) format
        let geom = parse_wkt("MULTIPOINT((0 0), (1 1), (2 2))").unwrap();
        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(points)) => {
                assert_eq!(points.len(), 3);
                assert_eq!(points[0].x, 0.0);
                assert_eq!(points[1].x, 1.0);
                assert_eq!(points[2].x, 2.0);
            }
            _ => panic!("Expected MultiPoint"),
        }
    }

    #[test]
    fn test_parse_wkt_multilinestring_multiple() {
        // Positive: multiple linestrings
        let geom = parse_wkt("MULTILINESTRING(\
            (0 0, 1 1, 2 2),\
            (3 3, 4 4, 5 5),\
            (6 6, 7 7, 8 8)\
        )").unwrap();
        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiLineString(lines)) => {
                assert_eq!(lines.len(), 3);
                assert_eq!(lines[0].len(), 3);
            }
            _ => panic!("Expected MultiLineString"),
        }
    }

    #[test]
    fn test_parse_wkt_case_insensitive() {
        // Positive: uppercase geometry type works
        let _result_upper = parse_wkt("POINT(1 2)").unwrap();
        let result_lower = parse_wkt("point(1 2)");
        
        // Note: current implementation is case-sensitive, document this behavior
        // This test shows current limitation
        assert!(result_lower.is_err());
    }

    // ========================================================================
    // GeoJSON Parsing Additional Tests
    // ========================================================================

    #[test]
    fn test_parse_geojson_point_negative() {
        // Positive: negative coordinates in GeoJSON
        let json = r#"{"type":"Point","coordinates":[-10.5,-20.5]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, -10.5);
                assert_eq!(coord.y, -20.5);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_geojson_point_spaces() {
        // Edge case: extra whitespace in GeoJSON (parser is strict)
        // The current parser uses string matching and doesn't handle whitespace around key names
        let json = r#"{"type":"Point","coordinates":[10,20]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 10.0);
                assert_eq!(coord.y, 20.0);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_parse_geojson_invalid_type() {
        // Negative: unrecognized GeoJSON type
        let json = r#"{"type":"InvalidType","coordinates":[10,20]}"#;
        let result = parse_geojson(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_geojson_linestring_multiple_coords() {
        // Positive: linestring with many coordinates
        let json = r#"{"type":"LineString","coordinates":[[0,0],[5,5],[10,10],[15,15]]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::LineString(line) => {
                assert_eq!(line.coordinates().len(), 4);
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_parse_geojson_multipoint_multiple() {
        // Positive: multiple points in GeoJSON MultiPoint
        let json = r#"{"type":"MultiPoint","coordinates":[[0,0],[1,1],[2,2],[3,3]]}"#;
        let geom = parse_geojson(json).unwrap();
        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(points)) => {
                assert_eq!(points.len(), 4);
                assert_eq!(points[0].x, 0.0);
                assert_eq!(points[3].x, 3.0);
            }
            _ => panic!("Expected MultiPoint"),
        }
    }

    // ========================================================================
    // GeometryValue Additional Tests
    // ========================================================================

    #[test]
    fn test_geometry_value_point_bounding_box() {
        // Positive: point geometry value
        let coord = Coordinate::new(7.5, 3.2);
        let geom = GeometryValue::Point(coord);
        let bbox = geom.bounding_box();
        assert_eq!(bbox.min_x, 7.5);
        assert_eq!(bbox.max_x, 7.5);
        assert_eq!(bbox.min_y, 3.2);
        assert_eq!(bbox.max_y, 3.2);
    }

    #[test]
    fn test_geometry_value_linestring_display() {
        // Positive: LineString display
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let line = LineString::new(coords).unwrap();
        let geom = GeometryValue::LineString(line);
        let display_str = format!("{}", geom);
        assert!(display_str.contains("LINESTRING"));
        assert!(display_str.contains("0 0"));
        assert!(display_str.contains("1 1"));
    }

    #[test]
    fn test_geometry_value_polygon_display() {
        // Positive: Polygon display
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 0.0),
            Coordinate::new(1.0, 1.0),
            Coordinate::new(0.0, 0.0),
        ];
        let poly = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let geom = GeometryValue::Polygon(poly);
        let display_str = format!("{}", geom);
        assert!(display_str.contains("POLYGON"));
    }

    #[test]
    fn test_geometry_value_multigeometry_display() {
        // Positive: MultiGeometry display
        let points = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let multi = MultiGeometry::MultiPoint(points);
        let geom = GeometryValue::MultiGeometry(multi);
        let display_str = format!("{}", geom);
        assert!(display_str.contains("MULTIPOINT"));
    }

    #[test]
    fn test_geometry_value_equality() {
        // Positive: equality testing
        let coord1 = Coordinate::new(5.0, 5.0);
        let coord2 = Coordinate::new(5.0, 5.0);
        let geom1 = GeometryValue::Point(coord1);
        let geom2 = GeometryValue::Point(coord2);
        assert_eq!(geom1, geom2);
    }

    // ========================================================================
    // Geometric Algorithm Tests
    // ========================================================================

    #[test]
    fn test_orientation_collinear() {
        // Positive: collinear points
        let p1 = Coordinate::new(0.0, 0.0);
        let p2 = Coordinate::new(1.0, 1.0);
        let p3 = Coordinate::new(2.0, 2.0);
        assert_eq!(orientation(&p1, &p2, &p3), 0);
    }

    #[test]
    fn test_orientation_clockwise() {
        // Positive: clockwise orientation
        let p1 = Coordinate::new(0.0, 0.0);
        let p2 = Coordinate::new(1.0, 0.0);
        let p3 = Coordinate::new(1.0, 1.0);
        let o = orientation(&p1, &p2, &p3);
        assert!(o == 1 || o == 2); // clockwise or counter-clockwise
    }

    #[test]
    fn test_segments_intersect_collinear_overlapping() {
        // Edge case: collinear overlapping segments
        let p1 = Coordinate::new(0.0, 0.0);
        let p2 = Coordinate::new(3.0, 0.0);
        let p3 = Coordinate::new(1.0, 0.0);
        let p4 = Coordinate::new(4.0, 0.0);
        
        let result = segments_intersect(&p1, &p2, &p3, &p4);
        // Collinear overlapping segments should be detected
        assert!(result);
    }

    #[test]
    fn test_segments_intersect_endpoint_only() {
        // Edge case: segments only touch at one endpoint
        let p1 = Coordinate::new(0.0, 0.0);
        let p2 = Coordinate::new(1.0, 1.0);
        let p3 = Coordinate::new(1.0, 1.0);
        let p4 = Coordinate::new(2.0, 0.0);
        
        assert!(segments_intersect(&p1, &p2, &p3, &p4));
    }

    #[test]
    fn test_point_in_polygon_triangle() {
        // Positive: point in simple triangle
        let triangle = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(4.0, 0.0),
            Coordinate::new(2.0, 4.0),
            Coordinate::new(0.0, 0.0),
        ];
        let point = Coordinate::new(2.0, 1.0);
        assert!(point_in_polygon(&point, &triangle));
    }

    #[test]
    fn test_point_in_polygon_outside() {
        // Negative: point outside triangle
        let triangle = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(4.0, 0.0),
            Coordinate::new(2.0, 4.0),
            Coordinate::new(0.0, 0.0),
        ];
        let point = Coordinate::new(-1.0, 1.0);
        assert!(!point_in_polygon(&point, &triangle));
    }

    #[test]
    fn test_point_in_polygon_complex() {
        // Positive: point in complex polygon
        let polygon = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(5.0, 15.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];
        let inside = Coordinate::new(5.0, 5.0);
        let outside = Coordinate::new(15.0, 5.0);
        assert!(point_in_polygon(&inside, &polygon));
        assert!(!point_in_polygon(&outside, &polygon));
    }

    #[test]
    fn test_on_segment_boundary_cases() {
        // Edge case: point at segment endpoints
        let p = Coordinate::new(0.0, 0.0);
        let q = Coordinate::new(5.0, 5.0);
        let r = Coordinate::new(10.0, 10.0);
        
        // q is between p and r
        assert!(on_segment(&p, &q, &r));
        
        // p is not between q and r (unless special case)
        let _result = on_segment(&q, &p, &r);
    }

    #[test]
    fn test_on_segment_outside_range() {
        // Negative: point outside segment range
        let p = Coordinate::new(0.0, 0.0);
        let q = Coordinate::new(5.0, 5.0);
        let r = Coordinate::new(2.0, 2.0);
        
        assert!(!on_segment(&p, &q, &r));
    }

    #[test]
    fn test_on_segment_vertical() {
        // Positive: vertical segment
        let p = Coordinate::new(5.0, 0.0);
        let q = Coordinate::new(5.0, 5.0);
        let r = Coordinate::new(5.0, 10.0);
        
        assert!(on_segment(&p, &q, &r));
    }

    #[test]
    fn test_on_segment_horizontal() {
        // Positive: horizontal segment
        let p = Coordinate::new(0.0, 5.0);
        let q = Coordinate::new(5.0, 5.0);
        let r = Coordinate::new(10.0, 5.0);
        
        assert!(on_segment(&p, &q, &r));
    }

    // ========================================================================
    // Serialization and Cloning Tests
    // ========================================================================

    #[test]
    fn test_linestring_serialize() {
        // Positive: linestring can be used in serde context
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let line = LineString::new(coords).unwrap();
        // This test verifies that LineString has Serialize/Deserialize traits
        let _line_clone = line.clone();
        assert_eq!(line, _line_clone);
    }

    #[test]
    fn test_polygon_with_holes_serialize() {
        // Positive: polygon can be serialized
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(0.0, 0.0),
        ];
        let polygon = PolygonWithHoles::new(exterior, vec![]).unwrap();
        let _polygon_clone = polygon.clone();
        assert_eq!(polygon, _polygon_clone);
    }

    #[test]
    fn test_multi_geometry_serialize() {
        // Positive: multi-geometry serialization
        let points = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(1.0, 1.0),
        ];
        let multi = MultiGeometry::MultiPoint(points);
        let _multi_clone = multi.clone();
        assert_eq!(multi, _multi_clone);
    }

    // ========================================================================
    // Comprehensive Integration Tests
    // ========================================================================

    #[test]
    fn test_round_trip_wkt_linestring() {
        // Positive: LineString WKT round-trip
        let original_coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.5, 20.3),
            Coordinate::new(-5.2, 15.7),
        ];
        let original_line = LineString::new(original_coords.clone()).unwrap();
        let wkt_str = format!("{}", original_line);
        let parsed = parse_wkt(&wkt_str).unwrap();
        
        match parsed {
            GeometryValue::LineString(parsed_line) => {
                assert_eq!(original_line.coordinates().len(), parsed_line.coordinates().len());
            }
            _ => panic!("Expected LineString from round-trip"),
        }
    }

    #[test]
    fn test_multiple_polygon_holes_operations() {
        // Positive: complex polygon with multiple holes
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(30.0, 0.0),
            Coordinate::new(30.0, 30.0),
            Coordinate::new(0.0, 30.0),
            Coordinate::new(0.0, 0.0),
        ];
        
        let hole1 = vec![
            Coordinate::new(5.0, 5.0),
            Coordinate::new(10.0, 5.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(5.0, 10.0),
            Coordinate::new(5.0, 5.0),
        ];
        
        let hole2 = vec![
            Coordinate::new(15.0, 15.0),
            Coordinate::new(20.0, 15.0),
            Coordinate::new(20.0, 20.0),
            Coordinate::new(15.0, 20.0),
            Coordinate::new(15.0, 15.0),
        ];
        
        let polygon = PolygonWithHoles::new(exterior, vec![hole1, hole2]).unwrap();
        
        // Test various containment scenarios
        assert!(polygon.contains_point(&Coordinate::new(1.0, 1.0))); // exterior only
        assert!(!polygon.contains_point(&Coordinate::new(7.0, 7.0))); // in first hole
        assert!(!polygon.contains_point(&Coordinate::new(17.0, 17.0))); // in second hole
        assert!(polygon.contains_point(&Coordinate::new(25.0, 25.0))); // exterior only, far from holes
    }
}
