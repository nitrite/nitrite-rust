use std::hash::Hash;

/// A 2D bounding box represented by minimum and maximum coordinates.
///
/// `BoundingBox` defines a rectangular area in 2D space using the minimum
/// (min_x, min_y) and maximum (max_x, max_y) corners. It's used extensively
/// in spatial indexing for representing spatial boundaries and query regions.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite_spatial::BoundingBox;
///
/// // Create a bounding box for a region from (0,0) to (100,100)
/// let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
///
/// // Check if a point is within the bounding box
/// if bbox.contains_point(50.0, 50.0) {
///     println!("Point is inside the bounding box");
/// }
/// ```
#[derive(Clone, PartialEq, Default, Debug, serde::Deserialize, serde::Serialize)]
pub struct BoundingBox {
    /// Minimum X coordinate
    pub min_x: f64,
    /// Minimum Y coordinate
    pub min_y: f64,
    /// Maximum X coordinate
    pub max_x: f64,
    /// Maximum Y coordinate
    pub max_y: f64,
}

impl Eq for BoundingBox {}

impl PartialOrd for BoundingBox {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BoundingBox {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.min_x
            .partial_cmp(&other.min_x)
            .unwrap()
            .then(self.min_y.partial_cmp(&other.min_y).unwrap())
            .then(self.max_x.partial_cmp(&other.max_x).unwrap())
            .then(self.max_y.partial_cmp(&other.max_y).unwrap())
    }
}

impl Hash for BoundingBox {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.min_x.to_bits().hash(state);
        self.min_y.to_bits().hash(state);
        self.max_x.to_bits().hash(state);
        self.max_y.to_bits().hash(state);
    }
}

impl std::fmt::Display for BoundingBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoundingBox({}, {}, {}, {})", self.min_x, self.min_y, self.max_x, self.max_y)
    }
}

impl BoundingBox {
    /// Creates a new bounding box with the specified coordinates.
    ///
    /// # Arguments
    ///
    /// * `min_x` - Minimum X coordinate
    /// * `min_y` - Minimum Y coordinate
    /// * `max_x` - Maximum X coordinate
    /// * `max_y` - Maximum Y coordinate
    ///
    /// # Returns
    ///
    /// A new `BoundingBox` instance
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> BoundingBox {
        BoundingBox {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }

    /// Converts this bounding box to a pretty-printed JSON string.
    pub fn to_pretty_json(&self, indent: usize) -> String {
        let mut json_str = String::new();
        json_str.push_str("{\n");
        json_str.push_str(&format!(
            "{:indent$}\"min_x\": {},\n",
            "",
            self.min_x,
            indent = indent + 2
        ));
        json_str.push_str(&format!(
            "{:indent$}\"min_y\": {},\n",
            "",
            self.min_y,
            indent = indent + 2
        ));
        json_str.push_str(&format!(
            "{:indent$}\"max_x\": {},\n",
            "",
            self.max_x,
            indent = indent + 2
        ));
        json_str.push_str(&format!(
            "{:indent$}\"max_y\": {}\n",
            "",
            self.max_y,
            indent = indent + 2
        ));
        json_str.push('}');
        json_str
    }

    /// Converts this bounding box to a debug string with formatting.
    pub fn to_debug_string(&self, indent: usize) -> String {
        let mut json_str = String::new();
        json_str.push_str("{\n");
        json_str.push_str(&format!(
            "{:indent$}\"min_x\": {},\n",
            "",
            self.min_x,
            indent = indent + 2
        ));
        json_str.push_str(&format!(
            "{:indent$}\"min_y\": {},\n",
            "",
            self.min_y,
            indent = indent + 2
        ));
        json_str.push_str(&format!(
            "{:indent$}\"max_x\": {},\n",
            "",
            self.max_x,
            indent = indent + 2
        ));
        json_str.push_str(&format!(
            "{:indent$}\"max_y\": {}\n",
            "",
            self.max_y,
            indent = indent + 2
        ));
        json_str.push('}');
        json_str
    }

    /// Returns the width of the bounding box.
    pub fn width(&self) -> f64 {
        self.max_x - self.min_x
    }

    /// Returns the height of the bounding box.
    pub fn height(&self) -> f64 {
        self.max_y - self.min_y
    }

    /// Returns the area of the bounding box.
    pub fn area(&self) -> f64 {
        self.width() * self.height()
    }

    /// Returns the center point of the bounding box.
    pub fn center(&self) -> (f64, f64) {
        ((self.min_x + self.max_x) / 2.0, (self.min_y + self.max_y) / 2.0)
    }

    /// Checks if this bounding box contains a point.
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        x >= self.min_x && x <= self.max_x && y >= self.min_y && y <= self.max_y
    }

    /// Checks if this bounding box contains another bounding box.
    pub fn contains(&self, other: &BoundingBox) -> bool {
        other.min_x >= self.min_x && other.max_x <= self.max_x
            && other.min_y >= self.min_y && other.max_y <= self.max_y
    }

    /// Checks if this bounding box intersects another bounding box.
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        self.min_x <= other.max_x && self.max_x >= other.min_x
            && self.min_y <= other.max_y && self.max_y >= other.min_y
    }

    /// Returns the union of this bounding box with another.
    pub fn union(&self, other: &BoundingBox) -> BoundingBox {
        BoundingBox::new(
            self.min_x.min(other.min_x),
            self.min_y.min(other.min_y),
            self.max_x.max(other.max_x),
            self.max_y.max(other.max_y),
        )
    }

    /// Returns the intersection of this bounding box with another, if they intersect.
    pub fn intersection(&self, other: &BoundingBox) -> Option<BoundingBox> {
        if !self.intersects(other) {
            return None;
        }
        Some(BoundingBox::new(
            self.min_x.max(other.min_x),
            self.min_y.max(other.min_y),
            self.max_x.min(other.max_x),
            self.max_y.min(other.max_y),
        ))
    }

    /// Checks if this bounding box is a point (zero area).
    pub fn is_point(&self) -> bool {
        self.min_x == self.max_x && self.min_y == self.max_y
    }

    /// Checks if this bounding box is valid (min <= max).
    pub fn is_valid(&self) -> bool {
        self.min_x <= self.max_x && self.min_y <= self.max_y
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_new() {
        let bbox = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        assert_eq!(bbox.min_x, 1.0);
        assert_eq!(bbox.min_y, 2.0);
        assert_eq!(bbox.max_x, 3.0);
        assert_eq!(bbox.max_y, 4.0);
    }

    #[test]
    fn test_default() {
        let bbox = BoundingBox::default();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.min_y, 0.0);
        assert_eq!(bbox.max_x, 0.0);
        assert_eq!(bbox.max_y, 0.0);
    }

    #[test]
    fn test_clone() {
        let bbox1 = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let bbox2 = bbox1.clone();
        assert_eq!(bbox1, bbox2);
    }

    #[test]
    fn test_equality() {
        let bbox1 = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let bbox2 = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let bbox3 = BoundingBox::new(1.0, 2.0, 3.0, 5.0);
        
        assert_eq!(bbox1, bbox2);
        assert_ne!(bbox1, bbox3);
    }

    #[test]
    fn test_ordering() {
        let bbox1 = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let bbox2 = BoundingBox::new(2.0, 2.0, 3.0, 4.0);
        let bbox3 = BoundingBox::new(1.0, 3.0, 3.0, 4.0);
        
        assert!(bbox1 < bbox2);
        assert!(bbox1 < bbox3);
        assert!(bbox2 > bbox1);
    }

    #[test]
    fn test_hash() {
        let bbox1 = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let bbox2 = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let bbox3 = BoundingBox::new(5.0, 6.0, 7.0, 8.0);
        
        let mut set = HashSet::new();
        set.insert(bbox1.clone());
        
        assert!(set.contains(&bbox2));
        assert!(!set.contains(&bbox3));
    }

    #[test]
    fn test_width_height_area() {
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 5.0);
        assert_eq!(bbox.width(), 10.0);
        assert_eq!(bbox.height(), 5.0);
        assert_eq!(bbox.area(), 50.0);
    }

    #[test]
    fn test_center() {
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let (cx, cy) = bbox.center();
        assert_eq!(cx, 5.0);
        assert_eq!(cy, 5.0);
    }

    #[test]
    fn test_contains_point() {
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        
        assert!(bbox.contains_point(5.0, 5.0));  // Inside
        assert!(bbox.contains_point(0.0, 0.0));  // Corner
        assert!(bbox.contains_point(10.0, 10.0)); // Corner
        assert!(bbox.contains_point(5.0, 0.0));  // Edge
        assert!(!bbox.contains_point(-1.0, 5.0)); // Outside
        assert!(!bbox.contains_point(11.0, 5.0)); // Outside
    }

    #[test]
    fn test_contains_bbox() {
        let outer = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let inner = BoundingBox::new(2.0, 2.0, 8.0, 8.0);
        let partial = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        let outside = BoundingBox::new(20.0, 20.0, 30.0, 30.0);
        
        assert!(outer.contains(&inner));
        assert!(!outer.contains(&partial));
        assert!(!outer.contains(&outside));
        assert!(!inner.contains(&outer));
    }

    #[test]
    fn test_intersects() {
        let bbox1 = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let bbox2 = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        let bbox3 = BoundingBox::new(20.0, 20.0, 30.0, 30.0);
        let bbox4 = BoundingBox::new(10.0, 10.0, 20.0, 20.0); // Touches corner
        
        assert!(bbox1.intersects(&bbox2));
        assert!(bbox2.intersects(&bbox1));
        assert!(!bbox1.intersects(&bbox3));
        assert!(bbox1.intersects(&bbox4)); // Touching counts as intersection
    }

    #[test]
    fn test_union() {
        let bbox1 = BoundingBox::new(0.0, 0.0, 5.0, 5.0);
        let bbox2 = BoundingBox::new(3.0, 3.0, 10.0, 10.0);
        
        let union = bbox1.union(&bbox2);
        assert_eq!(union.min_x, 0.0);
        assert_eq!(union.min_y, 0.0);
        assert_eq!(union.max_x, 10.0);
        assert_eq!(union.max_y, 10.0);
    }

    #[test]
    fn test_intersection() {
        let bbox1 = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let bbox2 = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        let bbox3 = BoundingBox::new(20.0, 20.0, 30.0, 30.0);
        
        let inter = bbox1.intersection(&bbox2);
        assert!(inter.is_some());
        let inter = inter.unwrap();
        assert_eq!(inter.min_x, 5.0);
        assert_eq!(inter.min_y, 5.0);
        assert_eq!(inter.max_x, 10.0);
        assert_eq!(inter.max_y, 10.0);
        
        assert!(bbox1.intersection(&bbox3).is_none());
    }

    #[test]
    fn test_is_point() {
        let point_bbox = BoundingBox::new(5.0, 5.0, 5.0, 5.0);
        let normal_bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        
        assert!(point_bbox.is_point());
        assert!(!normal_bbox.is_point());
    }

    #[test]
    fn test_is_valid() {
        let valid = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let invalid = BoundingBox::new(10.0, 10.0, 0.0, 0.0);
        let point = BoundingBox::new(5.0, 5.0, 5.0, 5.0);
        
        assert!(valid.is_valid());
        assert!(!invalid.is_valid());
        assert!(point.is_valid());
    }

    #[test]
    fn test_serialization() {
        let bbox = BoundingBox::new(1.5, 2.5, 3.5, 4.5);
        let json = serde_json::to_string(&bbox).unwrap();
        let deserialized: BoundingBox = serde_json::from_str(&json).unwrap();
        assert_eq!(bbox, deserialized);
    }

    #[test]
    fn test_to_pretty_json() {
        let bbox = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let json = bbox.to_pretty_json(0);
        assert!(json.contains("\"min_x\": 1"));
        assert!(json.contains("\"min_y\": 2"));
        assert!(json.contains("\"max_x\": 3"));
        assert!(json.contains("\"max_y\": 4"));
    }

    #[test]
    fn test_display() {
        let bbox = BoundingBox::new(1.0, 2.0, 3.0, 4.0);
        let display = format!("{}", bbox);
        assert_eq!(display, "BoundingBox(1, 2, 3, 4)");
    }

    #[test]
    fn test_negative_coordinates() {
        let bbox = BoundingBox::new(-10.0, -5.0, 10.0, 5.0);
        assert_eq!(bbox.width(), 20.0);
        assert_eq!(bbox.height(), 10.0);
        let (cx, cy) = bbox.center();
        assert_eq!(cx, 0.0);
        assert_eq!(cy, 0.0);
    }

    #[test]
    fn test_large_coordinates() {
        let bbox = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
        assert_eq!(bbox.width(), 360.0);
        assert_eq!(bbox.height(), 180.0);
        assert!(bbox.contains_point(0.0, 0.0));
    }

    #[test]
    fn test_self_intersection() {
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        assert!(bbox.intersects(&bbox));
        let inter = bbox.intersection(&bbox);
        assert_eq!(inter, Some(bbox));
    }
}
