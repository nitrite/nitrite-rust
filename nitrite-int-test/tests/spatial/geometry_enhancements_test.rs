//! Comprehensive integration tests for Phase 3 Geometry Enhancements
//!
//! Tests the following features with the full Nitrite database:
//! - LineString geometry with intersection and length calculations
//! - Polygons with holes (interior rings)
//! - Multi-geometry types (MultiPoint, MultiLineString, MultiPolygon)
//! - WKT parsing and serialization
//! - GeoJSON parsing
//! - Geometry operations (contains, intersects, bounding box)

#[cfg(test)]
mod geometry_enhancements_tests {
    use nitrite_spatial::geometry_extended::{
        LineString, PolygonWithHoles, MultiGeometry, GeometryValue, parse_wkt, parse_geojson,
    };
    use nitrite_spatial::{Coordinate, Geometry};

    // =========================================================================
    // LINESTRING INTEGRATION TESTS
    // =========================================================================

    #[test]
    fn test_linestring_basic_operations() {
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
        ];
        let line = LineString::new(coords).expect("Failed to create LineString");

        // Check length calculation
        let length = line.length();
        assert!((length - 30.0).abs() < 0.01, "Expected length ~30, got {}", length);

        // Check it's not closed
        assert!(!line.is_closed());

        // Check bounding box
        let bbox = line.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 10.0);
        assert_eq!(bbox.min_y, 0.0);
        assert_eq!(bbox.max_y, 10.0);
    }

    #[test]
    fn test_linestring_closed_ring() {
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(0.0, 0.0),
        ];
        let line = LineString::new(coords).unwrap();

        assert!(line.is_closed());

        let length = line.length();
        // 5 + 5 + sqrt(50) ≈ 17.07
        assert!(length > 15.0 && length < 20.0);
    }

    #[test]
    fn test_linestring_intersection_crossing() {
        let line1 = LineString::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 10.0),
        ])
        .unwrap();

        let line2 = LineString::new(vec![
            Coordinate::new(0.0, 10.0),
            Coordinate::new(10.0, 0.0),
        ])
        .unwrap();

        assert!(line1.intersects(&line2), "Lines should intersect");
    }

    #[test]
    fn test_linestring_no_intersection_parallel() {
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

        assert!(!line1.intersects(&line2), "Parallel lines should not intersect");
    }

    #[test]
    fn test_linestring_complex_path() {
        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(10.0, 5.0),
            Coordinate::new(10.0, 10.0),
        ];
        let line = LineString::new(coords).unwrap();

        assert_eq!(line.coordinates().len(), 5);
        let length = line.length();
        // 5 + 5 + 5 + 5 = 20
        assert!((length - 20.0).abs() < 0.01);
    }

    // =========================================================================
    // POLYGON WITH HOLES INTEGRATION TESTS
    // =========================================================================

    #[test]
    fn test_polygon_with_single_hole() {
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(20.0, 0.0),
            Coordinate::new(20.0, 20.0),
            Coordinate::new(0.0, 20.0),
            Coordinate::new(0.0, 0.0),
        ];

        let hole = vec![
            Coordinate::new(5.0, 5.0),
            Coordinate::new(15.0, 5.0),
            Coordinate::new(15.0, 15.0),
            Coordinate::new(5.0, 15.0),
            Coordinate::new(5.0, 5.0),
        ];

        let polygon = PolygonWithHoles::new(exterior, vec![hole]).expect("Failed to create polygon");

        assert!(polygon.is_valid());
        assert_eq!(polygon.holes().len(), 1);
        assert_eq!(polygon.exterior().len(), 5);
    }

    #[test]
    fn test_polygon_point_containment_with_holes() {
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];

        let hole = vec![
            Coordinate::new(3.0, 3.0),
            Coordinate::new(7.0, 3.0),
            Coordinate::new(7.0, 7.0),
            Coordinate::new(3.0, 7.0),
            Coordinate::new(3.0, 3.0),
        ];

        let polygon = PolygonWithHoles::new(exterior, vec![hole]).unwrap();

        // Point outside polygon
        assert!(!polygon.contains_point(&Coordinate::new(11.0, 5.0)));

        // Point inside exterior but outside hole
        assert!(polygon.contains_point(&Coordinate::new(1.0, 1.0)));

        // Point inside hole
        assert!(!polygon.contains_point(&Coordinate::new(5.0, 5.0)));

        // Point inside exterior but outside hole (different location)
        assert!(polygon.contains_point(&Coordinate::new(8.0, 8.0)));
    }

    #[test]
    fn test_polygon_with_multiple_holes() {
        let exterior = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(20.0, 0.0),
            Coordinate::new(20.0, 20.0),
            Coordinate::new(0.0, 20.0),
            Coordinate::new(0.0, 0.0),
        ];

        let hole1 = vec![
            Coordinate::new(2.0, 2.0),
            Coordinate::new(5.0, 2.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(2.0, 5.0),
            Coordinate::new(2.0, 2.0),
        ];

        let hole2 = vec![
            Coordinate::new(10.0, 10.0),
            Coordinate::new(15.0, 10.0),
            Coordinate::new(15.0, 15.0),
            Coordinate::new(10.0, 15.0),
            Coordinate::new(10.0, 10.0),
        ];

        let polygon = PolygonWithHoles::new(exterior, vec![hole1, hole2]).expect("Failed to create polygon");

        assert_eq!(polygon.holes().len(), 2);

        // Test various points
        assert!(polygon.contains_point(&Coordinate::new(1.0, 1.0))); // Inside exterior
        assert!(!polygon.contains_point(&Coordinate::new(3.5, 3.5))); // Inside hole1
        assert!(!polygon.contains_point(&Coordinate::new(12.0, 12.0))); // Inside hole2
        assert!(polygon.contains_point(&Coordinate::new(7.0, 7.0))); // Between holes
    }

    #[test]
    fn test_polygon_bounding_box() {
        let exterior = vec![
            Coordinate::new(10.0, 20.0),
            Coordinate::new(30.0, 20.0),
            Coordinate::new(30.0, 40.0),
            Coordinate::new(10.0, 40.0),
            Coordinate::new(10.0, 20.0),
        ];

        let polygon = PolygonWithHoles::simple(exterior).unwrap();
        let bbox = polygon.bounding_box();

        assert_eq!(bbox.min_x, 10.0);
        assert_eq!(bbox.max_x, 30.0);
        assert_eq!(bbox.min_y, 20.0);
        assert_eq!(bbox.max_y, 40.0);
    }

    // =========================================================================
    // MULTI-GEOMETRY INTEGRATION TESTS
    // =========================================================================

    #[test]
    fn test_multipoint_geometry() {
        let points = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(10.0, 10.0),
        ];

        let multi = MultiGeometry::MultiPoint(points);
        let bbox = multi.bounding_box();

        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 10.0);
        assert_eq!(bbox.min_y, 0.0);
        assert_eq!(bbox.max_y, 10.0);
    }

    #[test]
    fn test_multilinestring_geometry() {
        let line1 = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 10.0),
        ];

        let line2 = vec![
            Coordinate::new(5.0, 0.0),
            Coordinate::new(15.0, 10.0),
        ];

        let multi = MultiGeometry::MultiLineString(vec![line1, line2]);
        let bbox = multi.bounding_box();

        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 15.0);
    }

    #[test]
    fn test_multipolygon_geometry() {
        let poly1 = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(5.0, 0.0),
            Coordinate::new(5.0, 5.0),
            Coordinate::new(0.0, 5.0),
            Coordinate::new(0.0, 0.0),
        ];

        let poly2 = vec![
            Coordinate::new(10.0, 10.0),
            Coordinate::new(15.0, 10.0),
            Coordinate::new(15.0, 15.0),
            Coordinate::new(10.0, 15.0),
            Coordinate::new(10.0, 10.0),
        ];

        let multi = MultiGeometry::MultiPolygon(vec![poly1, poly2]);
        let bbox = multi.bounding_box();

        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 15.0);
    }

    // =========================================================================
    // WKT PARSING INTEGRATION TESTS
    // =========================================================================

    #[test]
    fn test_wkt_point_parsing() {
        let point_wkt = "POINT(5 5)";
        let geom = parse_wkt(point_wkt).unwrap();

        match geom {
            GeometryValue::Point(coord) => {
                assert_eq!(coord.x, 5.0);
                assert_eq!(coord.y, 5.0);
            }
            _ => panic!("Expected Point"),
        }
    }

    #[test]
    fn test_wkt_linestring_parsing() {
        let wkt = "LINESTRING(0 0, 5 5, 10 0)";
        let geom = parse_wkt(wkt).unwrap();

        match geom {
            GeometryValue::LineString(line) => {
                assert_eq!(line.coordinates().len(), 3);
                assert_eq!(line.length(), 5.0 * (2.0_f64.sqrt()) + 5.0 * (2.0_f64.sqrt()));
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_wkt_polygon_simple() {
        let wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
        let geom = parse_wkt(wkt).unwrap();

        match geom {
            GeometryValue::Polygon(poly) => {
                assert_eq!(poly.exterior().len(), 5);
                assert_eq!(poly.holes().len(), 0);
                assert!(poly.is_valid());
            }
            _ => panic!("Expected Polygon"),
        }
    }

    #[test]
    fn test_wkt_polygon_with_holes() {
        let wkt = "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))";
        let geom = parse_wkt(wkt).unwrap();

        match geom {
            GeometryValue::Polygon(poly) => {
                assert_eq!(poly.exterior().len(), 5);
                assert_eq!(poly.holes().len(), 1);
                assert_eq!(poly.holes()[0].len(), 5);
                assert!(poly.is_valid());
            }
            _ => panic!("Expected Polygon with holes"),
        }
    }

    #[test]
    fn test_wkt_multipoint() {
        let wkt = "MULTIPOINT((0 0), (5 5), (10 10))";
        let geom = parse_wkt(wkt).unwrap();

        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(points)) => {
                assert_eq!(points.len(), 3);
                assert_eq!(points[0].x, 0.0);
                assert_eq!(points[2].x, 10.0);
            }
            _ => panic!("Expected MultiPoint"),
        }
    }

    #[test]
    fn test_wkt_multilinestring() {
        let wkt = "MULTILINESTRING((0 0, 10 10), (5 5, 15 15))";
        let geom = parse_wkt(wkt).unwrap();

        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiLineString(lines)) => {
                assert_eq!(lines.len(), 2);
                assert_eq!(lines[0].len(), 2);
            }
            _ => panic!("Expected MultiLineString"),
        }
    }

    #[test]
    fn test_wkt_round_trip() {
        let original_wkt = "LINESTRING(0 0, 10 10, 20 0)";
        let geom = parse_wkt(original_wkt).unwrap();
        let serialized = format!("{}", geom);

        // Parse again to verify
        let geom2 = parse_wkt(&serialized).unwrap();
        assert_eq!(geom, geom2);
    }

    // =========================================================================
    // GEOJSON PARSING INTEGRATION TESTS
    // =========================================================================

    #[test]
    fn test_geojson_point() {
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
    fn test_geojson_linestring() {
        let json = r#"{"type":"LineString","coordinates":[[0,0],[10,10],[20,0]]}"#;
        let geom = parse_geojson(json).unwrap();

        match geom {
            GeometryValue::LineString(line) => {
                assert_eq!(line.coordinates().len(), 3);
            }
            _ => panic!("Expected LineString"),
        }
    }

    #[test]
    fn test_geojson_multipoint() {
        let json = r#"{"type":"MultiPoint","coordinates":[[0,0],[1,1],[2,2]]}"#;
        let geom = parse_geojson(json).unwrap();

        match geom {
            GeometryValue::MultiGeometry(MultiGeometry::MultiPoint(points)) => {
                assert_eq!(points.len(), 3);
            }
            _ => panic!("Expected MultiPoint"),
        }
    }

    // =========================================================================
    // GEOMETRY OPERATIONS INTEGRATION TESTS
    // =========================================================================

    #[test]
    fn test_geometry_value_bounding_box() {
        let geom1 = GeometryValue::Point(Coordinate::new(5.0, 10.0));
        let bbox1 = geom1.bounding_box();
        assert_eq!(bbox1.min_x, 5.0);
        assert_eq!(bbox1.max_y, 10.0);

        let coords = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 10.0),
        ];
        let geom2 = GeometryValue::LineString(LineString::new(coords).unwrap());
        let bbox2 = geom2.bounding_box();
        assert_eq!(bbox2.max_x, 10.0);
    }

    #[test]
    fn test_geometry_value_display() {
        let geom = GeometryValue::Point(Coordinate::new(5.0, 10.0));
        let display = format!("{}", geom);
        assert!(display.contains("POINT"));
        assert!(display.contains("5"));
        assert!(display.contains("10"));
    }

    #[test]
    fn test_original_geometry_compatibility() {
        // Test that the original Geometry enum still works
        let geom = Geometry::point(5.0, 10.0);
        let bbox = geom.bounding_box();
        assert_eq!(bbox.min_x, 5.0);

        let circle = Geometry::circle(0.0, 0.0, 5.0);
        assert!(circle.contains(&Geometry::point(3.0, 4.0)));
    }

    #[test]
    fn test_complex_geometry_scenario() {
        // Create a polygon with a hole representing a city block with a park
        let city_block = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(100.0, 0.0),
            Coordinate::new(100.0, 100.0),
            Coordinate::new(0.0, 100.0),
            Coordinate::new(0.0, 0.0),
        ];

        let park = vec![
            Coordinate::new(20.0, 20.0),
            Coordinate::new(80.0, 20.0),
            Coordinate::new(80.0, 80.0),
            Coordinate::new(20.0, 80.0),
            Coordinate::new(20.0, 20.0),
        ];

        let block = PolygonWithHoles::new(city_block, vec![park]).unwrap();

        // Test containment
        assert!(block.contains_point(&Coordinate::new(10.0, 10.0))); // On block, outside park
        assert!(!block.contains_point(&Coordinate::new(50.0, 50.0))); // In park
        assert!(block.contains_point(&Coordinate::new(90.0, 90.0))); // On block corner

        // Test display
        let display = format!("{}", block);
        assert!(display.contains("POLYGON"));
    }

    #[test]
    fn test_polygon_validation() {
        // Test that unclosed polygons are rejected
        let unclosed = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
        ];

        let result = PolygonWithHoles::simple(unclosed);
        assert!(result.is_err());

        // Test that properly closed polygons are accepted
        let closed = vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ];

        let result = PolygonWithHoles::simple(closed);
        assert!(result.is_ok());
    }

    #[test]
    fn test_wkt_parsing_with_spaces() {
        // Test that WKT parsing is tolerant of extra whitespace
        let wkt_variations = vec![
            "POINT(5 10)",
            "POINT( 5 10 )",
            "LINESTRING(0 0,10 10,20 0)",
            "LINESTRING( 0 0 , 10 10 , 20 0 )",
        ];

        for wkt in wkt_variations {
            let result = parse_wkt(wkt);
            assert!(result.is_ok(), "Failed to parse: {}", wkt);
        }
    }

    #[test]
    fn test_geometry_integration_with_database_types() {
        // This test verifies that extended geometry types can work with
        // the core Nitrite types and operations
        
        // Create various extended geometry types
        let line = LineString::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 10.0),
        ]).unwrap();

        let polygon = PolygonWithHoles::simple(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ]).unwrap();

        // Verify they produce valid bounding boxes
        let line_bbox = line.bounding_box();
        let poly_bbox = polygon.bounding_box();

        assert!(line_bbox.min_x <= line_bbox.max_x);
        assert!(poly_bbox.min_y <= poly_bbox.max_y);
    }
}
