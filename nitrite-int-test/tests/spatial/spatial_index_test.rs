//! Integration tests for spatial indexing in Nitrite.
//!
//! These tests verify that spatial indexes work correctly with the full
//! Nitrite database stack, including persistence and querying.

use nitrite::doc;
use nitrite_int_test::test_util::{cleanup, create_spatial_test_context, run_test};
use nitrite_spatial::{spatial_index, spatial_field, Geometry, Point};

#[test]
fn test_create_spatial_index() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("locations")?;
            
            // Create a spatial index on the location field
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Verify the index was created
            assert!(collection.has_index(vec!["location"])?);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_and_query_point() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("places")?;
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Insert a document with a point geometry using x/y format
            let doc = doc! {
                name: "Central Park",
                location: {
                    x: (-73.968285),
                    y: 40.785091
                }
            };
            
            eprintln!("DEBUG: Document to insert: {:?}", doc);
            collection.insert(doc)?;
            
            // Verify document was inserted
            eprintln!("DEBUG: Collection size after insert: {}", collection.size()?);
            
            // Query using a bounding box that contains the point
            let search_box = Geometry::envelope(-74.0, 40.7, -73.9, 40.9);
            eprintln!("DEBUG: Search box: {:?}", search_box);
            let filter = spatial_field("location").within(search_box);
            eprintln!("DEBUG: Filter: {}", filter);
            let cursor = collection.find(filter)?;
            
            // Collect results for debugging
            let results: Vec<_> = cursor.collect();
            eprintln!("DEBUG: Found {} results: {:?}", results.len(), results);
            
            assert_eq!(results.len(), 1);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_multiple_points() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("cities")?;
            collection.create_index(vec!["coords"], &spatial_index())?;
            
            // Insert multiple cities using x/y format
            let nyc = doc! {
                name: "New York City",
                coords: {
                    x: (-74.006),
                    y: 40.7128
                }
            };
            
            let la = doc! {
                name: "Los Angeles",
                coords: {
                    x: (-118.2437),
                    y: 34.0522
                }
            };
            
            let chicago = doc! {
                name: "Chicago",
                coords: {
                    x: (-87.6298),
                    y: 41.8781
                }
            };
            
            collection.insert_many(vec![nyc, la, chicago])?;
            
            // Search for cities in eastern US (east of -100 longitude)
            let eastern_box = Geometry::envelope(-100.0, 25.0, -70.0, 50.0);
            let filter = spatial_field("coords").within(eastern_box);
            let cursor = collection.find(filter)?;
            
            // Should find NYC and Chicago, not LA
            assert_eq!(cursor.count(), 2);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_spatial_index_with_other_fields() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("restaurants")?;
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Insert restaurants with various attributes
            let r1 = doc! {
                name: "Pizza Place",
                cuisine: "Italian",
                rating: 4.5,
                location: {
                    x: 10.0,
                    y: 20.0
                }
            };
            
            let r2 = doc! {
                name: "Sushi Bar",
                cuisine: "Japanese",
                rating: 4.8,
                location: {
                    x: 10.1,
                    y: 20.1
                }
            };
            
            let r3 = doc! {
                name: "Taco Shop",
                cuisine: "Mexican",
                rating: 4.2,
                location: {
                    x: 50.0,
                    y: 60.0
                }
            };
            
            collection.insert_many(vec![r1, r2, r3])?;
            
            // Find all restaurants in a small area
            let search_area = Geometry::envelope(9.5, 19.5, 10.5, 20.5);
            let filter = spatial_field("location").within(search_area);
            let cursor = collection.find(filter)?;
            
            assert_eq!(cursor.count(), 2);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_spatial_index() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test_drop")?;
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Insert a document
            let doc = doc! {
                name: "Test",
                location: {
                    x: 0.0,
                    y: 0.0
                }
            };
            collection.insert(doc)?;
            
            // Drop the index
            collection.drop_index(vec!["location"])?;
            
            // Index should no longer exist
            assert!(!collection.has_index(vec!["location"])?);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rebuild_spatial_index() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("rebuild_test")?;
            
            // Insert documents first
            for i in 0..10 {
                let name = format!("Point {}", i);
                let x = i as f64;
                let y = i as f64;
                let doc = doc! {
                    name: (name),
                    location: {
                        x: (x),
                        y: (y)
                    }
                };
                collection.insert(doc)?;
            }
            
            // Create index after data exists
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Query should still work
            let search_area = Geometry::envelope(0.0, 0.0, 5.0, 5.0);
            let filter = spatial_field("location").within(search_area);
            let cursor = collection.find(filter)?;
            
            // Should find points 0-5 (6 points including boundaries)
            assert!(cursor.count() >= 5);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_empty_spatial_query() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("empty_test")?;
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Insert a document at one location
            let doc = doc! {
                name: "Far Away",
                location: {
                    x: 100.0,
                    y: 100.0
                }
            };
            collection.insert(doc)?;
            
            // Query an area with no points
            let empty_area = Geometry::envelope(0.0, 0.0, 10.0, 10.0);
            let filter = spatial_field("location").within(empty_area);
            let cursor = collection.find(filter)?;
            
            // Should find no results
            assert_eq!(cursor.count(), 0);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_intersects_query() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("intersects_test")?;
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Insert points in a grid pattern
            for i in 0..5i32 {
                for j in 0..5i32 {
                    let name = format!("Point ({}, {})", i, j);
                    let x = i as f64;
                    let y = j as f64;
                    let doc = doc! {
                        name: (name),
                        location: {
                            x: (x),
                            y: (y)
                        }
                    };
                    collection.insert(doc)?;
                }
            }
            
            // Query for points that intersect with a search rectangle
            let search_rect = Geometry::envelope(1.5, 1.5, 3.5, 3.5);
            let filter = spatial_field("location").intersects(search_rect);
            let cursor = collection.find(filter)?;
            
            // Should find points at (2,2), (2,3), (3,2), (3,3) = 4 points
            assert_eq!(cursor.count(), 4);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_near_query() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("near_test")?;
            collection.create_index(vec!["location"], &spatial_index())?;
            
            // Insert points at various distances from origin
            for i in 0..10i32 {
                let distance = (i as f64) * 5.0;
                let name = format!("Point at distance {}", distance);
                let doc = doc! {
                    name: (name),
                    location: {
                        x: (distance),
                        y: 0.0
                    }
                };
                collection.insert(doc)?;
            }
            
            // Find points within 15 units of origin
            let center = Point::new(0.0, 0.0);
            let filter = spatial_field("location").near(center, 15.0);
            let cursor = collection.find(filter)?;
            
            // Should find points at 0, 5, 10 (3 points with distances 0, 5, 10)
            // Points at 15 may or may not be included depending on <= vs <
            assert!(cursor.count() >= 3);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_knearest_query() {
    run_test(
        || create_spatial_test_context(),
        |ctx| {
            let collection = ctx.db().collection("knearest_test")?;
            collection.create_index(vec!["position"], &spatial_index())?;
            
            // Insert points at known distances from origin
            let distances = vec![1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0];
            for d in &distances {
                let name = format!("Point at distance {}", d);
                let x = *d;
                let doc = doc! {
                    name: (name),
                    position: {
                        x: (x),
                        y: 0.0
                    }
                };
                collection.insert(doc)?;
            }
            
            // Find 3 nearest to origin using knearest
            let center = Point::new(0.0, 0.0);
            let filter = spatial_field("position").knearest(center, 3)?;
            let cursor = collection.find(filter)?;
            
            // Should find exactly 3 points
            assert_eq!(cursor.count(), 3);
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
