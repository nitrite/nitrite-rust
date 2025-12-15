//! Integration tests for batch-optimized write operations (insert and update)
//! These tests validate the `put_all` batch optimization for collections.

use nitrite::collection::insert_if_absent;
use nitrite::common::Value;
use nitrite::doc;
use nitrite::errors::ErrorKind;
use nitrite::filter::{all, field};
use nitrite::index::unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

// ============================================================================
// BATCH INSERT TESTS - POSITIVE SCENARIOS
// ============================================================================

#[test]
fn test_batch_insert_small_batch_uses_sequential() {
    // Batches <= 10 documents use sequential insert path
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("small_batch")?;
            
            let docs: Vec<_> = (0..10).map(|i| {
                doc!{
                    "index": i,
                    "value": (format!("value_{}", i))
                }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 10);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_large_batch_uses_optimized() {
    // Batches > 10 documents use optimized put_all path
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("large_batch")?;
            
            let docs: Vec<_> = (0..100).map(|i| {
                doc!{
                    "index": i,
                    "value": (format!("value_{}", i)),
                    "active": (i % 2 == 0)
                }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 100);
            
            // Verify all documents are queryable
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 100);
            
            // Verify filtering works
            let cursor = collection.find(field("active").eq(true))?;
            assert_eq!(cursor.count(), 50);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_returns_correct_ids() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_ids")?;
            
            let docs: Vec<_> = (0..25).map(|i| {
                doc!{
                    "seq": i
                }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            let ids = result.affected_nitrite_ids();
            
            assert_eq!(ids.len(), 25);
            
            // All IDs should be unique
            let mut unique_ids: Vec<_> = ids.iter().collect();
            unique_ids.sort();
            unique_ids.dedup();
            assert_eq!(unique_ids.len(), 25);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_with_nested_documents() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("nested")?;
            
            let docs: Vec<_> = (0..20).map(|i| {
                doc!{
                    "user": {
                        "name": (format!("user_{}", i)),
                        "profile": {
                            "age": (20 + i),
                            "email": (format!("user{}@example.com", i))
                        }
                    },
                    "settings": {
                        "theme": "dark",
                        "notifications": (i % 2 == 0)
                    }
                }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 20);
            
            // Query nested field
            let cursor = collection.find(field("user.profile.age").gte(25))?;
            assert_eq!(cursor.count(), 15);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_with_arrays() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("arrays")?;
            
            let docs: Vec<_> = (0..15).map(|i| {
                doc!{
                    "id": i,
                    "tags": (vec!["tag1", "tag2", "tag3"]),
                    "scores": (vec![i, i*2, i*3])
                }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 15);
            
            // Query array elements
            let cursor = collection.find(field("tags").elem_match(field("$").eq("tag2")))?;
            assert_eq!(cursor.count(), 15);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// BATCH INSERT TESTS - NEGATIVE SCENARIOS
// ============================================================================

#[test]
fn test_batch_insert_fails_on_unique_index_violation() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("unique_test")?;
            
            // Create unique index on "email" field
            collection.create_index(vec!["email"], &unique_index())?;
            
            // Insert first document
            collection.insert(doc!{ "email": "test@example.com", "name": "First" })?;
            
            // Try to insert a batch with duplicate email
            let docs: Vec<_> = (0..20).map(|i| {
                let email = if i == 10 {
                    "test@example.com".to_string() // Duplicate!
                } else {
                    format!("user{}@example.com", i)
                };
                doc!{
                    "email": email,
                    "name": (format!("User {}", i))
                }
            }).collect();
            
            let result = collection.insert_many(docs);
            assert!(result.is_err());
            
            let err = result.unwrap_err();
            assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
            
            // Original document should still exist, batch should be rolled back
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_duplicate_ids_detected() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("dup_ids")?;
            
            // Insert a document first
            let doc1 = doc!{ "value": "original" };
            let result = collection.insert(doc1.clone())?;
            let existing_id = result.affected_nitrite_ids()[0];
            
            // Try to insert a batch with the same ID
            let mut docs: Vec<_> = (0..15).map(|i| {
                doc!{
                    "value": (format!("new_{}", i))
                }
            }).collect();
            
            // Add document with existing ID
            let mut dup_doc = doc!{ "value": "duplicate" };
            dup_doc.put("_id", Value::NitriteId(existing_id))?;
            docs.push(dup_doc);
            
            let result = collection.insert_many(docs);
            assert!(result.is_err());
            
            let err = result.unwrap_err();
            assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
            
            // Only original document should exist
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_empty_batch() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("empty")?;
            
            let result = collection.insert_many(vec![])?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);
            
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 0);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// BATCH INSERT TESTS - EDGE CASES
// ============================================================================

#[test]
fn test_batch_insert_exactly_at_threshold() {
    // Exactly 10 documents - should use sequential path
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("threshold_10")?;
            
            let docs: Vec<_> = (0..10).map(|i| {
                doc!{ "index": i }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_just_above_threshold() {
    // 11 documents - should use optimized path
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("threshold_11")?;
            
            let docs: Vec<_> = (0..11).map(|i| {
                doc!{ "index": i }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 11);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_with_null_values() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("nulls")?;
            
            let docs: Vec<_> = (0..20).map(|i| {
                doc!{
                    "index": i,
                    "optional": (if i % 2 == 0 { Value::Null } else { Value::from(format!("val_{}", i)) })
                }
            }).collect();
            
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 20);
            
            // Query for null values
            let cursor = collection.find(field("optional").eq(Value::Null))?;
            assert_eq!(cursor.count(), 10);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_concurrent_operations() {
    // Test that batch insert works correctly with other operations
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("concurrent")?;
            
            // Insert initial batch
            let initial_docs: Vec<_> = (0..50).map(|i| {
                doc!{ "batch": 1, "index": i }
            }).collect();
            collection.insert_many(initial_docs)?;
            
            // Insert second batch
            let second_docs: Vec<_> = (50..100).map(|i| {
                doc!{ "batch": 2, "index": i }
            }).collect();
            collection.insert_many(second_docs)?;
            
            // Verify totals
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 100);
            
            let cursor = collection.find(field("batch").eq(1))?;
            assert_eq!(cursor.count(), 50);
            
            let cursor = collection.find(field("batch").eq(2))?;
            assert_eq!(cursor.count(), 50);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// BATCH UPDATE TESTS - POSITIVE SCENARIOS
// ============================================================================

#[test]
fn test_batch_update_small_batch() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("update_small")?;
            
            // Insert documents
            let docs: Vec<_> = (0..10).map(|i| {
                doc!{ "index": i, "status": "pending" }
            }).collect();
            collection.insert_many(docs)?;
            
            // Update all
            let result = collection.update(all(), &doc!{ "status": "completed" })?;
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            
            // Verify all updated
            let cursor = collection.find(field("status").eq("completed"))?;
            assert_eq!(cursor.count(), 10);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_large_batch() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("update_large")?;
            
            // Insert documents
            let docs: Vec<_> = (0..100).map(|i| {
                doc!{ "index": i, "status": "pending", "priority": (i % 3) }
            }).collect();
            collection.insert_many(docs)?;
            
            // Update documents with priority 0 (about 34 documents)
            let result = collection.update(
                field("priority").eq(0),
                &doc!{ "status": "high_priority" }
            )?;
            assert_eq!(result.affected_nitrite_ids().len(), 34);
            
            // Verify updates
            let cursor = collection.find(field("status").eq("high_priority"))?;
            assert_eq!(cursor.count(), 34);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_increments_revision() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("revisions")?;
            
            // Insert documents
            let docs: Vec<_> = (0..20).map(|i| {
                doc!{ "index": i, "value": 0 }
            }).collect();
            collection.insert_many(docs)?;
            
            // Get initial revisions
            let initial_docs: Vec<_> = collection.find(all())?.collect();
            let initial_revisions: Vec<i32> = initial_docs.iter()
                .map(|d| {
                    let doc = d.as_ref().unwrap();
                    let rev = doc.get("_revision").unwrap();
                    *rev.as_i32().unwrap()
                })
                .collect();
            
            // Update all documents
            collection.update(all(), &doc!{ "value": 1 })?;
            
            // Check revisions increased
            let updated_docs: Vec<_> = collection.find(all())?.collect();
            for (i, doc_result) in updated_docs.iter().enumerate() {
                let doc = doc_result.as_ref().unwrap();
                let rev = doc.get("_revision").unwrap();
                let new_rev = *rev.as_i32().unwrap();
                assert!(new_rev > initial_revisions[i], "Revision should increase after update");
            }
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_with_insert_if_absent() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("upsert")?;
            
            // Insert some documents
            let docs: Vec<_> = (0..10).map(|i| {
                doc!{ "index": i, "type": "existing" }
            }).collect();
            collection.insert_many(docs)?;
            
            // Update with insert_if_absent - should create new documents
            let update_doc = doc!{ "type": "upserted", "index": 999 };
            
            // This should insert since no documents match
            let result = collection.update_with_options(
                field("index").eq(999),
                &update_doc,
                &insert_if_absent()
            )?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            
            // Verify upsert worked
            let cursor = collection.find(field("type").eq("upserted"))?;
            assert_eq!(cursor.count(), 1);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// BATCH UPDATE TESTS - NEGATIVE SCENARIOS
// ============================================================================

#[test]
fn test_batch_update_unique_constraint_violation() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("update_unique")?;
            
            // Create unique index
            collection.create_index(vec!["email"], &unique_index())?;
            
            // Insert documents with unique emails
            let docs: Vec<_> = (0..20).map(|i| {
                doc!{
                    "index": i,
                    "email": (format!("user{}@example.com", i))
                }
            }).collect();
            collection.insert_many(docs)?;
            
            // Try to update one document to have a duplicate email
            // This targets index 5 but sets email to user0's email
            let result = collection.update(
                field("index").eq(5),
                &doc!{ "email": "user0@example.com" }
            );
            
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err.kind(), ErrorKind::UniqueConstraintViolation));
            
            // Original email should still be intact
            let cursor = collection.find(field("email").eq("user5@example.com"))?;
            assert_eq!(cursor.count(), 1);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_no_matching_documents() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("no_match")?;
            
            // Insert documents
            let docs: Vec<_> = (0..20).map(|i| {
                doc!{ "index": i, "status": "active" }
            }).collect();
            collection.insert_many(docs)?;
            
            // Try to update with non-matching filter
            let result = collection.update(
                field("index").eq(999),
                &doc!{ "status": "inactive" }
            )?;
            
            // No documents should be updated
            assert_eq!(result.affected_nitrite_ids().len(), 0);
            
            // All documents should still have original status
            let cursor = collection.find(field("status").eq("active"))?;
            assert_eq!(cursor.count(), 20);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// BATCH UPDATE TESTS - EDGE CASES
// ============================================================================

#[test]
fn test_batch_update_single_document() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("single_update")?;
            
            collection.insert(doc!{ "value": 1 })?;
            
            let result = collection.update(all(), &doc!{ "value": 2 })?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            
            let mut cursor = collection.find(all())?;
            let doc = cursor.first().unwrap()?;
            assert_eq!(doc.get("value")?, Value::I32(2));
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_with_complex_document() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("complex_update")?;
            
            // Insert documents
            let docs: Vec<_> = (0..25).map(|i| {
                doc!{
                    "index": i,
                    "user": {
                        "name": (format!("user_{}", i)),
                        "settings": {
                            "theme": "light"
                        }
                    }
                }
            }).collect();
            collection.insert_many(docs)?;
            
            // Update nested field
            let update = doc!{
                "user": {
                    "settings": {
                        "theme": "dark"
                    }
                }
            };
            
            let result = collection.update(
                field("index").lt(10),
                &update
            )?;
            
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// COMBINED INSERT AND UPDATE TESTS
// ============================================================================

#[test]
fn test_batch_insert_then_batch_update() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("insert_update")?;
            
            // Insert large batch
            let docs: Vec<_> = (0..100).map(|i| {
                doc!{
                    "index": i,
                    "status": "new",
                    "processed": false
                }
            }).collect();
            let insert_result = collection.insert_many(docs)?;
            assert_eq!(insert_result.affected_nitrite_ids().len(), 100);
            
            // Update all documents
            let update_result = collection.update(
                all(),
                &doc!{ "status": "processed", "processed": true }
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 100);
            
            // Verify all processed
            let cursor = collection.find(field("processed").eq(true))?;
            assert_eq!(cursor.count(), 100);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_insert_with_index_then_update() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("indexed_ops")?;
            
            // Create unique index
            collection.create_index(vec!["key"], &unique_index())?;
            
            // Insert documents
            let docs: Vec<_> = (0..50).map(|i| {
                doc!{
                    "key": (format!("key_{}", i)),
                    "value": i
                }
            }).collect();
            collection.insert_many(docs)?;
            
            // Update values (not the indexed key)
            let result = collection.update(
                field("value").lt(25),
                &doc!{ "value": 999 }
            )?;
            assert_eq!(result.affected_nitrite_ids().len(), 25);
            
            // Verify updates
            let cursor = collection.find(field("value").eq(999))?;
            assert_eq!(cursor.count(), 25);
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// ROLLBACK VERIFICATION TESTS
// ============================================================================

#[test]
fn test_batch_insert_rollback_on_index_failure() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("rollback_insert")?;
            
            // Create unique index
            collection.create_index(vec!["unique_field"], &unique_index())?;
            
            // Insert initial document
            collection.insert(doc!{ "unique_field": "existing", "marker": "original" })?;
            
            // Try to insert batch with duplicate - middle of batch
            let docs: Vec<_> = (0..30).map(|i| {
                let unique_val = if i == 15 {
                    "existing".to_string() // Duplicate in middle
                } else {
                    format!("unique_{}", i)
                };
                doc!{
                    "unique_field": unique_val,
                    "index": i
                }
            }).collect();
            
            let result = collection.insert_many(docs);
            assert!(result.is_err());
            
            // Only original document should exist (rollback worked)
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);
            
            let mut cursor = collection.find(field("marker").eq("original"))?;
            assert!(cursor.first().is_some());
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_rollback_on_unique_violation() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("rollback_update")?;
            
            // Create unique index
            collection.create_index(vec!["code"], &unique_index())?;
            
            // Insert documents
            let docs: Vec<_> = (0..20).map(|i| {
                doc!{
                    "index": i,
                    "code": (format!("code_{}", i)),
                    "category": (if i < 10 { "A" } else { "B" })
                }
            }).collect();
            collection.insert_many(docs)?;
            
            // Try to update all category A documents to have same code
            // This should fail because it would create duplicates
            let result = collection.update(
                field("category").eq("A"),
                &doc!{ "code": "code_10" } // code_10 belongs to category B
            );
            
            assert!(result.is_err());
            
            // Verify rollback - original codes should still be unique
            for i in 0..10 {
                let cursor = collection.find(field("code").eq(format!("code_{}", i)))?;
                assert_eq!(cursor.count(), 1, "code_{} should still exist", i);
            }
            
            Ok(())
        },
        cleanup,
    )
}

// ============================================================================
// PERFORMANCE VERIFICATION TESTS
// ============================================================================

#[test]
fn test_batch_insert_performance_large() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("performance")?;
            
            // Create 1000 documents
            let docs: Vec<_> = (0..1000).map(|i| {
                doc!{
                    "index": i,
                    "data": (format!("data_{}", i)),
                    "nested": {
                        "level1": {
                            "level2": {
                                "value": i
                            }
                        }
                    }
                }
            }).collect();
            
            let start = std::time::Instant::now();
            let result = collection.insert_many(docs)?;
            let elapsed = start.elapsed();
            
            assert_eq!(result.affected_nitrite_ids().len(), 1000);
            println!("Inserted 1000 documents in {:?}", elapsed);
            
            // Verify all documents are queryable
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1000);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_batch_update_performance_large() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("update_performance")?;
            
            // Insert 1000 documents
            let docs: Vec<_> = (0..1000).map(|i| {
                doc!{
                    "index": i,
                    "status": "pending"
                }
            }).collect();
            collection.insert_many(docs)?;
            
            // Update all 1000 documents
            let start = std::time::Instant::now();
            let result = collection.update(all(), &doc!{ "status": "completed" })?;
            let elapsed = start.elapsed();
            
            assert_eq!(result.affected_nitrite_ids().len(), 1000);
            println!("Updated 1000 documents in {:?}", elapsed);
            
            // Verify all updates
            let cursor = collection.find(field("status").eq("completed"))?;
            assert_eq!(cursor.count(), 1000);
            
            Ok(())
        },
        cleanup,
    )
}
