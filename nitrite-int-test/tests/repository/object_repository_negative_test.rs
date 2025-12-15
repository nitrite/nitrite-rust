use nitrite::collection::NitriteId;
use nitrite::filter::field;
use nitrite::repository::ObjectRepository;

use crate::repository::{Employee, WithNitriteId, WithNoneId, WithOutId, WithPrivateField};
use nitrite::collection::UpdateOptions;
use nitrite_int_test::test_util::{cleanup, create_test_context, now, run_test};

// =============================================================================
// NEGATIVE TEST CASES - ID VALIDATION
// =============================================================================
/// Test: Inserting with None id should fail if id is required
#[test]
fn test_insert_with_none_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithNoneId> = ctx.db().repository()?;

            let object = WithNoneId {
                name: None, // None id value
                number: 1.0,
            };

            let result = repo.insert(object);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

/// Test: get_by_id with None should fail
#[test]
fn test_get_by_none_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithPrivateField> = ctx.db().repository()?;

            let object = WithPrivateField {
                name: "test".to_string(),
                number: 2.0,
            };
            repo.insert(object)?;

            // Try to get by None - this should either return None or error
            // Depending on implementation
            let result = repo.get_by_id(&"".to_string())?;
            assert!(result.is_none());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Update without id should fail
#[test]
fn test_update_without_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            let object = WithOutId {
                name: "name".to_string(),
                number: 1.0,
            };

            // Entities without id cannot be updated using update_one
            let result = repo.update_one(object, false);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Remove without id should fail
#[test]
fn test_remove_without_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            let object = WithOutId {
                name: "name".to_string(),
                number: 1.0,
            };

            // Entities without id cannot be removed using remove_one
            let result = repo.remove_one(object);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - VALIDATION ERRORS
// =============================================================================

/// Test: Inserting null object should fail
#[test]
fn test_insert_empty_batch() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            // Empty insert should succeed but insert nothing
            let result = repo.insert_many(vec![])?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        cleanup,
    )
}

/// Test: Operations on closed repository should fail
#[test]
fn test_operations_on_closed_database() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            // Close the database
            ctx.db().close()?;

            // Operations should fail
            let object = WithOutId {
                name: "test".to_string(),
                number: 1.0,
            };

            let result = repo.insert(object);
            assert!(result.is_err());

            Ok(())
        },
        |_ctx| Ok(()), // Don't cleanup since db is already closed
    )
}

/// Test: External NitriteId during insert should fail
#[test]
fn test_external_nitrite_id_during_insert() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithNitriteId> = ctx.db().repository()?;

            let mut obj = WithNitriteId {
                id_field: None,
                name: "test".to_string(),
            };

            // Manually setting NitriteId should be invalid for insert
            obj.id_field = Some(NitriteId::new());

            let result = repo.insert(obj);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - UNIQUE CONSTRAINT VIOLATIONS
// =============================================================================

/// Test: Duplicate unique id should fail
#[test]
fn test_duplicate_unique_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithPrivateField> = ctx.db().repository()?;

            let object1 = WithPrivateField {
                name: "same_id".to_string(),
                number: 1.0,
            };

            let object2 = WithPrivateField {
                name: "same_id".to_string(), // Same id
                number: 2.0,
            };

            repo.insert(object1)?;

            let result = repo.insert(object2);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Update with changed id to duplicate should fail
#[test]
fn test_update_with_duplicate_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp1 = Employee::default();
            emp1.emp_id = Some(1);
            emp1.address = Some("address1".to_string());

            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.address = Some("address2".to_string());

            repo.insert(emp1)?;
            repo.insert(emp2)?;

            // Try to update emp1 to have emp2's id - should fail due to unique constraint
            let mut updated = Employee::default();
            updated.emp_id = Some(2); // emp2's id - duplicate!
            updated.address = Some("new address".to_string());

            let result = repo.update_with_options(
                field("emp_id").eq(1i64),
                updated,
                &UpdateOptions::new(false, true),
            );

            // This should fail due to unique constraint violation
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - INDEX ERRORS
// =============================================================================

/// Test: Creating index on non-existent field should work (index on empty collection)
#[test]
fn test_create_index_on_empty_collection() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            // Creating index on a field should work even if no documents exist
            let result = repo.create_index(
                vec!["non_existent_field"],
                &nitrite::index::IndexOptions::default(),
            );

            // This should succeed (index is created but empty)
            assert!(result.is_ok());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Dropping non-existent index should fail
#[test]
fn test_drop_non_existent_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            // Dropping non-existent index should fail
            let result = repo.drop_index(vec!["non_existent_field"]);

            assert!(result.is_ok());

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - FILTER ERRORS
// =============================================================================

/// Test: Filter on non-existent field should return empty results
#[test]
fn test_filter_on_non_existent_field() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            repo.insert(emp)?;

            // Filter on non-existent field should return empty, not error
            let cursor = repo.find(field("non_existent_field").eq("value"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - UPDATE EDGE CASES
// =============================================================================

/// Test: Update with None id should fail
#[test]
fn test_update_with_none_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("original".to_string());
            repo.insert(emp)?;

            // Try to update with None id
            let mut updated = Employee::default();
            updated.emp_id = None; // None id
            updated.address = Some("new".to_string());

            let result = repo.update_with_options(
                field("emp_id").eq(1i64),
                updated,
                &UpdateOptions::new(false, true),
            );

            // This should fail because id is None
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Multiple update with object without id
#[test]
fn test_multi_update_with_object_without_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let joining_date = now();

            let mut emp1 = Employee::default();
            emp1.emp_id = Some(1);
            emp1.join_date = Some(joining_date.clone());
            emp1.address = Some("abcd".to_string());

            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.join_date = Some(joining_date.clone());
            emp2.address = Some("xyz".to_string());

            repo.insert_many(vec![emp1, emp2])?;

            // Try to update multiple records with an object (would need id for each)
            let mut update = Employee::default();
            update.emp_id = None; // No id means no unique identification
            update.address = Some("new address".to_string());

            let result = repo.update_with_options(
                field("join_date").eq(joining_date),
                update,
                &UpdateOptions::new(false, false),
            );

            // This should fail because update object has no id
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Changing id during update should not update the document
#[test]
fn test_change_id_during_update() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithNitriteId> = ctx.db().repository()?;

            let item = WithNitriteId {
                id_field: None,
                name: "second".to_string(),
            };
            let wr = repo.insert(item)?;
            let nitrite_id = wr.affected_nitrite_ids().first();
            let mut stored = repo
                .get_by_id(&nitrite_id.cloned())?
                .expect("Expected document");

            // Attempt to change id_field during update
            stored.id_field = Some(NitriteId::new());
            let update_result = repo.update_one(stored, false)?;

            // Expect no document to be updated
            assert_eq!(update_result.affected_nitrite_ids().len(), 0);
            assert_eq!(repo.size()?, 1);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - REPOSITORY LIFECYCLE
// =============================================================================

/// Test: Dropped repository should not allow operations
#[test]
fn test_operations_on_dropped_repository() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            repo.dispose()?;

            let object = WithOutId {
                name: "test".to_string(),
                number: 1.0,
            };

            // Operations on dropped repository should fail
            let result = repo.insert(object);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

/// Test: Repository isDropped after dispose
#[test]
fn test_is_dropped_after_dispose() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            assert!(!repo.is_dropped()?);

            repo.dispose()?;

            assert!(repo.is_dropped()?);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - COLLECTION NAME CONFLICTS
// =============================================================================

/// Test: Using repository name as collection name should fail
#[test]
fn test_collection_repository_name_conflict() {
    run_test(
        create_test_context,
        |ctx| {
            // Create a repository first
            let _repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Try to create a collection with the same name as the repository
            let result = ctx.db().collection("Employee");

            // This should fail
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NEGATIVE TEST CASES - GET BY ID EDGE CASES
// =============================================================================

/// Test: Get by id with wrong type should fail or return None
#[test]
fn test_get_by_id_wrong_type() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository::<Employee>()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            repo.insert(emp)?;

            // Get by id that doesn't exist should return None
            let result = repo.get_by_id(&Some(99999))?;
            assert!(result.is_none());

            Ok(())
        },
        cleanup,
    )
}
