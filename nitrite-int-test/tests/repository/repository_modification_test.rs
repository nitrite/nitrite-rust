use nitrite::collection::{order_by, Document, UpdateOptions};
use nitrite::common::{SortOrder, NON_UNIQUE_INDEX, UNIQUE_INDEX};
use nitrite::filter::{all, field};
use nitrite::index::IndexOptions;
use nitrite::repository::ObjectRepository;

use crate::repository::{generate_company, Employee, Note};
use nitrite_int_test::test_util::{cleanup, create_test_context, now, run_test};
// =============================================================================
// INSERT OPERATIONS
// =============================================================================

#[test]
fn test_insert_single() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            assert_eq!(repo.size()?, 0);

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("test address".to_string());

            let result = repo.insert(emp)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_multiple() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let employees: Vec<Employee> = (0..10)
                .map(|i| {
                    let mut emp = Employee::default();
                    emp.emp_id = Some(i);
                    emp.address = Some(format!("address {}", i));
                    emp
                })
                .collect();

            let result = repo.insert_many(employees)?;
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            assert_eq!(repo.size()?, 10);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_with_nested_objects() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let company = generate_company();
            let note = Note {
                note_id: Some(1),
                text: Some("test note".to_string()),
            };

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.company = Some(company.clone());
            emp.employee_note = Some(note.clone());

            repo.insert(emp)?;

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert_eq!(found.company.unwrap().company_name, company.company_name);
            assert_eq!(found.employee_note.unwrap().text, note.text);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_preserves_order() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let employees: Vec<Employee> = (0..5)
                .map(|i| {
                    let mut emp = Employee::default();
                    emp.emp_id = Some(i);
                    emp
                })
                .collect();

            repo.insert_many(employees)?;

            let options = order_by("emp_id", SortOrder::Ascending);
            let cursor = repo.find_with_options(all(), &options)?;

            let mut expected_id = 0u64;
            for emp in cursor {
                let emp = emp?;
                assert_eq!(emp.emp_id, Some(expected_id));
                expected_id += 1;
            }

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// UPDATE OPERATIONS
// =============================================================================

#[test]
fn test_update_with_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("old address".to_string());
            repo.insert(emp.clone())?;

            // Update using filter and new object
            let mut updated = emp.clone();
            updated.address = Some("new address".to_string());

            let result = repo.update_with_options(
                field("emp_id").eq(1i64),
                updated,
                &UpdateOptions::default(),
            )?;

            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert_eq!(found.address, Some("new address".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_one() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("old address".to_string());
            repo.insert(emp.clone())?;

            // Update using object
            let mut updated = emp.clone();
            updated.address = Some("new address".to_string());

            let result = repo.update_one(updated, false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert_eq!(found.address, Some("new address".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_document() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("old address".to_string());
            repo.insert(emp)?;

            // Update using document
            let mut update_doc = Document::new();
            update_doc.put("address", "new address")?;

            let result = repo.update_document(field("emp_id").eq(1i64), &update_doc, true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert_eq!(found.address, Some("new address".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_multiple() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.join_date = Some(join_date.clone());
                emp.address = Some("old address".to_string());
                repo.insert(emp)?;
            }

            // Update all matching documents
            let mut update_doc = Document::new();
            update_doc.put("address", "new address")?;

            let result =
                repo.update_document(field("join_date").eq(join_date.clone()), &update_doc, false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 5);

            let cursor = repo.find(field("join_date").eq(join_date))?;
            for emp in cursor {
                let emp = emp?;
                assert_eq!(emp.address, Some("new address".to_string()));
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_just_once() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.join_date = Some(join_date.clone());
                emp.address = Some(format!("oldaddress{}", i));
                repo.insert(emp)?;
            }

            // Update only the first matching document
            let mut update_doc = Document::new();
            update_doc.put("address", "newaddress")?;

            let result =
                repo.update_document(field("join_date").eq(join_date.clone()), &update_doc, true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let cursor = repo.find(field("address").text("newaddress"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_upsert_insert() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            assert_eq!(repo.size()?, 0);

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("test".to_string());

            // Upsert when document doesn't exist should insert
            let result = repo.update_with_options(
                field("emp_id").eq(1i64),
                emp,
                &UpdateOptions::new(true, true),
            )?;

            assert_eq!(result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_upsert_update() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("old address".to_string());
            repo.insert(emp.clone())?;

            // Upsert when document exists should update
            let mut updated = emp.clone();
            updated.address = Some("new address".to_string());

            let result = repo.update_with_options(
                field("emp_id").eq(1i64),
                updated,
                &UpdateOptions::new(true, true),
            )?;

            assert_eq!(result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 1);

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert_eq!(found.address, Some("new address".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_null_value() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("test address".to_string());
            emp.join_date = Some(now());
            repo.insert(emp.clone())?;

            // Update to set join_date to null
            let mut updated = emp.clone();
            updated.join_date = None;

            let result = repo.update_one(updated, false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert!(found.join_date.is_none());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_nested_update() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let note = Note {
                note_id: Some(1),
                text: Some("original text".to_string()),
            };

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.employee_note = Some(note);
            repo.insert(emp)?;

            // Update nested field
            let mut update_doc = Document::new();
            update_doc.put("employee_note.text", "updated text")?;

            let result = repo.update_document(field("emp_id").eq(1i64), &update_doc, true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let found = repo.find(field("emp_id").eq(1i64))?.first().unwrap()?;
            assert_eq!(
                found.employee_note.unwrap().text,
                Some("updated text".to_string())
            );

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// REMOVE OPERATIONS
// =============================================================================

#[test]
fn test_remove_with_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            assert_eq!(repo.size()?, 10);

            // Remove specific record
            let result = repo.remove(field("emp_id").eq(5i64), true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 9);

            let found = repo.find(field("emp_id").eq(5i64))?.first();
            assert!(found.is_none());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_remove_multiple() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.join_date = Some(join_date.clone());
                repo.insert(emp)?;
            }

            // Remove all matching (just_once = false)
            let result = repo.remove(field("join_date").eq(join_date), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            assert_eq!(repo.size()?, 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_remove_just_once() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.join_date = Some(join_date.clone());
                repo.insert(emp)?;
            }

            // Remove only one
            let result = repo.remove(field("join_date").eq(join_date), true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_remove_one() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("test".to_string());
            repo.insert(emp.clone())?;

            assert_eq!(repo.size()?, 1);

            let result = repo.remove_one(emp)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_remove_all() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let result = repo.remove(all(), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 10);
            assert_eq!(repo.size()?, 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_remove_non_existent() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            repo.insert(emp)?;

            // Try to remove non-existent
            let result = repo.remove(field("emp_id").eq(999i64), true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);
            assert_eq!(repo.size()?, 1);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// INDEX OPERATIONS
// =============================================================================

#[test]
fn test_create_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            assert!(!repo.has_index(vec!["email_address"])?);

            repo.create_index(vec!["email_address"], &IndexOptions::new(NON_UNIQUE_INDEX))?;

            assert!(repo.has_index(vec!["email_address"])?);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_create_unique_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            repo.create_index(vec!["email_address"], &IndexOptions::new(UNIQUE_INDEX))?;

            let mut emp1 = Employee::default();
            emp1.emp_id = Some(1);
            emp1.email_address = Some("test@example.com".to_string());
            repo.insert(emp1)?;

            // Try to insert duplicate
            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.email_address = Some("test@example.com".to_string());

            let result = repo.insert(emp2);
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_list_indexes() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let initial_count = repo.list_indexes()?.len();

            repo.create_index(vec!["email_address"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            repo.create_index(vec!["blob"], &IndexOptions::new(NON_UNIQUE_INDEX))?;

            let indexes = repo.list_indexes()?;
            assert_eq!(indexes.len(), initial_count + 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_drop_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            repo.create_index(vec!["email_address"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            assert!(repo.has_index(vec!["email_address"])?);

            repo.drop_index(vec!["email_address"])?;
            assert!(!repo.has_index(vec!["email_address"])?);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_drop_all_indexes() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            repo.create_index(vec!["email_address"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            repo.create_index(vec!["blob"], &IndexOptions::new(NON_UNIQUE_INDEX))?;

            repo.drop_all_indexes()?;

            let indexes = repo.list_indexes()?;
            assert_eq!(indexes.len(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_rebuild_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Insert some data
            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.email_address = Some(format!("user{}@example.com", i));
                repo.insert(emp)?;
            }

            repo.create_index(vec!["email_address"], &IndexOptions::new(NON_UNIQUE_INDEX))?;

            // Rebuild index
            repo.rebuild_index(vec!["email_address"])?;

            // Index should still work
            let cursor = repo.find(field("email_address").eq("user2@example.com"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// COLLECTION OPERATIONS
// =============================================================================

#[test]
fn test_clear() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            assert_eq!(repo.size()?, 10);

            repo.clear()?;

            assert_eq!(repo.size()?, 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_size() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            assert_eq!(repo.size()?, 0);

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
                assert_eq!(repo.size()?, ((i + 1)));
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_is_open() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            assert!(repo.is_open()?);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_is_dropped() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            assert!(!repo.is_dropped()?);

            repo.dispose()?;

            assert!(repo.is_dropped()?);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_dispose() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            repo.insert(emp)?;

            repo.dispose()?;

            // After dispose, repository should not exist
            assert!(!ctx.db().has_repository::<Employee>()?);

            Ok(())
        },
        cleanup,
    )
}
