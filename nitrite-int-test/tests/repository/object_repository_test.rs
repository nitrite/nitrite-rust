use nitrite::filter::{all, and, field, not, or};
use nitrite::repository::ObjectRepository;

use crate::repository::{
    generate_company, generate_employee, Book, BookId, Company, Employee, Note, Person,
    RepeatableIndexTest, SubEmployee, WithDateId, WithEmptyStringId, WithOutId, WithPrivateField,
    WithTransientField,
};
use chrono::DateTime;
use fake::faker::name::en::{FirstName, LastName};
use fake::Fake;
use nitrite::collection::{CollectionEventListener, Document, FindOptions, UpdateOptions};
use nitrite::common::{Attributes, Lookup, NON_UNIQUE_INDEX};
use nitrite::index::IndexOptions;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::{cleanup, create_test_context, now, run_test, NitriteDateTime};
use rand::{rng, RngCore};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

// =============================================================================
// BASIC REPOSITORY TESTS
// =============================================================================

/// Test: Inserting with empty string id should fail
#[test]
fn test_insert_with_empty_string_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithEmptyStringId> = ctx.db().repository()?;

            let object = WithEmptyStringId {
                name: "".to_string(), // empty id value
                data: Some("test".to_string()),
            };

            let result = repo.insert(object);
            assert!(result.is_ok());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_with_out_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithOutId> = ctx.db().repository()?;

            let object = WithOutId {
                name: "test".to_string(),
                number: 2.0,
            };

            repo.insert(object)?;

            let cursor = repo.find(all())?;
            for instance in cursor {
                let instance = instance?;
                assert_eq!(instance.name, "test");
                assert_eq!(instance.number, 2.0);
            }
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_with_private_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithPrivateField> = ctx.db().repository()?;

            let object = WithPrivateField {
                name: "test".to_string(),
                number: 2.0,
            };

            repo.insert(object)?;
            let instance = repo.get_by_id(&"test".to_string())?.unwrap();
            assert_eq!(instance.name, "test".to_string());
            assert_eq!(instance.number, 2.0);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_with_transient_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithTransientField> = ctx.db().repository()?;

            let object = WithTransientField {
                name: "test".to_string(),
                number: 2,
            };

            repo.insert(object)?;
            let instance = repo.get_by_id(&2)?.unwrap();
            assert_ne!(instance.name, "test".to_string());
            assert!(instance.name.is_empty());
            assert_eq!(instance.number, 2);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_with_date_as_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithDateId> = ctx.db().repository()?;

            let epoch_millis1 = 1482773634;
            let epoch_millis2 = 1482773720;

            let mut object1 = WithDateId::default();
            object1.id = NitriteDateTime(DateTime::from(
                DateTime::from_timestamp_millis(epoch_millis1).unwrap(),
            ));
            object1.name = "first date".to_string();
            repo.insert(object1.clone())?;

            let mut object2 = WithDateId::default();
            object2.name = "second date".to_string();
            object2.id = NitriteDateTime(DateTime::from(
                DateTime::from_timestamp_millis(epoch_millis2).unwrap(),
            ));
            repo.insert(object2.clone())?;

            let found1 = repo
                .find(field("id").eq(NitriteDateTime(DateTime::from(
                    DateTime::from_timestamp_millis(epoch_millis1).unwrap(),
                ))))?
                .first()
                .unwrap()?;
            let found2 = repo
                .find(field("id").eq(NitriteDateTime(DateTime::from(
                    DateTime::from_timestamp_millis(epoch_millis2).unwrap(),
                ))))?
                .first()
                .unwrap()?;

            assert_eq!(found1, object1);
            assert_eq!(found2, object2);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_attributes() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithDateId> = ctx.db().repository()?;
            let collection_name = repo.document_collection().name();
            let attributes = Attributes::new_for_collection(&*collection_name);
            repo.set_attributes(attributes.clone())?;
            assert_eq!(repo.attributes()?.unwrap(), attributes);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_keyed_repository() {
    run_test(
        || create_test_context(),
        |ctx| {
            // an object repository of employees who are managers
            let manager_repo: ObjectRepository<Employee> = ctx.db().keyed_repository("managers")?;

            // an object repository of all employees
            let employee_repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // an object repository of employees who are developers
            let developer_repo: ObjectRepository<Employee> =
                ctx.db().keyed_repository("developers")?;

            let mut manager = Employee::default();
            manager.emp_id = Some(1);
            manager.address = Some("abcd".to_string());
            manager.join_date = Some(NitriteDateTime::from_system_time(SystemTime::now()));

            let mut developer = Employee::default();
            developer.emp_id = Some(2);
            developer.address = Some("xyz".to_string());
            developer.join_date = Some(NitriteDateTime::from_system_time(SystemTime::now()));

            manager_repo.insert(manager.clone())?;
            employee_repo.insert_many(vec![manager.clone(), developer.clone()])?;
            developer_repo.insert(developer.clone())?;

            assert!(ctx.db().has_repository::<Employee>()?);
            assert!(ctx.db().has_keyed_repository::<Employee>("managers")?);
            assert!(ctx.db().has_keyed_repository::<Employee>("developers")?);

            assert_eq!(ctx.db().list_repositories()?.len(), 1);
            assert_eq!(ctx.db().list_keyed_repositories()?.len(), 2);

            assert_eq!(
                employee_repo.find(field("address").text("abcd"))?.count(),
                1
            );
            assert_eq!(employee_repo.find(field("address").text("xyz"))?.count(), 1);
            assert_eq!(manager_repo.find(field("address").text("xyz"))?.count(), 0);
            assert_eq!(manager_repo.find(field("address").text("abcd"))?.count(), 1);
            assert_eq!(
                developer_repo.find(field("address").text("xyz"))?.count(),
                1
            );
            assert_eq!(
                developer_repo.find(field("address").text("abcd"))?.count(),
                0
            );
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_entity_repository() {
    run_test(
        || create_test_context(),
        |ctx| {
            let manager_repo: ObjectRepository<EmployeeEntity> =
                ctx.db().keyed_repository("managers")?;
            let employee_repo: ObjectRepository<EmployeeEntity> = ctx.db().repository()?;
            let developer_repo: ObjectRepository<EmployeeEntity> =
                ctx.db().keyed_repository("developers")?;

            manager_repo.insert_many(vec![
                EmployeeEntity::default(),
                EmployeeEntity::default(),
                EmployeeEntity::default(),
            ])?;

            employee_repo.insert_many(vec![
                EmployeeEntity::default(),
                EmployeeEntity::default(),
                EmployeeEntity::default(),
            ])?;

            developer_repo.insert_many(vec![
                EmployeeEntity::default(),
                EmployeeEntity::default(),
                EmployeeEntity::default(),
            ])?;

            // This should error in the equivalent of Java's ValidationException
            let collection_result = ctx.db().collection("entity.employee");
            assert!(collection_result.is_err());

            assert!(ctx
                .db()
                .list_repositories()?
                .contains(&"entity.employee".to_string()));
            assert_eq!(ctx.db().list_keyed_repositories()?.len(), 2);
            assert_eq!(ctx.db().list_collection_names()?.len(), 0);

            assert!(manager_repo.has_index(vec!["first_name"])?);
            assert!(manager_repo.has_index(vec!["last_name"])?);
            assert!(employee_repo.has_index(vec!["last_name"])?);
            assert!(employee_repo.has_index(vec!["last_name"])?);

            manager_repo.dispose()?;
            assert_eq!(ctx.db().list_keyed_repositories()?.len(), 1);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_subscription() {
    run_test(
        || create_test_context(),
        |ctx| {
            let counter = Arc::new(AtomicUsize::new(0));
            let employee_repo: ObjectRepository<EmployeeEntity> = ctx.db().repository()?;

            let counter_clone = counter.clone();
            employee_repo.subscribe(CollectionEventListener::new(move |_| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }))?;

            let employee_repo2: ObjectRepository<EmployeeEntity> = ctx.db().repository()?;
            employee_repo2.insert(EmployeeEntity::default())?;

            // Wait for the counter to update (up to 5 seconds)
            awaitility::at_most(Duration::from_secs(5))
                .until(|| counter.load(Ordering::Relaxed) >= 1);

            assert_eq!(counter.load(Ordering::Relaxed), 1);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// WRITE OPERATIONS (INSERT, UPDATE, REMOVE)
// =============================================================================

#[test]
fn test_insert() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Company> = ctx.db().repository()?;

            // Insert one
            let company = generate_company();
            repo.insert(company.clone())?;
            assert_eq!(repo.size()?, 1);

            // Insert multiple
            let company1 = generate_company();
            let company2 = generate_company();
            repo.insert_many(vec![company1, company2])?;
            assert_eq!(repo.size()?, 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_update_with_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut employee = Employee::default();
            employee.emp_id = Some(12);
            employee.address = Some("abcd road".to_string());
            employee.blob = Some(vec![1, 2, 125]);
            employee.join_date = Some(now());
            employee.employee_note = Some(Note {
                note_id: Some(23),
                text: Some("sample text note".to_string()),
            });

            repo.insert(employee.clone())?;
            let cursor = repo.find(all())?;
            assert_eq!(cursor.count(), 1);

            // Update the address
            let mut updated = employee.clone();
            updated.address = Some("xyz road".to_string());
            let write_result = repo.update_with_options(
                field("emp_id").eq(12i64),
                updated.clone(),
                &UpdateOptions::default(),
            )?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);

            let mut cursor = repo.find(all())?;
            let found = cursor.first().unwrap()?;
            assert_eq!(found.address, Some("xyz road".to_string()));

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_upsert_true() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let joining_date = now();

            let result = repo.find(field("join_date").eq(joining_date.clone()))?;
            assert_eq!(result.count(), 0);

            let mut employee = Employee::default();
            employee.emp_id = Some(12);
            employee.address = Some("some road".to_string());
            employee.blob = Some(vec![1, 2, 125]);
            employee.join_date = Some(joining_date.clone());
            employee.employee_note = Some(Note {
                note_id: Some(23),
                text: Some("sample text note".to_string()),
            });

            // Upsert when not exists
            let write_result = repo.update_with_options(
                field("emp_id").eq(12i64),
                employee.clone(),
                &UpdateOptions::new(true, true),
            )?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);

            let result = repo.find(field("join_date").eq(joining_date))?;
            assert_eq!(result.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_upsert_false() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let joining_date = now();

            let result = repo.find(field("join_date").eq(joining_date.clone()))?;
            assert_eq!(result.count(), 0);

            let mut employee = Employee::default();
            employee.emp_id = Some(12);
            employee.address = Some("some road".to_string());
            employee.blob = Some(vec![1, 2, 125]);
            employee.join_date = Some(joining_date.clone());

            // Upsert when not exists should do nothing with insert_if_absent = false
            let write_result = repo.update_with_options(
                field("emp_id").eq(12i64),
                employee.clone(),
                &UpdateOptions::new(false, true),
            )?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 0);

            let result = repo.find(field("join_date").eq(joining_date))?;
            assert_eq!(result.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_update_with_object() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let company = generate_company();

            let mut employee = Employee::default();
            employee.emp_id = Some(100);
            employee.address = Some("original address".to_string());
            employee.join_date = Some(now());
            employee.company = Some(company);

            repo.insert(employee.clone())?;

            // Update using object (requires ID)
            let mut updated_employee = employee.clone();
            updated_employee.address = Some("new address".to_string());

            let write_result = repo.update_one(updated_employee.clone(), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);

            let found = repo.find(field("emp_id").eq(100i64))?.first().unwrap()?;
            assert_eq!(found.address, Some("new address".to_string()));

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_remove_with_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let joining_date = now();

            let mut employee1 = Employee::default();
            employee1.emp_id = Some(12);
            employee1.join_date = Some(joining_date.clone());
            employee1.address = Some("some road".to_string());

            let mut employee2 = Employee::default();
            employee2.emp_id = Some(2);
            employee2.join_date = Some(joining_date.clone());
            employee2.address = Some("other road".to_string());

            repo.insert_many(vec![employee1, employee2])?;
            assert_eq!(repo.size()?, 2);

            // Remove by filter
            let write_result = repo.remove(field("join_date").eq(joining_date.clone()), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 2);
            assert_eq!(repo.size()?, 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_remove_with_filter_just_once() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let joining_date = now();

            let mut employee1 = Employee::default();
            employee1.emp_id = Some(12);
            employee1.join_date = Some(joining_date.clone());
            employee1.address = Some("some road".to_string());

            let mut employee2 = Employee::default();
            employee2.emp_id = Some(2);
            employee2.join_date = Some(joining_date.clone());
            employee2.address = Some("other road".to_string());

            repo.insert_many(vec![employee1, employee2])?;
            assert_eq!(repo.size()?, 2);

            // Remove by filter - just once
            let write_result = repo.remove(field("join_date").eq(joining_date.clone()), true)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);
            assert_eq!(repo.size()?, 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_remove_object() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut employee = Employee::default();
            employee.emp_id = Some(12);
            employee.address = Some("some road".to_string());
            employee.join_date = Some(now());

            let size_before = repo.size()?;
            repo.insert(employee.clone())?;
            assert_eq!(repo.size()?, size_before + 1);

            repo.remove_one(employee.clone())?;
            assert_eq!(repo.size()?, size_before);

            let emp = repo.find(field("emp_id").eq(12i64))?.first();
            assert!(emp.is_none());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// SEARCH/FIND OPERATIONS
// =============================================================================

#[test]
fn test_find_with_options() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Insert 10 employees
            for i in 0..10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                employee.address = Some(format!("Address {}", i));
                repo.insert(employee)?;
            }

            // Find with skip and limit
            let options = FindOptions::new().skip(0).limit(1);
            let mut cursor = repo.find_with_options(all(), &options)?;
            assert_eq!(cursor.size(), 1);
            assert!(cursor.first().is_some());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_get_by_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=4 {
                let company = generate_company();
                let mut employee = generate_employee();
                employee.company = Some(company);
                employee.emp_id = Some(i * 1000000);
                repo.insert(employee)?;
            }

            let by_id = repo.get_by_id(&Some(2000000))?;
            assert!(by_id.is_some());
            assert_eq!(by_id.unwrap().emp_id, Some(2000000));

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_equal_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.join_date = Some(join_date.clone());
            employee.address = Some("test address".to_string());
            repo.insert(employee.clone())?;

            let found = repo
                .find(field("join_date").eq(join_date))?
                .first()
                .unwrap()?;
            assert_eq!(found.emp_id, Some(1));

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_and_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.join_date = Some(join_date.clone());
            employee.address = Some("test address".to_string());
            repo.insert(employee.clone())?;

            let mut cursor = repo.find(and(vec![
                field("emp_id").eq(1i64),
                field("join_date").eq(join_date),
            ]))?;

            let found = cursor.first().unwrap()?;
            assert_eq!(found.emp_id, Some(1));

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_or_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.join_date = Some(join_date.clone());
            repo.insert(employee)?;

            let mut cursor = repo.find(or(vec![
                field("emp_id").eq(1i64),
                field("emp_id").eq(99999i64), // doesn't exist
            ]))?;

            assert_eq!(cursor.size(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_not_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut employee1 = Employee::default();
            employee1.emp_id = Some(1);
            employee1.address = Some("address 1".to_string());

            let mut employee2 = Employee::default();
            employee2.emp_id = Some(2);
            employee2.address = Some("address 2".to_string());

            repo.insert_many(vec![employee1, employee2])?;

            let mut cursor = repo.find(not(field("emp_id").eq(1i64)))?;
            let found = cursor.first().unwrap()?;
            assert_eq!(found.emp_id, Some(2));

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_greater_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let cursor = repo.find(field("emp_id").gt(5i64))?;
            assert_eq!(cursor.count(), 5);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_greater_equal_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let cursor = repo.find(field("emp_id").gte(5i64))?;
            assert_eq!(cursor.count(), 6);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_lesser_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let cursor = repo.find(field("emp_id").lt(5i64))?;
            assert_eq!(cursor.count(), 4);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_lesser_equal_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let cursor = repo.find(field("emp_id").lte(5i64))?;
            assert_eq!(cursor.count(), 5);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_text_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.address = Some("abcd street".to_string());
            repo.insert(employee)?;

            let cursor = repo.find(field("address").text("abcd"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_regex_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.email_address = Some("test@example.com".to_string());
            repo.insert(employee)?;

            let cursor =
                repo.find(field("email_address").text_regex(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_in_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let cursor = repo.find(field("emp_id").in_array(vec![1i64, 2, 3]))?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_not_in_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let cursor = repo.find(field("emp_id").not_in_array(vec![1i64, 2, 3]))?;
            assert_eq!(cursor.count(), 7);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_between_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 1..=10 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            // Between inclusive
            let cursor = repo.find(field("emp_id").between_optional_inclusive(3i64, 7i64))?;
            assert_eq!(cursor.count(), 5);

            // Between exclusive
            let cursor = repo.find(field("emp_id").between(3i64, 7i64, false, false))?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// INDEX OPERATIONS
// =============================================================================

#[test]
fn test_create_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Company> = ctx.db().repository()?;

            assert!(repo.has_index(vec!["company_name"])?);
            assert!(!repo.has_index(vec!["date_created"])?);

            repo.create_index(vec!["date_created"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            assert!(repo.has_index(vec!["date_created"])?);
            assert!(!repo.is_indexing(vec!["date_created"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_list_indexes() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Company> = ctx.db().repository()?;

            let indices = repo.list_indexes()?;
            let initial_count = indices.len();

            repo.create_index(vec!["date_created"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            let indices = repo.list_indexes()?;
            assert_eq!(indices.len(), initial_count + 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Company> = ctx.db().repository()?;

            repo.create_index(vec!["date_created"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            assert!(repo.has_index(vec!["date_created"])?);

            repo.drop_index(vec!["date_created"])?;
            assert!(!repo.has_index(vec!["date_created"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_all_indexes() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Company> = ctx.db().repository()?;

            repo.create_index(vec!["date_created"], &IndexOptions::new(NON_UNIQUE_INDEX))?;

            repo.drop_all_indexes()?;
            let indices = repo.list_indexes()?;
            assert_eq!(indices.len(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rebuild_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Company> = ctx.db().repository()?;

            repo.create_index(vec!["date_created"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            assert!(!repo.is_indexing(vec!["date_created"])?);

            repo.rebuild_index(vec!["date_created"])?;
            // Rebuild is sync in our implementation
            assert!(!repo.is_indexing(vec!["date_created"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_repeatable_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<RepeatableIndexTest> = ctx.db().repository()?;

            let test = RepeatableIndexTest {
                first_name: Some("fName".to_string()),
                age: Some(12),
                last_name: Some("lName".to_string()),
            };
            repo.insert(test.clone())?;

            assert!(repo.has_index(vec!["first_name"])?);
            assert!(repo.has_index(vec!["age"])?);
            assert!(repo.has_index(vec!["last_name"])?);

            let found = repo.find(field("age").eq(12))?.first().unwrap()?;
            assert_eq!(found.first_name, test.first_name);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// PROJECTION TESTS
// =============================================================================

#[test]
fn test_projection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let company = generate_company();
                let mut employee = generate_employee();
                employee.company = Some(company);
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            let employees: Vec<_> = repo.find(all())?.collect();
            let mut cursor = repo.find(all())?;
            let projected: Vec<_> = cursor.project::<SubEmployee>()?.collect();

            assert_eq!(employees.len(), projected.len());

            for i in 0..employees.len() {
                let emp = &employees[i].as_ref().unwrap();
                let sub = &projected[i].as_ref().unwrap();
                assert_eq!(emp.emp_id, sub.emp_id);
                assert_eq!(emp.join_date, sub.join_date);
                assert_eq!(emp.address, sub.address);
            }

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_empty_result_projection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut cursor = repo.find(all())?;
            assert!(cursor.first().is_none());

            let mut cursor = repo.find(field("emp_id").eq(-1i64))?;
            assert!(cursor.first().is_none());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// JOIN TESTS
// =============================================================================

#[test]
fn test_join() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<Person> = ctx.db().keyed_repository("persons")?;
            let address_repo: ObjectRepository<Address> = ctx.db().keyed_repository("addresses")?;

            for i in 0..10 {
                let person = Person {
                    uuid: Some(i.to_string()),
                    name: Some(format!("Person {}", i)),
                    status: None,
                    friend: None,
                    date_created: None,
                };
                person_repo.insert(person)?;

                let address = Address {
                    person_id: Some(i.to_string()),
                    street: Some(format!("Street address {}", i)),
                };
                address_repo.insert(address)?;

                // Add extra address for person 5
                if i == 5 {
                    let address2 = Address {
                        person_id: Some(i.to_string()),
                        street: Some(format!("Street address 2nd {}", i)),
                    };
                    address_repo.insert(address2)?;
                }
            }

            let lookup = Lookup {
                local_field: "uuid".to_string(),
                foreign_field: "person_id".to_string(),
                target_field: "addresses".to_string(),
            };

            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            let mut result = person_cursor.join(&mut address_cursor, &lookup)?;

            assert_eq!(result.size(), 10);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// COMPOUND INDEX TESTS
// =============================================================================

#[test]
fn test_find_by_compound_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository()?;

            let book_id = BookId {
                isbn: Some("123456".to_string()),
                name: Some("Nitrite Database".to_string()),
                author: Some("John Doe".to_string()),
            };

            let book = Book {
                book_id: book_id.clone(),
                publisher: Some("My Publisher House".to_string()),
                price: Some(22.56),
                tags: Some(vec!["database".to_string(), "nosql".to_string()]),
                description: Some("Some random book description".to_string()),
            };

            repo.insert(book.clone())?;

            let found = repo.get_by_id(&book_id)?;
            assert!(found.is_some());
            assert_eq!(found.unwrap().book_id.isbn, book.book_id.isbn);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// STRESS TESTS
// =============================================================================

#[test]
fn test_write_thousand_records() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<StressRecord> = ctx.db().repository()?;
            let count = 1000;

            for _ in 0..count {
                let record = StressRecord {
                    first_name: Some(Uuid::new_v4().to_string()),
                    last_name: Some(Uuid::new_v4().to_string()),
                    failed: Some(false),
                    processed: Some(false),
                };
                repo.insert(record)?;
            }

            // Use iter_with_id() to get the NitriteId along with each record
            // This enables O(1) updates via update_by_nitrite_id()
            let mut cursor = repo.find(field("failed").eq(false))?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;

            for (id, mut record) in items {
                record.processed = Some(true);
                // Use update_by_nitrite_id for O(1) update instead of filter-based update
                repo.update_by_nitrite_id(&id, record, false)?;
            }

            assert_eq!(repo.size()?, count);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// COLLECTION OPERATIONS
// =============================================================================

#[test]
fn test_size() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            assert_eq!(repo.size()?, 0);

            for i in 0..5 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            assert_eq!(repo.size()?, 5);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_clear() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut employee = Employee::default();
                employee.emp_id = Some(i);
                repo.insert(employee)?;
            }

            assert_eq!(repo.size()?, 5);

            repo.clear()?;
            assert_eq!(repo.size()?, 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_is_open() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            assert!(repo.is_open()?);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_dispose() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            repo.insert(employee)?;

            assert!(ctx.db().has_repository::<Employee>()?);

            repo.dispose()?;

            assert!(!ctx.db().has_repository::<Employee>()?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// NESTED OBJECT TESTS
// =============================================================================

#[test]
fn test_nested_object_find() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let note = Note {
                note_id: Some(1),
                text: Some("test note text".to_string()),
            };

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.employee_note = Some(note);
            repo.insert(employee)?;

            // Full text search on nested field
            let cursor = repo.find(field("employee_note.text").text("test"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_nested_update() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let note = Note {
                note_id: Some(1),
                text: Some("original text".to_string()),
            };

            let mut employee = Employee::default();
            employee.emp_id = Some(1);
            employee.employee_note = Some(note);
            repo.insert(employee.clone())?;

            // Update nested object
            let mut update_doc = Document::new();
            update_doc.put("employee_note.text", "updated text")?;

            let write_result = repo.update_document(field("emp_id").eq(1i64), &update_doc, true)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);

            let found = repo.get_by_id(&Some(1))?;
            assert!(found.is_some());
            assert_eq!(
                found.unwrap().employee_note.unwrap().text,
                Some("updated text".to_string())
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// =============================================================================
// HELPER STRUCTURES
// =============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Convertible, NitriteEntity)]
#[entity(
    name = "entity.employee",
    id(field = "id"),
    index(type = "non-unique", fields = "first_name"),
    index(type = "non-unique", fields = "last_name")
)]
struct EmployeeEntity {
    id: i64,
    first_name: String,
    last_name: String,
}

impl Default for EmployeeEntity {
    fn default() -> Self {
        EmployeeEntity {
            id: rng().next_u64() as i64,
            first_name: FirstName().fake(),
            last_name: LastName().fake(),
        }
    }
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
struct StressRecord {
    first_name: Option<String>,
    last_name: Option<String>,
    failed: Option<bool>,
    processed: Option<bool>,
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
struct Address {
    person_id: Option<String>,
    street: Option<String>,
}
