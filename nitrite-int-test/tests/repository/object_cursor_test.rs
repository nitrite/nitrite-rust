// Based on Java ObjectCursorTest.java
use nitrite::filter::{all, field};
use nitrite::repository::ObjectRepository;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[derive(Clone, Debug, Default, Convertible, NitriteEntity)]
pub struct ObjectCursorTestEntity {
    id: Option<String>,
    name: Option<String>,
    age: Option<i32>,
}

#[test]
fn test_cursor_iteration() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<ObjectCursorTestEntity> = ctx.db().repository()?;

            // Insert test data
            for i in 0..5 {
                let obj = ObjectCursorTestEntity {
                    id: Some(format!("test_{}", i)),
                    name: Some(format!("Name{}", i)),
                    age: Some(20 + i),
                };
                repo.insert(obj)?;
            }

            // Iterate through cursor
            let mut cursor = repo.find(all())?;
            let mut count = 0;
            while let Some(obj_result) = cursor.next() {
                let _obj = obj_result?;
                count += 1;
            }

            assert_eq!(count, 5);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_cursor_count() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<ObjectCursorTestEntity> = ctx.db().repository()?;

            // Insert test data
            for i in 0..10 {
                let obj = ObjectCursorTestEntity {
                    id: Some(format!("test_{}", i)),
                    name: Some(format!("Name{}", i)),
                    age: Some(20 + i),
                };
                repo.insert(obj)?;
            }

            // Count
            let cursor = repo.find(all())?;
            assert_eq!(cursor.count(), 10);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_cursor_with_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<ObjectCursorTestEntity> = ctx.db().repository()?;

            // Insert test data
            for i in 0..10 {
                let obj = ObjectCursorTestEntity {
                    id: Some(format!("test_{}", i)),
                    name: Some(if i % 2 == 0 { "Even" } else { "Odd" }.to_string()),
                    age: Some(20 + i),
                };
                repo.insert(obj)?;
            }

            // Find with filter
            let cursor = repo.find(field("name").eq("Even"))?;
            assert_eq!(cursor.count(), 5);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_cursor_empty() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<ObjectCursorTestEntity> = ctx.db().repository()?;

            // Find in empty repository
            let cursor = repo.find(all())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_cursor_first() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<ObjectCursorTestEntity> = ctx.db().repository()?;

            // Insert data
            let obj = ObjectCursorTestEntity {
                id: Some("test_1".to_string()),
                name: Some("Test".to_string()),
                age: Some(25),
            };
            repo.insert(obj)?;

            // Get first
            let mut cursor = repo.find(all())?;
            let first = cursor.next();
            assert!(first.is_some());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
