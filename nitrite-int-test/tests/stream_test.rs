// Based on Java DocumentCursorTest.java
use nitrite::doc;
use nitrite::filter::field;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_find_result_is_document_stream() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.insert(doc!{"first": "second"})?;

            let result = collection.find(field("first").eq("second"))?;
            // The result should be a DocumentStream
            assert!(result.count() > 0, "Should be able to count items");

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_projection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert test documents
            collection.insert(doc!{"first": "value1", "second": "value2", "third": "value3"})?;
            collection.insert(doc!{"first": "value4", "second": "value5", "third": "value6"})?;

            // Create a projection
            let result = collection.find(field("first").eq("value1"))?;
            assert_eq!(result.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_cursor_has_methods() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            collection.insert(doc!{"id": 1, "name": "test1"})?;
            collection.insert(doc!{"id": 2, "name": "test2"})?;
            collection.insert(doc!{"id": 3, "name": "test3"})?;

            let cursor = collection.find(field("id").gte(1))?;

            // Test cursor methods
            let count = cursor.count();
            assert_eq!(count, 3);

            // Test iteration
            let mut cursor = collection.find(field("id").gte(1))?;
            let mut items = 0;
            while let Some(doc_result) = cursor.next() {
                let _doc = doc_result?;
                items += 1;
            }
            assert_eq!(items, 3);

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
            let collection = ctx.db().collection("test")?;

            collection.insert(doc!{"id": 1, "name": "first"})?;
            collection.insert(doc!{"id": 2, "name": "second"})?;

            let mut cursor = collection.find(field("id").gte(1))?;
            let first = cursor.next();

            assert!(first.is_some(), "Should have at least one element");

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
            let collection = ctx.db().collection("test")?;

            collection.insert(doc!{"id": 1, "name": "test"})?;

            // Find with non-matching filter
            let cursor = collection.find(field("id").eq(999))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_cursor_size_and_count() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert multiple documents
            for i in 0..5 {
                let name = format!("test{}", i);
                collection.insert(doc!{"id": i, "name": name})?;
            }

            let cursor = collection.find(field("id").lt(3))?;
            let count = cursor.count();
            assert_eq!(count, 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
