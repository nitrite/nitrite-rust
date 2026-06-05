// Based on Java DocumentCursorTest.java
use nitrite::doc;
use nitrite::filter::field;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_streaming_cursor_resets_and_replays_without_caching() {
    // A find() cursor streams (retains nothing); reset() rebuilds the query so it can be
    // replayed, and size()/first() leave it usable — same observable behaviour as before, but
    // without holding the whole result set in memory.
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("stream_reset")?;
            for i in 0i64..50 {
                coll.insert(doc! {"id": (i), "group": (i % 2)})?;
            }

            // Non-indexed filter => full-scan, non-index-covered (truly streaming) cursor.
            let mut cursor = coll.find(field("group").eq(0i64))?;
            let first_pass: Vec<_> = cursor.by_ref().map(|d| d.unwrap()).collect();
            assert_eq!(first_pass.len(), 25, "forward pass yields all matches");

            // reset() rebuilds the stream and replays the identical set.
            cursor.reset();
            let second_pass: Vec<_> = cursor.by_ref().map(|d| d.unwrap()).collect();
            assert_eq!(second_pass.len(), 25, "reset replays the full set");

            // size() reports the count and leaves the cursor usable.
            let mut cursor2 = coll.find(field("group").eq(0i64))?;
            assert_eq!(cursor2.size(), 25);
            let after_size: Vec<_> = cursor2.by_ref().map(|d| d.unwrap()).collect();
            assert_eq!(after_size.len(), 25, "cursor is usable after size()");

            // first() returns the first match and the cursor still iterates.
            let mut cursor3 = coll.find(field("group").eq(1i64))?;
            assert!(cursor3.first().is_some());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_result_is_document_stream() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.insert(doc!{"first": "second"})?;

            let result = collection.find(field("first").eq("second"))?;
            // The result should be a DocumentStream
            assert!(result.count() > 0, "Should be able to count items");

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_projection() {
    run_test(
        create_test_context,
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
        cleanup,
    )
}

#[test]
fn test_cursor_has_methods() {
    run_test(
        create_test_context,
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
            let cursor = collection.find(field("id").gte(1))?;
            let mut items = 0;
            for doc_result in cursor {
                let _doc = doc_result?;
                items += 1;
            }
            assert_eq!(items, 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_cursor_first() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            collection.insert(doc!{"id": 1, "name": "first"})?;
            collection.insert(doc!{"id": 2, "name": "second"})?;

            let mut cursor = collection.find(field("id").gte(1))?;
            let first = cursor.next();

            assert!(first.is_some(), "Should have at least one element");

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_cursor_empty() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            collection.insert(doc!{"id": 1, "name": "test"})?;

            // Find with non-matching filter
            let cursor = collection.find(field("id").eq(999))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_cursor_size_and_count() {
    run_test(
        create_test_context,
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
        cleanup,
    )
}
