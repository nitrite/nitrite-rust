// Based on Java CollectionInsertNegativeTest.java
use nitrite::doc;
use nitrite::filter::all;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_insert_empty_document() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert empty document
            let result = collection.insert(doc! {})?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_duplicate_id() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Create index on a custom field to test unique constraint
            collection.create_index(vec!["custom_id"], &nitrite::index::unique_index())?;

            // Insert first document
            let doc1 = doc! {"custom_id": "same_id", "data": "first"};
            let result1 = collection.insert(doc1)?;
            assert_eq!(result1.affected_nitrite_ids().len(), 1);

            // Try to insert with same custom_id - should fail due to unique constraint
            let doc2 = doc! {"custom_id": "same_id", "data": "second"};
            let result2 = collection.insert(doc2);
            // Should fail due to unique constraint on custom_id
            assert!(
                result2.is_err(),
                "Inserting document with duplicate custom_id should fail"
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_null_document() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert document with null values
            let result = collection
                .insert(doc! {"name": "test", "value": (nitrite::common::Value::Null)})?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Verify it was inserted
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_large_document() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Create a large document
            let mut large_doc = doc! {};
            for i in 0..100 {
                large_doc.put(format!("field_{}", i), format!("value_{}", i))?;
            }

            let result = collection.insert(large_doc)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_many_empty_list() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert empty list
            let result = collection.insert_many(vec![])?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_many() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert multiple documents
            let docs = vec![
                doc! {"id": 1, "name": "doc1"},
                doc! {"id": 2, "name": "doc2"},
                doc! {"id": 3, "name": "doc3"},
            ];

            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            // Verify
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_with_special_characters() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert document with special characters
            let doc = doc! {
                "unicode": "你好",
                "emoji": "🎉",
                "special": "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/"
            };

            let result = collection.insert(doc)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
