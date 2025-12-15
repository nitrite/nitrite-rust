// Based on Java DocumentMetadataTest.java
// Tests for document metadata (revision, timestamps, etc.)
use nitrite::doc;
use nitrite::filter::field;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_insert_document() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert document
            collection.insert(doc!{"test_key": "test_value"})?;

            // Retrieve and verify
            let mut cursor = collection.find(field("test_key").eq("test_value"))?;
            if let Some(doc_result) = cursor.next() {
                let retrieved_doc = doc_result?;
                assert_eq!(
                    retrieved_doc
                        .get("test_key")
                        .unwrap()
                        .as_string()
                        .unwrap(),
                    "test_value"
                );
            }

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
            let collection = ctx.db().collection("test")?;

            // Insert document
            collection.insert(doc!{"data": "value1"})?;

            // Update document
            let result = collection.update(
                field("data").eq("value1"),
                &doc!{"data": "value2"},
            )?;
            assert!(!result.affected_nitrite_ids().is_empty());

            // Verify update
            let mut cursor = collection.find(field("data").eq("value2"))?;
            if let Some(doc_result) = cursor.next() {
                let updated_doc = doc_result?;
                assert_eq!(
                    updated_doc.get("data").unwrap().as_string().unwrap(),
                    "value2"
                );
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_remove_document() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert document
            collection.insert(doc!{"test_key": "test_value"})?;

            // Verify it exists
            let cursor = collection.find(field("test_key").eq("test_value"))?;
            assert_eq!(cursor.count(), 1);

            // Remove the document
            let result = collection.remove(field("test_key").eq("test_value"), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Verify it's gone
            let cursor = collection.find(field("test_key").eq("test_value"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_document_lifecycle() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert
            let insert_result = collection.insert(doc!{"value": 1})?;
            assert_eq!(insert_result.affected_nitrite_ids().len(), 1);

            // Find
            let mut cursor = collection.find(field("value").eq(1))?;
            let _ = cursor.next().unwrap()?;

            // Update
            let update_result = collection.update(field("value").eq(1), &doc!{"value": 2})?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 1);

            // Verify update
            let cursor = collection.find(field("value").eq(2))?;
            assert_eq!(cursor.count(), 1);

            // Remove
            let remove_result = collection.remove(field("value").eq(2), false)?;
            assert_eq!(remove_result.affected_nitrite_ids().len(), 1);

            // Verify removal
            let cursor = collection.find(field("value").eq(2))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}
