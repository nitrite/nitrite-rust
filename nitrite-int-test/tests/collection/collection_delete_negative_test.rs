// Based on Java CollectionDeleteNegativeTest.java
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite_int_test::test_util::{cleanup, create_test_context, insert_test_documents, run_test};

#[test]
fn test_delete_from_empty_collection() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Delete from empty collection should not fail
            let result = collection.remove(all(), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_delete_with_no_matching_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Delete with non-matching filter
            let result = collection.remove(field("first_name").eq("non_existent"), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);

            // Verify data still exists
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_delete_all() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Delete all documents
            let result = collection.remove(all(), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            // Verify
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_delete_with_just_once() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Delete with just_once = true (should delete only first match)
            let result = collection.remove(field("last_name").eq("ln2"), true)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Verify only one was deleted
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_delete_non_existent_field() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Delete with non-existent field filter
            let result = collection.remove(field("non_existent").eq("value"), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 0);

            // Original data should still exist
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_delete_with_complex_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Delete with complex filter
            let result = collection.remove(
                field("last_name").eq("ln1").and(field("first_name").eq("fn1")),
                false,
            )?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Verify
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_delete_and_reinsert() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert document
            let doc = doc!{"id": 1, "name": "test"};
            collection.insert(doc.clone())?;

            // Delete it
            let result = collection.remove(field("id").eq(1), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Reinsert it
            let result = collection.insert(doc)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Verify
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}
