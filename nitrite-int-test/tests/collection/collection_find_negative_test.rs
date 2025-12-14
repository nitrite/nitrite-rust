// Based on Java CollectionFindNegativeTest.java
use nitrite::collection::FindOptions;
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite_int_test::test_util::{cleanup, create_test_context, insert_test_documents, run_test};

#[test]
fn test_find_with_empty_collection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Find on empty collection should return empty cursor
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_with_no_matching_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Find with non-matching filter
            let cursor = collection.find(field("first_name").eq("non_existent"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_with_null_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert documents with null values
            collection.insert(doc! {"name": "test1", "age": 25})?;
            collection.insert(doc! {"name": "test2"})?; // age is null

            // Find with null
            let cursor = collection.find(field("age").eq(nitrite::common::Value::Null))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_on_non_existent_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Find on non-existent field should return empty
            let cursor = collection.find(field("non_existent").eq("value"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_with_skip_limit() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            // Test skip and limit
            let cursor = collection.find(all())?;
            let _all_count = cursor.count();

            // Skip 1 and limit 1
            let cursor =
                collection.find_with_options(all(), &FindOptions::default().skip(1).limit(1))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_with_invalid_field_path() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert nested document
            collection.insert(doc! {"user": {"name": "John", "age": 30}})?;

            // Find with nested field
            let cursor = collection.find(field("user.name").eq("John"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
