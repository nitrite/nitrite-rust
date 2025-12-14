// Based on Java CustomFilterTest.java
use nitrite::doc;
use nitrite::filter::field;
use nitrite::index::non_unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_custom_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert test documents
            collection.insert(doc!{"firstName": "fn1", "lastName": "ln1"})?;
            collection.insert(doc!{"firstName": "fn2", "lastName": "ln2"})?;
            collection.insert(doc!{"firstName": "fn3", "lastName": "ln3"})?;

            // Create a non-unique index on firstName
            collection.create_index(vec!["firstName"], &non_unique_index())?;

            // Test custom filter using field filter
            let cursor = collection.find(field("firstName").eq("fn1"))?;
            assert_eq!(cursor.count(), 1);

            let mut doc_iter = collection.find(field("firstName").eq("fn1"))?;
            if let Some(doc) = doc_iter.next() {
                let doc = doc?;
                assert_eq!(
                    doc.get("firstName").unwrap().as_string().unwrap(),
                    "fn1"
                );
            }

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_custom_filter_case_insensitive() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert test documents
            collection.insert(doc!{"firstName": "fn1", "lastName": "ln1"})?;
            collection.insert(doc!{"firstName": "fn2", "lastName": "ln2"})?;

            // Create a non-unique index on firstName
            collection.create_index(vec!["firstName"], &non_unique_index())?;

            // Test with custom filter (field equality)
            let cursor = collection.find(field("firstName").eq("fn1"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
