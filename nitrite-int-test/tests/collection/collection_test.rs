use nitrite::common::{Attributes, Value};
use nitrite::doc;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_get_name() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Assert that the collection name is "test"
            assert_eq!(collection.name(), "test");
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_dispose_collection() {
    run_test(
        create_test_context,
        |ctx| {
            let db = ctx.db();
            // Check that the collection exists
            assert!(!db.has_collection("test")?);
            let collection = db.collection("test")?;
            // Drop the collection
            collection.dispose()?;
            // Verify that the collection no longer exists
            assert!(!db.has_collection("test")?);
            // Also check that the collection is marked as closed
            assert!(collection.is_open().is_ok());
            assert!(!collection.is_open()?);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_close_connection() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Close the collection connection
            collection.close()?;
            // Verify that the collection is closed
            assert!(!collection.is_open()?);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_dispose_after_close() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Close the collection connection first
            collection.close()?;
            assert!(!collection.is_open()?);
            // Attempt to drop the collection after closing should error
            let result = collection.dispose();
            assert!(result.is_err());
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_operation_after_drop() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Drop the collection
            collection.dispose()?;
            // Attempt to insert a document after drop should return an error
            let doc = doc!{ "test": "test" };
            let result = collection.insert(doc);
            assert!(result.is_err());
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_attributes() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let attributes = collection.attributes()?;
            // Check that the collection has no attributes set
            assert!(attributes.is_none());
            // Set some attributes
            let mut attributes = Attributes::new();
            attributes.put("key1", Value::from("value1"));
            
            collection.set_attributes(attributes)?;
            
            // Retrieve the attributes again
            let attributes = collection.attributes()?;
            // Check that the attributes are set correctly
            assert!(attributes.is_some());
            let attributes = attributes.unwrap();
            assert_eq!(attributes.get("key1").unwrap(), &Value::from("value1"));
            Ok(())
        },
        cleanup,
    )}