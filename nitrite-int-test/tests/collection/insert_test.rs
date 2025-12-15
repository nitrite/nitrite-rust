use nitrite::common::Value;
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite_int_test::test_util::{cleanup, create_test_context, create_test_docs, run_test};


#[test]
fn test_insert() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            let document = doc!{
                "first_name": "John",
                "last_name": "Doe",
                "birth_day": 1234567890,
                "data": [1, 2, 3],
                "body": "This is a test document"
            };

            let result = collection.insert(document)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let cursor = collection.find(all())?;
            for document in cursor {
                let document = &document?;
                assert_eq!(document.get("first_name")?.as_string().unwrap().as_str(), "John");
                assert_eq!(document.get("last_name")?.as_string().unwrap().as_str(), "Doe");
                assert!(!document.get("birth_day")?.is_null());
                assert!(!document.get("data")?.is_null());
                assert!(!document.get("body")?.is_null());
                assert!(!document.get("_id")?.is_null());
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_batch() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            let docs = create_test_docs();
            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            let cursor = collection.find(all())?;

            for document in cursor {
                let document = &document?;
                assert!(!document.get("first_name")?.is_null());
                assert!(!document.get("last_name")?.is_null());
                assert!(!document.get("birth_day")?.is_null());
                assert!(!document.get("data")?.is_null());
                assert!(!document.get("body")?.is_null());
                assert!(!document.get("_id")?.is_null());
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_batch_hetero_docs() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            let mut docs = create_test_docs();
            let document = doc!{
                "test": "Nitrite Test"
            };
            docs.push(document);

            let result = collection.insert_many(docs)?;
            assert_eq!(result.affected_nitrite_ids().len(), 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_duplicate_documents() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert the same documents twice
            let docs = create_test_docs();
            let result1 = collection.insert_many(docs.clone())?;
            assert_eq!(result1.affected_nitrite_ids().len(), 3);

            let result2 = collection.insert_many(docs)?;
            assert_eq!(result2.affected_nitrite_ids().len(), 3);

            // We should have 6 documents now (no uniqueness constraint)
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 6);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_with_null_values() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Create documents with null values
            let doc1 = doc!{
                "first_name": "test",
                "last_name": (Value::Null), 
                "age": 30
            };

            let doc2 = doc!{
                "first_name": "another",
                "phone": (Value::Null),
                "email": "test@example.com"
            };

            let result = collection.insert_many(vec![doc1, doc2])?;
            assert_eq!(result.affected_nitrite_ids().len(), 2);

            // Verify the documents were inserted properly
            let mut cursor = collection.find(field("first_name").eq("test"))?;

            let doc = cursor.first().unwrap()?;
            assert!(doc.get("last_name")?.is_null());
            assert_eq!(doc.get("age")?, Value::I32(30));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_nested_documents() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Create documents with nested structures
            let doc = doc!{
                "name": "user1",
                "address": {
                    "street": "123 Main St",
                    "city": "Springfield",
                    "zipcode": "12345",
                    "coordinates": {
                        "latitude": 37.7749,
                        "longitude": (-122.4194)
                    }
                },
                "contact": {
                    "email": "user1@example.com",
                    "phone": {
                        "home": "555-1234",
                        "mobile": "555-5678"
                    }
                }
            };

            let result = collection.insert(doc)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Verify we can query nested fields
            let cursor = collection.find(field("address.city").eq("Springfield"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = collection.find(field("contact.phone.mobile").eq("555-5678"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_array_documents() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Create documents with arrays of different types
            let doc = doc!{
                "name": "test_arrays",
                "numbers": [1, 2, 3, 4, 5],
                "strings": ["one", "two", "three"],
                "mixed": [1, "two", true, 4.5],
                "objects": [
                    {"id": 1, "value": "first"},
                    {"id": 2, "value": "second"},
                    {"id": 3, "value": "third"}
                ]
            };

            let result = collection.insert(doc)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            // Test querying array elements
            let cursor = collection.find(field("numbers").elem_match(field("$").eq(3)))?;
            assert_eq!(cursor.count(), 1);

            let cursor = collection.find(field("strings").elem_match(field("$").eq("two")))?;
            assert_eq!(cursor.count(), 1);

            // Query nested objects in array
            let cursor = collection.find(field("objects.id").elem_match(field("$").eq(2)))?;
            assert_eq!(cursor.count(), 1);

            let cursor = collection.find(field("objects").elem_match(field("id").eq(2)))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_insert_batch_performance() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("performance")?;

            // Create a large batch of simple documents
            let mut docs = Vec::with_capacity(1000);
            for i in 0..1000 {
                docs.push(doc!{
                    "index": i,
                    "value": (format!("test_value_{}", i)),
                    "active": (i % 2 == 0)
                });
            }

            // Measure insertion time
            let start = std::time::Instant::now();
            let result = collection.insert_many(docs)?;
            let elapsed = start.elapsed();

            assert_eq!(result.affected_nitrite_ids().len(), 1000);
            println!("Inserted 1000 documents in {:?}", elapsed);

            // Verify all documents were inserted
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1000);

            // Test querying
            let cursor = collection.find(field("active").eq(true))?;
            assert_eq!(cursor.count(), 500);

            Ok(())
        },
        cleanup,
    )
}

#[test]
#[should_panic]
fn test_insert_with_custom_id() {
    run_test(
        create_test_context,
        |ctx| {
            let _ = ctx.db().collection("custom_id")?;

            // Create documents with custom _id field
            let _ = doc!{
                "_id": "custom_id_1",
                "name": "Document 1"
            };

            Ok(())
        },
        cleanup,
    )
}