use chrono::DateTime;
use nitrite::collection::{CollectionEventInfo, CollectionEventListener, CollectionEvents, NitriteCollection};
use nitrite::common::{atomic, ReadExecutor, Value, WriteExecutor};
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite::index::{full_text_index, non_unique_index, unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test, NitriteDateTime};

#[test]
fn test_collection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            let doc1 = doc!{
                "name": "Anindya",
                "color": ["red", "green", "blue"],
                "books": [
                    {
                        "name": "Book ABCD",
                        "tag": ["tag1", "tag2"]
                    },
                    {
                        "name": "Book EFGH",
                        "tag": ["tag3", "tag1"]
                    },
                    {
                        "name": "No Tag"
                    }
                ]
            };

            let doc2 = doc!{
                "name": "Bill",
                "color": ["purple", "yellow", "gray"],
                "books": [
                    {
                        "name": "Book abcd",
                        "tag": ["tag4", "tag5"]
                    },
                    {
                        "name": "Book wxyz",
                        "tag": ["tag3", "tag1"]
                    },
                    {
                        "name": "No Tag 2"
                    }
                ]
            };

            let doc3 = doc!{
                "name": "John",
                "color": ["black", "sky", "violet"],
                "books": [
                    {
                        "name": "Book Mnop",
                        "tag": ["tag6", "tag2"]
                    },
                    {
                        "name": "Book ghij",
                        "tag": ["tag3", "tag7"]
                    },
                    {
                        "name": "No Tag"
                    }
                ]
            };

            collection.create_index(vec!["color"], &non_unique_index())?;
            collection.create_index(vec!["books.tag"], &non_unique_index())?;
            collection.create_index(vec!["books.name"], &full_text_index())?;

            let write_result = collection.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()])?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 3);

            let cursor = collection.find(field("color").eq("red"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = collection.find(field("books.name").text_case_insensitive("abcd"))?;
            assert_eq!(cursor.count(), 2);

            let cursor = collection.find(field("books.tag").eq("tag2"))?;
            assert_eq!(cursor.count(), 2);

            let cursor = collection.find(field("books.tag").eq("tag5"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = collection.find(field("books.tag").eq("tag10"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            collection.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["first_name"])?);

            collection.create_index(vec!["last_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["last_name"])?);

            collection.create_index(vec!["body"], &full_text_index())?;
            assert!(collection.has_index(vec!["body"])?);

            collection.create_index(vec!["birth_day"], &non_unique_index())?;
            assert!(collection.has_index(vec!["birth_day"])?);

            // Insert test documents
            insert_test_documents(&collection)?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_list_indexes() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            assert_eq!(collection.list_indexes()?.len(), 0);

            collection.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["first_name"])?);

            collection.create_index(vec!["last_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["last_name"])?);

            collection.create_index(vec!["body"], &full_text_index())?;
            assert!(collection.has_index(vec!["body"])?);

            assert_eq!(collection.list_indexes()?.len(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["first_name"])?);

            collection.drop_index(vec!["first_name"])?;
            assert!(!collection.has_index(vec!["first_name"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_all_indexes() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.drop_all_indexes()?;

            collection.create_index(vec!["first_name"], &non_unique_index())?;
            collection.create_index(vec!["last_name"], &non_unique_index())?;
            collection.create_index(vec!["body"], &full_text_index())?;
            collection.create_index(vec!["birth_day"], &non_unique_index())?;

            assert_eq!(collection.list_indexes()?.len(), 4);

            collection.drop_all_indexes()?;
            assert_eq!(collection.list_indexes()?.len(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_has_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            
            assert!(!collection.has_index(vec!["last_name"])?);
            collection.create_index(vec!["last_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["last_name"])?);

            assert!(!collection.has_index(vec!["body"])?);
            collection.create_index(vec!["body"], &full_text_index())?;
            assert!(collection.has_index(vec!["body"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_delete_with_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            
            collection.create_index(vec!["first_name"], &non_unique_index())?;
            collection.create_index(vec!["body"], &full_text_index())?;

            insert_test_documents(&collection)?;

            let result = collection.remove(field("first_name").eq("fn1"), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 2);

            let result = collection.remove(field("body").text("Lorem"), false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rebuild_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.create_index(vec!["body"], &full_text_index())?;
            
            insert_test_documents(&collection)?;
            
            let indices = collection.list_indexes()?;
            for idx in indices {
                let fields = idx.index_fields().field_names();
                let field_names = fields.iter().map(|x| x.as_ref()).collect();
                collection.rebuild_index(field_names)?;
            }

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_null_value_in_indexed_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.create_index(vec!["first_name"], &non_unique_index())?;
            collection.create_index(vec!["birth_day"], &non_unique_index())?;
            
            insert_test_documents(&collection)?;

            let document = doc!{
                "first_name": (Value::Null),
                "last_name": "ln1",
                "birth_day": (Value::Null),
                "data": [1_u8, 2_u8, 3_u8],
                "list": ["one", "two", "three"],
                "body": "a quick brown fox jump over the lazy dog"
            };

            collection.insert(document)?;
            
            // If we got here without errors, the test passes
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_all_and_create_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let mut collection = ctx.db().collection("test")?;
            collection.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["first_name"])?);
            
            collection.drop_all_indexes()?;
            assert!(!collection.has_index(vec!["first_name"])?);

            collection.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["first_name"])?);

            // Get a fresh handle to the collection
            collection = ctx.db().collection("test")?;
            assert!(collection.has_index(vec!["first_name"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_numeric_field_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.drop_all_indexes()?;
            collection.remove(all(), false)?;

            let doc1 = doc!{"field": 5};
            let doc2 = doc!{"field": 4.3};
            let doc3 = doc!{"field": 0.03};
            let doc4 = doc!{"field": 4};
            let doc5 = doc!{"field": 5.0};

            collection.insert_many(vec![doc1, doc2, doc3, doc4, doc5])?;

            let cursor = collection.find(field("field").eq(5))?;
            assert_eq!(cursor.count(), 1);

            collection.create_index(vec!["field"], &non_unique_index())?;

            let cursor = collection.find(field("field").eq(5))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_index_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            use rand::Rng;
            let collection = ctx.db().collection("index-test")?;
            let mut rng = rand::rng();
            
            let mut documents = Vec::with_capacity(10000);
            for _ in 0..10000 {
                let document = doc!{
                    "first": (rng.random::<i32>()),
                    "second": (rng.random::<f64>())
                };
                documents.push(document);
            }
            collection.insert_many(documents)?;

            let events = atomic(Vec::new());
            let events_clone = events.clone();
            collection.subscribe(CollectionEventListener::new(move |event_info: CollectionEventInfo| {
                match event_info.event_type() {
                    CollectionEvents::Insert => panic!("wrong event Insert"),
                    CollectionEvents::Update => panic!("wrong event Update"),
                    CollectionEvents::Remove => panic!("wrong event Remove"),
                    CollectionEvents::IndexStart | CollectionEvents::IndexEnd => {
                        let event_item = event_info.item().unwrap();
                        if let Some(fields) = event_item.as_array() {
                            let mut field_names = fields.iter().map(|x| x.as_string().unwrap().to_string()).collect::<Vec<_>>();
                            events_clone.write_with(|e| e.append(&mut field_names));                            
                        }
                        Ok(())
                    }
                }
            })).expect("Failed to subscribe to collection events");

            collection.create_index(vec!["first"], &non_unique_index())?;
            assert_eq!(collection.find(all())?.count(), 10000);

            collection.create_index(vec!["second"], &non_unique_index())?;
            assert_eq!(collection.find(all())?.count(), 10000);

            // Check that we got some events
            events.read_with(|context| {
                assert!(!context.is_empty());                
            });
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_invalid_unique_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Create a unique index on last_name
            collection.create_index(vec!["last_name"], &unique_index())?;
            assert!(collection.has_index(vec!["last_name"])?);

            // This should fail with UniqueConstraintError since we're inserting documents with duplicate last_name values
            let result = insert_test_documents(&collection);

            // Assert that the error is a UniqueConstraintError
            assert!(result.is_err());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_index_on_array() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Create a unique index on data (which is an array field)
            collection.create_index(vec!["data"], &unique_index())?;
            assert!(collection.has_index(vec!["data"])?);

            // This should fail with UniqueConstraintError since the data field has repetition
            let result = insert_test_documents(&collection);

            // Assert that the error is a UniqueConstraintError
            assert!(result.is_err());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_on_invalid_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert documents first
            insert_test_documents(&collection)?;

            // Try to create an index on a field that doesn't exist
            let result = collection.create_index(vec!["my-value"], &unique_index());

            // Assert that the error is a UniqueConstraintError
            assert!(result.is_err());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_full_text_on_non_text_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Insert documents first
            insert_test_documents(&collection)?;

            // Try to create a full-text index on a non-text field (birth_day)
            let result = collection.create_index(vec!["data"], &full_text_index());

            // Assert that the error is an IndexingError
            assert!(result.is_err());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_index_on_non_indexed_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Try to drop an index on a field that is not indexed
            let result = collection.drop_index(vec!["data"]);

            // Assert that there is no error
            assert!(result.is_ok());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rebuild_index_invalid() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;

            // Try to rebuild an index on a field that doesn't exist
            let result = collection.rebuild_index(vec!["unknown"]);

            // Assert that the error is an IndexingError
            assert!(result.is_err());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_index_and_search_on_null_values() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("index-on-null")?;
            
            collection.insert(doc!{
                "first": (Value::Null),
                "second": 123,
                "third": (vec![Value::I32(1), Value::I32(2), Value::Null])
            })?;
            
            collection.insert(doc!{
                "first": "abcd",
                "second": 456,
                "third": [3, 1]
            })?;
            
            collection.insert(doc!{
                "first": "xyz",
                "second": 789,
                "third": (Value::Null)
            })?;

            collection.create_index(vec!["first"], &non_unique_index())?;
            assert_eq!(collection.find(field("first").eq(Value::Null))?.count(), 1);

            collection.create_index(vec!["third"], &non_unique_index())?;
            assert_eq!(collection.find(field("third").eq(Value::Null))?.count(), 2);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// Helper function to insert test documents
fn insert_test_documents(collection: &NitriteCollection) -> nitrite::errors::NitriteResult<()> {
    use chrono::{TimeZone, Utc};
    
    let doc1 = doc!{
        "first_name": "fn1",
        "last_name": "ln1",
        "birth_day": (NitriteDateTime::new(DateTime::from(Utc.with_ymd_and_hms(2012, 6, 1, 16, 0, 0).unwrap()))),
        "data": [1_u8, 2_u8, 3_u8],
        "list": ["one", "two", "three"],
        "body": "a quick brown fox jump over the lazy dog"
    };

    let doc2 = doc!{
        "first_name": "fn2",
        "last_name": "ln2",
        "birth_day": (NitriteDateTime::new(DateTime::from(Utc.with_ymd_and_hms(2012, 7, 1, 16, 2, 48).unwrap()))),
        "data": [3_u8, 4_u8, 3_u8],
        "list": ["three", "four", "five"],
        "body": "quick hello world"
    };

    let doc3 = doc!{
        "first_name": "fn3",
        "last_name": "ln2",
        "birth_day": (NitriteDateTime::new(DateTime::from(Utc.with_ymd_and_hms(2012, 8, 1, 16, 2, 48).unwrap()))),
        "data": [5_u8, 4_u8, 3_u8],
        "list": ["five", "six", "seven"],
        "body": "Lorem ipsum dolor sit amet"
    };

    collection.insert_many(vec![doc1, doc2, doc3])?;
    Ok(())
}

#[test]
fn test_rebuild_index_with_valid_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test_defect_18")?;

            // Insert some documents
            let doc1 = doc!{
                "name": "Alice",
                "age": 25
            };

            let doc2 = doc!{
                "name": "Bob",
                "age": 30
            };

            collection.insert_many(vec![doc1, doc2])?;

            // Create an index
            collection.create_index(vec!["name"], &non_unique_index())?;

            // Rebuild the index - should work without panic
            collection.rebuild_index(vec!["name"])?;

            // Verify the collection still works
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_rebuild_index_with_nonexistent_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test_defect_18_non_existent")?;

            // Insert some documents
            let doc1 = doc!{
                "name": "Charlie",
                "age": 35
            };

            collection.insert_many(vec![doc1])?;

            // Try to rebuild an index that doesn't exist - should return error, not panic
            let result = collection.rebuild_index(vec!["nonexistent_field"]);
            assert!(result.is_err(), "Expected error when rebuilding non-existent index");

            // Verify the collection still works
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_rebuild_index_with_data() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test_defect_18_active_indexing")?;

            // Insert some documents
            let mut docs = vec![];
            for i in 0..50 {
                let name = format!("User {}", i);
                let doc = doc!{
                    "id": i,
                    "name": name
                };
                docs.push(doc);
            }
            collection.insert_many(docs)?;

            // Create an index
            collection.create_index(vec!["name"], &non_unique_index())?;

            // Rebuild the index - should work without panic
            collection.rebuild_index(vec!["name"])?;

            // Verify the index was rebuilt and collection still works
            assert!(collection.has_index(vec!["name"])?);
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 50);

            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_rebuild_multiple_indexes() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test_defect_18_multi_index")?;

            // Insert some documents
            let doc1 = doc!{
                "name": "David",
                "email": "david@example.com",
                "age": 28
            };

            let doc2 = doc!{
                "name": "Eve",
                "email": "eve@example.com",
                "age": 32
            };

            collection.insert_many(vec![doc1, doc2])?;

            // Create multiple indexes
            collection.create_index(vec!["name"], &non_unique_index())?;
            collection.create_index(vec!["email"], &non_unique_index())?;

            // Rebuild each index
            collection.rebuild_index(vec!["name"])?;
            collection.rebuild_index(vec!["email"])?;

            // Verify both indexes exist
            assert!(collection.has_index(vec!["name"])?);
            assert!(collection.has_index(vec!["email"])?);

            // Verify collection still works
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}
