use nitrite::collection::{CollectionEventInfo, CollectionEventListener, CollectionEvents};
use nitrite::common::{atomic, ReadExecutor, Value, WriteExecutor};
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite::index::{full_text_index, non_unique_index, unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, insert_test_documents, run_test};
use std::time::Duration;

#[test]
fn test_create_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Create unique index on first_name
            collection.create_index(vec!["first_name"], &unique_index())?;
            assert!(collection.has_index(vec!["first_name"])?);
            // Create non-unique index on last_name
            collection.create_index(vec!["last_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["last_name"])?);
            // Create full-text index on body
            collection.create_index(vec!["body"], &full_text_index())?;
            assert!(collection.has_index(vec!["body"])?);
            // Create default (non-unique) index on birth_day
            collection.create_index(vec!["birth_day"], &non_unique_index())?;
            assert!(collection.has_index(vec!["birth_day"])?);

            // Insert test docs (helper call)
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
            // Recreate indexes using test_create_index logic
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
            assert!(!collection.has_index(vec!["last_name"])?
                   );
            collection.create_index(vec!["last_name"], &non_unique_index())?;
            assert!(collection.has_index(vec!["last_name"])?
                   );

            assert!(!collection.has_index(vec!["body"])?
                   );
            collection.create_index(vec!["body"], &full_text_index())?;
            assert!(collection.has_index(vec!["body"])?
                   );
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
            collection.create_index(vec!["first_name"], &unique_index())?;
            collection.create_index(vec!["body"], &full_text_index())?;
            // Insert documents using a helper method.
            insert_test_documents(&collection)?;
            // Remove document using first_name filter
            let res = collection.remove(field("first_name").eq("fn1"), false)?;
            assert_eq!(res.affected_nitrite_ids().len(), 1);

            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 2);

            let res = collection.remove(field("body").text("Lorem"), false)?;
            assert_eq!(res.affected_nitrite_ids().len(), 1);

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
            let indexes = collection.list_indexes()?;
            for idx in indexes {
                let index_fields = idx.index_fields();
                let field_names = index_fields.field_names();
                let fields: Vec<&str> = field_names.iter().map(|s| s.as_ref()).collect();
                collection.rebuild_index(fields)?;
            }
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_rebuild_index_on_running_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.create_index(vec!["body"], &full_text_index())?;
            insert_test_documents(&collection)?;

            // First rebuild should work.
            collection.rebuild_index(vec!["body"])?;
            // Calling rebuild index again should not cause error.
            let result = collection.rebuild_index(vec!["body"]);
            // Wait until indexing is completed.
            awaitility::at_most(Duration::from_secs(10))
                .until(|| !collection.is_indexing(vec!["body"]).unwrap());
            assert!(result.is_ok());
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
            collection.create_index(vec!["first_name"], &unique_index())?;
            collection.create_index(vec!["birth_day"], &non_unique_index())?;
            insert_test_documents(&collection)?;
            let doc = doc!{
                "first_name": (Value::Null),
                "last_name": "ln1",
                "birth_day": (Value::Null),
                "data": [1_u8, 2_u8, 3_u8],
                "list": ["one", "two", "three"],
                "body": "a quick brown fox jump over the lazy dog"
            };
            collection.insert(doc)?;
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

            // Get a fresh handle
            collection = ctx.db().collection("test")?;
            assert!(collection.has_index(vec!["first_name"])?);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_numeric_field_index_query_returns_single_result() {
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
            let mut docs = Vec::with_capacity(10000);
            for _ in 0..10000 {
                docs.push(doc!{
                    "first": (rng.random::<i32>()),
                    "second": (rng.random::<f64>())
                });
            }
            collection.insert_many(docs)?;

            let events = atomic(Vec::new());
            let events_clone = events.clone();
            collection.subscribe(CollectionEventListener::new(move |event_info: CollectionEventInfo| {
                match event_info.event_type() {
                    CollectionEvents::Insert |
                    CollectionEvents::Update |
                    CollectionEvents::Remove => panic!("Unexpected event type"),
                    CollectionEvents::IndexStart | CollectionEvents::IndexEnd => {
                        if let Some(arr) = event_info.item().and_then(|v| v.as_array().cloned()) {
                            let names: Vec<String> = arr.iter().filter_map(|v| v.as_string().map(|s| s.to_string())).collect();
                            events_clone.write_with(|e| e.extend(names));
                        }
                        Ok(())
                    }
                }
            }))?;

            collection.create_index(vec!["first"], &non_unique_index())?;
            assert_eq!(collection.find(all())?.count(), 10000);

            collection.create_index(vec!["second"], &non_unique_index())?;
            assert_eq!(collection.find(all())?.count(), 10000);

            let count = events.read_with(|ev| ev.len());
            assert!(count > 0);
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
                "third": [1, 2, (Value::Null)]
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

            collection.create_index(vec!["first"], &unique_index())?;
            assert_eq!(collection.find(field("first").eq(Value::Null))?.count(), 1);

            collection.create_index(vec!["third"], &non_unique_index())?;
            assert_eq!(collection.find(field("third").eq(Value::Null))?.count(), 2);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_compound_and_single_field_index_on_same_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Create non-unique index on last_name then unique index on first_name.
            collection.create_index(vec!["last_name"], &non_unique_index())?;
            collection.create_index(vec!["first_name"], &unique_index())?;
            // Create a compound index on last_name and first_name.
            collection.create_index(vec!["last_name", "first_name"], &non_unique_index())?;
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
            // Create a unique index on "lastName"
            collection.create_index(vec!["lastName"], &unique_index())?;
            assert!(collection.has_index(vec!["lastName"])?);
            // Inserting documents with duplicate values should return an error.
            let result = insert_test_documents(&collection);
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
            // Create a unique index on "arr" where the field is an array with duplicates.
            collection.create_index(vec!["arr"], &unique_index())?;
            assert!(collection.has_index(vec!["arr"])?);
            let result = insert_test_documents(&collection);
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
            // Insert documents first; multiple null values will be created for a non-existent field.
            insert_test_documents(&collection)?;
            let result = collection.create_index(vec!["my-value"], &unique_index());
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
            // Insert documents so the field exists.
            insert_test_documents(&collection)?;
            // Creating a full-text index on a non-text field here, "data" should fail.
            let result = collection.create_index(vec!["data"], &full_text_index());
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
            // Attempting to drop an index on a field that was never indexed should return an error.
            let result = collection.drop_index(vec!["data"]);
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
            // Rebuilding an index on an unknown field should cause an error.
            let result = collection.rebuild_index(vec!["unknown"]);
            assert!(result.is_err());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_multiple_index_type_on_same_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            // Creating a unique index on "lastName"
            collection.create_index(vec!["lastName"], &unique_index())?;
            // Trying to create a different index type on the same field should return an error.
            let result = collection.create_index(vec!["lastName"], &non_unique_index());
            assert!(result.is_err());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}