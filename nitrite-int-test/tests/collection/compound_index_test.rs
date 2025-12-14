use chrono::{DateTime, Local};
use nitrite::collection::{CollectionEventInfo, CollectionEventListener, CollectionEvents};
use nitrite::common::Value;
use nitrite::doc;
use nitrite::filter::{all, and, field};
use nitrite::index::{full_text_index, non_unique_index, unique_index};
use nitrite_int_test::test_util::{
    cleanup, create_test_context, create_test_docs, run_test, NitriteDateTime,
};

#[test]
fn test_create_check_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;

            assert!(coll.has_index(vec!["first_name"])?);
            assert!(coll.has_index(vec!["first_name", "last_name"])?);

            assert!(!coll.has_index(vec!["first_name", "last_name", "birth_day"])?);
            assert!(!coll.has_index(vec!["last_name", "first_name"])?);
            assert!(!coll.has_index(vec!["last_name"])?);

            coll.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(coll.has_index(vec!["first_name"])?);

            coll.create_index(vec!["last_name"], &non_unique_index())?;
            assert!(coll.has_index(vec!["last_name"])?);

            let result = coll.insert_many(create_test_docs())?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_create_multi_key_index_first_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["arr", "last_name"], &non_unique_index())?;

            assert!(coll.has_index(vec!["arr"])?);
            assert!(coll.has_index(vec!["arr", "last_name"])?);
            assert!(!coll.has_index(vec!["last_name"])?);

            let result = coll.insert_many(create_test_docs())?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

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
            let coll = ctx.db().collection("test")?;
            assert_eq!(coll.list_indexes()?.len(), 0);

            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert_eq!(coll.list_indexes()?.len(), 1);

            coll.create_index(vec!["first_name"], &non_unique_index())?;
            assert_eq!(coll.list_indexes()?.len(), 2);

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
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert_eq!(coll.list_indexes()?.len(), 1);
            assert!(coll.has_index(vec!["first_name", "last_name"])?);
            assert!(coll.has_index(vec!["first_name"])?);

            coll.drop_index(vec!["first_name", "last_name"])?;
            assert_eq!(coll.list_indexes()?.len(), 0);

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
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert!(coll.has_index(vec!["first_name", "last_name"])?);
            assert!(coll.has_index(vec!["first_name"])?);
            assert!(!coll.has_index(vec!["last_name"])?);

            coll.create_index(vec!["first_name"], &non_unique_index())?;
            assert!(coll.has_index(vec!["first_name"])?);
            assert!(coll.has_index(vec!["first_name", "last_name"])?);
            assert!(!coll.has_index(vec!["last_name"])?);

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
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            coll.create_index(vec!["first_name"], &non_unique_index())?;
            assert_eq!(coll.list_indexes()?.len(), 2);

            coll.drop_all_indexes()?;
            assert_eq!(coll.list_indexes()?.len(), 0);

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
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;

            assert!(coll.has_index(vec!["first_name", "last_name"])?);
            assert!(coll.has_index(vec!["first_name"])?);

            let result = coll.insert_many(create_test_docs())?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            coll.rebuild_index(vec!["first_name", "last_name"])?;
            assert!(coll.has_index(vec!["first_name", "last_name"])?);
            assert!(coll.has_index(vec!["first_name"])?);
            assert_eq!(coll.list_indexes()?.len(), 1);

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
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;

            assert!(coll.has_index(vec!["first_name", "last_name"])?);
            assert!(coll.has_index(vec!["first_name"])?);

            let result = coll.insert_many(create_test_docs())?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            let mut result = coll.remove(
                and(vec![
                    field("first_name").eq("fn1"),
                    field("last_name").eq("ln1"),
                ]),
                false,
            )?;
            assert_eq!(result.count(), 1);
            assert_eq!(coll.size()?, 2);

            result = coll.remove(
                and(vec![
                    field("first_name").eq("fn2"),
                    field("birth_day").gte(NitriteDateTime::new(DateTime::from(Local::now()))),
                ]),
                false,
            )?;
            assert_eq!(result.count(), 0);
            assert_eq!(coll.size()?, 2);

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
            let coll = ctx.db().collection("test")?;
            let result = coll.insert_many(create_test_docs())?;
            assert_eq!(result.affected_nitrite_ids().len(), 3);

            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert!(coll.has_index(vec!["first_name", "last_name"])?);

            coll.rebuild_index(vec!["first_name", "last_name"])?;
            assert!(coll.has_index(vec!["first_name", "last_name"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_null_values_indexed_fields() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;

            let mut doc1 = doc! {
                first_name: (Value::Null),
                last_name: "ln1",
                birth_day: (NitriteDateTime::new(DateTime::from(Local::now()))),
                data: [1u8, 2u8, 3u8],
                list: ["one", "two", "three"],
                body: "a quick brown fox jump over the lazy dog",
            };

            let result = coll.insert(doc1)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);

            let mut cursor = coll.find(field("first_name").eq(Value::Null))?;
            let mut first = cursor.first().unwrap()?;
            assert_eq!(first.get("last_name").unwrap(), "ln1".into());
            assert_eq!(first.get("first_name").unwrap(), Value::Null);

            doc1 = doc! {
                first_name: "fn4",
                data: [1u8, 2u8, 3u8],
                list: ["one", "two", "three"],
                body: "a quick brown fox jump over the lazy dog",
            };
            coll.insert(doc1)?;

            cursor = coll.find(field("last_name").eq(Value::Null))?;
            first = cursor.first().unwrap()?;
            assert_eq!(first.get("first_name").unwrap(), "fn4".into());
            assert_eq!(first.get("last_name").unwrap(), Value::Null);

            cursor = coll.find(and(vec![
                field("birth_day").eq(Value::Null),
                field("last_name").eq(Value::Null),
            ]))?;
            first = cursor.first().unwrap()?;
            assert_eq!(first.get("first_name").unwrap(), "fn4".into());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_all_indexes_and_create_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert_eq!(coll.list_indexes()?.len(), 1);

            coll.drop_all_indexes()?;
            assert_eq!(coll.list_indexes()?.len(), 0);

            coll.create_index(vec!["first_name"], &non_unique_index())?;
            assert_eq!(coll.list_indexes()?.len(), 1);

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
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert!(coll.has_index(vec!["first_name"])?);

            let cursor = coll.find(and(vec![
                field("first_name").eq("fn1"),
                field("last_name").eq("ln1"),
            ]))?;
            let find_plan = cursor.find_plan().unwrap();
            assert!(find_plan.index_scan_filter().is_some());
            assert!(find_plan.full_scan_filter().is_none());

            coll.drop_all_indexes()?;
            let cursor = coll.find(and(vec![
                field("first_name").eq("fn1"),
                field("last_name").eq("ln1"),
            ]))?;
            let find_plan = cursor.find_plan().unwrap();
            assert!(find_plan.index_scan_filter().is_none());
            assert!(find_plan.full_scan_filter().is_some());

            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            let cursor = coll.find(and(vec![
                field("first_name").eq("fn1"),
                field("last_name").eq("ln1"),
            ]))?;
            let find_plan = cursor.find_plan().unwrap();
            assert!(find_plan.index_scan_filter().is_some());
            assert!(find_plan.full_scan_filter().is_none());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_by_index_on_different_number_type() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.drop_all_indexes()?;
            coll.remove(all(), false)?;

            let doc1 = doc! ({ "field1": 5 });
            let doc2 = doc! ({ "field1": 4.3, "field2": 3.5 });
            let doc3 = doc! ({ "field1": 0.03, "field2": 5 });
            let doc4 = doc! ({ "field1": 4, "field2": 4.5 });
            let doc5 = doc! ({ "field1": 5.0, "field2": 5.0 });

            coll.insert_many(vec![doc1, doc2, doc3, doc4, doc5])?;

            let cursor = coll.find(and(vec![field("field1").eq(0.03), field("field2").eq(5)]))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(and(vec![
                field("field1").eq(5),
                field("field2").eq(Value::Null),
            ]))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("field1").eq(5))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("field1").eq(5.0))?;
            assert_eq!(cursor.count(), 1);

            coll.create_index(vec!["field1", "field2"], &unique_index())?;
            let cursor = coll.find(and(vec![field("field1").eq(0.03), field("field2").eq(5)]))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(and(vec![
                field("field1").eq(5),
                field("field2").eq(Value::Null),
            ]))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("field1").eq(5))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("field1").eq(5.0))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_index_event() {
    use rand::Rng;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("index-test")?;
            let mut rng = rand::rng();
            for _ in 0..10000 {
                let document = doc! ({
                    "first": (rng.random::<i32>()),
                    "second": (rng.random::<f64>()),
                });
                coll.insert(document)?;
            }

            let failed = Arc::new(AtomicBool::new(false));
            let completed = Arc::new(AtomicBool::new(false));
            let failed_clone = Arc::clone(&failed);
            let completed_clone = Arc::clone(&completed);

            let handle = coll.subscribe(CollectionEventListener::new(
                move |event: CollectionEventInfo| {
                    match event.event_type() {
                        CollectionEvents::Insert
                        | CollectionEvents::Remove
                        | CollectionEvents::Update => {
                            failed_clone.store(true, Ordering::SeqCst);
                        }
                        CollectionEvents::IndexStart | CollectionEvents::IndexEnd => {
                            completed_clone.store(true, Ordering::SeqCst);
                        }
                    }
                    Ok(())
                },
            ))?;

            coll.create_index(vec!["first", "second"], &non_unique_index())?;
            assert_eq!(coll.find(all())?.count(), 10000);

            awaitility::at_most(Duration::from_secs(10)).until(|| completed.load(Ordering::SeqCst));
            assert!(!failed.load(Ordering::SeqCst));

            coll.unsubscribe(handle.unwrap())?;
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
            let coll = ctx.db().collection("index-on-null")?;

            let doc1 = doc! ({ "first": (Value::Null), "second": 123, "third": (vec![Value::from(1), Value::from(2), Value::Null]) });
            let doc2 = doc! ({ "first": "abcd", "second": 456, "third": (vec![3, 1]) });
            let doc3 = doc! ({ "first": "xyz", "second": 789, "third": (Value::Null) });

            coll.insert_many(vec![doc1, doc2, doc3])?;

            coll.create_index(vec!["third", "first"], &non_unique_index())?;

            let cursor = coll.find(field("first").eq(Value::Null))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("third").eq(Value::Null))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_create_invalid_unique_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;

            let dt2 = NitriteDateTime::parse_from_rfc3339("2010-06-12T12:05:35+05:30");
            let dt3 = NitriteDateTime::parse_from_rfc3339("2014-04-17T22:25:44-04:00");

            let doc2 = doc! {
                first_name: "fn2",
                last_name: "ln2",
                birth_day: dt2,
                data: (vec![3u8, 4u8, 3u8]),
                list: (vec!["three", "four", "five"]),
                body: "quick hello world from nitrite",
            };

            let doc3 = doc! {
                first_name: "fn3",
                last_name: "ln2",
                birth_day: dt3,
                data: (vec![9u8, 4u8, 8u8]),
                body: "Lorem ipsum dolor sit amet, consectetur \
                adipiscing elit. Sed nunc mi, mattis ullamcorper \
                dignissim vitae, condimentum non lorem.",
            };

            let doc1 = doc! ({
                first_name: "fn3",
                last_name: "ln2",
                birth_day: (NitriteDateTime::new(DateTime::from(Local::now()))),
                data: (vec![1, 2, 3]),
                list: (vec!["one", "two", "three"]),
                body: "a quick brown fox jump over the lazy dog",
            });

            coll.create_index(vec!["last_name", "first_name"], &unique_index())?;
            coll.insert_many(vec![doc1, doc2, doc3])?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_create_unique_multi_key_index_on_array() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["arr", "last_name"], &unique_index())?;
            coll.insert_many(create_test_docs())?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_create_on_invalid_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.insert_many(create_test_docs())?;
            // multiple null values will be created
            coll.create_index(vec!["my-value", "last_name"], &unique_index())?;
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
            let coll = ctx.db().collection("test")?;
            coll.drop_index(vec!["data", "first_name"])?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_rebuild_index_invalid() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.rebuild_index(vec!["unknown", "first_name"])?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn create_multiple_index_type_on_same_fields() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["last_name", "first_name"], &unique_index())?;
            coll.create_index(vec!["last_name", "first_name"], &non_unique_index())?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_index_already_exists() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name", "last_name"], &unique_index())?;
            assert!(coll.has_index(vec!["first_name"])?);
            coll.create_index(vec!["first_name", "last_name"], &non_unique_index())?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_create_compound_text_index() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["body", "last_name"], &full_text_index())?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_create_multi_key_index_second_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["last_name", "data"], &non_unique_index())?;
            assert!(coll.has_index(vec!["last_name"])?);
            coll.insert_many(create_test_docs())?;
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
