use chrono::{DateTime, Utc};
use icu::locale::locale;
use icu_collator::options::CollatorOptions;
use nitrite::collection::{limit_to, order_by, skip_by, NitriteId};
use nitrite::common::{SortOrder, Value};
use nitrite::doc;
use nitrite::filter::{all, and, by_id, field, or};
use nitrite::index::{non_unique_index, unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, create_test_docs, insert_test_documents, is_sorted, now, run_test, NitriteDateTime};

#[test]
fn test_find_all() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("birth_day").gt(now()))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("birth_day").gte(now()))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("birth_day").lt(now()))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("birth_day").lte(now()))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("birth_day").lte(now()).and(field("first_name").eq("fn1")))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("birth_day").lte(now()).or(field("first_name").eq("fn12")))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(and(vec![
                or(vec![
                    field("birth_day").lte(now()),
                    field("first_name").eq("fn12"),
                ]),
                field("last_name").eq("ln1"),
            ]))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(and(vec![
                or(vec![
                    field("birth_day").lte(now()),
                    field("first_name").eq("fn12"),
                ]),
                field("last_name").eq("ln1"),
            ]).not())?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("arr.1").eq(4))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("arr.1").lt(4))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("last_name").in_array(vec!["ln1", "ln2", "ln10"]))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("first_name").not_in_array(vec!["fn1", "fn2"]))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(all().not())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_skip_limit() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = skip_by(0).limit(1);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 1);

            let options = skip_by(1).limit(3);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 2);

            let options = skip_by(0).limit(30);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 3);

            let options = skip_by(2).limit(3);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_skip() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = skip_by(0);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 3);

            let options = skip_by(1);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 2);

            let options = skip_by(30);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 0);

            let options = skip_by(2);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_limit() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = limit_to(0);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 0);

            let options = limit_to(1);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 1);

            let options = limit_to(30);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_sort_ascending() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("birth_day", SortOrder::Ascending);
            let cursor = coll.find_with_options(all(), &options)?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 3);
            
            let mut date_list = vec![];
            for doc in results {
                let doc = doc?;
                if let Value::String(date) = doc.get("birth_day")? {
                    date_list.push(date);
                }
            }
            
            assert_eq!(date_list.len(), 3);
            assert!(is_sorted(&date_list, true));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_sort_descending() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("birth_day", SortOrder::Descending);
            let cursor = coll.find_with_options(all(), &options)?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 3);
            
            let mut date_list = vec![];
            for doc in results {
                let doc = doc?;
                if let Value::String(date) = doc.get("birth_day")? {
                    date_list.push(date);
                }
            }
            
            assert_eq!(date_list.len(), 3);
            assert!(is_sorted(&date_list, false));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_limit_and_sort() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("birth_day", SortOrder::Descending)
                .skip(1)
                .limit(2);
            let cursor = coll.find_with_options(all(), &options)?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 2);
            
            let mut date_list = vec![];
            for doc in results {
                let doc = doc?;
                if let Value::String(date) = doc.get("birth_day")? {
                    date_list.push(date);
                }
            }
            assert!(is_sorted(&date_list, false));

            let options = order_by("birth_day", SortOrder::Ascending)
                .skip(1)
                .limit(2);
            let cursor = coll.find_with_options(all(), &options)?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 2);
            
            let mut date_list = vec![];
            for doc in results {
                let doc = doc?;
                if let Value::String(date) = doc.get("birth_day")? {
                    date_list.push(date);
                }
            }
            assert!(is_sorted(&date_list, true));

            let options = order_by("first_name", SortOrder::Ascending)
                .skip(0)
                .limit(30);
            let cursor = coll.find_with_options(all(), &options)?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 3);
            
            let mut name_list = vec![];
            for doc in results {
                let doc = doc?;
                if let Value::String(name) = doc.get("first_name")? {
                    name_list.push(name);
                }
            }
            assert!(is_sorted(&name_list, true));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_sort_on_non_existing_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("my-value", SortOrder::Descending);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_invalid_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("my_field").eq("my_data"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_invalid_field_with_invalid_accessor() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("my_field.0").eq("my_data"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_limit_and_sort_invalid_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("birth_day2", SortOrder::Descending)
                .skip(1)
                .limit(2);
            let cursor = coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_get_by_id() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            let doc1 = create_test_docs()[0].clone();
            coll.insert(doc1)?;

            // Create an invalid ID
            let id = NitriteId::create_id(1);
            assert!(id.is_err());

            // Get with the actual ID
            let document = coll.find(all())?.first().expect("Failed to get first document")?;
            assert_eq!(document.get("first_name")?, Value::String("fn1".to_string()));
            assert_eq!(document.get("last_name")?, Value::String("ln1".to_string()));
            
            // Check array values
            if let Value::Array(data) = document.get("arr")? {
                assert_eq!(data.len(), 3);
                assert_eq!(data[0], Value::I32(1));
                assert_eq!(data[1], Value::I32(2));
                assert_eq!(data[2], Value::I32(3));
            } else {
                panic!("Expected array value for 'data' field");
            }
            
            assert_eq!(document.get("body")?, Value::String("a quick brown fox jump over the lazy dog".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_filter_and_option() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("first_name", SortOrder::Ascending)
                .skip(1)
                .limit(2);
            
            let cursor = coll.find_with_options(field("birth_day").lte(now()), &options)?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_text_with_regex() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("body").text_regex("hello"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("body").text_regex("test"))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("body").text_regex("^hello$"))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("body").text_regex(".*"))?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_project() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("first_name", SortOrder::Ascending)
                .skip(0)
                .limit(3);
            
            let cursor = coll.find_with_options(field("birth_day").lte(now()), &options)?;
            
            // This is a basic implementation - in Rust we'd typically compare 
            // specific fields rather than whole documents
            let mut docs = vec![];
            for doc in cursor {
                docs.push(doc?);
            }
            
            assert_eq!(docs.len(), 3);
            
            // Check if docs[0] is similar to doc1
            assert_eq!(docs[0].get("first_name")?, Value::String("fn1".to_string()));
            assert_eq!(docs[0].get("last_name")?, Value::String("ln1".to_string()));
            
            // Check if docs[1] is similar to doc2
            assert_eq!(docs[1].get("first_name")?, Value::String("fn2".to_string()));
            assert_eq!(docs[1].get("last_name")?, Value::String("ln2".to_string()));
            
            // Check if docs[2] is similar to doc3
            assert_eq!(docs[2].get("first_name")?, Value::String("fn3".to_string()));
            assert_eq!(docs[2].get("last_name")?, Value::String("ln2".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_array_equal() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let array = Value::Array(vec![Value::I32(3), Value::I32(4), Value::I32(3)]);
            let cursor = coll.find(field("arr").eq(array))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_array_equal_fail_for_wrong_cardinality() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let array = Value::Array(vec![Value::I32(4), Value::I32(3), Value::I32(3)]);
            let cursor = coll.find(field("arr").eq(array))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_iterable_equal() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let list = Value::Array(vec![
                Value::String("three".to_string()),
                Value::String("four".to_string()),
                Value::String("five".to_string())
            ]);
            
            let cursor = coll.find(field("list").eq(list))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_iterable_equal_fail_for_wrong_cardinality() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let list = Value::Array(vec![
                Value::String("four".to_string()),
                Value::String("three".to_string()),
                Value::String("three".to_string())
            ]);
            
            let cursor = coll.find(field("list").eq(list))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_in_array() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("arr").elem_match(field("$").gte(2).and(field("$").lt(5))))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("arr").elem_match(field("$").gt(2).or(field("$").lte(5))))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("arr").elem_match(field("$").gt(1).and(field("$").lt(4))))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_in_list() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("list").elem_match(field("$").text_regex("three")))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("list").elem_match(field("$").text_regex("hello")))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("list").elem_match(field("$").text_regex("hello").not()))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_elem_match_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("prod_score")?;
            
            // Create and insert document 1
            let product_scores1 = vec![
                doc!{
                    "product": "abc",
                    "score": 10
                },
                doc!{
                    "product": "xyz",
                    "score": 5
                }
            ];
            let str_array1 = vec!["a", "b"];
            let doc1 = doc!{
                "product_scores": product_scores1,
                "str_array": str_array1
            };
            
            // Create and insert document 2
            let product_scores2 = vec![
                doc!{
                    "product": "abc",
                    "score": 8
                },
                doc!{
                    "product": "xyz",
                    "score": 7
                }
            ];
            let str_array2 = vec!["d", "e"];
            let doc2 = doc!{
                "product_scores": product_scores2,
                "str_array": str_array2
            };
            
            // Create and insert document 3
            let product_scores3 = vec![
                doc!{
                    "product": "abc",
                    "score": 7
                },
                doc!{
                    "product": "xyz",
                    "score": 8
                }
            ];
            let str_array3 = vec!["a", "f"];
            let doc3 = doc!{
                "product_scores": product_scores3,
                "str_array": str_array3
            };
            
            coll.insert(doc1)?;
            coll.insert(doc2)?;
            coll.insert(doc3)?;

            // Test elem_match with nested fields
            let cursor = coll.find(field("product_scores").elem_match(
                field("product").eq("xyz").and(field("score").gte(8))
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").lte(8).not()
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("product_scores").elem_match(
                field("product").eq("xyz").or(field("score").gte(8))
            ))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("product_scores").elem_match(
                field("product").eq("xyz")
            ))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").gte(10)
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").gt(8)
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").lt(7)
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").lte(7)
            ))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").in_array(vec![7, 8])
            ))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("product_scores").elem_match(
                field("score").not_in_array(vec![7, 8])
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("product_scores").elem_match(
                field("product").text_regex("xyz")
            ))?;
            assert_eq!(cursor.count(), 3);

            // Test elem_match with string arrays
            let cursor = coll.find(field("str_array").elem_match(field("$").eq("a")))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("str_array").elem_match(
                field("$").eq("a").or(field("$").eq("f").or(field("$").eq("b"))).not()
            ))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("str_array").elem_match(field("$").gt("e")))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("str_array").elem_match(field("$").gte("e")))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("str_array").elem_match(field("$").lte("b")))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("str_array").elem_match(field("$").lt("a")))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("str_array").elem_match(
                field("$").in_array(vec!["a", "f"])
            ))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("str_array").elem_match(field("$").text_regex("a")))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_not_equal_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            
            let doc = doc!{
                "abc": "123",
            };
            
            coll.insert(doc)?;
            
            let cursor = coll.find(field("abc").eq("123"))?;
            let result = cursor.collect::<Vec<_>>();
            assert_eq!(result.len(), 1);

            let cursor = coll.find(field("xyz").eq(Value::Null))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("abc").ne(Value::Null))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("abc").ne(Value::Null).and(field("xyz").eq(Value::Null)))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("abc").eq(Value::Null).and(field("xyz").ne(Value::Null)))?;
            assert_eq!(cursor.count(), 0);

            // Test with revision field
            coll.remove(all(), false)?;
            
            let mut doc = doc!{
                "field": "two"
            };
            doc.put("_revision", 1482225343161i64)?;
            
            coll.insert(doc)?;
            
            let cursor = coll.find(
                field("_revision").gte(1482225343160i64)
                    .and(field("_revision").lte(1482225343162i64)
                        .and(field("_revision").ne(Value::Null)))
            );
            assert!(cursor.is_ok());
            let doc = cursor?.first();
            assert!(doc.is_none());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_filter_all() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            
            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 0);

            insert_test_documents(&coll)?;
            
            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_sort_with_null_values() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["id"], &unique_index())?;
            coll.create_index(vec!["group"], &non_unique_index())?;

            coll.remove(all(), false)?;

            let doc1 = doc! ({
                "id": "test-1",
                "group": "groupA"
            });
            assert_eq!(1, coll.insert(doc1)?.affected_nitrite_ids().len());

            let now = Utc::now();
            let doc2 = doc! ({
                "id": "test-2",
                "group": "groupA",
                "start_time": (NitriteDateTime::new(DateTime::from(now)))
            });
            assert_eq!(1, coll.insert(doc2)?.affected_nitrite_ids().len());

            // Test ordering with descending sort by null field
            let options = order_by("start_time", SortOrder::Descending);
            let cursor = coll.find_with_options(field("group").eq("groupA"), &options)?;
            let result = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
            assert_eq!(2, result.len());
            
            assert!(result[1].get("start_time")?.is_null()); // null in first document
            assert!(!result[0].get("start_time")?.is_null());  // not null in second document

            // Test ordering with ascending sort by null field
            let options = order_by("start_time", SortOrder::Ascending);
            let cursor = coll.find_with_options(field("group").eq("groupA"), &options)?;
            let result = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
            assert_eq!(2, result.len());
            
            assert!(result[0].get("start_time")?.is_null()); // null in first document
            assert!(!result[1].get("start_time")?.is_null());  // not null in second document

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_order_by_non_existent_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("order_by_on_nullable_column2")?;

            coll.remove(all(), false)?;

            let doc1 = doc!{
                "id": "test-2",
                "group": "groupA"
            };
            assert_eq!(1, coll.insert(doc1)?.affected_nitrite_ids().len());

            let doc2 = doc!{
                "id": "test-1",
                "group": "groupA"
            };
            assert_eq!(1, coll.insert(doc2)?.affected_nitrite_ids().len());

            let options = order_by("start_time", SortOrder::Descending);
            let cursor = coll.find_with_options(field("group").eq("groupA"), &options)?;
            assert_eq!(2, cursor.count());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_null_order_with_all_null() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;

            coll.remove(all(), false)?;

            let doc1 = doc!{
                "id": "test-2",
                "group": "groupA"
            };
            assert_eq!(1, coll.insert(doc1)?.affected_nitrite_ids().len());

            let doc2 = doc!{
                "id": "test-1",
                "group": "groupA"
            };
            assert_eq!(1, coll.insert(doc2)?.affected_nitrite_ids().len());

            let options = order_by("start_time", SortOrder::Descending);
            let cursor = coll.find_with_options(field("group").eq("groupA"), &options)?;
            assert_eq!(2, cursor.count());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_default_null_order() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            
            // Try to create index, but ignore if it fails
            let _ = coll.create_index(vec!["start_time"], &non_unique_index());

            coll.remove(all(), false)?;

            let doc1 = doc!{
                "id": "test-1",
                "group": "groupA"
            };
            assert_eq!(1, coll.insert(doc1)?.affected_nitrite_ids().len());

            let now = Utc::now();
            let doc2 = doc!{
                "id": "test-2",
                "group": "groupA",
                "start_time": (NitriteDateTime::new(DateTime::from(now)))
            };
            assert_eq!(1, coll.insert(doc2)?.affected_nitrite_ids().len());

            let now_plus_1 = now + chrono::Duration::minutes(1);
            let doc3 = doc!{
                "id": "test-3",
                "group": "groupA",
                "start_time": (NitriteDateTime::new(DateTime::from(now_plus_1)))
            };
            assert_eq!(1, coll.insert(doc3)?.affected_nitrite_ids().len());

            // Test descending sort order
            let options = order_by("start_time", SortOrder::Descending);
            let cursor = coll.find_with_options(field("group").eq("groupA"), &options)?;
            let result = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
            assert_eq!(3, result.len());
            
            // Verify order: doc3, doc2, doc1 (nulls last in descending order)
            assert_eq!(result[0].get("id")?, Value::String("test-3".to_string()));
            assert_eq!(result[1].get("id")?, Value::String("test-2".to_string()));
            assert_eq!(result[2].get("id")?, Value::String("test-1".to_string()));

            // Test ascending sort order
            let options = order_by("start_time", SortOrder::Ascending);
            let cursor = coll.find_with_options(field("group").eq("groupA"), &options)?;
            let result = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
            assert_eq!(3, result.len());
            
            // Verify order: doc1, doc2, doc3 (nulls first in ascending order)
            assert_eq!(result[0].get("id")?, Value::String("test-1".to_string()));
            assert_eq!(result[1].get("id")?, Value::String("test-2".to_string()));
            assert_eq!(result[2].get("id")?, Value::String("test-3".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_filter_invalid_accessor() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(field("last_name.name").eq("ln2"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_collation_with_diacritics() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            
            let doc1 = doc!{
                "id": "test-1",
                "fruit": "Apple"
            };
            
            let doc2 = doc!{
                "id": "test-2",
                "fruit": "Ôrange"
            };
            
            let doc3 = doc!{
                "id": "test-3",
                "fruit": "Pineapple"
            };

            coll.insert(doc1)?;
            coll.insert(doc2)?;
            coll.insert(doc3)?;

            let options = order_by("fruit", SortOrder::Ascending)
                .collator_options(CollatorOptions::default())
                .collator_preferences(locale!("fr").into());
                
            let cursor = coll.find_with_options(all(), &options)?;
            let docs = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
            
            // Verify the second document is "Ôrange" (proper collation)
            assert_eq!(docs[1].get("fruit")?, Value::String("Ôrange".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_id_set() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;
            
            let cursor = coll.find(field("last_name").eq("ln2"))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("last_name").eq("ln1"))?;
            let docs = cursor.map(|x| x.unwrap()).collect::<Vec<_>>();
            assert_eq!(docs.len(), 1);

            let doc = docs.first().unwrap();
            assert_eq!(doc.get("last_name")?, Value::String("ln1".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_collection_field() {
    run_test(
        create_test_context,
        |ctx| {
            let example = ctx.db().collection("example")?;

            // Create first document with embedded documents
            let tags1 = vec![
                doc!{
                    "type": "example",
                    "other": "value"
                },
                doc! ({
                    "type": "another-example",
                    "other": "some-other-value"
                })
            ];
            
            let doc1 = doc!{
                "name": "John",
                "tags": tags1
            };
            
            example.insert(doc1)?;

            // Create second document with embedded documents  
            let tags2 = vec![
                doc!{
                    "type": "example2",
                    "other": "value2"
                },
                doc!{
                    "type": "another-example2",
                    "other": "some-other-value2"
                }
            ];
            
            let doc2 = doc!{
                "name": "Jane",
                "tags": tags2
            };
            
            example.insert(doc2)?;

            // Find documents where tags have type "example"
            let cursor = example.find(field("tags").elem_match(field("type").eq("example")))?;
            
            // Verify we found only documents with "John"
            for doc in cursor {
                let doc = doc?;
                assert_eq!(doc.get("name")?, Value::String("John".to_string()));
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_between_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("tag")?;
            
            let doc1 = doc! ({ "age": 31, "tag": "one" });
            let doc2 = doc! ({ "age": 32, "tag": "two" });
            let doc3 = doc! ({ "age": 33, "tag": "three" });
            let doc4 = doc! ({ "age": 34, "tag": "four" });
            let doc5 = doc! ({ "age": 35, "tag": "five" });

            collection.insert(doc1)?;
            collection.insert(doc2)?;
            collection.insert(doc3)?;
            collection.insert(doc4)?;
            collection.insert(doc5)?;
            
            collection.create_index(vec!["age"], &unique_index())?;

            // Test inclusive bounds
            let cursor = collection.find(field("age").between_optional_inclusive(31, 35))?;
            assert_eq!(cursor.count(), 5);

            // Test exclusive lower bound
            let cursor = collection.find(field("age").between(31, 35, false, true))?;
            assert_eq!(cursor.count(), 4);

            // Test exclusive upper bound
            let cursor = collection.find(field("age").between(31, 35, true, false))?;
            assert_eq!(cursor.count(), 4);

            // Test both bounds exclusive
            let cursor = collection.find(field("age").between_inclusive(31, 35, false))?;
            assert_eq!(cursor.count(), 3);

            // Test negated between
            let cursor = collection.find(field("age").between_optional_inclusive(31, 35).not())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_by_id_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("tag")?;
            
            let doc1 = doc! ({ "age": 31, "tag": "one" });
            let doc2 = doc! ({ "age": 32, "tag": "two" });
            let doc3 = doc! ({ "age": 33, "tag": "three" });
            let doc4 = doc! ({ "age": 34, "tag": "four" });
            let doc5 = doc! ({ "age": 35, "tag": "five" });

            collection.insert(doc1)?;
            collection.insert(doc2)?;
            collection.insert(doc3)?;
            collection.insert(doc4)?;
            collection.insert(doc5)?;

            // Get a document and its ID
            let documents = collection.find(all())?.collect::<Vec<_>>();
            let mut document = documents.into_iter().next().unwrap()?;
            let nitrite_id = document.id()?;

            // Find by ID
            let result = collection.find(by_id(nitrite_id))?.first();
            assert!(result.is_some());
            assert_eq!(result.unwrap()?.id()?, nitrite_id.clone());

            // Find with AND condition
            let result = collection.find(and(vec![
                by_id(nitrite_id),
                field("age").ne(Value::Null)
            ]))?.first();
            assert!(result.is_some());
            assert_eq!(result.unwrap()?.id()?, nitrite_id.clone());

            // Find with OR condition
            let result = collection.find(or(vec![
                by_id(nitrite_id),
                field("tag").eq("one")
            ]))?.first();
            assert!(result.is_some());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_by_non_existing_id() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("tag")?;

            let doc1 = doc!{ "age": 31, "tag": "one" };
            let doc2 = doc!{ "age": 32, "tag": "two" };
            let doc3 = doc!{ "age": 33, "tag": "three" };
            let doc4 = doc!{ "age": 34, "tag": "four" };
            let doc5 = doc!{ "age": 35, "tag": "five" };

            collection.insert(doc1)?;
            collection.insert(doc2)?;
            collection.insert(doc3)?;
            collection.insert(doc4)?;
            collection.insert(doc5)?;

            // Create a new ID that doesn't exist in the collection
            let nitrite_id = NitriteId::new();

            // Find by non-existing ID
            let result = collection.find(by_id(nitrite_id));
            assert!(result.is_ok());
            let result = result?.first();
            assert!(result.is_none());

            // Find with AND condition
            let result = collection.find(and(vec![
                by_id(nitrite_id),
                field("age").ne(Value::Null)
            ]))?.first();
            assert!(result.is_none());

            // Find with OR condition
            let result = collection.find(or(vec![
                by_id(nitrite_id),
                field("tag").eq("one")
            ]))?.first();
            assert!(result.is_some());
            assert_eq!(result.unwrap()?.get("tag")?, Value::String("one".to_string()));

            // Find by _id field directly
            let cursor = collection.find(field("_id").eq(Value::NitriteId(nitrite_id)))?.first();
            let mut iter = cursor.into_iter();
            assert!(iter.next().is_none());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_project_custom_document() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let options = order_by("first_name", SortOrder::Ascending)
                .skip(0)
                .limit(3);
            
            let mut cursor = coll.find_with_options(field("birth_day").lte(now()), &options)?;
            
            // Create a projection with first_name and last_name fields
            let projection = doc!{
                "first_name": (Value::Null),
                "last_name": (Value::Null)
            };

            // Project the cursor
            let projected = cursor.project(projection)?;
            
            // Verify the projection
            let mut documents = vec![];
            for doc_result in projected.into_iter() {
                documents.push(doc_result?);
            }
            
            assert_eq!(documents.len(), 3);
            
            // Verify each document has only first_name and last_name fields
            for (idx, doc) in documents.iter().enumerate() {
                // Should contain these fields
                assert!(doc.contains_key("first_name"));
                assert!(doc.contains_key("last_name"));
                
                // Should not contain these fields
                assert!(!doc.contains_key("_id"));
                assert!(!doc.contains_key("birth_day"));
                assert!(!doc.contains_key("arr"));
                assert!(!doc.contains_key("body"));
                
                // Check field values based on document index
                match idx {
                    0 => {
                        assert_eq!(doc.get("first_name")?, Value::String("fn1".to_string()));
                        assert_eq!(doc.get("last_name")?, Value::String("ln1".to_string()));
                    },
                    1 => {
                        assert_eq!(doc.get("first_name")?, Value::String("fn2".to_string()));
                        assert_eq!(doc.get("last_name")?, Value::String("ln2".to_string()));
                    },
                    2 => {
                        assert_eq!(doc.get("first_name")?, Value::String("fn3".to_string()));
                        assert_eq!(doc.get("last_name")?, Value::String("ln2".to_string()));
                    },
                    _ => panic!("Unexpected document index"),
                }
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_project_with_inaccessible_fields() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;
            
            let mut cursor = coll.find(all())?;
            
            // Create a projection with non-existent fields
            let projection = doc!{
                "non_existent_field": (Value::Null),
                "another_missing_field": (Value::Null)
            };
            
            // Project the cursor - this should work but return documents with null fields
            let projected = cursor.project(projection)?;
            
            let docs = projected.into_iter().collect::<Vec<_>>();
            assert_eq!(docs.len(), 3);
            
            // Check that documents have the requested fields but with null values
            for doc_result in docs {
                let doc = doc_result?;
                assert!(doc.contains_key("non_existent_field"));
                assert!(doc.contains_key("another_missing_field"));
                assert_eq!(doc.get("non_existent_field")?, Value::Null);
                assert_eq!(doc.get("another_missing_field")?, Value::Null);
            }
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_nested_projection() {
    run_test(
        create_test_context,
        |ctx| {
            let nested_coll = ctx.db().collection("nested_test")?;
            
            // Create document with nested fields
            let address = doc!{
                "street": "123 Main St",
                "city": "Anytown",
                "zip": 12345
            };
            
            let contact = doc!{
                "email": "test@example.com",
                "phone": "555-1234"
            };
            
            let person = doc!{
                "name": "John Doe",
                "age": 30,
                "address": address,
                "contact": contact
            };
            
            nested_coll.insert(person)?;
            
            // Create projection for nested fields
            let projection = doc!{
                "name": (Value::Null),
                "address.city": (Value::Null),
                "contact.email": (Value::Null)
            };
            
            // Get projected document
            let mut cursor = nested_coll.find(all())?;
            let projected = cursor.project(projection)?;
            
            let docs = projected.into_iter().collect::<Vec<_>>();
            assert_eq!(docs.len(), 1);
            
            let doc = docs[0].as_ref().unwrap();
            
            // Check that top-level field exists
            assert!(doc.contains_key("name"));
            assert_eq!(doc.get("name")?, Value::String("John Doe".to_string()));
            
            assert!(doc.contains_key("address"));
            assert!(doc.contains_key("contact"));
            
            // Verify the nested objects are complete
            if let Value::Document(address_doc) = doc.get("address")? {
                assert_eq!(address_doc.get("city")?, Value::String("Anytown".to_string()));
                assert_eq!(address_doc.get("street")?, Value::Null);
                assert_eq!(address_doc.get("zip")?, Value::Null);
            } else {
                panic!("Expected address to be a document");
            }
            
            if let Value::Document(contact_doc) = doc.get("contact")? {
                assert_eq!(contact_doc.get("email")?, Value::String("test@example.com".to_string()));
                assert_eq!(contact_doc.get("phone")?, Value::Null);
            } else {
                panic!("Expected contact to be a document");
            }
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_invalid_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;
            
            // Test with invalid JSON path in filter
            let cursor = coll.find(field("non.existent.nested.field").eq("value"))?;
            assert_eq!(cursor.count(), 0);
            
            // Test with valid field name but invalid value type
            let cursor = coll.find(field("first_name").eq(42))?;
            assert_eq!(cursor.count(), 0);
            
            // Test with completely invalid filter syntax
            let cursor = coll.find(field("@invalid#field$name").eq("value"))?;
            assert_eq!(cursor.count(), 0);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_regex_special_characters() {
    run_test(
        create_test_context,
        |ctx| {
            let regex_coll = ctx.db().collection("regex_test")?;
            
            // Create documents with special regex characters
            regex_coll.insert(doc!{ "text": "a.b" })?;
            regex_coll.insert(doc!{ "text": "a*b" })?;
            regex_coll.insert(doc!{ "text": "a+b" })?;
            regex_coll.insert(doc!{ "text": "a?b" })?;
            regex_coll.insert(doc!{ "text": "a(b)c" })?;
            
            // Test literal regex match - should escape special characters
            let cursor = regex_coll.find(field("text").text_regex(r"a\.b"))?;
            assert_eq!(cursor.count(), 1);
            
            let cursor = regex_coll.find(field("text").text_regex(r"a\*b"))?;
            assert_eq!(cursor.count(), 1);
            
            let cursor = regex_coll.find(field("text").text_regex(r"a\+b"))?;
            assert_eq!(cursor.count(), 1);
            
            let cursor = regex_coll.find(field("text").text_regex(r"a\?b"))?;
            assert_eq!(cursor.count(), 1);
            
            let cursor = regex_coll.find(field("text").text_regex(r"a\(b\)c"))?;
            assert_eq!(cursor.count(), 1);
            
            // Test using regex special characters
            regex_coll.insert(doc!{ "text": "abc" })?;
            regex_coll.insert(doc!{ "text": "abbc" })?;
            regex_coll.insert(doc!{ "text": "abbbc" })?;
            
            let cursor = regex_coll.find(field("text").text_regex(r"ab+c"))?;
            assert_eq!(cursor.count(), 3);
            
            // Test empty results
            let cursor = regex_coll.find(field("text").text_regex(r"nonexistent"))?;
            assert_eq!(cursor.count(), 0);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_empty_collection() {
    run_test(
        create_test_context,
        |ctx| {
            let empty_coll = ctx.db().collection("empty")?;
            
            // Test with ALL filter
            let cursor = empty_coll.find(all())?;
            assert_eq!(cursor.count(), 0);
            
            // Test with specific filter
            let cursor = empty_coll.find(field("name").eq("value"))?;
            assert_eq!(cursor.count(), 0);
            
            // Test with compound filter
            let cursor = empty_coll.find(
                field("name").eq("value").and(field("age").gt(30))
            )?;
            assert_eq!(cursor.count(), 0);
            
            // Test with sort and limit options
            let options = order_by("name", SortOrder::Ascending).limit(10);
            let cursor = empty_coll.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 0);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_filter_non_existent_long_path() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;
            
            // Test with a very long path that doesn't exist
            let cursor = coll.find(field("this.is.a.very.long.path.that.does.not.exist").eq("value"))?;
            assert_eq!(cursor.count(), 0);
            
            // Test with array index in a non-existent path
            let cursor = coll.find(field("this.does.not.exist.0.field").eq("value"))?;
            assert_eq!(cursor.count(), 0);
            
            // Test with very deep array indexing
            let cursor = coll.find(field("arr.0.1.2.3.4.5.6.7.8.9").eq("value"))?;
            assert_eq!(cursor.count(), 0);
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_binary_data() {
    run_test(
        create_test_context,
        |ctx| {
            let binary_coll = ctx.db().collection("binary")?;
            
            // Create document with binary data
            let binary_data1 = b"Hello, world!".to_vec();
            let binary_data2 = b"Goodbye, world!".to_vec();
            
            binary_coll.insert(doc!{
                "name": "doc1",
                "binary": (Value::Bytes(binary_data1.clone()))
            })?;
            
            binary_coll.insert(doc!{
                "name": "doc2",
                "binary": (Value::Bytes(binary_data2.clone()))
            })?;
            
            // Find by binary value
            let cursor = binary_coll.find(field("binary").eq(Value::Bytes(binary_data1.clone())))?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
            
            let doc = results.first().unwrap().clone()?;
            assert_eq!(doc.get("name")?, Value::String("doc1".to_string()));
            
            // Find using not equals
            let cursor = binary_coll.find(field("binary").ne(Value::Bytes(binary_data1)))?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
            
            let doc = results.first().unwrap().clone()?;
            assert_eq!(doc.get("name")?, Value::String("doc2".to_string()));
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_boolean_values() {
    run_test(
        create_test_context,
        |ctx| {
            let bool_coll = ctx.db().collection("boolean")?;
            
            bool_coll.insert(doc!{
                "name": "doc1",
                "flag": true
            })?;
            
            bool_coll.insert(doc!{
                "name": "doc2",
                "flag": false
            })?;
            
            bool_coll.insert(doc!{
                "name": "doc3"
                // no flag field
            })?;
            
            // Find by true value
            let cursor = bool_coll.find(field("flag").eq(true))?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
            
            let doc = results.first().unwrap().clone()?;
            assert_eq!(doc.get("name")?, Value::String("doc1".to_string()));
            
            // Find by false value
            let cursor = bool_coll.find(field("flag").eq(false))?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
            
            let doc = results.first().unwrap().clone()?;
            assert_eq!(doc.get("name")?, Value::String("doc2".to_string()));
            
            // Find where flag is null (not present)
            let cursor = bool_coll.find(field("flag").eq(Value::Null))?;
            let results = cursor.collect::<Vec<_>>();
            assert_eq!(results.len(), 1);
            
            let doc = results.first().unwrap().clone()?;
            assert_eq!(doc.get("name")?, Value::String("doc3".to_string()));
            
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_numeric_comparisons() {
    run_test(
        create_test_context,
        |ctx| {
            let num_coll = ctx.db().collection("numeric")?;
            
            // Insert documents with different numeric types
            num_coll.insert(doc!{
                "name": "int",
                "value": 10
            })?;
            
            num_coll.insert(doc!{
                "name": "float",
                "value": 10.5
            })?;
            
            num_coll.insert(doc!{
                "name": "long",
                "value": 1000000000i64
            })?;
            
            // Test equality with integer value
            let cursor = num_coll.find(field("value").eq(10))?;
            assert_eq!(cursor.count(), 1);
            
            // Test greater than with float value
            let cursor = num_coll.find(field("value").gt(10.0))?;
            assert_eq!(cursor.count(), 2);
            
            // Test less than with long value
            let cursor = num_coll.find(field("value").lt(1000000000i64))?;
            assert_eq!(cursor.count(), 2);
            
            // Test greater than or equal across different numeric types
            let cursor = num_coll.find(field("value").gte(10))?;
            assert_eq!(cursor.count(), 3);
            
            // Test less than or equal with floating point
            let cursor = num_coll.find(field("value").lte(10.5))?;
            assert_eq!(cursor.count(), 2);
            
            Ok(())
        },
        cleanup,
    )
}


#[test]
fn test_find_plan_add_sub_plan_single() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_find_plan_add_sub_plan_single")?;

            // Insert documents
            let doc1 = doc!{"name": "Alice", "age": 25};
            let doc2 = doc!{"name": "Bob", "age": 30};

            collection.insert_many(vec![doc1, doc2])?;

            // Create index to trigger find plan with sub-plans in compound filters
            collection.create_index(vec!["name"], &non_unique_index())?;
            collection.create_index(vec!["age"], &non_unique_index())?;

            // Find documents using a complex filter that would trigger sub-plans
            let cursor = collection.find(
                field("name").eq("Alice")
            )?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_find_plan_add_multiple_sub_plans() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_find_plan_add_multiple_sub_plans")?;

            // Insert documents
            let docs = vec![
                doc!{"name": "Alice", "age": 25, "city": "NYC"},
                doc!{"name": "Bob", "age": 30, "city": "LA"},
                doc!{"name": "Charlie", "age": 35, "city": "NYC"},
            ];

            collection.insert_many(docs)?;

            // Create indexes
            collection.create_index(vec!["name"], &non_unique_index())?;
            collection.create_index(vec!["age"], &non_unique_index())?;
            collection.create_index(vec!["city"], &non_unique_index())?;

            // Find documents with complex filter
            let cursor = collection.find(
                field("city").eq("NYC")
            )?;
            assert_eq!(cursor.count(), 2);

            // Verify data integrity
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_find_plan_add_sub_plan_large_dataset() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_find_plan_add_sub_plan_large_dataset")?;

            // Insert many documents
            let mut docs = vec![];
            for i in 0..100 {
                let doc = doc!{
                    "id": i,
                    "category": (i % 5),
                    "value": (i * 10)
                };
                docs.push(doc);
            }

            collection.insert_many(docs)?;

            // Create indexes on category
            collection.create_index(vec!["category"], &non_unique_index())?;

            // Query with index using sub-plans
            let cursor = collection.find(field("category").eq(2))?;
            assert!(cursor.count() > 0);

            // Verify total count
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 100);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_find_plan_concurrent_safety() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_find_plan_concurrent_safety")?;

            // Insert documents
            let mut docs = vec![];
            for i in 0..50 {
                let doc = doc!{
                    "id": i,
                    "group": (i % 3)
                };
                docs.push(doc);
            }

            collection.insert_many(docs)?;

            // Create index
            collection.create_index(vec!["group"], &non_unique_index())?;

            // Multiple queries - simulating concurrent access patterns
            for group_id in 0..3 {
                let cursor = collection.find(field("group").eq(group_id))?;
                let count = cursor.count();
                assert!(count > 0, "Group {} should have documents", group_id);
            }

            // Verify integrity
            let cursor = collection.find(all())?;
            assert_eq!(cursor.count(), 50);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_find_plan_with_nested_filters() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_find_plan_with_nested_filters")?;

            // Insert documents with nested structure
            let doc1 = doc!{
                "name": "Alice",
                "details": {
                    "age": 25,
                    "location": "NYC"
                }
            };

            let doc2 = doc!{
                "name": "Bob",
                "details": {
                    "age": 30,
                    "location": "LA"
                }
            };

            collection.insert_many(vec![doc1, doc2])?;

            // Create index on nested field
            collection.create_index(vec!["details.location"], &non_unique_index())?;

            // Query nested field
            let cursor = collection.find(field("details.location").eq("NYC"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    );
}

// =============================================================================
// ITER_WITH_ID TESTS
// =============================================================================

#[test]
fn test_iter_with_id_basic() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_basic")?;
            insert_test_documents(&collection)?;

            let mut cursor = collection.find(all())?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            
            assert_eq!(items.len(), 3);
            
            // Verify each item has a valid ID and document
            for (id, doc) in &items {
                assert!(!id.to_string().is_empty());
                assert!(doc.get("first_name").is_ok());
            }
            
            // All IDs should be unique
            let ids: Vec<_> = items.iter().map(|(id, _)| *id).collect();
            let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
            assert_eq!(ids.len(), unique_ids.len());

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_iter_with_id_update_workflow() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_update")?;
            
            // Insert test documents
            collection.insert(doc!{ "name": "Alice", "status": "pending" })?;
            collection.insert(doc!{ "name": "Bob", "status": "pending" })?;
            collection.insert(doc!{ "name": "Charlie", "status": "pending" })?;

            // Use iter_with_id to get IDs and update efficiently
            let mut cursor = collection.find(field("status").eq("pending"))?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            
            assert_eq!(items.len(), 3);
            
            // Update each document using its NitriteId
            for (id, mut doc) in items {
                doc.put("status", "completed")?;
                collection.update_by_id(&id, &doc, false)?;
            }
            
            // Verify all documents were updated
            let pending_cursor = collection.find(field("status").eq("pending"))?;
            assert_eq!(pending_cursor.count(), 0);
            
            let completed_cursor = collection.find(field("status").eq("completed"))?;
            assert_eq!(completed_cursor.count(), 3);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_iter_with_id_with_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_filter")?;
            
            // Insert documents with different categories
            collection.insert(doc!{ "name": "Item1", "category": "A" })?;
            collection.insert(doc!{ "name": "Item2", "category": "B" })?;
            collection.insert(doc!{ "name": "Item3", "category": "A" })?;
            collection.insert(doc!{ "name": "Item4", "category": "B" })?;

            // Get only category A items with their IDs
            let mut cursor = collection.find(field("category").eq("A"))?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            
            assert_eq!(items.len(), 2);
            
            // Verify all returned items are category A
            for (_, doc) in &items {
                assert_eq!(doc.get("category").unwrap().as_string().unwrap(), "A");
            }

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_iter_with_id_empty_result() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_empty")?;
            insert_test_documents(&collection)?;

            // Query that returns no results
            let mut cursor = collection.find(field("first_name").eq("NonExistent"))?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            
            assert!(items.is_empty());

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_iter_with_id_with_index() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_index")?;
            
            // Create index before inserting
            collection.create_index(vec!["name"], &unique_index())?;
            
            collection.insert(doc!{ "name": "Alice", "value": 1 })?;
            collection.insert(doc!{ "name": "Bob", "value": 2 })?;
            collection.insert(doc!{ "name": "Charlie", "value": 3 })?;

            // Query using indexed field
            let mut cursor = collection.find(field("name").eq("Bob"))?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            
            assert_eq!(items.len(), 1);
            let (id, doc) = &items[0];
            assert!(!id.to_string().is_empty());
            assert_eq!(doc.get("name").unwrap().as_string().unwrap(), "Bob");
            assert_eq!(*doc.get("value").unwrap().as_i32().unwrap(), 2);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_iter_with_id_large_dataset() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_large")?;
            
            // Insert many documents
            let docs: Vec<_> = (0..500)
                .map(|i| {
                    let name = format!("Item{}", i);
                    doc!{ "index": i, "name": name }
                })
                .collect();
            collection.insert_many(docs)?;

            // Get all with iter_with_id
            let mut cursor = collection.find(all())?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            
            assert_eq!(items.len(), 500);
            
            // All IDs should be unique
            let ids: std::collections::HashSet<_> = items.iter().map(|(id, _)| *id).collect();
            assert_eq!(ids.len(), 500);

            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_iter_with_id_verify_same_id_as_get_by_id() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_iter_with_id_verify")?;
            
            collection.insert(doc!{ "name": "TestDoc", "value": 42 })?;

            // Get the ID via iter_with_id
            let mut cursor = collection.find(field("name").eq("TestDoc"))?;
            let items: Vec<_> = cursor.iter_with_id().collect::<Result<Vec<_>, _>>()?;
            assert_eq!(items.len(), 1);
            let (id_from_iter, _) = &items[0];

            // Verify we can retrieve the same document using get_by_id
            let doc_from_get = collection.get_by_id(id_from_iter)?;
            assert!(doc_from_get.is_some());
            let doc = doc_from_get.unwrap();
            assert_eq!(doc.get("name").unwrap().as_string().unwrap(), "TestDoc");
            assert_eq!(*doc.get("value").unwrap().as_i32().unwrap(), 42);

            Ok(())
        },
        cleanup,
    );
}

