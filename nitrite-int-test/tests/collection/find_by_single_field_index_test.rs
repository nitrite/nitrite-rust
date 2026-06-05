use nitrite::collection::order_by;
use nitrite::common::{SortOrder, Value};
use nitrite::filter::{all, and, field, or};
use nitrite::index::{full_text_index, non_unique_index, unique_index};
use nitrite_int_test::test_util::{
    cleanup, create_test_context, insert_test_documents, is_sorted, now, run_test
};

#[test]
fn test_find_by_unique_index() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["first_name"], &unique_index())?;
            let cursor = coll.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("first_name").eq("fn10"))?;
            assert_eq!(cursor.count(), 0);

            coll.create_index(vec!["birth_day"], &unique_index())?;
            let cursor = coll.find(field("birth_day").gt(now()))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("birth_day").lte(now()))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(
                field("birth_day").lte(now()).and(field("first_name").eq("fn1")),
            )?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(
                field("birth_day").lte(now()).or(field("first_name").eq("fn12")),
            )?;
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

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_by_non_unique_index() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["last_name"], &non_unique_index())?;
            coll.create_index(vec!["birth_day"], &non_unique_index())?;

            let cursor = coll.find(field("last_name").eq("ln2"))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("last_name").eq("ln20"))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("birth_day").gt(now()))?;
            assert_eq!(cursor.count(), 0);

            let cursor = coll.find(field("birth_day").lte(now()))?;
            assert_eq!(cursor.count(), 3);

            let cursor = coll.find(
                field("birth_day").lte(now()).and(field("first_name").eq("fn1")),
            )?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(
                field("birth_day").lte(now()).or(field("first_name").eq("fn12")),
            )?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_by_full_text_index_after_insert() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            assert!(coll.has_index(vec!["body"])?);

            let cursor = coll.find(field("body").text("Lorem"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("body").text("nosql"))?;
            assert_eq!(cursor.count(), 0);

            coll.drop_index(vec!["body"])?;
            let result = coll.find(field("body").text("Lorem"));
            assert!(result.is_err());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_by_index_sort_ascending() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["birth_day"], &unique_index())?;
            let cursor = coll.find_with_options(all(), &order_by("birth_day", SortOrder::Ascending))?;
            // assert_eq!(cursor.count(), 3);

            let mut date_list = Vec::new();
            for doc in cursor {
                let doc = doc?;
                if let Ok(Value::String(date)) = doc.get("birth_day") {
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
fn test_find_by_index_sort_descending() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["birth_day"], &unique_index())?;
            let cursor = coll.find_with_options(all(), &order_by("birth_day", SortOrder::Descending))?;
            

            let mut date_list = Vec::new();
            for doc in cursor {
                let doc = doc?;
                if let Ok(Value::String(date)) = doc.get("birth_day") {
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
fn test_find_after_dropped_index() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["first_name"], &unique_index())?;
            let cursor = coll.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 1);

            coll.drop_index(vec!["first_name"])?;
            let cursor = coll.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_indexed_multi_bound_range_query_matches_full_scan() {
    // Regression: a multi-bound range query on a single-field index (e.g. `age >= 30 AND
    // age <= 50`) must use *both* bounds at the index level and return the exact same set as
    // an unindexed full scan — not "everything above the lower bound, post-filtered".
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("range")?;
            for age in 0i64..100 {
                coll.insert(nitrite::doc! { id: (age), age: (age) })?;
            }

            // Ground truth from an unindexed full scan: ages 30..=50 inclusive = 21 docs.
            let inclusive = || field("age").gte(30i64).and(field("age").lte(50i64));
            assert_eq!(coll.find(inclusive())?.count(), 21, "full-scan ground truth");

            // Index the field; the same query must return the identical, exact set.
            coll.create_index(vec!["age"], &non_unique_index())?;

            let mut ages: Vec<i64> = Vec::new();
            for doc in coll.find(inclusive())? {
                if let Ok(Value::I64(age)) = doc?.get("age") {
                    ages.push(age);
                }
            }
            ages.sort_unstable();
            assert_eq!(
                ages,
                (30..=50).collect::<Vec<_>>(),
                "indexed range must equal exactly ages 30..=50 (both bounds applied)"
            );

            // Exclusive bounds: 30 < age < 50 -> 31..=49 = 19 docs.
            assert_eq!(
                coll.find(field("age").gt(30i64).and(field("age").lt(50i64)))?.count(),
                19
            );
            // Contradictory range yields nothing.
            assert_eq!(
                coll.find(field("age").gte(50i64).and(field("age").lte(30i64)))?.count(),
                0
            );
            // Degenerate single-value range.
            assert_eq!(
                coll.find(field("age").gte(42i64).and(field("age").lte(42i64)))?.count(),
                1
            );

            // The `between` API must be index-accelerated identically to `gte().and(lte())`.
            assert_eq!(coll.find(field("age").between_inclusive(30i64, 50i64, true))?.count(), 21);
            // And nested inside an AND with another predicate (exercises 3 same-field bounds via
            // the intersection fallback).
            assert_eq!(
                coll.find(
                    field("age")
                        .between_inclusive(30i64, 50i64, true)
                        .and(field("age").gt(40i64)),
                )?
                .count(),
                10 // ages 41..=50
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_compound_index_terminal_range_matches_full_scan() {
    // A compound index `[folder, date]` queried with an equality prefix and a range on the
    // terminal field (`folder == 3 AND date BETWEEN 20 AND 40`) must bound the range at the
    // index level and return the exact same set as a full scan.
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("compound_range")?;
            for folder in 0i64..10 {
                for date in 0i64..100 {
                    coll.insert(nitrite::doc! {
                        id: (folder * 100 + date),
                        folder: (folder),
                        date: (date)
                    })?;
                }
            }

            let q = || {
                field("folder")
                    .eq(3i64)
                    .and(field("date").gte(20i64))
                    .and(field("date").lte(40i64))
            };

            // Ground truth from an unindexed full scan: dates 20..=40 in folder 3 = 21 docs.
            assert_eq!(coll.find(q())?.count(), 21, "full-scan ground truth");

            // With the compound index the result must be identical and exact.
            coll.create_index(vec!["folder", "date"], &non_unique_index())?;
            let mut dates: Vec<i64> = Vec::new();
            for doc in coll.find(q())? {
                let doc = doc?;
                if let Ok(Value::I64(d)) = doc.get("date") {
                    dates.push(d);
                }
                // Every returned document must be in the queried folder.
                assert_eq!(doc.get("folder").ok(), Some(Value::I64(3)));
            }
            dates.sort_unstable();
            assert_eq!(
                dates,
                (20..=40).collect::<Vec<_>>(),
                "compound terminal range must equal exactly dates 20..=40 in folder 3"
            );

            // A different folder over the same date range yields nothing.
            assert_eq!(
                coll.find(
                    field("folder")
                        .eq(7i64)
                        .and(field("date").gte(200i64))
                        .and(field("date").lte(400i64)),
                )?
                .count(),
                0
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_text_with_wild_card() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            let cursor = coll.find(field("body").text("Lo*"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("body").text("*rem"))?;
            let vec = cursor.collect::<Vec<_>>();
            assert_eq!(vec.len(), 1);

            let cursor = coll.find(field("body").text("*or*"))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_text_with_empty_string() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            let cursor = coll.find(field("body").text(""))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}


#[test]
#[should_panic]
fn test_find_text_with_wild_card_multiple_word() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            
            // This should fail because multiple words with wildcards are not supported
            let cursor = coll.find(field("body").text("*ipsum dolor*"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
#[should_panic]
fn test_find_text_with_only_wild_card() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            
            // This should fail because lone wildcard is not supported
            let cursor = coll.find(field("body").text("*"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}