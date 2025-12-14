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
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_by_non_unique_index() {
    run_test(
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_by_full_text_index_after_insert() {
    run_test(
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_by_index_sort_ascending() {
    run_test(
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_by_index_sort_descending() {
    run_test(
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_after_dropped_index() {
    run_test(
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_text_with_wild_card() {
    run_test(
        || create_test_context(),
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
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_text_with_empty_string() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            let cursor = coll.find(field("body").text(""))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}


#[test]
#[should_panic]
fn test_find_text_with_wild_card_multiple_word() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            
            // This should fail because multiple words with wildcards are not supported
            let cursor = coll.find(field("body").text("*ipsum dolor*"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_find_text_with_only_wild_card() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["body"], &full_text_index())?;
            
            // This should fail because lone wildcard is not supported
            let cursor = coll.find(field("body").text("*"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}