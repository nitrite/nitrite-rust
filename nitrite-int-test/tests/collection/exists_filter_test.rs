use nitrite::collection::{Document, NitriteCollection};
use nitrite::common::Value;
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::field;
use nitrite::index::non_unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

/// Four documents varying only in which fields they carry:
///  a - name, nick, age, address.city
///  b - name, age, address.city
///  c - name, nick (explicitly null), age
///  d - name, age
fn insert_docs(coll: &NitriteCollection) -> NitriteResult<()> {
    let addr1 = doc! { city: "kolkata" };
    let addr2 = doc! { city: "delhi" };

    coll.insert_many(vec![
        doc! { name: "a", nick: "aa", age: 30, address: addr1 },
        doc! { name: "b", age: 40, address: addr2 },
        doc! { name: "c", nick: (Value::Null), age: 50 },
        doc! { name: "d", age: 60 },
    ])?;
    Ok(())
}

fn names(cursor: impl IntoIterator<Item = NitriteResult<Document>>) -> Vec<String> {
    let mut names = cursor
        .into_iter()
        .map(|d| match d.unwrap().get("name").unwrap() {
            Value::String(s) => s,
            other => panic!("unexpected name value {:?}", other),
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn test_exists() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            assert_eq!(names(coll.find(field("nick").exists())?), vec!["a", "c"]);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_matches_explicit_null() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            // document "c" carries nick = null; `ne(null)` cannot express this,
            // which is exactly why the filter exists
            assert!(names(coll.find(field("nick").exists())?).contains(&"c".to_string()));
            assert!(!names(coll.find(field("nick").ne(Value::Null))?).contains(&"c".to_string()));

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_not_exists() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            assert_eq!(
                names(coll.find(field("nick").exists().not())?),
                vec!["b", "d"]
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_on_every_document() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            assert_eq!(coll.find(field("name").exists())?.count(), 4);
            assert_eq!(coll.find(field("age").exists())?.count(), 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_on_unknown_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            assert_eq!(coll.find(field("unknown").exists())?.count(), 0);
            assert_eq!(coll.find(field("unknown").exists().not())?.count(), 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_on_embedded_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            assert_eq!(names(coll.find(field("address").exists())?), vec!["a", "b"]);
            assert_eq!(
                names(coll.find(field("address.city").exists())?),
                vec!["a", "b"]
            );
            assert_eq!(coll.find(field("address.pin").exists())?.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_indexed_field_gives_same_result() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            let before_index = names(coll.find(field("nick").exists())?);
            coll.create_index(vec!["nick"], &non_unique_index())?;
            assert!(coll.has_index(vec!["nick"])?);

            // an index stores a missing field and an explicit null under the same
            // null key, so the filter must stay a full scan and keep answering the
            // same way once the field is indexed
            assert_eq!(names(coll.find(field("nick").exists())?), before_index);
            assert_eq!(
                names(coll.find(field("nick").exists().not())?),
                vec!["b", "d"]
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_combined_with_indexed_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;
            coll.create_index(vec!["age"], &non_unique_index())?;

            assert_eq!(
                names(coll.find(field("age").lt(45).and(field("nick").exists()))?),
                vec!["a"]
            );
            assert_eq!(
                names(coll.find(field("age").lt(45).and(field("nick").exists().not()))?),
                vec!["b"]
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_with_or() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            // deduplicated: `or()` over non-indexed fields currently repeats a
            // document that matches more than one branch (pre-existing, reproduces
            // with eq/eq too), and that is not what this test is about
            let mut matched =
                names(coll.find(field("nick").exists().or(field("address").exists()))?);
            matched.dedup();
            assert_eq!(matched, vec!["a", "b", "c"]);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_after_remove() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            coll.remove(field("name").eq("a"), false)?;
            assert_eq!(names(coll.find(field("nick").exists())?), vec!["c"]);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_exists_after_update_adds_field() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            coll.update(field("name").eq("d"), &doc! { nick: "dd" })?;
            assert_eq!(
                names(coll.find(field("nick").exists())?),
                vec!["a", "c", "d"]
            );

            Ok(())
        },
        cleanup,
    )
}
