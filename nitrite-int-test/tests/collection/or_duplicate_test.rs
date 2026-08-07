use nitrite::collection::{Document, NitriteCollection};
use nitrite::common::Value;
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::field;
use nitrite::index::non_unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

/// `a` satisfies both branches of every OR below, `b` only the second one.
fn insert_docs(coll: &NitriteCollection) -> NitriteResult<()> {
    coll.insert_many(vec![
        doc! { name: "a", x: 1, y: 2 },
        doc! { name: "b", x: 9, y: 2 },
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
fn test_or_without_index_does_not_duplicate() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;

            let filter = field("x").eq(1).or(field("y").eq(2));
            assert_eq!(names(coll.find(filter.clone())?), vec!["a", "b"]);
            assert_eq!(coll.find(filter)?.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_or_with_one_indexed_branch_does_not_duplicate() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;
            coll.create_index(vec!["x"], &non_unique_index())?;

            let filter = field("x").eq(1).or(field("y").eq(2));
            assert_eq!(names(coll.find(filter.clone())?), vec!["a", "b"]);
            assert_eq!(coll.find(filter)?.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_or_with_all_branches_indexed_does_not_duplicate() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_docs(&coll)?;
            coll.create_index(vec!["x"], &non_unique_index())?;
            coll.create_index(vec!["y"], &non_unique_index())?;

            let filter = field("x").eq(1).or(field("y").eq(2));
            assert_eq!(names(coll.find(filter.clone())?), vec!["a", "b"]);
            assert_eq!(coll.find(filter)?.count(), 2);

            Ok(())
        },
        cleanup,
    )
}
