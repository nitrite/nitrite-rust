use nitrite::common::Value;
use nitrite::filter::{and, field, or};
use nitrite::index::unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, insert_test_documents, run_test};

#[test]
fn test_find_by_and_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["list", "last_name", "first_name"], &unique_index())?;

            let cursor = coll.find(and(vec![
                field("last_name").eq("ln2"),
                field("first_name").ne("fn1"),
                field("list").eq("four"),
            ]))?;
            
            let find_plan = cursor.find_plan().unwrap();
            assert!(find_plan.full_scan_filter().is_none());
            assert!(find_plan.index_descriptor().is_some());
            assert_eq!(&find_plan.index_descriptor().unwrap(), coll.list_indexes()?.get(0).unwrap());
            
            let index_scan_filter = find_plan.index_scan_filter().unwrap();
            assert_eq!(index_scan_filter.filters()[0].to_string(), field("list").eq("four").to_string());
            assert_eq!(index_scan_filter.filters()[1].to_string(), field("last_name").eq("ln2").to_string());
            assert_eq!(index_scan_filter.filters()[2].to_string(), field("first_name").ne("fn1").to_string());
            
            let documents: Vec<_> = cursor.collect();
            assert_eq!(documents.len(), 1);
            let doc = documents[0].as_ref().unwrap();
            assert_eq!(
                doc.get("body")?,
                Value::String("quick hello world from nitrite".to_string())
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_find_by_or_filter_and_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            coll.create_index(vec!["last_name", "first_name"], &unique_index())?;
            
            let cursor = coll.find(or(vec![
                and(vec![
                    field("last_name").eq("ln2"),
                    field("first_name").ne("fn1"),
                ]),
                and(vec![
                    field("first_name").eq("fn3"),
                    field("last_name").eq("ln2"),
                ]),
            ]))?;
            
            let find_plan = cursor.find_plan().unwrap();
            assert!(find_plan.index_scan_filter().is_none());
            assert!(find_plan.full_scan_filter().is_none());
            assert!(find_plan.sub_plans().is_some());
            assert_eq!(find_plan.sub_plans().unwrap().len(), 2);
            
            let sub_plans = find_plan.sub_plans().unwrap();
            assert!(sub_plans[0].index_scan_filter().is_some());
            assert!(sub_plans[1].index_scan_filter().is_some());
            
            let mut documents: Vec<_> = cursor.collect();            
            assert_eq!(
                documents.iter().filter(|d| {
                    d.as_ref().unwrap().get("first_name").unwrap() == Value::String("fn2".to_string())
                        && d.as_ref().unwrap().get("last_name").unwrap() == Value::String("ln2".to_string())
                }).count(),
                1
            );
            
            // Remove duplicates based on the ID
            documents.dedup_by(|a, b| {
                a.clone().unwrap().id().unwrap() == b.clone().unwrap().id().unwrap()
            });
            assert_eq!(
                documents.iter().filter(|d| {
                    d.as_ref().unwrap().get("first_name").unwrap() == Value::String("fn3".to_string())
                        && d.as_ref().unwrap().get("last_name").unwrap() == Value::String("ln2".to_string())
                }).count(),
                1
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}