use nitrite::doc;
use nitrite::filter::field;
use nitrite::index::{full_text_index, non_unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[test]
fn test_collection() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("test")?;

            let doc1 = doc! ({
                "name": "Anindya",
                "color": ["red", "green", "blue"],
                "books": [
                    { "name": "Book ABCD", "tag": ["tag1", "tag2"] },
                    { "name": "Book EFGH", "tag": ["tag3", "tag1"] },
                    { "name": "No Tag" }
                ]
            });

            let doc2 = doc! ({
                "name": "Bill",
                "color": ["purple", "yellow", "gray"],
                "books": [
                    { "name": "Book abcd", "tag": ["tag4", "tag5"] },
                    { "name": "Book wxyz", "tag": ["tag3", "tag1"] },
                    { "name": "No Tag 2" }
                ]
            });

            let doc3 = doc! ({
                "name": "John",
                "color": ["black", "sky", "violet"],
                "books": [
                    { "name": "Book Mnop", "tag": ["tag6", "tag2"] },
                    { "name": "Book ghij", "tag": ["tag3", "tag7"] },
                    { "name": "No Tag" }
                ]
            });

            coll.create_index(vec!["color"], &non_unique_index())?;
            coll.create_index(vec!["books.tag"], &non_unique_index())?;
            coll.create_index(vec!["books.name"], &full_text_index())?;

            let write_result = coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()])?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 3);

            let cursor = coll.find(field("color").eq("red"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("books.name").text("abcd"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("books.name").text_case_insensitive("abcd"))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("books.tag").eq("tag2"))?;
            assert_eq!(cursor.count(), 2);

            let cursor = coll.find(field("books.tag").eq("tag5"))?;
            assert_eq!(cursor.count(), 1);

            let cursor = coll.find(field("books.tag").eq("tag10"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}