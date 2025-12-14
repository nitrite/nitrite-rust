use crate::repository::WithNitriteId;
use nitrite::collection::NitriteId;
use nitrite::filter::all;
use nitrite::repository::ObjectRepository;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

// Test to verify that the id_field is automatically populated and can be used to update the document.
#[test]
fn test_nitrite_id_field() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithNitriteId> = ctx.db().repository()?;

            let item1 = WithNitriteId {
                id_field: None, // id_field should be None initially
                name: "first".to_string(),
            };

            let item2 = WithNitriteId {
                id_field: None, // id_field should be None initially
                name: "second".to_string(),
            };

            // Insert two items.
            repo.insert_many(vec![item1, item2])?;

            // Verify that each document has a non-None id_field.
            let cursor = repo.find(all())?;
            for item in cursor {
                let item = item?;
                assert!(item.id_field.is_some());
            }
            
            let mut cursor = repo.find(all())?;
            // Get the first document, update its name, and update it in the repository.
            let mut first = cursor.first().expect("Expected first document")?;
            first.name = "third".to_string();
            let id = first.id_field.clone().expect("Expected id to be set");
            repo.update_one(first.clone(), true)?;

            // Retrieve the document by id and verify it equals the updated item.
            let by_id = repo.get_by_id(&Some(id))?;
            assert_eq!(first, by_id.unwrap());
            assert_eq!(repo.size()?, 2);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// Test to verify that setting an id_field before insert leads to an error.
#[test]
fn test_set_id_during_insert() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithNitriteId> = ctx.db().repository()?;

            let mut item = WithNitriteId {
                id_field: None, // id_field should be None initially
                name: "first".to_string(),
            };
            // Manually setting id_field should be invalid.
            item.id_field = Some(NitriteId::new());

            let result = repo.insert(item);
            assert!(result.is_err());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// // Test to verify that changing the id_field during update does not update the document.
#[test]
fn test_change_id_during_update() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<WithNitriteId> = ctx.db().repository()?;

            let item = WithNitriteId {
                id_field: None, // id_field should be None initially
                name: "second".to_string(),
            };
            let wr = repo.insert(item)?;
            let nitrite_id = wr.affected_nitrite_ids().first();
            let mut stored = repo.get_by_id(&nitrite_id.cloned())?.expect("Expected document to be found");
            // Attempt to change id_field during update.
            stored.id_field = Some(NitriteId::new());
            let update_result = repo.update_one(stored, false)?;
            // Expect no document to be updated.
            assert_eq!(update_result.affected_nitrite_ids().len(), 0);
            assert_eq!(repo.size()?, 1);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
