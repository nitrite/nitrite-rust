use nitrite::collection::NitriteCollection;
use nitrite::common::{Lookup, Value};
use nitrite::doc;
use nitrite::filter::all;
use nitrite_int_test::test_util::{cleanup, create_test_context, insert_test_documents, run_test};

#[test]
fn test_join_all() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let foreign_collection = ctx.db().collection("foreign")?;

            // Insert test data into both collections
            insert_test_documents(&collection)?;
            insert_foreign_documents(&foreign_collection)?;

            // Create the lookup configuration
            let lookup = Lookup::new("first_name", "f_name", "personal_details");
            
            let mut cursor = collection.find(all())?;
            let mut foreign_cursor = foreign_collection.find(all())?;

            // Perform the join
            let result = cursor.join(&mut foreign_cursor, &lookup)?;
            let result = result.collect::<Result<Vec<_>, _>>()?;
            assert_eq!(result.len(), 3);
                        
            // Verify join results
            for document in result {
                assert!(document.get("first_name")?.is_string());
                
                // Check that personal_details field was created by the join
                let personal_details = document.get("personal_details")?;
                assert!(personal_details.is_array());
                
                if let Value::Array(details) = personal_details {
                    // If personal_details has items, verify structure
                    if !details.is_empty() {
                        // Get first joined record
                        if let Value::Document(detail) = &details[0] {
                            // Assert foreign document has expected structure
                            assert!(detail.get("f_name")?.is_string());
                            assert!(detail.get("l_name")?.is_string());
                        } else {
                            panic!("Expected document in personal_details array");
                        }
                    }
                } else {
                    panic!("Expected personal_details to be an array");
                }
            }

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// Helper function to insert foreign documents
fn insert_foreign_documents(collection: &NitriteCollection) -> nitrite::errors::NitriteResult<()> {
    let doc1 = doc!{
        "f_name": "fn1",
        "l_name": "ln1", 
        "address": {
            "street": "5th Avenue",
            "city": "New York",
            "country": "USA"
        }
    };

    let doc2 = doc!{
        "f_name": "fn2",
        "l_name": "ln2",
        "address": {
            "street": "Sydney Harbour Bridge",
            "city": "Sydney",
            "country": "Australia"
        }
    };

    let doc3 = doc!{
        "f_name": "fn3",
        "l_name": "ln3",
        "address": {
            "street": "Oxford Street",
            "city": "London",
            "country": "UK"
        }
    };

    collection.insert_many(vec![doc1, doc2, doc3])?;
    Ok(())
}