// Projection and Join tests for ObjectRepository
// Based on Java tests: ProjectionTest.java and RepositoryJoinTest.java

use crate::repository::{generate_company, generate_employee, Employee, SubEmployee};
use nitrite::collection::{FindOptions, NitriteId};
use nitrite::common::Lookup;
use nitrite::filter::all;
use nitrite::repository::ObjectRepository;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

// Entity for join tests
#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
#[entity(id(field = "id"))]
pub struct JoinPerson {
    pub nitrite_id: Option<NitriteId>,
    pub id: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
pub struct JoinAddress {
    pub nitrite_id: Option<NitriteId>,
    pub person_id: Option<String>,
    pub street: Option<String>,
}

// ========================================
// Projection Tests
// ========================================

#[test]
fn test_projection_has_more() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository::<Employee>()?;
            let company = generate_company();
            for _ in 0..10 {
                let mut employee = generate_employee();
                employee.company = Some(company.clone());
                repo.insert(employee)?;
            }
            
            let mut cursor = repo.find(all())?;
            let projected = cursor.project::<SubEmployee>()?;
            
            // Count the projected results
            let count = projected.count();
            assert!(count > 0, "Projected cursor should have items");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_projection_size() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository::<Employee>()?;
            let company = generate_company();
            for _ in 0..10 {
                let mut employee = generate_employee();
                employee.company = Some(company.clone());
                repo.insert(employee)?;
            }
            
            let mut cursor = repo.find(all())?;
            let mut projected = cursor.project::<SubEmployee>()?;
            
            let size = projected.size();
            assert_eq!(size, 10, "Projected cursor size should be 10");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_projection_fields() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository::<Employee>()?;
            let company = generate_company();
            for _ in 0..5 {
                let mut employee = generate_employee();
                employee.company = Some(company.clone());
                repo.insert(employee)?;
            }
            
            let mut cursor = repo.find(all())?;
            let projected = cursor.project::<SubEmployee>()?;
            
            for result in projected {
                let sub_employee = result?;
                // SubEmployee should have emp_id, join_date, address
                assert!(sub_employee.emp_id.is_some(), "SubEmployee should have emp_id");
                assert!(sub_employee.address.is_some(), "SubEmployee should have address");
            }
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_projection_empty_result() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            
            let mut cursor = repo.find(all())?;
            let mut projected = cursor.project::<SubEmployee>()?;
            
            let size = projected.size();
            assert_eq!(size, 0, "Empty repository should have 0 projected items");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

// ========================================
// Join Tests
// ========================================

#[test]
fn test_join_basic() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<JoinPerson> = ctx.db().repository::<JoinPerson>()?;
            let address_repo: ObjectRepository<JoinAddress> = ctx.db().repository::<JoinAddress>()?;
            
            // Insert persons and addresses
            for i in 0..10 {
                let person = JoinPerson {
                    nitrite_id: None,
                    id: Some(i.to_string()),
                    name: Some(format!("Person {}", i)),
                };
                person_repo.insert(person)?;
                
                let address = JoinAddress {
                    nitrite_id: None,
                    person_id: Some(i.to_string()),
                    street: Some(format!("Street address {}", i)),
                };
                address_repo.insert(address)?;
                
                // Insert an extra address for person 5
                if i == 5 {
                    let address2 = JoinAddress {
                        nitrite_id: None,
                        person_id: Some(i.to_string()),
                        street: Some(format!("Street address 2nd {}", i)),
                    };
                    address_repo.insert(address2)?;
                }
            }
            
            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            
            let lookup = Lookup::new("id", "person_id", "addresses");
            let mut joined_cursor = person_cursor.join(&mut address_cursor, &lookup)?;
            
            let size = joined_cursor.size();
            assert_eq!(size, 10, "Joined cursor size should be 10");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_join_with_multiple_matches() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<JoinPerson> = ctx.db().keyed_repository::<JoinPerson>("persons")?;
            let address_repo: ObjectRepository<JoinAddress> = ctx.db().keyed_repository::<JoinAddress>("addresses")?;
            
            // Insert one person with multiple addresses
            let person = JoinPerson {
                nitrite_id: None,
                id: Some("1".to_string()),
                name: Some("Person with multiple addresses".to_string()),
            };
            person_repo.insert(person)?;
            
            // Insert multiple addresses for the same person
            for i in 0..3 {
                let address = JoinAddress {
                    nitrite_id: None,
                    person_id: Some("1".to_string()),
                    street: Some(format!("Street {}", i)),
                };
                address_repo.insert(address)?;
            }
            
            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            
            let lookup = Lookup::new("id", "person_id", "addresses");
            let joined_cursor = person_cursor.join(&mut address_cursor, &lookup)?;
            
            let mut count = 0;
            for result in joined_cursor {
                let _ = result?;
                count += 1;
            }
            
            assert_eq!(count, 1, "Should have exactly one person in the join result");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_join_no_matches() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<JoinPerson> = ctx.db().keyed_repository("persons")?;
            let address_repo: ObjectRepository<JoinAddress> = ctx.db().keyed_repository("addresses")?;
            
            // Insert person
            let person = JoinPerson {
                nitrite_id: None,
                id: Some("1".to_string()),
                name: Some("Person without address".to_string()),
            };
            person_repo.insert(person)?;
            
            // Insert address with different person_id
            let address = JoinAddress {
                nitrite_id: None,
                person_id: Some("999".to_string()),
                street: Some("Some street".to_string()),
            };
            address_repo.insert(address)?;
            
            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            
            let lookup = Lookup::new("id", "person_id", "addresses");
            let joined_cursor = person_cursor.join(&mut address_cursor, &lookup)?;
            
            let mut count = 0;
            for result in joined_cursor {
                let _ = result?;
                count += 1;
            }
            
            assert_eq!(count, 1, "Should have one person even with no matching addresses");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_join_empty_foreign_cursor() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<JoinPerson> = ctx.db().keyed_repository("persons")?;
            let address_repo: ObjectRepository<JoinAddress> = ctx.db().keyed_repository("addresses")?;
            
            // Insert person
            let person = JoinPerson {
                nitrite_id: None,
                id: Some("1".to_string()),
                name: Some("Person".to_string()),
            };
            person_repo.insert(person)?;
            
            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            
            let lookup = Lookup::new("id", "person_id", "addresses");
            let joined_cursor = person_cursor.join(&mut address_cursor, &lookup)?;
            
            let mut count = 0;
            for result in joined_cursor {
                let _ = result?;
                count += 1;
            }
            
            assert_eq!(count, 1, "Should still return person even with empty foreign cursor");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_join_empty_local_cursor() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<JoinPerson> = ctx.db().keyed_repository("persons")?;
            let address_repo: ObjectRepository<JoinAddress> = ctx.db().keyed_repository("addresses")?;
            
            // Only insert address
            let address = JoinAddress {
                nitrite_id: None,
                person_id: Some("1".to_string()),
                street: Some("Some street".to_string()),
            };
            address_repo.insert(address)?;
            
            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            
            let lookup = Lookup::new("id", "person_id", "addresses");
            let mut joined_cursor = person_cursor.join(&mut address_cursor, &lookup)?;
            
            let size = joined_cursor.size();
            assert_eq!(size, 0, "Empty local cursor should result in 0 join results");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_join_iteration() {
    run_test(
        || create_test_context(),
        |ctx| {
            let person_repo: ObjectRepository<JoinPerson> = ctx.db().keyed_repository("persons")?;
            let address_repo: ObjectRepository<JoinAddress> = ctx.db().keyed_repository("addresses")?;
            
            for i in 0..5 {
                let person = JoinPerson {
                    nitrite_id: None,
                    id: Some(i.to_string()),
                    name: Some(format!("Person {}", i)),
                };
                person_repo.insert(person)?;
                
                let address = JoinAddress {
                    nitrite_id: None,
                    person_id: Some(i.to_string()),
                    street: Some(format!("Street {}", i)),
                };
                address_repo.insert(address)?;
            }
            
            let mut person_cursor = person_repo.find(all())?;
            let mut address_cursor = address_repo.find(all())?;
            
            let lookup = Lookup::new("id", "person_id", "addresses");
            let joined_cursor = person_cursor.join(&mut address_cursor, &lookup)?;
            
            let mut iteration_count = 0;
            for result in joined_cursor {
                let _ = result?;
                iteration_count += 1;
            }
            
            assert_eq!(iteration_count, 5, "Should iterate over all 5 persons");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

// ========================================
// Projection Iterator Tests
// ========================================

#[test]
fn test_projection_iterator() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let company = generate_company();
            for _ in 0..5 {
                let mut employee = generate_employee();
                employee.company = Some(company.clone());
                repo.insert(employee)?;
            }
            
            let mut cursor = repo.find(all())?;
            let projected = cursor.project::<SubEmployee>()?;
            
            let mut iteration_count = 0;
            for result in projected {
                let sub_employee = result?;
                assert!(sub_employee.emp_id.is_some());
                iteration_count += 1;
            }
            
            assert_eq!(iteration_count, 5, "Should iterate over all 5 projected employees");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_projection_with_find_options() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let company = generate_company();
            for _ in 0..10 {
                let mut employee = generate_employee();
                employee.company = Some(company.clone());
                repo.insert(employee)?;
            }
            
            let options = FindOptions::new().skip(2).limit(5);
            let mut cursor = repo.find_with_options(all(), &options)?;
            let mut projected = cursor.project::<SubEmployee>()?;
            
            let size = projected.size();
            assert_eq!(size, 5, "Should have 5 projected items after skip and limit");
            
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}
