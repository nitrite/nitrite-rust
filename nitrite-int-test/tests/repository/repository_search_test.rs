use nitrite::filter::{all, and, field, not, or};
use nitrite::repository::ObjectRepository;

use crate::repository::{
    Book, BookId, ElemMatch, Employee, Note
    , ProductScore,
};
use nitrite::collection::{order_by, FindOptions};
use nitrite::common::{SortOrder, Value};
use nitrite_int_test::test_util::{cleanup, create_test_context, now, run_test};

// =============================================================================
// FIND WITH OPTIONS TESTS
// =============================================================================

#[test]
fn test_find_with_skip_limit() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Insert 10 employees
            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.address = Some(format!("Address {}", i));
                repo.insert(emp)?;
            }

            // Test skip
            let options = FindOptions::new().skip(5);
            let cursor = repo.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 5);

            // Test limit
            let options = FindOptions::new().limit(3);
            let cursor = repo.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 3);

            // Test skip and limit
            let options = FindOptions::new().skip(2).limit(3);
            let cursor = repo.find_with_options(all(), &options)?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_sort_ascending() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Insert employees in random order
            for i in [5, 3, 8, 1, 9, 2, 7, 4, 6, 10] {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let options = order_by("emp_id", SortOrder::Ascending);
            let cursor = repo.find_with_options(all(), &options)?;

            let mut prev_id: Option<u64> = None;
            for emp in cursor {
                let emp = emp?;
                if let Some(prev) = prev_id {
                    assert!(emp.emp_id.unwrap() >= prev);
                }
                prev_id = emp.emp_id;
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_with_sort_descending() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Insert employees in random order
            for i in [5, 3, 8, 1, 9, 2, 7, 4, 6, 10] {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let options = order_by("emp_id", SortOrder::Descending);
            let cursor = repo.find_with_options(all(), &options)?;

            let mut prev_id: Option<u64> = None;
            for emp in cursor {
                let emp = emp?;
                if let Some(prev) = prev_id {
                    assert!(emp.emp_id.unwrap() <= prev);
                }
                prev_id = emp.emp_id;
            }

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// FILTER TESTS
// =============================================================================

#[test]
fn test_equal_filter_by_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").eq(3i64))?;
            assert_eq!(cursor.count(), 1);

            let mut cursor = repo.find(field("emp_id").eq(3i64))?;
            let found = cursor.first().unwrap()?;
            assert_eq!(found.emp_id, Some(3));

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
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").ne(3i64))?;
            assert_eq!(cursor.count(), 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_string_equal_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<ProductScore> = ctx.db().repository::<ProductScore>()?;

            repo.insert(ProductScore {
                product: Some("test".to_string()),
                score: Some(1.0),
            })?;

            repo.insert(ProductScore {
                product: Some("test".to_string()),
                score: Some(2.0),
            })?;

            repo.insert(ProductScore {
                product: Some("another-test".to_string()),
                score: Some(3.0),
            })?;

            let cursor = repo.find(field("product").eq("test"))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_and_filter_with_multiple_conditions() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;
            let join_date = now();

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("test address".to_string());
            emp.join_date = Some(join_date.clone());
            repo.insert(emp)?;

            // Different employee
            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.address = Some("other address".to_string());
            emp2.join_date = Some(join_date.clone());
            repo.insert(emp2)?;

            let cursor = repo.find(and(vec![
                field("emp_id").eq(1i64),
                field("address").text("test"),
                field("join_date").eq(join_date),
            ]))?;

            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_or_filter_with_multiple_conditions() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.address = Some(format!("address {}", i));
                repo.insert(emp)?;
            }

            let cursor = repo.find(or(vec![
                field("emp_id").eq(1i64),
                field("emp_id").eq(3i64),
                field("emp_id").eq(5i64),
            ]))?;

            assert_eq!(cursor.count(), 2); // 5 doesn't exist

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_not_filter_negation() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(not(field("emp_id").eq(2i64)))?;
            assert_eq!(cursor.count(), 4);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// COMPARISON FILTERS
// =============================================================================

#[test]
fn test_greater_than_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").gt(5i64))?;
            assert_eq!(cursor.count(), 4); // 6, 7, 8, 9

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_greater_than_or_equal_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").gte(5i64))?;
            assert_eq!(cursor.count(), 5); // 5, 6, 7, 8, 9

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_less_than_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").lt(5i64))?;
            assert_eq!(cursor.count(), 5); // 0, 1, 2, 3, 4

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_less_than_or_equal_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").lte(5i64))?;
            assert_eq!(cursor.count(), 6); // 0, 1, 2, 3, 4, 5

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_between_filter_inclusive() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            // Between inclusive (both bounds)
            let cursor = repo.find(field("emp_id").between_optional_inclusive(3i64, 7i64))?;
            assert_eq!(cursor.count(), 5); // 3, 4, 5, 6, 7

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_between_filter_exclusive() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            // Between exclusive (both bounds)
            let cursor = repo.find(field("emp_id").between(3i64, 7i64, false, false))?;
            assert_eq!(cursor.count(), 3); // 4, 5, 6

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_between_filter_mixed() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            // Between with lower inclusive, upper exclusive
            let cursor = repo.find(field("emp_id").between(3i64, 7i64, true, false))?;
            assert_eq!(cursor.count(), 4); // 3, 4, 5, 6

            // Between with lower exclusive, upper inclusive
            let cursor = repo.find(field("emp_id").between(3i64, 7i64, false, true))?;
            assert_eq!(cursor.count(), 4); // 4, 5, 6, 7

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// TEXT SEARCH FILTERS
// =============================================================================

#[test]
fn test_text_filter_basic() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("this is a test address".to_string());
            repo.insert(emp)?;

            let cursor = repo.find(field("address").text("test"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_text_filter_case_insensitive() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.address = Some("This Is A Test Address".to_string());
            repo.insert(emp)?;

            let cursor = repo.find(field("address").text_case_insensitive("test"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_regex_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp1 = Employee::default();
            emp1.emp_id = Some(1);
            emp1.email_address = Some("test@example.com".to_string());
            repo.insert(emp1)?;

            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.email_address = Some("invalid-email".to_string());
            repo.insert(emp2)?;

            let cursor = repo.find(
                field("email_address").text_regex(r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"),
            )?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// IN / NOT IN FILTERS
// =============================================================================

#[test]
fn test_in_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").in_array(vec![2i64, 4, 6, 8]))?;
            assert_eq!(cursor.count(), 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_not_in_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").not_in_array(vec![2i64, 4, 6, 8]))?;
            assert_eq!(cursor.count(), 6);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_in_filter_with_non_existent_values() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(field("emp_id").in_array(vec![100i64, 200, 300]))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// ELEMENT MATCH FILTERS (FOR ARRAY FIELDS)
// =============================================================================

#[test]
fn test_elem_match_with_nested_objects() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<ElemMatch> = ctx.db().repository::<ElemMatch>()?;

            let score1 = ProductScore {
                product: Some("abc".to_string()),
                score: Some(10.0),
            };
            let score2 = ProductScore {
                product: Some("xyz".to_string()),
                score: Some(5.0),
            };
            let score3 = ProductScore {
                product: Some("abc".to_string()),
                score: Some(7.0),
            };
            let score4 = ProductScore {
                product: Some("xyz".to_string()),
                score: Some(8.0),
            };

            let elem1 = ElemMatch {
                id: Some(1),
                str_array: Some(vec!["a".to_string(), "b".to_string()]),
                product_scores: Some(vec![score1, score2]),
            };

            let elem2 = ElemMatch {
                id: Some(2),
                str_array: Some(vec!["c".to_string(), "d".to_string()]),
                product_scores: Some(vec![score3, score4]),
            };

            repo.insert_many(vec![elem1, elem2])?;

            // Find where product_scores has element with product="xyz" and score>=8
            let cursor = repo.find(field("product_scores").elem_match(and(vec![
                field("product").eq("xyz"),
                field("score").gte(8.0f64),
            ])))?;

            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_elem_match_with_string_array() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<ElemMatch> = ctx.db().repository()?;

            let elem1 = ElemMatch {
                id: Some(1),
                str_array: Some(vec!["a".to_string(), "b".to_string()]),
                product_scores: None,
            };

            let elem2 = ElemMatch {
                id: Some(2),
                str_array: Some(vec!["c".to_string(), "d".to_string()]),
                product_scores: None,
            };

            let elem3 = ElemMatch {
                id: Some(3),
                str_array: Some(vec!["a".to_string(), "f".to_string()]),
                product_scores: None,
            };

            repo.insert_many(vec![elem1, elem2, elem3])?;

            // Find elements where str_array contains "a"
            // Use field("$") to refer to individual array elements
            let cursor = repo.find(field("str_array").elem_match(field("$").eq("a")))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// NESTED FIELD SEARCH
// =============================================================================

#[test]
fn test_find_by_nested_field() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let note = Note {
                note_id: Some(1),
                text: Some("important meeting notes".to_string()),
            };

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.employee_note = Some(note);
            repo.insert(emp)?;

            let note2 = Note {
                note_id: Some(2),
                text: Some("random text".to_string()),
            };

            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.employee_note = Some(note2);
            repo.insert(emp2)?;

            // Search by nested text field
            let cursor = repo.find(field("employee_note.text").text("meeting"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_by_nested_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let note = Note {
                note_id: Some(100),
                text: Some("note text".to_string()),
            };

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.employee_note = Some(note);
            repo.insert(emp)?;

            let cursor = repo.find(field("employee_note.note_id").eq(100i64))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// COMPOUND FILTERS
// =============================================================================

#[test]
fn test_complex_compound_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                emp.address = Some(if i % 2 == 0 {
                    "even address".to_string()
                } else {
                    "odd address".to_string()
                });
                repo.insert(emp)?;
            }

            // Complex filter: (emp_id > 3 AND emp_id < 8) OR emp_id = 0
            let cursor = repo.find(or(vec![
                and(vec![field("emp_id").gt(3i64), field("emp_id").lt(8i64)]),
                field("emp_id").eq(0i64),
            ]))?;

            assert_eq!(cursor.count(), 5); // 0, 4, 5, 6, 7

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
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            // Empty repository
            let cursor = repo.find(all())?;
            assert_eq!(cursor.count(), 0);

            // Insert some data
            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(all())?;
            assert_eq!(cursor.count(), 5);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// CURSOR OPERATIONS
// =============================================================================

#[test]
fn test_cursor_size() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..10 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let mut cursor = repo.find(all())?;
            assert_eq!(cursor.size(), 10);

            // Size should be replayable
            assert_eq!(cursor.size(), 10);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_cursor_first() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let mut cursor = repo.find(all())?;
            let first = cursor.first();
            assert!(first.is_some());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_cursor_first_empty() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut cursor = repo.find(all())?;
            let first = cursor.first();
            assert!(first.is_none());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_cursor_iteration() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            for i in 0..5 {
                let mut emp = Employee::default();
                emp.emp_id = Some(i);
                repo.insert(emp)?;
            }

            let cursor = repo.find(all())?;
            let collected: Vec<_> = cursor.collect();
            assert_eq!(collected.len(), 5);

            Ok(())
        },
        cleanup,
    )
}

// =============================================================================
// EDGE CASES
// =============================================================================

#[test]
fn test_search_with_null_values() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp1 = Employee::default();
            emp1.emp_id = Some(1);
            emp1.address = Some("test".to_string());
            repo.insert(emp1)?;

            let mut emp2 = Employee::default();
            emp2.emp_id = Some(2);
            emp2.address = None; // null address
            repo.insert(emp2)?;

            // Search for non-null addresses
            let cursor = repo.find(field("address").text("test"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_search_empty_string() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Employee> = ctx.db().repository()?;

            let mut emp = Employee::default();
            emp.emp_id = Some(1);
            emp.email_address = Some("".to_string()); // Use email_address which doesn't have full-text index
            repo.insert(emp)?;

            let cursor = repo.find(field("email_address").eq(""))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_find_by_entity_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository()?;

            let book = Book {
                book_id: BookId {
                    isbn: Some("123456".to_string()),
                    name: Some("Test Book".to_string()),
                    author: Some("Author Name".to_string()),
                },
                publisher: Some("Publisher".to_string()),
                price: Some(29.99),
                tags: None,
                description: None,
            };

            repo.insert(book.clone())?;

            // Find by entity id
            let cursor = repo.find(field("book_id").eq(Value::Document({
                let mut doc = nitrite::collection::Document::new();
                doc.put("isbn", "123456").unwrap();
                doc.put("name", "Test Book").unwrap();
                doc.put("author", "Author Name").unwrap();
                doc
            })))?;

            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}
