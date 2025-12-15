// Compound Index tests for ObjectRepository
// Based on Java tests: RepositoryCompoundIndexTest.java

use crate::repository::{generate_book, Book, BookId, MyBook};
use nitrite::common::NON_UNIQUE_INDEX;
use nitrite::filter::all;
use nitrite::index::IndexOptions;
use nitrite::repository::ObjectRepository;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

// ========================================
// Compound Index Tests
// ========================================

#[test]
fn test_find_by_compound_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            let book_id = BookId {
                isbn: Some("123456".to_string()),
                name: Some("Nitrite Database".to_string()),
                author: Some("John Doe".to_string()),
            };
            
            let book = Book {
                book_id: book_id.clone(),
                publisher: Some("My Publisher House".to_string()),
                price: Some(22.56),
                tags: Some(vec!["database".to_string(), "nosql".to_string()]),
                description: Some("Some random book description".to_string()),
            };
            
            repo.insert(book)?;
            
            let book_by_id = repo.get_by_id(&book_id)?;
            assert!(book_by_id.is_some(), "Should find book by compound ID");
            
            let found_book = book_by_id.unwrap();
            assert_eq!(found_book.book_id.isbn, Some("123456".to_string()));
            assert_eq!(found_book.book_id.name, Some("Nitrite Database".to_string()));
            assert_eq!(found_book.book_id.author, Some("John Doe".to_string()));
            assert_eq!(found_book.publisher, Some("My Publisher House".to_string()));
            assert_eq!(found_book.price, Some(22.56));
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_compound_id_not_found() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            let book = generate_book();
            repo.insert(book)?;
            
            let non_existent_id = BookId {
                isbn: Some("non-existent".to_string()),
                name: Some("Non-existent Book".to_string()),
                author: Some("Unknown Author".to_string()),
            };
            
            let book_by_id = repo.get_by_id(&non_existent_id)?;
            assert!(book_by_id.is_none(), "Should not find book with non-existent compound ID");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_compound_index_existence() {
    run_test(
        create_test_context,
        |ctx| {
            // Book entity has compound index on (price, publisher)
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            let book = generate_book();
            repo.insert(book)?;
            
            // Check if compound index exists for price+publisher
            let has_compound_index = repo.has_index(vec!["price", "publisher"])?;
            assert!(has_compound_index, "Should have compound index on (price, publisher)");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_list_compound_indexes() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            let book = generate_book();
            repo.insert(book)?;
            
            let indexes = repo.list_indexes()?;
            
            // Book has: 
            // - non-unique index on tags
            // - full-text index on description
            // - unique compound index on (price, publisher)
            // - id index on book_id (embedded fields: isbn, book_name)
            
            let compound_index = indexes.iter().find(|idx| {
                idx.index_fields().field_names().len() > 1
            });
            
            assert!(compound_index.is_some(), "Should have at least one compound index");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_create_compound_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            let book = generate_book();
            repo.insert(book)?;
            
            // Create a new compound index on (tags, description)
            repo.create_index(vec!["tags", "description"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            
            let has_new_index = repo.has_index(vec!["tags", "description"])?;
            assert!(has_new_index, "Should have created new compound index");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_drop_compound_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            let book = generate_book();
            repo.insert(book)?;
            
            // Create a compound index to drop
            repo.create_index(vec!["tags", "description"], &IndexOptions::new(NON_UNIQUE_INDEX))?;
            
            // Verify index exists
            let has_index = repo.has_index(vec!["tags", "description"])?;
            assert!(has_index, "Compound index should exist before drop");
            
            // Drop the index
            repo.drop_index(vec!["tags", "description"])?;
            
            // Verify index is gone
            let has_index_after = repo.has_index(vec!["tags", "description"])?;
            assert!(!has_index_after, "Compound index should not exist after drop");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_rebuild_compound_index() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            for _ in 0..10 {
                let book = generate_book();
                repo.insert(book)?;
            }
            
            // Rebuild compound index
            repo.rebuild_index(vec!["price", "publisher"])?;
            
            // Verify index still exists and works
            let has_index = repo.has_index(vec!["price", "publisher"])?;
            assert!(has_index, "Compound index should exist after rebuild");
            
            // Verify data is intact
            let cursor = repo.find(all())?;
            let count = cursor.count();
            assert_eq!(count, 10, "Should still have all 10 books after index rebuild");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_update_with_compound_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            let book_id = BookId {
                isbn: Some("123456".to_string()),
                name: Some("Original Name".to_string()),
                author: Some("John Doe".to_string()),
            };
            
            let book = Book {
                book_id: book_id.clone(),
                publisher: Some("Original Publisher".to_string()),
                price: Some(10.00),
                tags: Some(vec!["original".to_string()]),
                description: Some("Original description".to_string()),
            };
            
            repo.insert(book)?;
            
            // Get the book
            let book = repo.get_by_id(&book_id)?;
            assert!(book.is_some());
            
            let mut book = book.unwrap();
            book.publisher = Some("Updated Publisher".to_string());
            book.price = Some(20.00);
            
            // Update the book
            let result = repo.update_one(book, false)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            
            // Verify update
            let updated_book = repo.get_by_id(&book_id)?;
            assert!(updated_book.is_some());
            let updated_book = updated_book.unwrap();
            assert_eq!(updated_book.publisher, Some("Updated Publisher".to_string()));
            assert_eq!(updated_book.price, Some(20.00));
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_delete_with_compound_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            let book_id = BookId {
                isbn: Some("123456".to_string()),
                name: Some("Book to Delete".to_string()),
                author: Some("John Doe".to_string()),
            };
            
            let book = Book {
                book_id: book_id.clone(),
                publisher: Some("Publisher".to_string()),
                price: Some(10.00),
                tags: Some(vec!["delete".to_string()]),
                description: Some("Book to be deleted".to_string()),
            };
            
            repo.insert(book)?;
            
            // Verify book exists
            let book = repo.get_by_id(&book_id)?;
            assert!(book.is_some());
            
            // Delete the book
            let book = book.unwrap();
            let result = repo.remove_one(book)?;
            assert_eq!(result.affected_nitrite_ids().len(), 1);
            
            // Verify deletion
            let deleted_book = repo.get_by_id(&book_id)?;
            assert!(deleted_book.is_none(), "Book should be deleted");
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_multiple_books_same_partial_id() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            // Insert two books with same ISBN but different names
            let book1 = Book {
                book_id: BookId {
                    isbn: Some("same-isbn".to_string()),
                    name: Some("Book One".to_string()),
                    author: Some("Author A".to_string()),
                },
                publisher: Some("Publisher 1".to_string()),
                price: Some(10.00),
                tags: None,
                description: None,
            };
            
            let book2 = Book {
                book_id: BookId {
                    isbn: Some("same-isbn".to_string()),
                    name: Some("Book Two".to_string()),
                    author: Some("Author B".to_string()),
                },
                publisher: Some("Publisher 2".to_string()),
                price: Some(20.00),
                tags: None,
                description: None,
            };
            
            repo.insert(book1)?;
            repo.insert(book2)?;
            
            // Should find book1
            let book_id1 = BookId {
                isbn: Some("same-isbn".to_string()),
                name: Some("Book One".to_string()),
                author: Some("Author A".to_string()),
            };
            let found1 = repo.get_by_id(&book_id1)?;
            assert!(found1.is_some());
            assert_eq!(found1.unwrap().publisher, Some("Publisher 1".to_string()));
            
            // Should find book2
            let book_id2 = BookId {
                isbn: Some("same-isbn".to_string()),
                name: Some("Book Two".to_string()),
                author: Some("Author B".to_string()),
            };
            let found2 = repo.get_by_id(&book_id2)?;
            assert!(found2.is_some());
            assert_eq!(found2.unwrap().publisher, Some("Publisher 2".to_string()));
            
            // Total count should be 2
            let cursor = repo.find(all())?;
            assert_eq!(cursor.count(), 2);
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_compound_unique_index_violation() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            // Insert first book
            let book1 = Book {
                book_id: BookId {
                    isbn: Some("isbn-1".to_string()),
                    name: Some("Book One".to_string()),
                    author: Some("Author".to_string()),
                },
                publisher: Some("Same Publisher".to_string()),
                price: Some(10.00),  // Same price+publisher combo
                tags: None,
                description: None,
            };
            
            repo.insert(book1)?;
            
            // Try to insert book with same (price, publisher) - should fail due to unique index
            let book2 = Book {
                book_id: BookId {
                    isbn: Some("isbn-2".to_string()),
                    name: Some("Book Two".to_string()),
                    author: Some("Another Author".to_string()),
                },
                publisher: Some("Same Publisher".to_string()),
                price: Some(10.00),  // Same price+publisher combo
                tags: None,
                description: None,
            };
            
            // This should fail due to unique constraint on (price, publisher)
            let result = repo.insert(book2);
            assert!(result.is_err(), "Should fail to insert duplicate compound unique index values");
            
            Ok(())
        },
        cleanup,
    );
}

// ========================================
// Keyed Repository with Compound Index Tests
// ========================================

#[test]
fn test_keyed_repository_compound_index() {
    run_test(
        create_test_context,
        |ctx| {
            // MyBook uses key "my_books"
            let repo: ObjectRepository<MyBook> = ctx.db().repository::<MyBook>()?;
            
            let book = MyBook {
                book_id: BookId {
                    isbn: Some("keyed-isbn".to_string()),
                    name: Some("Keyed Book".to_string()),
                    author: Some("Keyed Author".to_string()),
                },
                publisher: Some("Keyed Publisher".to_string()),
                price: Some(15.00),
                tags: Some(vec!["keyed".to_string()]),
                description: Some("A book in keyed repository".to_string()),
            };
            
            repo.insert(book)?;
            
            let book_id = BookId {
                isbn: Some("keyed-isbn".to_string()),
                name: Some("Keyed Book".to_string()),
                author: Some("Keyed Author".to_string()),
            };
            
            let found = repo.get_by_id(&book_id)?;
            assert!(found.is_some());
            assert_eq!(found.unwrap().publisher, Some("Keyed Publisher".to_string()));
            
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_compound_index_with_null_fields() {
    run_test(
        create_test_context,
        |ctx| {
            let repo: ObjectRepository<Book> = ctx.db().repository::<Book>()?;
            
            // Insert book with null price in compound index
            let book = Book {
                book_id: BookId {
                    isbn: Some("null-test-isbn".to_string()),
                    name: Some("Null Test Book".to_string()),
                    author: Some("Null Test Author".to_string()),
                },
                publisher: Some("Null Test Publisher".to_string()),
                price: None,  // Null price in compound index (price, publisher)
                tags: None,
                description: None,
            };
            
            repo.insert(book)?;
            
            let book_id = BookId {
                isbn: Some("null-test-isbn".to_string()),
                name: Some("Null Test Book".to_string()),
                author: Some("Null Test Author".to_string()),
            };
            
            let found = repo.get_by_id(&book_id)?;
            assert!(found.is_some());
            
            let book = found.unwrap();
            assert!(book.price.is_none());
            assert_eq!(book.publisher, Some("Null Test Publisher".to_string()));
            
            Ok(())
        },
        cleanup,
    );
}
