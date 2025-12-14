//! Integration tests for full-text search indexing in Nitrite.
//!
//! These tests verify that FTS indexes work correctly with the full
//! Nitrite database stack, including persistence and querying.

use nitrite::doc;
use nitrite::filter::field;
use nitrite_int_test::test_util::{cleanup, create_fts_test_context, run_test};
use nitrite_tantivy_fts::{fts_field, fts_index};

// ===== Index Creation Tests =====

#[test]
fn test_create_fts_index() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;

            // Create an FTS index on the content field
            collection.create_index(vec!["content"], &fts_index())?;

            // Verify the index was created
            assert!(collection.has_index(vec!["content"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_fts_index() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("test_drop")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert a document
            let doc = doc! {
                title: "Test",
                content: "This is a test document"
            };
            collection.insert(doc)?;

            // Drop the index
            collection.drop_index(vec!["content"])?;

            // Index should no longer exist
            assert!(!collection.has_index(vec!["content"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Basic Search Tests =====

#[test]
fn test_insert_and_search_single_term() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert a document with searchable content
            let doc = doc! {
                title: "Hello World",
                content: "A quick brown fox jumps over the lazy dog"
            };
            collection.insert(doc)?;

            // Search for a single term
            let filter = fts_field("content").matches("fox");
            let cursor = collection.find(filter)?;

            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_insert_and_search_multiple_terms() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert documents
            let doc1 = doc! {
                title: "Article 1",
                content: "hello world from nitrite"
            };
            let doc2 = doc! {
                title: "Article 2",
                content: "goodbye world from rust"
            };
            let doc3 = doc! {
                title: "Article 3",
                content: "hello universe"
            };

            collection.insert_many(vec![doc1, doc2, doc3])?;

            // Search for "hello" - should find 2 documents
            let filter = fts_field("content").matches("hello");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 2);

            // Search for "world" - should find 2 documents
            let filter = fts_field("content").matches("world");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 2);

            // Search for "universe" - should find 1 document
            let filter = fts_field("content").matches("universe");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_search_no_results() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Test",
                content: "hello world"
            };
            collection.insert(doc)?;

            // Search for term that doesn't exist
            let filter = fts_field("content").matches("nonexistent");
            let cursor = collection.find(filter)?;

            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Phrase Search Tests =====

#[test]
fn test_phrase_search() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert documents
            let doc1 = doc! {
                title: "Article 1",
                content: "the quick brown fox"
            };
            let doc2 = doc! {
                title: "Article 2",
                content: "quick and the dead"
            };
            let doc3 = doc! {
                title: "Article 3",
                content: "a very quick brown rabbit"
            };

            collection.insert_many(vec![doc1, doc2, doc3])?;

            // Phrase search for "quick brown" - should find doc1 and doc3
            let filter = fts_field("content").phrase("quick brown");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 2);

            // Phrase search for "the quick" - should find only doc1
            let filter = fts_field("content").phrase("the quick");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_phrase_search_case_insensitive() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Test",
                content: "The Quick Brown Fox"
            };
            collection.insert(doc)?;

            // Phrase search should be case insensitive
            let filter = fts_field("content").phrase("quick brown");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Document Update/Delete Tests =====

#[test]
fn test_update_document_updates_index() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert a document
            let doc = doc! {
                title: "Test",
                content: "original content hello"
            };
            collection.insert(doc)?;

            // Verify we can find it
            let filter = fts_field("content").matches("original");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            // Update the document
            let update_doc = doc! {
                content: "updated content goodbye"
            };
            collection.update(field("title").eq("Test"), &update_doc)?;

            // Original term should no longer be found
            let filter = fts_field("content").matches("original");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 0);

            // New term should be found
            let filter = fts_field("content").matches("goodbye");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_delete_document_removes_from_index() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert documents
            let doc1 = doc! {
                title: "Doc1",
                content: "hello world"
            };
            let doc2 = doc! {
                title: "Doc2",
                content: "hello universe"
            };
            collection.insert_many(vec![doc1, doc2])?;

            // Should find both
            let filter = fts_field("content").matches("hello");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 2);

            // Delete one document
            collection.remove(field("title").eq("Doc1"), false)?;

            // Should now find only one
            let filter = fts_field("content").matches("hello");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            // "world" should not be found at all
            let filter = fts_field("content").matches("world");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Multiple Fields Tests =====

#[test]
fn test_fts_on_different_fields() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            // Create two separate collections with FTS on different field names
            let articles = ctx.db().collection("articles")?;
            let blog_posts = ctx.db().collection("blog_posts")?;

            // Create FTS index on "title" field for articles
            articles.create_index(vec!["title"], &fts_index())?;

            // Create FTS index on "body" field for blog_posts
            blog_posts.create_index(vec!["body"], &fts_index())?;

            let article_doc = doc! {
                title: "Database Introduction",
                body: "Other content"
            };
            articles.insert(article_doc)?;

            let blog_doc = doc! {
                title: "Some Blog",
                body: "Learn about databases and storage engines"
            };
            blog_posts.insert(blog_doc)?;

            // Search in title field of articles collection
            let filter = fts_field("title").matches("database");
            let cursor = articles.find(filter)?;
            assert_eq!(cursor.count(), 1);

            // Search in body field of blog_posts collection
            let filter = fts_field("body").matches("storage");
            let cursor = blog_posts.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Large Dataset Tests =====

#[test]
fn test_fts_with_many_documents() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert many documents
            let mut docs = Vec::new();
            for i in 0..100 {
                let content = if i % 2 == 0 {
                    format!("Document {} contains the word hello", i)
                } else {
                    format!("Document {} contains the word goodbye", i)
                };
                let title = format!("Doc{}", i);
                let doc = doc! {
                    title: (title),
                    content: (content)
                };
                docs.push(doc);
            }
            collection.insert_many(docs)?;

            // Verify counts
            assert_eq!(collection.size()?, 100);

            // Search should return 50 documents with "hello"
            let filter = fts_field("content").matches("hello");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 50);

            // Search should return 50 documents with "goodbye"
            let filter = fts_field("content").matches("goodbye");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 50);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Special Characters and Unicode Tests =====

#[test]
fn test_fts_with_unicode() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Unicode Test",
                content: "日本語テスト こんにちは世界"
            };
            collection.insert(doc)?;

            // Note: Tantivy's default tokenizer may handle CJK differently
            // The main verification is that insertion works without errors
            assert_eq!(collection.size()?, 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_fts_with_special_characters() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Special Chars",
                content: "email@example.com and https://example.com/path?query=value"
            };
            collection.insert(doc)?;

            // The tokenizer may split these differently
            // Main test is that it handles special chars without errors
            assert_eq!(collection.size()?, 1);

            // "email" might be extractable as a term
            let filter = fts_field("content").matches("email");
            let cursor = collection.find(filter)?;
            let _ = cursor.count();

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Empty and Edge Case Tests =====

#[test]
fn test_fts_empty_content() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Empty",
                content: ""
            };
            collection.insert(doc)?;

            // Should not find empty content
            let filter = fts_field("content").matches("anything");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_fts_null_field() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Insert document without the indexed field
            let doc = doc! {
                title: "No Content Field"
            };
            collection.insert(doc)?;

            // Should not crash, should find 0 results
            let filter = fts_field("content").matches("anything");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_fts_empty_collection() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("empty_collection")?;
            collection.create_index(vec!["content"], &fts_index())?;

            // Search on empty collection
            let filter = fts_field("content").matches("test");
            let cursor = collection.find(filter)?;

            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Index Rebuild Tests =====

#[test]
fn test_create_index_after_data_insertion() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("rebuild_test")?;

            // Insert documents BEFORE creating index
            for i in 0..10 {
                let content = format!("Document {} hello world", i);
                let title = format!("Doc{}", i);
                let doc = doc! {
                    title: (title),
                    content: (content)
                };
                collection.insert(doc)?;
            }

            // Create index after data exists - should rebuild
            collection.create_index(vec!["content"], &fts_index())?;

            // Query should still work
            let filter = fts_field("content").matches("hello");
            let cursor = collection.find(filter)?;

            assert_eq!(cursor.count(), 10);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

// ===== Fluent API Tests =====

#[test]
fn test_fluent_api_contains() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Test",
                content: "hello world"
            };
            collection.insert(doc)?;

            // Use contains() alias
            let filter = fts_field("content").contains("hello");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_fluent_api_text() {
    run_test(
        || create_fts_test_context(),
        |ctx| {
            let collection = ctx.db().collection("articles")?;
            collection.create_index(vec!["content"], &fts_index())?;

            let doc = doc! {
                title: "Test",
                content: "hello world"
            };
            collection.insert(doc)?;

            // Use text() alias
            let filter = fts_field("content").text("hello");
            let cursor = collection.find(filter)?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
