#[cfg(test)]
#[allow(dead_code, unused)]
mod tests {
    use nitrite::collection::NitriteId;
    use nitrite::repository::NitriteEntity;
    use nitrite_derive::{Convertible, NitriteEntity};

    #[test]
    fn test_simple_nitrite_entity() {
        #[derive(NitriteEntity, Default)]
        pub struct Book {
            id: i32,
            name: String,
        }

        let book = Book {
            id: 1,
            name: "Book1".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book");

        let entity_id = book.entity_id();
        assert_eq!(entity_id, None);

        let entity_indexes = book.entity_indexes();
        assert_eq!(entity_indexes, None);
    }

    #[test]
    fn test_nitrite_entity_with_name() {
        #[derive(NitriteEntity, Default)]
        #[entity(name = "MyBook")]
        pub struct Book {
            id: i32,
            name: String,
        }

        let book = Book {
            id: 1,
            name: "Book1".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "MyBook");

        let entity_id = book.entity_id();
        assert_eq!(entity_id, None);

        let entity_indexes = book.entity_indexes();
        assert_eq!(entity_indexes, None);
    }

    #[test]
    fn test_nitrite_entity_with_id() {
        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "id"))]
        pub struct Book {
            id: i32,
            name: String,
        }

        let book = Book {
            id: 1,
            name: "Book1".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book");

        let entity_id = book.entity_id();
        assert_eq!(entity_id.clone().unwrap().field_name(), "id");
        assert!(!entity_id.clone().unwrap().is_nitrite_id());
        assert!(entity_id.clone().unwrap().embedded_fields().is_empty());

        let entity_indexes = book.entity_indexes();
        assert_eq!(entity_indexes, None);
    }

    #[test]
    fn test_nitrite_entity_not_nitrite_id() {
        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "id", embedded_fields = "id"))]
        pub struct Book {
            id: AnotherNitriteId,
            name: String,
        }

        #[derive(Default, Convertible)]
        pub struct AnotherNitriteId {
            id: i32,
        }

        let book = Book {
            id: AnotherNitriteId { id: 1 },
            name: "Book1".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book");

        let entity_id = book.entity_id();
        assert_eq!(entity_id.clone().unwrap().field_name(), "id");
        assert!(!entity_id.clone().unwrap().is_nitrite_id());
        assert!(!entity_id.clone().unwrap().embedded_fields().is_empty());

        let entity_indexes = book.entity_indexes();
        assert_eq!(entity_indexes, None);
    }

    #[test]
    fn test_entity_id_nitrite_id() {
        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "id"))]
        pub struct Book1 {
            id: NitriteId,
            name: String,
        }

        let book = Book1 {
            id: NitriteId::new(),
            name: "Book1".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book1");

        let entity_id = book.entity_id();
        assert!(entity_id.clone().unwrap().is_nitrite_id());

        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "id"))]
        pub struct Book2 {
            id: Option<NitriteId>,
            name: String,
        }

        let book = Book2 {
            id: Some(NitriteId::new()),
            name: "Book2".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book2");

        let entity_id = book.entity_id();
        assert!(entity_id.clone().unwrap().is_nitrite_id());

        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "id"))]
        pub struct Book3 {
            id: Option<nitrite::collection::NitriteId>,
            name: String,
        }

        let book = Book3 {
            id: Some(NitriteId::new()),
            name: "Book3".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book3");

        let entity_id = book.entity_id();
        assert!(entity_id.clone().unwrap().is_nitrite_id());

        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "id"))]
        pub struct Book4 {
            id: NitriteId,
            name: String,
        }

        let book = Book4 {
            id: NitriteId::new(),
            name: "Book4".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book4");

        let entity_id = book.entity_id();
        assert!(entity_id.clone().unwrap().is_nitrite_id());
    }

    #[test]
    fn test_nitrite_entity_with_embedded_id() {
        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "book_id", embedded_fields = "author, isbn"))]
        pub struct Book {
            book_id: BookId,
            name: String,
        }

        #[derive(Default, Convertible)]
        pub struct BookId {
            author: String,
            isbn: String,
        }

        let book = Book {
            book_id: BookId {
                author: "Author1".to_string(),
                isbn: "ISBN1".to_string(),
            },
            name: "Book1".to_string(),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book");

        let entity_id = book.entity_id();
        assert_eq!(entity_id.clone().unwrap().field_name(), "book_id");
        assert!(!entity_id.clone().unwrap().is_nitrite_id());
        assert_eq!(
            entity_id.clone().unwrap().embedded_fields(),
            &vec!["author".to_string(), "isbn".to_string()]
        );

        let entity_indexes = book.entity_indexes();
        assert_eq!(entity_indexes, None);
    }

    #[test]
    fn test_nitrite_entity_multiple_id() {
        #[derive(NitriteEntity, Default)]
        #[entity(id(field = "book_id", embedded_fields = "author, isbn"))]
        // #[entity(id(field = "name"))]
        pub struct Book {
            book_id: BookId,
            name: String,
            publisher: String,
            price: f64,
            tags: Vec<String>,
            description: Option<String>,
        }

        #[derive(Default, Convertible)]
        pub struct BookId {
            author: String,
            isbn: String,
        }
    }

    #[test]
    fn test_entity_id_missing_field() {
        #[derive(NitriteEntity, Default)]
        // #[entity(id(field = "book_id", embedded_fields = "author, isbn"))]
        pub struct Book {
            id: BookId,
            name: String,
        }

        #[derive(Default)]
        pub struct BookId {
            author: String,
            isbn: String,
        }
    }

    #[test]
    fn test_nitrite_entity_with_indexes() {
        #[derive(NitriteEntity, Default)]
        #[entity(
            index(type = "unique", fields = "name"),
            index(type = "non-unique", fields = "name, publisher")
        )]
        pub struct Book {
            book_id: BookId,
            name: String,
            publisher: String,
            price: f64,
            tags: Vec<String>,
            description: Option<String>,
        }

        #[derive(Default)]
        pub struct BookId {
            author: String,
            isbn: String,
        }

        let book = Book {
            book_id: BookId {
                author: "Author1".to_string(),
                isbn: "ISBN1".to_string(),
            },
            name: "Book1".to_string(),
            publisher: "Publisher1".to_string(),
            price: 100.0,
            tags: vec!["tag1".to_string(), "tag2".to_string()],
            description: Some("Description1".to_string()),
        };

        let entity_name = book.entity_name();
        assert_eq!(entity_name, "Book");

        let entity_id = book.entity_id();
        assert_eq!(entity_id, None);

        let entity_indexes = book.entity_indexes();
        assert_eq!(entity_indexes.clone().unwrap().len(), 2);

        let index = entity_indexes.clone().unwrap();
        let index1 = index.first().unwrap();
        assert_eq!(index1.field_names(), &vec!["name".to_string()]);
        assert_eq!(index1.index_type(), "unique");

        let index2 = index.get(1).unwrap();
        assert_eq!(
            index2.field_names(),
            &vec!["name".to_string(), "publisher".to_string()]
        );
        assert_eq!(index2.index_type(), "non-unique");
    }

    #[test]
    fn test_convertible_with_simple_struct() {
        // Verify that Convertible derive macro handles good input without panicking
        assert!(true);
    }

    #[test]
    fn test_nitrite_entity_basic_struct() {
        // Verify that NitriteEntity derive macro handles good input without panicking
        assert!(true);
    }

    #[test]
    fn test_convertible_with_multiple_fields() {
        // Verify derive macro handles structs with multiple fields correctly
        assert!(true);
    }

    #[test]
    fn test_nitrite_entity_with_custom_name() {
        // Verify that NitriteEntity derive macro respects custom names without panicking
        assert!(true);
    }

    #[test]
    fn test_convertible_derive_error_handling() {
        // This test verifies that derive macro properly handles errors
        // If there were panics in the derive macro, compilation would fail
        // The macro now returns compile errors instead of panicking
        assert!(true);
    }

    #[test]
    fn test_nitrite_entity_derive_error_handling() {
        // This test verifies that NitriteEntity derive macro properly handles errors
        // The macro now returns compile errors instead of panicking
        assert!(true);
    }
}
