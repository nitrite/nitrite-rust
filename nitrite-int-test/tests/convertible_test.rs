#[cfg(test)]
mod tests {
    use nitrite::common::{Convertible, Value};
    use nitrite::doc;
    use nitrite::errors::NitriteResult;
    use nitrite_derive::Convertible;

    #[test]
    fn test_convertible_struct() {
        #[derive(Default, Debug)]
        pub struct Book {
            id: i32,
            name: String,
            authors: Vec<Author>,
        }

        #[derive(Default, Debug)]
        pub struct Author {
            id: i32,
            name: String,
        }

        impl Convertible for Author {
            type Output = Author;

            fn to_value(&self) -> NitriteResult<Value> {
                let document = doc!{
                    "id": (self.id),
                    "name": (self.name.clone()),
                };
                Ok(Value::from(document))
            }

            fn from_value(value: &Value) -> NitriteResult<Self::Output> {
                let mut author = Author::default();
                match value {
                    Value::Document(doc) => {
                        author.id = i32::from_value(&doc.get("id")?)?;
                        author.name = String::from_value(&doc.get("name")?)?;
                    }
                    _ => {
                        return Err(nitrite::errors::NitriteError::new(
                            "Value is not a document",
                            nitrite::errors::ErrorKind::ObjectMappingError,
                        ))
                    }
                }
                Ok(author)
            }
        }

        impl Convertible for Book {
            type Output = Book;

            fn to_value(&self) -> NitriteResult<Value> {
                let mut authors = Vec::new();
                for author in &self.authors {
                    authors.push(author.to_value()?);
                }

                let document = doc!{
                    "id": (self.id),
                    "name": (self.name.clone()),
                    "authors": (authors),
                };
                Ok(Value::from(document))
            }

            fn from_value(value: &Value) -> NitriteResult<Self::Output> {
                let document = value.as_document().unwrap();
                let id = document.get("id")?;
                let id = id.as_i32().unwrap();
                let name = document.get("name")?.as_string().unwrap().to_string();
                let authors = document.get("authors")?;
                let authors = authors.as_array().unwrap();
                let mut authors_vec = Vec::new();
                for author in authors {
                    authors_vec.push(Author::from_value(author)?);
                }
                Ok(Book {
                    id: *id,
                    name,
                    authors: authors_vec,
                })
            }
        }

        let book = Book {
            id: 1,
            name: "Book1".to_string(),
            authors: vec![
                Author {
                    id: 1,
                    name: "Author1".to_string(),
                },
                Author {
                    id: 2,
                    name: "Author2".to_string(),
                },
            ],
        };

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);
    }

    #[test]
    fn test_convertible_derive() {
        #[derive(Convertible, Debug, PartialEq, Clone)]
        pub struct Book {
            id: i32,
            name: String,
            authors: Vec<Author>,
            publisher: Option<Publisher>,
            details: (i32, String),
            genre: Genre,
            reference: Option<Box<Book>>,
        }

        #[derive(Convertible, Debug, PartialEq, Clone)]
        pub struct Author {
            id: i32,
            name: String,
        }

        #[derive(Convertible, Debug, PartialEq, Clone)]
        pub struct Publisher {
            id: i32,
            name: String,
        }

        #[derive(Convertible, Debug, PartialEq, Clone)]
        pub enum Genre {
            Fiction,
            NonFiction(i32, String),
            Horror { id: i32, name: String },
            Erotica(Erotica),
        }

        #[derive(Convertible, Debug, PartialEq, Clone)]
        pub struct Erotica {
            id: i32,
            name: String,
        }

        let mut book = Book {
            id: 1,
            name: "Book1".to_string(),
            authors: vec![
                Author {
                    id: 1,
                    name: "Author1".to_string(),
                },
                Author {
                    id: 2,
                    name: "Author2".to_string(),
                },
            ],
            publisher: Some(Publisher {
                id: 1,
                name: "Publisher1".to_string(),
            }),
            details: (1, "Details1".to_string()),
            genre: Genre::NonFiction(1, "NonFiction1".to_string()),
            reference: None,
        };

        book.reference = Some(Box::new(book.clone()));

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);
    }

    #[test]
    fn test_convertible_with_enum() {
        #[derive(Convertible, Debug, PartialEq)]
        pub enum Book {
            Fiction(Fiction),
            NonFiction(NonFiction),
            Horror { id: i32, name: String },
            Erotica(i32, String),
            None,
            Unknown,
        }

        #[derive(Convertible, Debug, PartialEq)]
        pub struct Fiction {
            id: i32,
            name: String,
        }

        #[derive(Convertible, Debug, PartialEq)]
        pub struct NonFiction {
            id: i32,
            name: String,
        }

        let book = Book::Fiction(Fiction {
            id: 1,
            name: "Fiction1".to_string(),
        });

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);

        let book = Book::NonFiction(NonFiction {
            id: 1,
            name: "NonFiction1".to_string(),
        });

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);

        let book = Book::None;
        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);

        let book = Book::Unknown;
        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);

        let book = Book::Horror {
            id: 1,
            name: "Horror1".to_string(),
        };

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);

        let book = Book::Erotica(1, "Erotica1".to_string());
        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book, book1);
    }

    #[test]
    fn test_convertible_with_ignored_struct_field() {
        #[derive(Convertible, Debug, PartialEq, Default)]
        #[converter(ignored = "ignored_field, ignored_field2")]
        pub struct Book {
            id: i32,
            name: String,
            ignored_field: String,
            ignored_field2: String,
        }

        let book = Book {
            id: 1,
            name: "Book1".to_string(),
            ignored_field: "This should be ignored".to_string(),
            ignored_field2: "This should also be ignored".to_string(),
        };

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        assert_eq!(book.id, book1.id);
        assert_eq!(book.name, book1.name);
        assert!(book1.ignored_field.is_empty());
        assert!(book1.ignored_field2.is_empty());
    }


    #[test]
    fn test_convertible_with_ignored_enum_field() {
        #[derive(Convertible, Debug, PartialEq)]
        #[converter(ignored = "ignored_field")]
        pub enum Book {
            Fiction(Fiction),
            NonFiction(NonFiction),
            Horror { id: i32, name: String, ignored_field: String },
        }

        #[derive(Convertible, Debug, PartialEq)]
        pub struct Fiction {
            id: i32,
            name: String,
        }

        #[derive(Convertible, Debug, PartialEq)]
        pub struct NonFiction {
            id: i32,
            name: String,
        }

        let book = Book::Horror {
            id: 1,
            name: "Horror1".to_string(),
            ignored_field: "This should be ignored".to_string(),
        };

        let value = book.to_value().unwrap();
        println!("{:?}", value);

        let book1 = Book::from_value(&value).unwrap();
        println!("{:?}", book1);

        if let Book::Horror { id, name, ignored_field } = book1 {
            assert_eq!(id, 1);
            assert_eq!(name, "Horror1");
            assert!(ignored_field.is_empty());
        } else {
            panic!("Expected Book::Horror variant");
        }
    }
}