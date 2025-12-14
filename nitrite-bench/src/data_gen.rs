//! Data generators for benchmarks

use fake::faker::address::en::CityName;
use fake::faker::company::en::*;
use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::Fake;
use nitrite::collection::Document;
use nitrite::doc;
use rand::Rng;

/// Generate simple documents for CRUD and indexing benchmarks
pub fn generate_simple_docs(count: usize) -> Vec<Document> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let first_name: String = FirstName().fake();
            let last_name: String = LastName().fake();
            let email: String = FreeEmail().fake();
            let company: String = CompanyName().fake();
            let age: i64 = rng.gen_range(18..80);
            let salary: f64 = rng.gen_range(30000.0..200000.0);

            doc! {
                id: (i as i64),
                firstName: (first_name),
                lastName: (last_name),
                email: (email),
                company: (company),
                age: (age),
                salary: (salary),
                active: (rng.gen_bool(0.8))
            }
        })
        .collect()
}

/// Generate documents with spatial data (x, y coordinates)
pub fn generate_spatial_docs(count: usize) -> Vec<Document> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|i| {
            let name: String = CityName().fake();
            // Generate coordinates within a 1000x1000 grid
            let x: f64 = rng.gen_range(0.0..1000.0);
            let y: f64 = rng.gen_range(0.0..1000.0);

            doc! {
                id: (i as i64),
                name: (name),
                location: {
                    x: (x),
                    y: (y)
                }
            }
        })
        .collect()
}

/// Generate documents with text content for FTS benchmarks
pub fn generate_fts_docs(count: usize) -> Vec<Document> {
    (0..count)
        .map(|i| {
            let title: String = Sentence(3..8).fake();
            let content: String = Paragraphs(2..5).fake::<Vec<String>>().join(" ");
            let author: String = Name().fake();

            doc! {
                id: (i as i64),
                title: (title),
                content: (content),
                author: (author)
            }
        })
        .collect()
}

/// Generate a single simple document for insert benchmarks
pub fn generate_single_doc(id: usize) -> Document {
    let mut rng = rand::thread_rng();
    let first_name: String = FirstName().fake();
    let last_name: String = LastName().fake();
    let email: String = FreeEmail().fake();
    let age: i64 = rng.gen_range(18..80);

    doc! {
        id: (id as i64),
        firstName: (first_name),
        lastName: (last_name),
        email: (email),
        age: (age)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_simple_docs() {
        let docs = generate_simple_docs(10);
        assert_eq!(docs.len(), 10);
    }

    #[test]
    fn test_generate_spatial_docs() {
        let docs = generate_spatial_docs(10);
        assert_eq!(docs.len(), 10);
    }

    #[test]
    fn test_generate_fts_docs() {
        let docs = generate_fts_docs(10);
        assert_eq!(docs.len(), 10);
    }
}
