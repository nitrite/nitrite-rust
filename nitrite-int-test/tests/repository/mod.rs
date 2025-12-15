mod compound_index_test;
mod nitrite_id_as_id_test;
mod object_repository_negative_test;
mod object_repository_test;
mod projection_join_test;
mod repository_modification_test;
mod repository_search_test;
mod repository_factory_test;
mod object_cursor_test;

use fake::faker::address::en::{CityName, CountryCode, StreetName, ZipCode};
use fake::faker::barcode::en::Isbn;
use fake::faker::chrono::en::DateTime;
use fake::faker::company::en::{Buzzword, CatchPhrase, CompanyName};
use fake::faker::internet::en::FreeEmail;
use fake::faker::lorem::en::Paragraphs;
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::Fake;
use nitrite::collection::NitriteId;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::NitriteDateTime;
use rand::{random, rng, RngCore};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
#[entity(
    name = "books",
    id(field = "book_id", embedded_fields = "isbn, name"),
    index(type = "non-unique", fields = "tags"),
    index(type = "full-text", fields = "description"),
    index(type = "unique", fields = "price, publisher")
)]
pub struct Book {
    pub book_id: BookId,
    pub publisher: Option<String>,
    pub price: Option<f64>,
    pub tags: Option<Vec<String>>,
    pub description: Option<String>,
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
#[entity(
    name = "my_books",
    id(field = "book_id", embedded_fields = "isbn, name"),
    index(type = "non-unique", fields = "tags"),
    index(type = "full-text", fields = "description"),
    index(type = "unique", fields = "price, publisher")
)]
pub struct MyBook {
    pub book_id: BookId,
    pub publisher: Option<String>,
    pub price: Option<f64>,
    pub tags: Option<Vec<String>>,
    pub description: Option<String>,
}

#[derive(Debug, Convertible, Default, Clone)]
pub struct BookId {
    pub isbn: Option<String>,
    pub name: Option<String>,
    pub author: Option<String>,
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
pub struct StructA {
    pub b: Option<StructB>,
    pub uuid: Option<String>,
    pub name: Option<String>,
    pub blob: Option<Vec<u8>>,
}

impl StructA {
    pub fn create(seed: i32) -> Self {
        let uuid = Uuid::from_u64_pair(seed as u64, (seed + 50) as u64);

        StructA {
            b: Some(StructB::create(seed)),
            uuid: Some(format!("{}", uuid)),
            name: Some(format!("{:x}", uuid)),
            blob: Some(vec![seed as u8; 10]),
        }
    }
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
pub struct StructB {
    pub number: Option<i32>,
    pub text: Option<String>,
}

impl StructB {
    pub fn create(seed: i32) -> Self {
        StructB {
            number: Some(seed + 100),
            text: Some(format!("{:b}", seed)),
        }
    }
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
pub struct StructC {
    pub id: Option<i64>,
    pub digit: Option<f64>,
    pub parent: Option<StructA>,
}

impl StructC {
    pub fn create(seed: i32) -> Self {
        StructC {
            id: Some(seed as i64 * 5000),
            digit: Some(seed as f64 * 69.65),
            parent: Some(StructA::create(seed)),
        }
    }
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
#[entity(
    id(field = "emp_id"),
    index(type = "non-unique", fields = "join_date"),
    index(type = "full-text", fields = "address"),
    index(type = "full-text", fields = "employee_note.text")
)]
pub struct Employee {
    pub emp_id: Option<u64>,
    pub join_date: Option<NitriteDateTime>,
    pub address: Option<String>,
    pub email_address: Option<String>,
    pub company: Option<Company>,
    pub blob: Option<Vec<u8>>,
    pub employee_note: Option<Note>,
}

#[derive(Debug, Convertible, Default, Clone)]
pub struct Note {
    pub note_id: Option<u64>,
    pub text: Option<String>,
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
#[entity(
    id(field = "company_id"),
    index(type = "unique", fields = "company_name")
)]
pub struct Company {
    pub company_id: Option<u64>,
    pub company_name: Option<String>,
    pub date_created: Option<NitriteDateTime>,
    pub departments: Option<Vec<String>>,
    pub employee_record: Option<HashMap<String, Vec<Employee>>>,
}

#[derive(Debug, Convertible, Default, NitriteEntity)]
pub struct ProductScore {
    pub product: Option<String>,
    pub score: Option<f64>,
}

#[derive(Debug, Convertible, Default, NitriteEntity)]
pub struct ElemMatch {
    pub id: Option<i64>,
    pub str_array: Option<Vec<String>>,
    pub product_scores: Option<Vec<ProductScore>>,
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
#[entity(
    name = "my_person",
    index(type = "non-unique", fields = "status"),
    index(type = "full-text", fields = "name")
)]
pub struct Person {
    pub uuid: Option<String>,
    pub name: Option<String>,
    pub status: Option<String>,
    pub friend: Option<Box<Person>>,
    pub date_created: Option<NitriteDateTime>,
}

#[derive(Debug, Convertible, Default)]
pub struct EncryptedPerson {
    pub name: Option<String>,
    pub credit_card_number: Option<String>,
    pub cvv: Option<String>,
    pub expiry_date: Option<NitriteDateTime>,
}

#[derive(Debug, Convertible, NitriteEntity, Default, Clone)]
#[entity(
    index(type = "unique", fields = "first_name"),
    index(type = "non-unique", fields = "age"),
    index(type = "full-text", fields = "last_name")
)]
pub struct RepeatableIndexTest {
    pub first_name: Option<String>,
    pub age: Option<i32>,
    pub last_name: Option<String>,
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
pub struct SubEmployee {
    pub emp_id: Option<u64>,
    pub join_date: Option<NitriteDateTime>,
    pub address: Option<String>,
}

#[derive(Debug, Convertible, Default)]
pub struct WithCircularReference {
    pub name: Option<String>,
    pub parent: Option<Box<WithCircularReference>>,
}

#[derive(Debug, Convertible, Default, NitriteEntity, PartialEq, Clone)]
pub struct WithDateId {
    pub id: NitriteDateTime,
    pub name: String,
}

#[derive(Debug, Convertible, NitriteEntity, Default, PartialEq, Clone)]
#[entity(id(field = "id_field"))]
pub struct WithNitriteId {
    pub id_field: Option<NitriteId>,
    pub name: String,
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
#[entity(id(field = "name"))]
pub struct WithNoneId {
    pub name: Option<String>,
    pub number: f64,
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
#[entity(id(field = "nested_id"))]
pub struct WithoutEmbeddedId {
    pub nested_id: Option<NestedId>,
    pub data: String,
}

#[derive(Debug, Convertible, Default)]
pub struct NestedId {
    pub id: f64,
}

#[derive(Debug, Convertible, Default, NitriteEntity)]
pub struct WithOutId {
    pub name: String,
    pub number: f64,
}

#[derive(Debug, Convertible, Default)]
pub struct ProductId {
    pub unique_id: Option<String>,
    pub product_code: Option<String>,
}

#[derive(Debug, Convertible, Default)]
pub struct Manufacturer {
    pub name: Option<String>,
    pub address: Option<String>,
    pub unique_id: Option<i32>,
}

#[derive(Debug, Convertible, Default)]
pub struct Product {
    pub product_id: Option<ProductId>,
    pub manufacturer: Option<Manufacturer>,
    pub product_name: Option<String>,
    pub price: Option<f64>,
}

#[derive(Debug, Convertible, Default)]
pub struct MiniProduct {
    pub unique_id: Option<String>,
    pub manufacturer_name: Option<String>,
    pub price: Option<f64>,
}

#[derive(Debug, Convertible, Default, NitriteEntity)]
#[entity(id(field = "name"))]
pub struct WithPrivateField {
    name: String,
    number: f64,
}

#[derive(Debug, Convertible, Default, NitriteEntity)]
#[entity(id(field = "number"))]
#[converter(ignored = "name")]
pub struct WithTransientField {
    pub name: String,
    pub number: i64,
}

#[derive(Debug, Convertible, NitriteEntity, Default)]
#[entity(id(field = "name"))]
struct WithEmptyStringId {
    name: String,
    data: Option<String>,
}

pub fn generate_company() -> Company {
    let departments = vec![
        "HR".to_string(),
        "Engineering".to_string(),
        "Sales".to_string(),
        "Marketing".to_string(),
        "Support".to_string(),
        "Finance".to_string(),
    ];

    let mut company = Company {
        company_id: Some(rng().next_u64()),
        company_name: Some(CompanyName().fake()),
        date_created: Some(NitriteDateTime(DateTime().fake())),
        departments: Some(departments.clone()),
        employee_record: Some(HashMap::new()),
    };

    for department in departments {
        let employee = generate_employee_records(4);
        if let Some(record) = company.employee_record.as_mut() {
            record
                .entry(department)
                .or_insert_with(Vec::new)
                .extend(employee);
        }
    }

    let mut comp = company.clone();
    for employees in comp.employee_record.as_mut().unwrap().values_mut() {
        for employee in employees.iter_mut() {
            employee.company = Some(company.clone());
        }
    }

    company
}

pub fn generate_employee_records(count: u32) -> Vec<Employee> {
    let mut employees = Vec::new();
    for _ in 0..count {
        employees.push(generate_employee());
    }
    employees
}

pub fn generate_employee() -> Employee {
    Employee {
        emp_id: Some(rng().next_u64()),
        join_date: Some(NitriteDateTime(DateTime().fake())),
        address: Some(format!(
            "{}, {}, {}, {}",
            StreetName().fake::<String>(),
            CityName().fake::<String>(),
            CountryCode().fake::<String>(),
            ZipCode().fake::<String>()
        )),
        email_address: Some(FreeEmail().fake::<String>().to_string()),
        company: None,
        blob: Some(vec![random::<u8>(); 10]),
        employee_note: Some(generate_note()),
    }
}

pub fn generate_note() -> Note {
    Note {
        note_id: Some(rng().next_u64()),
        text: Some(Paragraphs(3..5).fake::<Vec<String>>().join(". ").to_string()),
    }
}

pub fn generate_book() -> Book {
    let book_id = BookId {
        isbn: Some(Isbn().fake::<String>().to_string()),
        name: Some(Buzzword().fake::<String>().to_string()),
        author: Some(format!(
            "{} {}",
            FirstName().fake::<String>(),
            LastName().fake::<String>()
        )),
    };

    Book {
        book_id,
        publisher: Some(CompanyName().fake::<String>()),
        price: Some(random::<f64>() * 100.0),
        tags: Some(vec![
            CatchPhrase().fake::<String>(),
            CatchPhrase().fake::<String>(),
            CatchPhrase().fake::<String>(),
        ]),
        description: Some(Paragraphs(1..3).fake::<Vec<String>>().join(". ").to_string()),
    }
}

pub fn generate_product() -> Product {
    Product {
        product_id: Some(generate_product_id()),
        manufacturer: Some(generate_manufacturer()),
        product_name: Some(format!(
            "{} {}",
            Buzzword().fake::<String>(),
            Name().fake::<String>()
        )),
        price: Some(random::<f64>() * 100.0),
    }
}

pub fn generate_product_id() -> ProductId {
    ProductId {
        unique_id: Some(format!("{}", Uuid::new_v4())),
        product_code: Some(Isbn().fake::<String>().to_string()),
    }
}

pub fn generate_manufacturer() -> Manufacturer {
    Manufacturer {
        name: Some(CompanyName().fake::<String>()),
        address: Some(format!(
            "{}, {}, {}, {}",
            StreetName().fake::<String>(),
            CityName().fake::<String>(),
            CountryCode().fake::<String>(),
            ZipCode().fake::<String>()
        )),
        unique_id: Some(random::<i32>()),
    }
}