mod transaction_collection_test;
mod transaction_repository_test;

use fake::faker::name::en::{FirstName, LastName};
use fake::Fake;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::NitriteDateTime;
use rand::{rng, RngCore};

/// Transaction test data entity - equivalent to Java's TxData
#[derive(Debug, Clone, PartialEq, Convertible, NitriteEntity, Default)]
#[entity(id(field = "id"))]
pub struct TxData {
    pub id: i64,
    pub name: String,
}

impl TxData {
    pub fn new(id: i64, name: &str) -> Self {
        TxData {
            id,
            name: name.to_string(),
        }
    }
}

/// Sub employee entity for mixed transaction tests
#[derive(Debug, Clone, PartialEq, Convertible, NitriteEntity, Default)]
#[entity(id(field = "emp_id"))]
pub struct SubEmployee {
    pub emp_id: i64,
    pub address: String,
    pub join_date: Option<NitriteDateTime>,
}

impl SubEmployee {
    pub fn generate() -> Self {
        SubEmployee {
            emp_id: rng().next_u64() as i64,
            address: format!(
                "{} {}",
                FirstName().fake::<String>(),
                LastName().fake::<String>()
            ),
            join_date: Some(NitriteDateTime::from_system_time(std::time::SystemTime::now())),
        }
    }
}
