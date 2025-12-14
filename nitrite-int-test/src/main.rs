use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::{all, field};
use nitrite::repository::ObjectRepository;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::{cleanup, create_test_context};

#[derive(Debug, Convertible, Default, NitriteEntity)]
pub struct StressRecord {
    pub first_name: Option<String>,
    pub processed: Option<bool>,
    pub last_name: Option<String>,
    pub failed: Option<bool>,
    pub note: Option<String>,
}

fn main() -> NitriteResult<()> {
    println!("Starting stress test...");
    let ctx = create_test_context()?;

    let count = 1000000;
    let repo: ObjectRepository<StressRecord> = ctx.db().repository()?;

    let start = std::time::Instant::now();
    for _ in 0..count {
        let mut record = StressRecord::default();
        record.first_name = Some(uuid::Uuid::new_v4().to_string());
        record.failed = Some(false);
        record.last_name = Some(uuid::Uuid::new_v4().to_string());
        record.processed = Some(false);

        repo.insert(record)?;
    }
    let elapsed = start.elapsed();
    println!("Inserted {} records in {:?}", count, elapsed);

    let start = std::time::Instant::now();
    let mut cursor = repo.find(field("failed").eq(false))?;
    let count = cursor.size();
    for record in cursor {
        let mut record = record?;
        record.processed = Some(true);
    }
    let elapsed = start.elapsed();
    println!("Processed {} records in {:?}", count, elapsed);

    let start = std::time::Instant::now();
    repo.update_document(
        all(),
        &doc!{
            "processed": true,
        },
        false,
    )?;
    let elapsed = start.elapsed();
    println!("Updated all records in {:?}", elapsed);

    let start = std::time::Instant::now();
    let mut cursor = repo.find(field("processed").eq(true))?;
    println!("Counted {} processed records in {:?}", cursor.size(), start.elapsed());

    cleanup(ctx)
}
