use chrono::{DateTime, FixedOffset, Local, Offset, Utc};
use nitrite::collection::{Document, NitriteCollection};
use nitrite::common::{Convertible, Value};
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::nitrite::Nitrite;
use nitrite_fjall_adapter::FjallModule;
use std::backtrace::Backtrace;
use std::time::{Duration, Instant, SystemTime};
use std::{env, fs, thread};

/// Runs a test with retry logic and error handling.
/// Tests run on the current thread to avoid thread exhaustion when running many tests in parallel.
pub fn run_test<T, B, A>(before: B, test: T, after: A) -> ()
where
    T: Fn(TestContext) -> NitriteResult<()> + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
    B: Fn() -> NitriteResult<TestContext> + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
    A: Fn(TestContext) -> NitriteResult<()> + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
{
    const MAX_RETRIES: u32 = 3;
    let mut last_error: Option<String> = None;
    let mut last_backtrace: Option<String> = None;

    for attempt in 1..=MAX_RETRIES {
        let start_time = Instant::now();

        let result = std::panic::catch_unwind(|| {
            let backtrace = Backtrace::capture();
            let ctx_result = before();
            match ctx_result {
                Ok(ctx) => {
                    let test_result = test(ctx.clone());
                    match test_result {
                        Ok(_) => {
                            let after_result = after(ctx.clone());
                            match after_result {
                                Ok(_) => Ok(()),
                                Err(e) => Err((
                                    format!("After run failed: {:?}", e),
                                    backtrace.to_string(),
                                )),
                            }
                        }
                        Err(e) => {
                            let _ = after(ctx.clone());
                            Err((format!("Test failed: {:?}", e), backtrace.to_string()))
                        }
                    }
                }
                Err(e) => Err((format!("Before run failed: {:?}", e), backtrace.to_string())),
            }
        });

        let elapsed = start_time.elapsed();

        match result {
            Ok(Ok(_)) => return, // Test passed
            Ok(Err((e, bt))) => {
                last_error = Some(e.clone());
                last_backtrace = Some(bt);
                if attempt < MAX_RETRIES {
                    eprintln!(
                        "\n========== Test Attempt {}/{} Failed (took {:?}) ==========",
                        attempt, MAX_RETRIES, elapsed
                    );
                    eprintln!("Error: {}", e);
                    eprintln!("Retrying in {}ms...\n", 100 * attempt);
                    thread::sleep(Duration::from_millis(100 * attempt as u64));
                }
            }
            Err(panic_err) => {
                let err_msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    format!("Unknown panic: {:?}", panic_err.type_id())
                };

                last_error = Some(format!("Panic: {}", err_msg));
                last_backtrace = Some(Backtrace::capture().to_string());

                if attempt < MAX_RETRIES {
                    eprintln!(
                        "\n========== Test Attempt {}/{} Panicked (took {:?}) ==========",
                        attempt, MAX_RETRIES, elapsed
                    );
                    eprintln!("{}", last_error.as_ref().unwrap());
                    eprintln!("Retrying in {}ms...\n", 100 * attempt);
                    thread::sleep(Duration::from_millis(100 * attempt as u64));
                }
            }
        }
    }

    // All retries exhausted - print full details
    eprintln!("\n==================== TEST FAILED ====================");
    eprintln!("Failed after {} attempts", MAX_RETRIES);
    eprintln!("Last error: {}", last_error.as_deref().unwrap_or("Unknown"));
    if let Some(bt) = &last_backtrace {
        if !bt.is_empty() && !bt.contains("disabled") {
            eprintln!("\nBacktrace:\n{}", bt);
        }
    }
    eprintln!("=====================================================\n");

    panic!(
        "Test failed after {} attempts. Last error: {}",
        MAX_RETRIES,
        last_error.unwrap_or_default()
    );
}

#[derive(Clone)]
pub struct TestContext {
    path: String,
    db: Nitrite,
}

impl TestContext {
    pub fn new(path: String, db: Nitrite) -> Self {
        Self { path, db }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn db(&self) -> Nitrite {
        self.db.clone()
    }
}

pub fn random_path() -> String {
    let id = uuid::Uuid::new_v4();
    let temp_dir = env::temp_dir();
    temp_dir.join(id.to_string()).to_str().unwrap().to_string()
}

#[cfg(feature = "fjall")]
pub fn create_test_context() -> NitriteResult<TestContext> {
    use nitrite::errors::{ErrorKind, NitriteError};

    const MAX_ATTEMPTS: u32 = 3;
    let mut last_error: Option<NitriteError> = None;

    for attempt in 1..=MAX_ATTEMPTS {
        let path = random_path();

        // Ensure the path doesn't already exist (unlikely with UUID but be safe)
        if std::path::Path::new(&path).exists() {
            let _ = fs::remove_dir_all(&path);
            thread::sleep(Duration::from_millis(10));
        }

        // Use low_memory_preset for tests to minimize thread count
        // This uses only 1 flush worker and 1 compaction worker per database
        let storage_module = FjallModule::with_config()
            .db_path(&path)
            .low_memory_preset()
            .build();

        match Nitrite::builder()
            .load_module(storage_module)
            .open_or_create(None, None)
        {
            Ok(db) => {
                return Ok(TestContext::new(path, db));
            }
            Err(e) => {
                last_error = Some(e);
                // Clean up failed path
                let _ = fs::remove_dir_all(&path);

                if attempt < MAX_ATTEMPTS {
                    eprintln!(
                        "Warning: Failed to create test context (attempt {}/{}): {:?}",
                        attempt, MAX_ATTEMPTS, last_error
                    );
                    thread::sleep(Duration::from_millis(50 * attempt as u64));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        NitriteError::new("Failed to create test context", ErrorKind::InternalError)
    }))
}

#[cfg(feature = "fjall")]
pub fn create_spatial_test_context() -> NitriteResult<TestContext> {
    use nitrite::errors::{ErrorKind, NitriteError};
    use nitrite_spatial::SpatialModule;

    const MAX_ATTEMPTS: u32 = 3;
    let mut last_error: Option<NitriteError> = None;

    for attempt in 1..=MAX_ATTEMPTS {
        let path = random_path();

        // Ensure the path doesn't already exist (unlikely with UUID but be safe)
        if std::path::Path::new(&path).exists() {
            let _ = fs::remove_dir_all(&path);
            thread::sleep(Duration::from_millis(10));
        }

        // Use low_memory_preset for tests to minimize thread count
        // This uses only 1 flush worker and 1 compaction worker per database
        let storage_module = FjallModule::with_config()
            .db_path(&path)
            .low_memory_preset()
            .build();

        match Nitrite::builder()
            .load_module(storage_module)
            .load_module(SpatialModule)
            .open_or_create(None, None)
        {
            Ok(db) => {
                return Ok(TestContext::new(path, db));
            }
            Err(e) => {
                last_error = Some(e);
                // Clean up failed path
                let _ = fs::remove_dir_all(&path);

                if attempt < MAX_ATTEMPTS {
                    eprintln!(
                        "Warning: Failed to create spatial test context (attempt {}/{}): {:?}",
                        attempt, MAX_ATTEMPTS, last_error
                    );
                    thread::sleep(Duration::from_millis(50 * attempt as u64));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        NitriteError::new(
            "Failed to create spatial test context",
            ErrorKind::InternalError,
        )
    }))
}

#[cfg(feature = "fjall")]
pub fn create_fts_test_context() -> NitriteResult<TestContext> {
    use nitrite::errors::{ErrorKind, NitriteError};
    use nitrite_tantivy_fts::TantivyFtsModule;

    const MAX_ATTEMPTS: u32 = 3;
    let mut last_error: Option<NitriteError> = None;

    for attempt in 1..=MAX_ATTEMPTS {
        let path = random_path();

        // Ensure the path doesn't already exist (unlikely with UUID but be safe)
        if std::path::Path::new(&path).exists() {
            let _ = fs::remove_dir_all(&path);
            thread::sleep(Duration::from_millis(10));
        }

        // Use low_memory_preset for tests to minimize thread count
        let storage_module = FjallModule::with_config()
            .db_path(&path)
            .low_memory_preset()
            .build();

        match Nitrite::builder()
            .load_module(storage_module)
            .load_module(TantivyFtsModule::default())
            .open_or_create(None, None)
        {
            Ok(db) => {
                return Ok(TestContext::new(path, db));
            }
            Err(e) => {
                last_error = Some(e);
                // Clean up failed path
                let _ = fs::remove_dir_all(&path);

                if attempt < MAX_ATTEMPTS {
                    eprintln!(
                        "Warning: Failed to create FTS test context (attempt {}/{}): {:?}",
                        attempt, MAX_ATTEMPTS, last_error
                    );
                    thread::sleep(Duration::from_millis(50 * attempt as u64));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        NitriteError::new(
            "Failed to create FTS test context",
            ErrorKind::InternalError,
        )
    }))
}

#[cfg(feature = "memory")]
pub fn create_test_context() -> NitriteResult<TestContext> {
    let path = random_path();

    let db = Nitrite::builder().open_or_create(None, None)?;

    Ok(TestContext::new(path, db))
}

#[cfg(feature = "fjall")]
pub fn cleanup(ctx: TestContext) -> NitriteResult<()> {
    // Close the database first
    if let Err(e) = ctx.db().close() {
        eprintln!("Warning: Failed to close database: {:?}", e);
    }

    // Give the database time to release file handles
    thread::sleep(Duration::from_millis(50));

    // Remove the database directory with robust retry logic
    let path = ctx.path().to_string();
    let max_retries = 15;
    let mut base_delay_ms = 50u64;

    for retry in 0..max_retries {
        // Check if path exists before attempting removal
        if !std::path::Path::new(&path).exists() {
            return Ok(());
        }

        match fs::remove_dir_all(&path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) if retry < max_retries - 1 => {
                let kind = e.kind();
                match kind {
                    // Resource busy or permission errors - worth retrying
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::WouldBlock => {
                        // Exponential backoff with jitter
                        let jitter = (retry as u64 * 7) % 20;
                        let delay = base_delay_ms + jitter;
                        thread::sleep(Duration::from_millis(delay));
                        base_delay_ms = std::cmp::min(base_delay_ms * 2, 1000);
                    }
                    // Directory not found - already cleaned up
                    std::io::ErrorKind::NotFound => {
                        return Ok(());
                    }
                    // Other errors - still retry but with shorter backoff
                    _ => {
                        thread::sleep(Duration::from_millis(base_delay_ms));
                        base_delay_ms = std::cmp::min(base_delay_ms + 50, 500);
                    }
                }
            }
            Err(e) => {
                // Final attempt failed - log but don't fail the test
                // Temp files will be cleaned up by OS eventually
                eprintln!(
                    "Warning: Failed to remove test directory {} after {} attempts: {:?}",
                    path, max_retries, e
                );
                return Ok(());
            }
        }
    }

    Ok(())
}

#[cfg(feature = "memory")]
pub fn cleanup(ctx: TestContext) -> NitriteResult<()> {
    // Close the database
    ctx.db().close()?;

    // Clean up any orphaned rtree temp files
    // In memory mode, spatial indexes create rtree files in temp directory
    cleanup_rtree_temp_files();

    Ok(())
}

/// Clean up orphaned rtree temp files in the system temp directory.
/// These files are created by spatial indexes when no base path is provided.
#[cfg(feature = "memory")]
fn cleanup_rtree_temp_files() {
    let temp_dir = std::env::temp_dir();
    if let Ok(entries) = fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                // Match files like "nitrite_*.rtree"
                if file_name.starts_with("nitrite_") && file_name.ends_with(".rtree") {
                    if let Err(e) = fs::remove_file(&path) {
                        eprintln!(
                            "Warning: Failed to remove rtree temp file {:?}: {:?}",
                            path, e
                        );
                    }
                }
            }
        }
    }
}

pub fn create_test_docs() -> Vec<Document> {
    let dt1 = NitriteDateTime::parse_from_rfc3339("2012-07-01T02:15:22+02:00");
    let dt2 = NitriteDateTime::parse_from_rfc3339("2010-06-12T12:05:35+05:30");
    let dt3 = NitriteDateTime::parse_from_rfc3339("2014-04-17T22:25:44-04:00");

    let doc1 = doc! {
        first_name: "fn1",
        last_name: "ln1",
        birth_day: dt1,
        data: (vec![1u8, 2u8, 3u8]),
        arr: [1, 2, 3],
        list: (vec!["one", "two", "three"]),
        body: "a quick brown fox jump over the lazy dog",
    };

    let doc2 = doc! {
        first_name: "fn2",
        last_name: "ln2",
        birth_day: dt2,
        data: (vec![3u8, 4u8, 3u8]),
        arr: [3, 4, 3],
        list: (vec!["three", "four", "five"]),
        body: "quick hello world from nitrite",
    };

    let doc3 = doc! {
        first_name: "fn3",
        last_name: "ln2",
        birth_day: dt3,
        data: (vec![9u8, 4u8, 8u8]),
        arr: [9, 4, 8],
        body: "Lorem ipsum dolor sit amet, consectetur \
        adipiscing elit. Sed nunc mi, mattis ullamcorper \
        dignissim vitae, condimentum non lorem.",
    };

    vec![doc1, doc2, doc3]
}

pub fn insert_test_documents(collection: &NitriteCollection) -> NitriteResult<()> {
    collection.insert_many(create_test_docs())?;
    Ok(())
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct NitriteDateTime(pub DateTime<FixedOffset>);

impl NitriteDateTime {
    pub fn new(dt: DateTime<FixedOffset>) -> Self {
        Self(dt)
    }

    pub fn parse_from_rfc3339(s: &str) -> Self {
        let dt = DateTime::parse_from_rfc3339(s).unwrap();
        NitriteDateTime(dt)
    }

    pub fn from_system_time(system_time: SystemTime) -> Self {
        let duration = system_time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let dt =
            DateTime::<Utc>::from_timestamp(duration.as_secs() as i64, duration.subsec_nanos());
        let fixed_offset = Local::now().offset().fix();
        let dt_fixed: DateTime<FixedOffset> = dt.unwrap().with_timezone(&fixed_offset);
        NitriteDateTime(dt_fixed)
    }
}

impl Into<Value> for NitriteDateTime {
    fn into(self) -> Value {
        Value::String(self.0.to_rfc3339())
    }
}

impl From<Value> for NitriteDateTime {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => {
                let dt = DateTime::parse_from_rfc3339(&s).unwrap();
                NitriteDateTime(dt)
            }
            _ => panic!("Invalid value type"),
        }
    }
}

impl Convertible for NitriteDateTime {
    type Output = Self;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::String(self.0.to_rfc3339()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::String(s) => {
                let dt = DateTime::parse_from_rfc3339(&s).unwrap();
                Ok(NitriteDateTime(dt))
            }
            _ => panic!("Invalid value type"),
        }
    }
}

pub fn is_sorted<T: Ord>(iterable: impl IntoIterator<Item = T>, ascending: bool) -> bool {
    let mut iter = iterable.into_iter();
    if let Some(mut prev) = iter.next() {
        for current in iter {
            if ascending {
                if prev > current {
                    return false;
                }
            } else {
                if prev < current {
                    return false;
                }
            }
            prev = current;
        }
    }
    true
}

pub fn now() -> NitriteDateTime {
    let now_utc: DateTime<Utc> = Utc::now();
    let fixed_offset = FixedOffset::east_opt(6 * 60 * 60).unwrap();
    let now_fixed: DateTime<FixedOffset> = now_utc.with_timezone(&fixed_offset);
    NitriteDateTime(now_fixed)
}
