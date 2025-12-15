use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::{ID_GENERATOR, NO2};
use once_cell::sync::Lazy;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};

static NO2_STR: Lazy<String> = Lazy::new(|| NO2.to_string());

static ID_TOO_LARGE_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        &format!("NitriteId validation error: id value must be less than 10^19 ({})", 10u64.pow(19)),
        ErrorKind::InvalidId
    )
});

static ID_TOO_SMALL_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        &format!("NitriteId validation error: id value must be greater than or equal to 10^18 ({})", 10u64.pow(18)),
        ErrorKind::InvalidId
    )
});

static MAX_VALUE: Lazy<u64> = Lazy::new(|| 10u64.pow(19));
static MIN_VALUE: Lazy<u64> = Lazy::new(|| 10u64.pow(18));

/// A unique identifier for documents in Nitrite.
///
/// Each document in a collection is uniquely identified by a `NitriteId`. The ID is
/// automatically generated using a Snowflake-like distributed ID generator if not
/// explicitly provided when inserting a document.
///
/// # ID Generation
///
/// Nitrite uses a Snowflake-based ID generator that produces 64-bit unsigned integers
/// in the range [10^18, 10^19). This ensures:
/// - Uniqueness across all documents and replicas
/// - Approximate timestamp ordering
/// - No central coordination required
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::collection::NitriteId;
/// use nitrite::collection::Document;
///
/// // Auto-generate an ID
/// let id = NitriteId::new();
///
/// // Create a specific ID (if valid)
/// let id = NitriteId::create_id(1000000000000000001)?;
///
/// // Use with documents
/// let mut doc = Document::new();
/// // Document ID is auto-set during insertion
/// ```
///
/// # Storage
///
/// The ID is stored in the `_id` field of documents. When retrieving or updating
/// documents, you can use `get_by_id()` for O(1) access.
#[derive(PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy, serde::Deserialize, serde::Serialize)]
pub struct NitriteId {
    id_value: u64,
}

impl NitriteId {
    /// Generates a new unique `NitriteId`.
    ///
    /// Uses the internal Snowflake ID generator to create a unique ID
    /// based on timestamp and machine information.
    ///
    /// # Returns
    ///
    /// A new unique `NitriteId`
    pub fn new() -> Self {
        let id_value = ID_GENERATOR.get_id();
        NitriteId {
            id_value,
        }
    }

    /// Creates a `NitriteId` from a specific value.
    ///
    /// The value must be within the valid range [10^18, 10^19).
    /// This is useful when you want to use custom ID values, such as UUIDs
    /// converted to integers or existing ID schemes.
    ///
    /// # Arguments
    ///
    /// * `id_value` - A 64-bit unsigned integer ID
    ///
    /// # Returns
    ///
    /// `Ok(NitriteId)` if the value is valid, or `Err(NitriteError)` if it's outside
    /// the valid range
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let id = NitriteId::create_id(1000000000000000001)?;
    /// ```
    pub fn create_id(id_value: u64) -> NitriteResult<NitriteId> {
        NitriteId::valid_id(id_value)?;
        Ok(NitriteId { id_value })
    }

    /// Gets the numeric value of this ID.
    ///
    /// # Returns
    ///
    /// The 64-bit unsigned integer value of this ID
    pub fn id_value(&self) -> u64 {
        self.id_value
    }

    pub(crate) fn valid_id(id_value: u64) -> NitriteResult<bool> {    
        if id_value >= *MAX_VALUE {
            log::error!("Id value is too large");
            return Err(ID_TOO_LARGE_ERROR.clone());
        } else if id_value < *MIN_VALUE {
            log::error!("Id value is too small");
            return Err(ID_TOO_SMALL_ERROR.clone());
        }
        
        Ok(true)
    }
}

impl Default for NitriteId {
    fn default() -> Self {
        NitriteId::new()
    }
}

impl Debug for NitriteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]{}", self.id_value, &*NO2_STR)
    }
}

impl Display for NitriteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]{}", self.id_value, &*NO2_STR)
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::NitriteId;
    use crate::errors::ErrorKind;
    use crate::ID_GENERATOR;
    use parking_lot::RwLock;
    use std::cmp::Ordering;

    #[test]
    fn test_new_id() {
        let id = NitriteId::new();
        assert!(id.id_value > 0);
        assert_eq!(id.id_value.to_string().len(), 19);
    }

    #[test]
    fn test_create_id() {
        let id_value = ID_GENERATOR.get_id();
        let id = NitriteId::create_id(id_value);
        assert!(id.is_ok());
        assert_eq!(id.unwrap().id_value, id_value);

        let id = NitriteId::create_id(123);
        assert!(id.is_err());
        assert_eq!(id.err().unwrap().kind(), &ErrorKind::InvalidId);
    }

    #[test]
    fn test_create_id_with_empty_id() {
        let result = NitriteId::create_id(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_id() {
        let id = NitriteId::valid_id(1324567890123456789);
        assert!(id.is_ok());
    }

    #[test]
    fn test_valid_id_with_empty_id() {
        let id = NitriteId::valid_id(0);
        assert!(id.is_err());
        assert_eq!(id.err().unwrap().kind(), &ErrorKind::InvalidId);
    }

    #[test]
    fn test_valid_id_with_invalid_id() {
        let id = NitriteId::valid_id(123);
        assert!(id.is_err());
        assert_eq!(id.err().unwrap().kind(), &ErrorKind::InvalidId);
    }

    #[test]
    fn test_display() {
        let id = NitriteId::create_id(1234567890123456789).unwrap();
        assert_eq!(format!("{}", id), "[1234567890123456789]NO₂");
    }

    #[test]
    fn test_debug() {
        let id = NitriteId::create_id(1234567890123456789).unwrap();
        assert_eq!(format!("{:?}", id), "[1234567890123456789]NO₂");
    }

    #[test]
    fn test_cmp() {
        let id1 = NitriteId::create_id(1234567890123456788).unwrap();
        let id2 = NitriteId::create_id(1234567890123456789).unwrap();
        assert_eq!(id1.cmp(&id2), Ordering::Less);
    }

    #[test]
    fn test_uniqueness() {
        let mut ids = Vec::new();
        for _ in 0..100 {
            ids.push(NitriteId::new());
        }

        let mut unique_ids = ids.clone();
        unique_ids.sort();
        unique_ids.dedup();
        assert_eq!(ids.len(), unique_ids.len());
    }

    #[test]
    #[should_panic(expected = "validation error")]
    fn test_limit_max() {
        let _ = NitriteId::create_id(u64::MAX).unwrap();
    }

    #[test]
    #[should_panic(expected = "validation error")]
    fn test_limit_min() {
        let _ = NitriteId::create_id(u64::MIN).unwrap();
    }

    #[test]
    fn test_equal() {
        let one = NitriteId::create_id(1234567890123456789).unwrap();
        let two = NitriteId::create_id(1234567890123456789).unwrap();
        assert_eq!(one, two);

        let three = NitriteId::create_id(1234567890123456780).unwrap();
        assert_ne!(one, three);
    }

    #[test]
    fn ord_trait_works() {
        let one = NitriteId::create_id(1234567890123456780).unwrap();
        let two = NitriteId::create_id(1234567890123456789).unwrap();
        assert!(one < two);
    }

    #[test]
    fn default_trait_works() {
        let id = NitriteId::default();
        assert!(id.id_value > 0);
        assert_eq!(id.id_value.to_string().len(), 19);
    }

    #[test]
    fn clone_trait_works() {
        let id = NitriteId::create_id(1234567890123456789).unwrap();
        let cloned_id = id;
        assert_eq!(id, cloned_id);
    }

    #[test]
    fn hash_trait_works() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let id1 = NitriteId::create_id(1234567890123456788).unwrap();
        let id2 = NitriteId::create_id(1234567890123456788).unwrap();
        let id3 = NitriteId::create_id(1234567890123456789).unwrap();

        let mut hasher1 = DefaultHasher::new();
        id1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        id2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut hasher3 = DefaultHasher::new();
        id3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }
    
    #[test]
    fn test_multithreaded_id_generation() {
        use std::sync::Arc;
        use std::thread;

        let set = Arc::new(RwLock::new(std::collections::HashSet::new()));
        let mut handles = vec![];

        for _ in 0..100 {
            let set = set.clone();
            let handle = thread::spawn(move || {
                let id = NitriteId::new();
                {
                    let set = set.read();
                    if set.contains(&id.id_value) {
                        panic!("Duplicate id found");
                    }
                }
                {
                    let mut set = set.write();
                    set.insert(id.id_value);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
