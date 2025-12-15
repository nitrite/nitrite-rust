use crate::collection::NitriteId;

/// The result of a write operation (insert, update, delete).
///
/// `WriteResult` contains information about a write operation, including
/// the list of NitriteIds that were affected. This allows you to track
/// which documents were modified.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::common::PersistentCollection;
/// use nitrite::collection::Document;
///
/// let mut collection = db.collection("users")?;
/// let mut doc = Document::new();
/// doc.put("name", "Alice")?;
///
/// // Insert returns a WriteResult
/// let result = collection.insert(doc)?;
///
/// // Get the IDs of inserted documents
/// for id in result.affected_nitrite_ids() {
///     println!("Inserted document with ID: {}", id);
/// }
/// ```
#[derive(Debug)]
pub struct WriteResult {
    nitrite_ids: Vec<NitriteId>,
}

impl WriteResult {
    /// Creates a new `WriteResult` with the specified affected IDs.
    ///
    /// # Arguments
    ///
    /// * `nitrite_ids` - A vector of NitriteIds that were affected by the write operation
    pub fn new(nitrite_ids: Vec<NitriteId>) -> Self {
        Self { nitrite_ids }
    }

    /// Gets the list of NitriteIds affected by the write operation.
    ///
    /// # Returns
    ///
    /// A reference to the vector of affected NitriteIds
    pub fn affected_nitrite_ids(&self) -> &Vec<NitriteId> {
        &self.nitrite_ids
    }
}

impl Iterator for WriteResult {
    type Item = NitriteId;

    fn next(&mut self) -> Option<Self::Item> {
        self.nitrite_ids.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_result_new() {
        let nitrite_ids = vec![NitriteId::new(), NitriteId::new()];
        let write_result = WriteResult::new(nitrite_ids.clone());
        assert_eq!(write_result.affected_nitrite_ids(), &nitrite_ids);
    }

    #[test]
    fn test_write_result_get_nitrite_ids() {
        let nitrite_ids = vec![NitriteId::new(), NitriteId::new()];
        let write_result = WriteResult::new(nitrite_ids.clone());
        assert_eq!(write_result.affected_nitrite_ids(), &nitrite_ids);
    }

    #[test]
    fn test_write_result_iterator() {
        let nitrite_id1 = NitriteId::new();
        let nitrite_id2 = NitriteId::new();
        let mut write_result = WriteResult::new(vec![nitrite_id1, nitrite_id2]);

        assert_eq!(write_result.next(), Some(nitrite_id2));
        assert_eq!(write_result.next(), Some(nitrite_id1));
        assert_eq!(write_result.next(), None);
    }
}