/// Options for controlling update operations on documents.
///
/// `UpdateOptions` allows you to specify the behavior of an update operation,
/// such as whether to insert a new document if no matches are found, or update
/// only the first matching document.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::collection::UpdateOptions;
///
/// // Update only the first match
/// let options = UpdateOptions::just_once();
///
/// // Insert if no matches found
/// let options = UpdateOptions::insert_if_absent();
///
/// // Custom options
/// let options = UpdateOptions::new(true, false);
/// ```
#[derive(Default)]
pub struct UpdateOptions {
    insert_if_absent: bool,
    just_once: bool,
}

impl UpdateOptions {
    /// Creates a new `UpdateOptions` with specified behavior.
    ///
    /// # Arguments
    ///
    /// * `insert_if_absent` - If true, insert the update as a new document if no matches found
    /// * `just_once` - If true, update only the first matching document
    pub fn new(insert_if_absent: bool, just_once: bool) -> Self {
        Self {
            insert_if_absent,
            just_once,
        }
    }

    /// Returns whether to insert if no matching documents are found.
    pub fn is_insert_if_absent(&self) -> bool {
        self.insert_if_absent
    }

    /// Returns whether to update only the first matching document.
    pub fn is_just_once(&self) -> bool {
        self.just_once
    }
}


/// Creates `UpdateOptions` with insert-if-absent behavior.
///
/// If no documents match the update filter, a new document will be inserted.
pub fn insert_if_absent() -> UpdateOptions {
    UpdateOptions::new(true, false)
}

/// Creates `UpdateOptions` that updates only the first matching document.
///
/// If multiple documents match the filter, only the first one will be updated.
pub fn just_once() -> UpdateOptions {
    UpdateOptions::new(false, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_options_new() {
        let options = UpdateOptions::new(true, false);
        assert!(options.is_insert_if_absent());
        assert!(!options.is_just_once());

        let options = UpdateOptions::new(false, true);
        assert!(!options.is_insert_if_absent());
        assert!(options.is_just_once());
    }

    #[test]
    fn test_update_options_default() {
        let options = UpdateOptions::default();
        assert!(!options.is_insert_if_absent());
        assert!(!options.is_just_once());
    }

    #[test]
    fn test_insert_if_absent() {
        let options = insert_if_absent();
        assert!(options.is_insert_if_absent());
        assert!(!options.is_just_once());
    }

    #[test]
    fn test_just_once() {
        let options = just_once();
        assert!(!options.is_insert_if_absent());
        assert!(options.is_just_once());
    }
}