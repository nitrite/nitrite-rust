use backtrace::Backtrace;
use serde::{de, ser};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::result::Result;

use crate::{atomic, Atomic};

/// Error kinds for Nitrite operations
///
/// This enum represents all possible error types that can occur during Nitrite database operations.
/// Each error kind describes a specific category of failure, enabling precise error handling.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::errors::{NitriteError, ErrorKind, NitriteResult};
///
/// fn example() -> NitriteResult<()> {
///     Err(NitriteError::new("Index not found", ErrorKind::IndexNotFound))
/// }
/// ```
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ErrorKind {
    // Filter Errors - actively used in pattern matching
    /// Error during filter evaluation or construction
    FilterError,
    
    // Indexing Errors - actively used in index operations
    /// Generic indexing error
    IndexingError,
    /// Index does not exist
    IndexNotFound,
    /// Index already exists at the specified location
    IndexAlreadyExists,
    /// Failed to build or rebuild an index
    IndexBuildFailed,
    /// Index data is corrupted
    IndexCorrupted,
    /// Index type does not match operation
    IndexTypeMismatch,
    /// An indexing operation is already in progress
    IndexingInProgress,
    
    // ID and Identity Errors - actively used in collection operations
    /// The provided ID is invalid
    InvalidId,
    /// The entity is not identifiable
    NotIdentifiable,
    /// The requested resource was not found
    NotFound,
    
    // Operation Errors - actively used for invalid/unsupported operations
    /// The operation is not valid in the current context
    InvalidOperation,
    
    // IO and Storage Errors - actively used in file/store operations
    /// Generic IO error
    IOError,
    /// The disk is full
    DiskFull,
    /// The file was not found
    FileNotFound,
    /// Permission denied for file operation
    PermissionDenied,
    /// File data is corrupted
    FileCorrupted,
    /// Error accessing file
    FileAccessError,
    
    // Data Encoding Errors - actively used in serialization/UTF-8 conversion
    /// Error encoding or decoding data
    EncodingError,
    /// Error mapping object to/from document
    ObjectMappingError,
    
    // Security Errors - actively used in authentication/authorization
    /// Security-related error (authentication, authorization, etc.)
    SecurityError,
    
    // Constraint Violation Errors - actively used in index uniqueness checks
    /// A unique constraint was violated
    UniqueConstraintViolation,
    
    // Validation Errors - actively used in field/data validation
    /// Generic validation error
    ValidationError,
    /// Invalid data type for operation
    InvalidDataType,
    /// Invalid field name
    InvalidFieldName,
    /// A required field is missing
    MissingRequiredField,
    
    // Collection/Repository Errors - actively used in collection lookups
    /// Collection does not exist
    CollectionNotFound,
    /// Repository does not exist
    RepositoryNotFound,
    
    // Event Errors - actively used in event bus operations
    /// Error in event processing
    EventError,
    
    // Plugin Errors - actively used in plugin loading
    /// Generic plugin error
    PluginError,
    /// Failed to load a plugin
    PluginLoadFailed,
    
    // Backend and Store Errors - actively used in store state management
    /// Error from storage backend
    BackendError,
    /// Store has not been initialized
    StoreNotInitialized,
    /// Store has already been closed
    StoreAlreadyClosed,

    // Migration Errors - actively used in migration operations
    /// Error during schema migration
    MigrationError,
    
    // Extension Errors - allows external crates to plug in their own error types
    // The String contains the extension name/category (e.g., "spatial", "fulltext")
    /// Error from an extension module (e.g., spatial, fulltext)
    Extension(String),
    
    // Generic/Internal Errors - used as fallback
    /// Internal error (usually indicates a bug)
    InternalError,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::FilterError => write!(f, "Filter error"),
            ErrorKind::IndexingError => write!(f, "Indexing error"),
            ErrorKind::IndexNotFound => write!(f, "Index not found"),
            ErrorKind::IndexAlreadyExists => write!(f, "Index already exists"),
            ErrorKind::IndexBuildFailed => write!(f, "Index build failed"),
            ErrorKind::IndexCorrupted => write!(f, "Index corrupted"),
            ErrorKind::IndexTypeMismatch => write!(f, "Index type mismatch"),
            ErrorKind::IndexingInProgress => write!(f, "Indexing in progress"),
            ErrorKind::InvalidId => write!(f, "Invalid ID"),
            ErrorKind::NotIdentifiable => write!(f, "Not identifiable"),
            ErrorKind::NotFound => write!(f, "Not found"),
            ErrorKind::InvalidOperation => write!(f, "Invalid operation"),
            ErrorKind::IOError => write!(f, "IO error"),
            ErrorKind::DiskFull => write!(f, "Disk full"),
            ErrorKind::FileNotFound => write!(f, "File not found"),
            ErrorKind::PermissionDenied => write!(f, "Permission denied"),
            ErrorKind::FileCorrupted => write!(f, "File corrupted"),
            ErrorKind::FileAccessError => write!(f, "File access error"),
            ErrorKind::EncodingError => write!(f, "Encoding error"),
            ErrorKind::ObjectMappingError => write!(f, "Object mapping error"),
            ErrorKind::SecurityError => write!(f, "Security error"),
            ErrorKind::UniqueConstraintViolation => write!(f, "Unique constraint violation"),
            ErrorKind::ValidationError => write!(f, "Validation error"),
            ErrorKind::InvalidDataType => write!(f, "Invalid data type"),
            ErrorKind::InvalidFieldName => write!(f, "Invalid field name"),
            ErrorKind::MissingRequiredField => write!(f, "Missing required field"),
            ErrorKind::CollectionNotFound => write!(f, "Collection not found"),
            ErrorKind::RepositoryNotFound => write!(f, "Repository not found"),
            ErrorKind::EventError => write!(f, "Event error"),
            ErrorKind::PluginError => write!(f, "Plugin error"),
            ErrorKind::PluginLoadFailed => write!(f, "Plugin load failed"),
            ErrorKind::BackendError => write!(f, "Backend error"),
            ErrorKind::StoreNotInitialized => write!(f, "Store not initialized"),
            ErrorKind::StoreAlreadyClosed => write!(f, "Store already closed"),
            ErrorKind::MigrationError => write!(f, "Migration error"),
            ErrorKind::Extension(name) => write!(f, "{} error", name),
            ErrorKind::InternalError => write!(f, "Internal error"),
        }
    }
}

/// Custom Nitrite error type.
///
/// `NitriteError` encapsulates error information including the error message, kind, and optional cause.
/// It supports error chaining and backtraces for debugging.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::errors::{NitriteError, ErrorKind};
///
/// // Create a simple error
/// let err = NitriteError::new("Index not found", ErrorKind::IndexNotFound);
///
/// // Create an error with a cause
/// let cause = NitriteError::new("IO failed", ErrorKind::IOError);
/// let err = NitriteError::new_with_cause("Index build failed", ErrorKind::IndexBuildFailed, cause);
/// ```
///
/// # Type alias
///
/// The `NitriteResult<T>` type alias is equivalent to `Result<T, NitriteError>` and is used
/// throughout the codebase for operations that can fail.
#[derive(Clone)]
pub struct NitriteError {
    message: String,
    error_kind: ErrorKind,
    cause: Option<Box<NitriteError>>,
    backtrace: Atomic<Backtrace>,
}

impl NitriteError {
    /// Creates a new `NitriteError` with the specified message and error kind.
    ///
    /// # Arguments
    ///
    /// * `message` - A description of the error
    /// * `error_kind` - The category of error
    ///
    /// # Returns
    ///
    /// A new `NitriteError` instance.
    pub fn new(message: &str, error_kind: ErrorKind) -> Self {
        NitriteError {
            message: message.to_string(),
            error_kind,
            cause: None,
            backtrace: atomic(Backtrace::new()),
        }
    }

    /// Creates a new `NitriteError` with a cause error.
    ///
    /// This creates an error chain where the cause error is preserved for debugging.
    ///
    /// # Arguments
    ///
    /// * `message` - A description of the error
    /// * `error_type` - The category of error
    /// * `cause` - The underlying error that caused this error
    ///
    /// # Returns
    ///
    /// A new `NitriteError` instance with the cause error attached.
    pub fn new_with_cause(message: &str, error_type: ErrorKind, cause: NitriteError) -> Self {
        NitriteError {
            message: message.to_string(),
            error_kind: error_type,
            cause: Some(Box::new(cause)),
            backtrace: atomic(Backtrace::new()),
        }
    }
    
    pub fn message(&self) -> &str {
        &self.message
    }
    
    pub fn kind(&self) -> &ErrorKind {
        &self.error_kind
    }
    
    pub fn cause(&self) -> Option<&Box<NitriteError>> {
        self.cause.as_ref()
    }
}

impl Display for NitriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Debug for NitriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // print error message with stack trace followed by cause
        match &self.cause {
            Some(cause) => write!(f, "{}\nCaused by: {:?}", self.message, cause),
            None => write!(f, "{}\n{:?}", self.message, self.backtrace.read()),
        }
    }
}

impl Error for NitriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Some(cause) => Some(cause.as_ref()),
            None => None,
        }
    }
}

/// A result type alias for Nitrite operations.
///
/// `NitriteResult<T>` is shorthand for `Result<T, NitriteError>`.
/// All fallible Nitrite operations return this type.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::errors::NitriteResult;
///
/// fn find_collection(name: &str) -> NitriteResult<String> {
///     // Return success
///     Ok(name.to_string())
///     // Or return error
///     // Err(NitriteError::new("Collection not found", ErrorKind::CollectionNotFound))
/// }
/// ```
pub type NitriteResult<T> = Result<T, NitriteError>;

impl de::Error for NitriteError {
    fn custom<T: Display>(msg: T) -> Self {
        NitriteError::new(&msg.to_string(), ErrorKind::ObjectMappingError)
    }
}

impl ser::Error for NitriteError {
    fn custom<T: Display>(msg: T) -> Self {
        NitriteError::new(&msg.to_string(), ErrorKind::ObjectMappingError)
    }
}

// From trait implementations for automatic error conversion
impl From<std::io::Error> for NitriteError {
    fn from(err: std::io::Error) -> Self {
        let error_kind = match err.kind() {
            std::io::ErrorKind::NotFound => ErrorKind::FileNotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::AlreadyExists => ErrorKind::FileCorrupted,
            _ => ErrorKind::IOError,
        };
        NitriteError::new(&format!("IO error: {}", err), error_kind)
    }
}

impl From<std::string::FromUtf8Error> for NitriteError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        NitriteError::new(
            &format!("UTF-8 encoding error: {}", err),
            ErrorKind::EncodingError,
        )
    }
}

impl From<std::fmt::Error> for NitriteError {
    fn from(err: std::fmt::Error) -> Self {
        NitriteError::new(
            &format!("Formatting error: {}", err),
            ErrorKind::InternalError,
        )
    }
}

impl From<std::num::ParseIntError> for NitriteError {
    fn from(err: std::num::ParseIntError) -> Self {
        NitriteError::new(
            &format!("Integer parsing error: {}", err),
            ErrorKind::InvalidDataType,
        )
    }
}

impl From<std::num::ParseFloatError> for NitriteError {
    fn from(err: std::num::ParseFloatError) -> Self {
        NitriteError::new(
            &format!("Float parsing error: {}", err),
            ErrorKind::InvalidDataType,
        )
    }
}

impl From<String> for NitriteError {
    fn from(msg: String) -> Self {
        NitriteError::new(&msg, ErrorKind::InternalError)
    }
}

impl From<&str> for NitriteError {
    fn from(msg: &str) -> Self {
        NitriteError::new(msg, ErrorKind::InternalError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_io_error() -> Box<dyn Error + Send + Sync> {
        Box::new(std::io::Error::other("IO Error"))
    }

    #[test]
    fn nitrite_error_new_creates_error() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        assert_eq!(error.message, "An error occurred");
        assert_eq!(error.error_kind, ErrorKind::IOError);
        assert!(error.cause.is_none());
    }

    #[test]
    fn nitrite_error_new_with_cause_creates_error() {
        let cause = create_io_error();
        let error = NitriteError::new_with_cause(
            "An error occurred",
            ErrorKind::IOError,
            NitriteError::new(&cause.to_string(), ErrorKind::IOError),
        );
        assert_eq!(error.message, "An error occurred");
        assert_eq!(error.error_kind, ErrorKind::IOError);
        assert!(error.cause.is_some());
    }

    #[test]
    fn nitrite_error_message_returns_message() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        assert_eq!(error.message(), "An error occurred");
    }

    #[test]
    fn nitrite_error_kind_returns_kind() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        assert_eq!(error.kind(), &ErrorKind::IOError);
    }

    #[test]
    fn nitrite_error_cause_returns_cause() {
        let cause = create_io_error();
        let error = NitriteError::new_with_cause(
            "An error occurred",
            ErrorKind::IOError,
            NitriteError::new(&cause.to_string(), ErrorKind::IOError),
        );
        assert!(error.cause().is_some());
    }

    #[test]
    fn nitrite_error_cause_returns_none_when_no_cause() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        assert!(error.cause().is_none());
    }

    #[test]
    fn nitrite_error_display_formats_correctly() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        let formatted = format!("{}", error);
        assert_eq!(formatted, "An error occurred");
    }

    #[test]
    fn nitrite_error_debug_formats_correctly() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        let formatted = format!("{:?}", error);
        assert!(formatted.contains("An error occurred"));
    }

    #[test]
    fn nitrite_error_debug_formats_with_cause() {
        let cause = create_io_error();
        let error = NitriteError::new_with_cause(
            "An error occurred",
            ErrorKind::IOError,
            NitriteError::new(&cause.to_string(), ErrorKind::IOError),
        );
        let formatted = format!("{:?}", error);
        assert!(formatted.contains("An error occurred"));
        assert!(formatted.contains("Caused by:"));
    }

    #[test]
    fn nitrite_error_source_returns_cause() {
        let cause = create_io_error();
        let error = NitriteError::new_with_cause(
            "An error occurred",
            ErrorKind::IOError,
            NitriteError::new(&cause.to_string(), ErrorKind::IOError),
        );
        assert!(error.source().is_some());
    }

    #[test]
    fn nitrite_error_source_returns_none_when_no_cause() {
        let error = NitriteError::new("An error occurred", ErrorKind::IOError);
        assert!(error.source().is_none());
    }

    #[test]
    fn nitrite_error_ser_custom_creates_error() {
        let error = NitriteError::new("Custom error", ErrorKind::ObjectMappingError);
        assert_eq!(error.message(), "Custom error");
        assert_eq!(error.kind(), &ErrorKind::ObjectMappingError);
    }

    // Test Filter Errors
    #[test]
    fn test_filter_errors() {
        let filter_error = NitriteError::new("Invalid filter syntax", ErrorKind::FilterError);
        assert_eq!(filter_error.kind(), &ErrorKind::FilterError);
    }

    // Test ID and Identity Errors
    #[test]
    fn test_id_errors() {
        let invalid_id = NitriteError::new("Invalid ID format", ErrorKind::InvalidId);
        assert_eq!(invalid_id.kind(), &ErrorKind::InvalidId);

        let not_identifiable = NitriteError::new("Entity not identifiable", ErrorKind::NotIdentifiable);
        assert_eq!(not_identifiable.kind(), &ErrorKind::NotIdentifiable);
    }

    // Test Indexing Errors
    #[test]
    fn test_indexing_errors() {
        let index_error = NitriteError::new("Indexing failed", ErrorKind::IndexingError);
        assert_eq!(index_error.kind(), &ErrorKind::IndexingError);

        let not_found = NitriteError::new("Index not found", ErrorKind::IndexNotFound);
        assert_eq!(not_found.kind(), &ErrorKind::IndexNotFound);

        let exists = NitriteError::new("Index already exists", ErrorKind::IndexAlreadyExists);
        assert_eq!(exists.kind(), &ErrorKind::IndexAlreadyExists);

        let build_failed = NitriteError::new("Index build failed", ErrorKind::IndexBuildFailed);
        assert_eq!(build_failed.kind(), &ErrorKind::IndexBuildFailed);

        let corrupted = NitriteError::new("Index corrupted", ErrorKind::IndexCorrupted);
        assert_eq!(corrupted.kind(), &ErrorKind::IndexCorrupted);

        let type_mismatch = NitriteError::new("Index type mismatch", ErrorKind::IndexTypeMismatch);
        assert_eq!(type_mismatch.kind(), &ErrorKind::IndexTypeMismatch);

        let in_progress = NitriteError::new("Indexing in progress", ErrorKind::IndexingInProgress);
        assert_eq!(in_progress.kind(), &ErrorKind::IndexingInProgress);
    }

    // Test IO and Storage Errors
    #[test]
    fn test_io_errors() {
        let io_error = NitriteError::new("IO error", ErrorKind::IOError);
        assert_eq!(io_error.kind(), &ErrorKind::IOError);

        let disk_full = NitriteError::new("Disk full", ErrorKind::DiskFull);
        assert_eq!(disk_full.kind(), &ErrorKind::DiskFull);

        let file_not_found = NitriteError::new("File not found", ErrorKind::FileNotFound);
        assert_eq!(file_not_found.kind(), &ErrorKind::FileNotFound);

        let permission = NitriteError::new("Permission denied", ErrorKind::PermissionDenied);
        assert_eq!(permission.kind(), &ErrorKind::PermissionDenied);

        let corrupted = NitriteError::new("File corrupted", ErrorKind::FileCorrupted);
        assert_eq!(corrupted.kind(), &ErrorKind::FileCorrupted);

        let access = NitriteError::new("File access error", ErrorKind::FileAccessError);
        assert_eq!(access.kind(), &ErrorKind::FileAccessError);
    }

    // Test Security Errors
    #[test]
    fn test_security_errors() {
        let security = NitriteError::new("Security error", ErrorKind::SecurityError);
        assert_eq!(security.kind(), &ErrorKind::SecurityError);
    }

    // Test Constraint Violation Errors
    #[test]
    fn test_constraint_errors() {
        let unique = NitriteError::new("Unique constraint violated", ErrorKind::UniqueConstraintViolation);
        assert_eq!(unique.kind(), &ErrorKind::UniqueConstraintViolation);
    }

    // Test Validation Errors
    #[test]
    fn test_validation_errors() {
        let validation = NitriteError::new("Validation failed", ErrorKind::ValidationError);
        assert_eq!(validation.kind(), &ErrorKind::ValidationError);

        let invalid_type = NitriteError::new("Invalid data type", ErrorKind::InvalidDataType);
        assert_eq!(invalid_type.kind(), &ErrorKind::InvalidDataType);

        let invalid_name = NitriteError::new("Invalid field name", ErrorKind::InvalidFieldName);
        assert_eq!(invalid_name.kind(), &ErrorKind::InvalidFieldName);

        let missing = NitriteError::new("Missing required field", ErrorKind::MissingRequiredField);
        assert_eq!(missing.kind(), &ErrorKind::MissingRequiredField);
    }

    // Test Collection/Repository Errors
    #[test]
    fn test_collection_repository_errors() {
        let not_found = NitriteError::new("Collection not found", ErrorKind::CollectionNotFound);
        assert_eq!(not_found.kind(), &ErrorKind::CollectionNotFound);

        let repo_not_found = NitriteError::new("Repository not found", ErrorKind::RepositoryNotFound);
        assert_eq!(repo_not_found.kind(), &ErrorKind::RepositoryNotFound);
    }

    // Test Event/Subscription Errors
    #[test]
    fn test_event_subscription_errors() {
        let event = NitriteError::new("Event error", ErrorKind::EventError);
        assert_eq!(event.kind(), &ErrorKind::EventError);
    }

    // Test Plugin Errors
    #[test]
    fn test_plugin_errors() {
        let plugin = NitriteError::new("Plugin error", ErrorKind::PluginError);
        assert_eq!(plugin.kind(), &ErrorKind::PluginError);

        let load_failed = NitriteError::new("Plugin load failed", ErrorKind::PluginLoadFailed);
        assert_eq!(load_failed.kind(), &ErrorKind::PluginLoadFailed);
    }

    // Test Backend/Store Errors
    #[test]
    fn test_backend_store_errors() {
        let backend = NitriteError::new("Backend error", ErrorKind::BackendError);
        assert_eq!(backend.kind(), &ErrorKind::BackendError);

        let not_init = NitriteError::new("Store not initialized", ErrorKind::StoreNotInitialized);
        assert_eq!(not_init.kind(), &ErrorKind::StoreNotInitialized);

        let closed = NitriteError::new("Store already closed", ErrorKind::StoreAlreadyClosed);
        assert_eq!(closed.kind(), &ErrorKind::StoreAlreadyClosed);
    }

    // Test Internal and Unknown Errors
    #[test]
    fn test_internal_errors() {
        let internal = NitriteError::new("Internal error", ErrorKind::InternalError);
        assert_eq!(internal.kind(), &ErrorKind::InternalError);
    }

    // Test Extension Errors - for external crates to plug in their own error types
    #[test]
    fn test_extension_errors() {
        // Extensions can use the Extension variant with their own name
        let spatial_ext = NitriteError::new("Spatial index error", ErrorKind::Extension("spatial".to_string()));
        assert_eq!(spatial_ext.kind(), &ErrorKind::Extension("spatial".to_string()));
        
        let fulltext_ext = NitriteError::new("Full-text search error", ErrorKind::Extension("FullText".to_string()));
        assert_eq!(fulltext_ext.kind(), &ErrorKind::Extension("FullText".to_string()));
        
        // Different extensions have different error kinds
        assert_ne!(spatial_ext.kind(), fulltext_ext.kind());
        
        // Display should show the extension name
        let display = format!("{}", ErrorKind::Extension("MyExtension".to_string()));
        assert_eq!(display, "MyExtension error");
    }

    // Test error hierarchy and chaining
    #[test]
    fn test_error_chain_with_different_kinds() {
        let root_cause = NitriteError::new("File not found", ErrorKind::FileNotFound);
        let mid_level = NitriteError::new_with_cause(
            "Failed to read store",
            ErrorKind::IOError,
            root_cause,
        );
        let top_level = NitriteError::new_with_cause(
            "Cannot initialize database",
            ErrorKind::BackendError,
            mid_level,
        );

        assert_eq!(top_level.kind(), &ErrorKind::BackendError);
        assert!(top_level.cause().is_some());

        if let Some(cause_box) = top_level.cause() {
            assert_eq!(cause_box.kind(), &ErrorKind::IOError);
        }
    }

    // Test error comparison for all error kinds
    #[test]
    fn test_error_kind_equality() {
        let error1 = NitriteError::new("Error 1", ErrorKind::IndexNotFound);
        let error2 = NitriteError::new("Error 2", ErrorKind::IndexNotFound);
        let error3 = NitriteError::new("Error 3", ErrorKind::IndexAlreadyExists);

        assert_eq!(error1.kind(), error2.kind());
        assert_ne!(error1.kind(), error3.kind());
    }

    // Test error message preservation across different error kinds
    #[test]
    fn test_error_message_preservation() {
        let messages = vec![
            ("Filter error message", ErrorKind::FilterError),
            ("Index not found message", ErrorKind::IndexNotFound),
            ("Disk full message", ErrorKind::DiskFull),
            ("Object mapping error message", ErrorKind::ObjectMappingError),
            ("Security error message", ErrorKind::SecurityError),
        ];

        for (msg, kind) in &messages {
            let error = NitriteError::new(msg, kind.clone());
            assert_eq!(error.message(), *msg);
            assert_eq!(error.kind(), kind);
        }
    }

    // Test From<std::io::Error>
    #[test]
    fn test_from_io_error_not_found() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let nitrite_err: NitriteError = io_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::FileNotFound);
        assert!(nitrite_err.message().contains("IO error"));
    }

    #[test]
    fn test_from_io_error_permission_denied() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let nitrite_err: NitriteError = io_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::PermissionDenied);
        assert!(nitrite_err.message().contains("IO error"));
    }

    #[test]
    fn test_from_io_error_other() {
        let io_err = std::io::Error::other("unknown io error");
        let nitrite_err: NitriteError = io_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::IOError);
        assert!(nitrite_err.message().contains("IO error"));
    }

    // Test From<std::string::FromUtf8Error>
    #[test]
    fn test_from_utf8_error() {
        let invalid_utf8 = vec![0xFF, 0xFE];
        let utf8_err = String::from_utf8(invalid_utf8).unwrap_err();
        let nitrite_err: NitriteError = utf8_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::EncodingError);
        assert!(nitrite_err.message().contains("UTF-8"));
    }

    // Test From<std::fmt::Error>
    #[test]
    fn test_from_fmt_error() {
        use std::fmt::Write;
        // Create a write that always fails
        struct FailingWriter;
        
        impl Write for FailingWriter {
            fn write_str(&mut self, _: &str) -> std::fmt::Result {
                Err(std::fmt::Error)
            }
        }
        
        let fmt_err = write!(&mut FailingWriter, "test").unwrap_err();
        let nitrite_err: NitriteError = fmt_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::InternalError);
        assert!(nitrite_err.message().contains("Formatting"));
    }

    // Test From<std::num::ParseIntError>
    #[test]
    fn test_from_parse_int_error() {
        let parse_err = "not_a_number".parse::<i32>().unwrap_err();
        let nitrite_err: NitriteError = parse_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::InvalidDataType);
        assert!(nitrite_err.message().contains("Integer parsing"));
    }

    // Test From<std::num::ParseFloatError>
    #[test]
    fn test_from_parse_float_error() {
        let parse_err = "not_a_float".parse::<f64>().unwrap_err();
        let nitrite_err: NitriteError = parse_err.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::InvalidDataType);
        assert!(nitrite_err.message().contains("Float parsing"));
    }

    // Test From<String>
    #[test]
    fn test_from_string() {
        let msg = String::from("test error message");
        let nitrite_err: NitriteError = msg.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::InternalError);
        assert_eq!(nitrite_err.message(), "test error message");
    }

    // Test From<&str>
    #[test]
    fn test_from_str() {
        let msg = "test error message";
        let nitrite_err: NitriteError = msg.into();
        
        assert_eq!(nitrite_err.kind(), &ErrorKind::InternalError);
        assert_eq!(nitrite_err.message(), "test error message");
    }

    // Test chaining From conversions
    #[test]
    fn test_from_conversion_in_result_chain() {
        fn operation_that_fails_with_io() -> NitriteResult<String> {
            let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
            Err(io_err.into())
        }
        
        let result = operation_that_fails_with_io();
        assert!(result.is_err());
        
        if let Err(err) = result {
            assert_eq!(err.kind(), &ErrorKind::FileNotFound);
        }
    }

    // Test multiple From conversions
    #[test]
    fn test_multiple_from_conversions() {
        let io_err: NitriteError = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied"
        ).into();
        assert_eq!(io_err.kind(), &ErrorKind::PermissionDenied);

        let utf8_err: NitriteError = String::from_utf8(vec![0xFF]).unwrap_err().into();
        assert_eq!(utf8_err.kind(), &ErrorKind::EncodingError);

        let str_err: NitriteError = "string error".into();
        assert_eq!(str_err.kind(), &ErrorKind::InternalError);
    }

    // Test ? operator with From trait
    #[test]
    fn test_question_mark_operator_with_from() {
        fn parse_number_operation() -> NitriteResult<i32> {
            let num: i32 = "12345".parse()?;
            Ok(num)
        }

        let result = parse_number_operation();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 12345);
    }

    #[test]
    fn test_question_mark_operator_with_parse_error() {
        fn parse_number_operation() -> NitriteResult<i32> {
            let num: i32 = "not_a_number".parse()?;
            Ok(num)
        }

        let result = parse_number_operation();
        assert!(result.is_err());
        
        if let Err(err) = result {
            assert_eq!(err.kind(), &ErrorKind::InvalidDataType);
        }
    }
}