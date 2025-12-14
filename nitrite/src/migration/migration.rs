use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::migration::{InstructionSet, InstructionType};
use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

/// Represents a single migration step with its type and arguments.
///
/// # Purpose
/// Encapsulates a discrete database schema migration operation with all necessary context
/// including the operation type, scope (collection/entity), and operation-specific arguments.
///
/// # Characteristics
/// - Immutable after creation (all fields are public and set at construction)
/// - Clone-able for reuse and sharing across execution contexts
/// - Stores arguments in type-erased form via MigrationArguments enum
/// - Scope is optional (database-level operations have None for collection/entity)
///
/// # Fields
/// * `instruction_type` - Type of migration operation (e.g., AddUser, DropCollection)
/// * `collection_name` - Optional: collection affected by this step (None for database-level ops)
/// * `entity_name` - Optional: entity type name for repository operations
/// * `key` - Optional: key field name for repository operations
/// * `arguments` - Type-erased arguments specific to the operation type
#[derive(Clone)]
pub struct MigrationStep {
    pub instruction_type: InstructionType,
    pub collection_name: Option<String>,
    pub entity_name: Option<String>,
    pub key: Option<String>,
    pub arguments: MigrationArguments,
}

impl std::fmt::Debug for MigrationStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationStep")
            .field("instruction_type", &self.instruction_type)
            .field("collection_name", &self.collection_name)
            .field("entity_name", &self.entity_name)
            .field("key", &self.key)
            .field("arguments", &"<arguments>")
            .finish()
    }
}

/// Type-erased arguments container for migration operations.
///
/// # Purpose
/// Provides a flexible mechanism to store arguments of any type (including closures) for
/// migration operations. Uses Arc and Any trait object pattern for thread-safe type erasure.
///
/// # Characteristics
/// - Supports 0 to 4+ arguments via different variants
/// - Thread-safe: all Arc contents implement Send + Sync
/// - Type-erased: arguments must be downcast to correct type when retrieved
/// - Clone-able: arguments are shared via Arc, not copied
///
/// # Variants
/// * `None` - No arguments (e.g., drop_all_indices)
/// * `Single(Arc<T>)` - One argument (e.g., field_name: String)
/// * `Double(Arc<T1>, Arc<T2>)` - Two arguments (e.g., old_name, new_name)
/// * `Triple(Arc<T1>, Arc<T2>, Arc<T3>)` - Three arguments
/// * `Quad(Arc<T1>, Arc<T2>, Arc<T3>, Arc<T4>)` - Four arguments
/// * `Multiple(Vec<Arc<dyn Any>>)` - Variable number of arguments
#[derive(Clone)]
pub enum MigrationArguments {
    None,
    Single(Arc<dyn Any + Send + Sync>),
    Double(Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>),
    Triple(Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>),
    Quad(Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>),
    Multiple(Vec<Arc<dyn Any + Send + Sync>>),
}

impl std::fmt::Debug for MigrationArguments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationArguments::None => write!(f, "None"),
            MigrationArguments::Single(_) => write!(f, "Single(<value>)"),
            MigrationArguments::Double(_, _) => write!(f, "Double(<value>, <value>)"),
            MigrationArguments::Triple(_, _, _) => write!(f, "Triple(<value>, <value>, <value>)"),
            MigrationArguments::Quad(_, _, _, _) => write!(f, "Quad(<value>, <value>, <value>, <value>)"),
            MigrationArguments::Multiple(_) => write!(f, "Multiple(<values>)"),
        }
    }
}

impl MigrationArguments {
    /// Downcasts single argument to specified type.
    ///
    /// # Arguments
    /// * `T` - Target type for downcast
    ///
    /// # Returns
    /// `Ok(T)` - Cloned value if downcast succeeds
    /// `Err(NitriteError)` - If variant is not Single or type mismatch
    pub fn as_single<T: Any + Send + Sync + Clone + 'static>(&self) -> NitriteResult<T> {
        match self {
            MigrationArguments::Single(arg) => {
                arg.downcast_ref::<T>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast single argument",
                        ErrorKind::ValidationError,
                    ))
            }
            _ => Err(NitriteError::new(
                "Expected single argument",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Downcasts two arguments to specified types.
    ///
    /// # Arguments
    /// * `T1` - Target type for first argument
    /// * `T2` - Target type for second argument
    ///
    /// # Returns
    /// `Ok((T1, T2))` - Tuple of cloned values if both downcasts succeed
    /// `Err(NitriteError)` - If variant is not Double or any type mismatch
    pub fn as_double<T1, T2>(&self) -> NitriteResult<(T1, T2)>
    where
        T1: Any + Send + Sync + Clone + 'static,
        T2: Any + Send + Sync + Clone + 'static,
    {
        match self {
            MigrationArguments::Double(arg1, arg2) => {
                let a = arg1.downcast_ref::<T1>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast first argument",
                        ErrorKind::ValidationError,
                    ))?;
                let b = arg2.downcast_ref::<T2>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast second argument",
                        ErrorKind::ValidationError,
                    ))?;
                Ok((a, b))
            }
            _ => Err(NitriteError::new(
                "Expected double arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Downcasts three arguments to specified types.
    ///
    /// # Arguments
    /// * `T1` - Target type for first argument
    /// * `T2` - Target type for second argument
    /// * `T3` - Target type for third argument
    ///
    /// # Returns
    /// `Ok((T1, T2, T3))` - Tuple of cloned values if all downcasts succeed
    /// `Err(NitriteError)` - If variant is not Triple or any type mismatch
    pub fn as_triple<T1, T2, T3>(&self) -> NitriteResult<(T1, T2, T3)>
    where
        T1: Any + Send + Sync + Clone + 'static,
        T2: Any + Send + Sync + Clone + 'static,
        T3: Any + Send + Sync + Clone + 'static,
    {
        match self {
            MigrationArguments::Triple(arg1, arg2, arg3) => {
                let a = arg1.downcast_ref::<T1>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast first argument",
                        ErrorKind::ValidationError,
                    ))?;
                let b = arg2.downcast_ref::<T2>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast second argument",
                        ErrorKind::ValidationError,
                    ))?;
                let c = arg3.downcast_ref::<T3>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast third argument",
                        ErrorKind::ValidationError,
                    ))?;
                Ok((a, b, c))
            }
            _ => Err(NitriteError::new(
                "Expected triple arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Downcasts four arguments to specified types.
    ///
    /// # Arguments
    /// * `T1` - Target type for first argument
    /// * `T2` - Target type for second argument
    /// * `T3` - Target type for third argument
    /// * `T4` - Target type for fourth argument
    ///
    /// # Returns
    /// `Ok((T1, T2, T3, T4))` - Tuple of cloned values if all downcasts succeed
    /// `Err(NitriteError)` - If variant is not Quad or any type mismatch
    pub fn as_quad<T1, T2, T3, T4>(&self) -> NitriteResult<(T1, T2, T3, T4)>
    where
        T1: Any + Send + Sync + Clone + 'static,
        T2: Any + Send + Sync + Clone + 'static,
        T3: Any + Send + Sync + Clone + 'static,
        T4: Any + Send + Sync + Clone + 'static,
    {
        match self {
            MigrationArguments::Quad(arg1, arg2, arg3, arg4) => {
                let a = arg1.downcast_ref::<T1>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast first argument",
                        ErrorKind::ValidationError,
                    ))?;
                let b = arg2.downcast_ref::<T2>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast second argument",
                        ErrorKind::ValidationError,
                    ))?;
                let c = arg3.downcast_ref::<T3>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast third argument",
                        ErrorKind::ValidationError,
                    ))?;
                let d = arg4.downcast_ref::<T4>()
                    .cloned()
                    .ok_or_else(|| NitriteError::new(
                        "Failed to downcast fourth argument",
                        ErrorKind::ValidationError,
                    ))?;
                Ok((a, b, c, d))
            }
            _ => Err(NitriteError::new(
                "Expected quad arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Downcasts multiple arguments to specified type.
    ///
    /// # Arguments
    /// * `T` - Target type for all arguments (must be same for all)
    ///
    /// # Returns
    /// `Ok(Vec<T>)` - Vector of cloned values if variant is Multiple and all downcast
    /// `Err(NitriteError)` - If variant is not Multiple or any type mismatch
    pub fn as_multiple<T: Any + Send + Sync + Clone + 'static>(&self) -> NitriteResult<Vec<T>> {
        match self {
            MigrationArguments::Multiple(args) => {
                args.iter()
                    .map(|arg| {
                        arg.downcast_ref::<T>()
                            .cloned()
                            .ok_or_else(|| NitriteError::new(
                                "Failed to downcast argument in multiple",
                                ErrorKind::ValidationError,
                            ))
                    })
                    .collect()
            }
            _ => Err(NitriteError::new(
                "Expected multiple arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Returns single argument as Arc<dyn Any> without downcast.
    ///
    /// # Returns
    /// `Ok(Arc<dyn Any>)` - Arc reference to underlying value
    /// `Err(NitriteError)` - If variant is not Single
    pub fn as_any_single(&self) -> NitriteResult<Arc<dyn Any + Send + Sync>> {
        match self {
            MigrationArguments::Single(arg) => Ok(Arc::clone(arg)),
            _ => Err(NitriteError::new(
                "Expected single argument",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Returns two arguments as Arc<dyn Any> without downcast.
    ///
    /// # Returns
    /// `Ok((Arc, Arc))` - Tuple of Arc references
    /// `Err(NitriteError)` - If variant is not Double
    pub fn as_any_double(&self) -> NitriteResult<(Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>)> {
        match self {
            MigrationArguments::Double(arg1, arg2) => Ok((Arc::clone(arg1), Arc::clone(arg2))),
            _ => Err(NitriteError::new(
                "Expected double arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Returns three arguments as Arc<dyn Any> without downcast.
    ///
    /// # Returns
    /// `Ok((Arc, Arc, Arc))` - Tuple of Arc references
    /// `Err(NitriteError)` - If variant is not Triple
    pub fn as_any_triple(&self) -> NitriteResult<(Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>, Arc<dyn Any + Send + Sync>)> {
        match self {
            MigrationArguments::Triple(arg1, arg2, arg3) => Ok((Arc::clone(arg1), Arc::clone(arg2), Arc::clone(arg3))),
            _ => Err(NitriteError::new(
                "Expected triple arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Returns multiple arguments as Vec<Arc<dyn Any>> without downcast.
    ///
    /// # Returns
    /// `Ok(Vec<Arc>)` - Vector of Arc references
    /// `Err(NitriteError)` - If variant is not Multiple
    pub fn as_any_multiple(&self) -> NitriteResult<Vec<Arc<dyn Any + Send + Sync>>> {
        match self {
            MigrationArguments::Multiple(args) => Ok(args.iter().map(Arc::clone).collect()),
            _ => Err(NitriteError::new(
                "Expected multiple arguments",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Returns count of arguments for this variant.
    ///
    /// # Returns
    /// Count as usize: 0 for None, 1 for Single, 2 for Double, etc.
    pub fn arg_count(&self) -> usize {
        match self {
            MigrationArguments::None => 0,
            MigrationArguments::Single(_) => 1,
            MigrationArguments::Double(_, _) => 2,
            MigrationArguments::Triple(_, _, _) => 3,
            MigrationArguments::Quad(_, _, _, _) => 4,
            MigrationArguments::Multiple(args) => args.len(),
        }
    }
}

/// Abstract base class for user-defined migrations.
///
/// # Purpose
/// Represents a versioned database schema migration that can be applied to upgrade
/// the database from one schema version to another. Encapsulates migration steps
/// and provides lazy execution semantics.
///
/// # Characteristics
/// - Immutable public API with internal mutable state via Arc<Mutex<>>
/// - Clone-able: clones share the same underlying migration state via Arc
/// - Lazy execution: migration steps are generated on first call to steps()
/// - Idempotent: multiple steps() calls return cached results (execute() called once)
/// - Closure-based: migration logic defined by user-provided function
///
/// # Usage
/// ```ignore
/// let migration = Migration::new(1, 2, |instruction| {
///     instruction.for_database().add_user("admin", "password")
///         .drop_collection("test");
///     instruction.for_collection("users").rename("customers");
///     instruction.for_repository("books", None).delete_field("price");
///     Ok(())
/// });
///
/// // Apply to database
/// let db = Nitrite::builder()
///     .schema_version(2)
///     .add_migration(migration)
///     .open_or_create(Some("admin"), Some("password"))?;
/// ```
#[derive(Debug, Clone)]
pub struct Migration {
    inner: Arc<MigrationInner>,
}

impl Migration {
    /// Creates a new migration for upgrading from one schema version to another.
    ///
    /// # Arguments
    /// * `from_version` - Source schema version
    /// * `to_version` - Target schema version
    /// * `migrate` - Closure that defines migration operations via InstructionSet
    ///
    /// # Returns
    /// Migration instance wrapping the migration logic
    ///
    /// # Behavior
    /// - The migrate closure is not executed immediately
    /// - Execution is deferred until first call to steps() or execute()
    /// - Closure receives immutable InstructionSet reference for building instructions
    pub fn new(from_version: u32, to_version: u32, migrate: impl Fn(&InstructionSet) -> NitriteResult<()> + Send + Sync + 'static) -> Self {
        Migration {
            inner: Arc::new(MigrationInner {
                from_version,
                to_version,
                migration_steps: Mutex::new(VecDeque::new()),
                executed: AtomicBool::new(false),
                migrate: Box::new(migrate),
            }),
        }
    }

    /// Returns the source schema version for this migration.
    ///
    /// # Returns
    /// Schema version as u32
    pub fn from_version(&self) -> u32 {
        self.inner.from_version
    }

    /// Returns the target schema version for this migration.
    ///
    /// # Returns
    /// Schema version as u32
    pub fn to_version(&self) -> u32 {
        self.inner.to_version
    }

    /// Returns all migration steps, triggering lazy execution on first call.
    ///
    /// # Returns
    /// `Ok(Vec<MigrationStep>)` - All migration steps generated by the migration closure
    /// `Err(NitriteError)` - If migration execution fails or state access fails
    ///
    /// # Behavior
    /// - First call executes the migration closure to generate steps
    /// - Subsequent calls return cached steps without re-execution
    /// - Executed flag prevents duplicate execution
    pub fn steps(&self) -> NitriteResult<Vec<MigrationStep>> {
        let executed = self.inner.executed.load(std::sync::atomic::Ordering::SeqCst);
        
        if !executed {
            self.execute()?;
        }

        let steps = self.inner.migration_steps.lock()
            .map_err(|_| NitriteError::new("Failed to acquire lock on migration_steps", ErrorKind::ValidationError))?;
        Ok(steps.iter().cloned().collect())
    }

    /// Executes the migration closure to generate and store migration steps.
    ///
    /// # Returns
    /// `Ok(())` - Migration executed successfully
    /// `Err(NitriteError)` - If migration closure fails or state access fails
    ///
    /// # Behavior
    /// - Creates InstructionSet with empty initial steps
    /// - Calls user-provided migrate closure with InstructionSet
    /// - Collects generated steps from InstructionSet
    /// - Sets executed flag to prevent re-execution
    pub(crate) fn execute(&self) -> NitriteResult<()> {
        let steps = self.get_all_steps()?;
        let instruction_set = InstructionSet::new(steps.into());
        (self.inner.migrate)(&instruction_set)?;
        
        // Replace migration steps with the steps from InstructionSet (which may have been modified by the closure)
        let new_steps = instruction_set.get_steps()?;
        let mut migration_steps = self.inner.migration_steps.lock()
            .map_err(|_| NitriteError::new("Failed to acquire lock on migration_steps", ErrorKind::ValidationError))?;
        migration_steps.clear();
        for step in new_steps {
            migration_steps.push_back(step);
        }
        
        self.inner.executed.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())        
    }

    /// Adds a migration step to the internal step queue.
    ///
    /// # Arguments
    /// * `step` - MigrationStep to add
    ///
    /// # Returns
    /// `Ok(())` - Step added successfully
    /// `Err(NitriteError)` - If state access fails
    pub(crate) fn add_step(&self, step: MigrationStep) -> NitriteResult<()> {
        let mut steps = self.inner.migration_steps.lock()
            .map_err(|_| NitriteError::new("Failed to acquire lock on migration_steps", ErrorKind::ValidationError))?;
        steps.push_back(step);
        Ok(())
    }

    /// Returns all currently stored migration steps without execution.
    ///
    /// # Returns
    /// `Ok(Vec<MigrationStep>)` - All accumulated steps
    /// `Err(NitriteError)` - If state access fails
    pub(crate) fn get_all_steps(&self) -> NitriteResult<Vec<MigrationStep>> {
        let steps = self.inner.migration_steps.lock()
            .map_err(|_| NitriteError::new("Failed to acquire lock on migration_steps", ErrorKind::ValidationError))?;
        Ok(steps.iter().cloned().collect())
    }
}

/// Internal implementation details for Migration.
///
/// # Purpose
/// Stores the actual state and closure for a migration. Wrapped in Arc for shared ownership
/// and thread-safe access via Mutex/AtomicBool synchronization primitives.
///
/// # Characteristics
/// - Not directly exposed in public API (wrapped by Migration)
/// - Thread-safe: mutation through Mutex<VecDeque> and AtomicBool
/// - Stores user-provided migrate closure as Box trait object
/// - Immutable version fields for validation
pub struct MigrationInner {
    from_version: u32,
    to_version: u32,
    migration_steps: Mutex<VecDeque<MigrationStep>>,
    executed: AtomicBool,
    migrate: Box<dyn Fn(&InstructionSet) -> NitriteResult<()> + Send + Sync>,
}

impl std::fmt::Debug for MigrationInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationInner")
            .field("from_version", &self.from_version)
            .field("to_version", &self.to_version)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== MigrationStep Tests ====================

    #[test]
    fn test_migration_step_creation() {
        // Test MigrationStep creation with basic fields
        let step = MigrationStep {
            instruction_type: InstructionType::AddUser,
            collection_name: Some("users".to_string()),
            entity_name: Some("User".to_string()),
            key: Some("id".to_string()),
            arguments: MigrationArguments::None,
        };

        assert_eq!(step.instruction_type, InstructionType::AddUser);
        assert_eq!(step.collection_name, Some("users".to_string()));
        assert_eq!(step.entity_name, Some("User".to_string()));
        assert_eq!(step.key, Some("id".to_string()));
    }

    #[test]
    fn test_migration_step_with_no_optional_fields() {
        // Test MigrationStep with all None optional fields
        let step = MigrationStep {
            instruction_type: InstructionType::CustomInstruction,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };

        assert_eq!(step.collection_name, None);
        assert_eq!(step.entity_name, None);
        assert_eq!(step.key, None);
    }

    #[test]
    fn test_migration_step_clone() {
        // Test that MigrationStep can be cloned
        let step = MigrationStep {
            instruction_type: InstructionType::DropCollection,
            collection_name: Some("test".to_string()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };

        let cloned = step.clone();
        assert_eq!(cloned.instruction_type, step.instruction_type);
        assert_eq!(cloned.collection_name, step.collection_name);
    }

    #[test]
    fn test_migration_step_debug_format() {
        // Test Debug implementation for MigrationStep
        let step = MigrationStep {
            instruction_type: InstructionType::AddUser,
            collection_name: Some("col".to_string()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };

        let debug_str = format!("{:?}", step);
        assert!(debug_str.contains("MigrationStep"));
        assert!(debug_str.contains("AddUser"));
    }

    // ==================== MigrationArguments::None Tests ====================

    #[test]
    fn test_migration_arguments_none() {
        // Test None variant creation and arg_count
        let args = MigrationArguments::None;
        assert_eq!(args.arg_count(), 0);
    }

    #[test]
    fn test_migration_arguments_none_debug() {
        // Test Debug format for None
        let args = MigrationArguments::None;
        let debug_str = format!("{:?}", args);
        assert_eq!(debug_str, "None");
    }

    #[test]
    fn test_migration_arguments_none_as_single_error() {
        // Test as_single() returns error for None
        let args = MigrationArguments::None;
        let result: NitriteResult<String> = args.as_single();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_double_error() {
        // Test as_double() returns error for None
        let args = MigrationArguments::None;
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_triple_error() {
        // Test as_triple() returns error for None
        let args = MigrationArguments::None;
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_quad_error() {
        // Test as_quad() returns error for None
        let args = MigrationArguments::None;
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_multiple_error() {
        // Test as_multiple() returns error for None
        let args = MigrationArguments::None;
        let result: NitriteResult<Vec<String>> = args.as_multiple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_any_single_error() {
        // Test as_any_single() returns error for None
        let args = MigrationArguments::None;
        let result = args.as_any_single();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_any_double_error() {
        // Test as_any_double() returns error for None
        let args = MigrationArguments::None;
        let result = args.as_any_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_any_triple_error() {
        // Test as_any_triple() returns error for None
        let args = MigrationArguments::None;
        let result = args.as_any_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_none_as_any_multiple_error() {
        // Test as_any_multiple() returns error for None
        let args = MigrationArguments::None;
        let result = args.as_any_multiple();
        assert!(result.is_err());
    }

    // ==================== MigrationArguments::Single Tests ====================

    #[test]
    fn test_migration_arguments_single_creation() {
        // Test Single variant creation
        let value = Arc::new("test" as &str);
        let args = MigrationArguments::Single(value);
        assert_eq!(args.arg_count(), 1);
    }

    #[test]
    fn test_migration_arguments_single_as_single_success() {
        // Test successful as_single() for Single variant
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value);
        let result: NitriteResult<String> = args.as_single();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test".to_string());
    }

    #[test]
    fn test_migration_arguments_single_as_single_type_mismatch() {
        // Test as_single() with wrong type
        let value = Arc::new(42i32);
        let args = MigrationArguments::Single(value);
        let result: NitriteResult<String> = args.as_single();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_single_as_double_error() {
        // Test as_double() returns error for Single
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value);
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_single_debug() {
        // Test Debug format for Single
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value);
        let debug_str = format!("{:?}", args);
        assert_eq!(debug_str, "Single(<value>)");
    }

    #[test]
    fn test_migration_arguments_single_as_any_single_success() {
        // Test successful as_any_single()
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value.clone());
        let result = args.as_any_single();
        assert!(result.is_ok());
    }

    #[test]
    fn test_migration_arguments_single_clone() {
        // Test clone for Single variant
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value);
        let cloned = args.clone();
        assert_eq!(cloned.arg_count(), 1);
    }

    // ==================== MigrationArguments::Double Tests ====================

    #[test]
    fn test_migration_arguments_double_creation() {
        // Test Double variant creation
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1, arg2);
        assert_eq!(args.arg_count(), 2);
    }

    #[test]
    fn test_migration_arguments_double_as_double_success() {
        // Test successful as_double()
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1, arg2);
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_ok());
        let (a, b) = result.unwrap();
        assert_eq!(a, "first".to_string());
        assert_eq!(b, "second".to_string());
    }

    #[test]
    fn test_migration_arguments_double_as_double_first_type_mismatch() {
        // Test as_double() with first argument wrong type
        let arg1 = Arc::new(42i32);
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1, arg2);
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_double_as_double_second_type_mismatch() {
        // Test as_double() with second argument wrong type
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new(42i32);
        let args = MigrationArguments::Double(arg1, arg2);
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_double_as_triple_error() {
        // Test as_triple() returns error for Double
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1, arg2);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_double_debug() {
        // Test Debug format for Double
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1, arg2);
        let debug_str = format!("{:?}", args);
        assert_eq!(debug_str, "Double(<value>, <value>)");
    }

    #[test]
    fn test_migration_arguments_double_as_any_double_success() {
        // Test successful as_any_double()
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1.clone(), arg2.clone());
        let result = args.as_any_double();
        assert!(result.is_ok());
    }

    // ==================== MigrationArguments::Triple Tests ====================

    #[test]
    fn test_migration_arguments_triple_creation() {
        // Test Triple variant creation
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        assert_eq!(args.arg_count(), 3);
    }

    #[test]
    fn test_migration_arguments_triple_as_triple_success() {
        // Test successful as_triple()
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_ok());
        let (a, b, c) = result.unwrap();
        assert_eq!(a, "first".to_string());
        assert_eq!(b, "second".to_string());
        assert_eq!(c, "third".to_string());
    }

    #[test]
    fn test_migration_arguments_triple_as_triple_first_type_mismatch() {
        // Test as_triple() with first argument wrong type
        let arg1 = Arc::new(42i32);
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_triple_as_triple_second_type_mismatch() {
        // Test as_triple() with second argument wrong type
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new(42i32);
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_triple_as_triple_third_type_mismatch() {
        // Test as_triple() with third argument wrong type
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new(42i32);
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_triple_as_quad_error() {
        // Test as_quad() returns error for Triple
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_triple_debug() {
        // Test Debug format for Triple
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let debug_str = format!("{:?}", args);
        assert_eq!(debug_str, "Triple(<value>, <value>, <value>)");
    }

    #[test]
    fn test_migration_arguments_triple_as_any_triple_success() {
        // Test successful as_any_triple()
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1.clone(), arg2.clone(), arg3.clone());
        let result = args.as_any_triple();
        assert!(result.is_ok());
    }

    // ==================== MigrationArguments::Quad Tests ====================

    #[test]
    fn test_migration_arguments_quad_creation() {
        // Test Quad variant creation
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        assert_eq!(args.arg_count(), 4);
    }

    #[test]
    fn test_migration_arguments_quad_as_quad_success() {
        // Test successful as_quad()
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_ok());
        let (a, b, c, d) = result.unwrap();
        assert_eq!(a, "first".to_string());
        assert_eq!(b, "second".to_string());
        assert_eq!(c, "third".to_string());
        assert_eq!(d, "fourth".to_string());
    }

    #[test]
    fn test_migration_arguments_quad_as_quad_first_type_mismatch() {
        // Test as_quad() with first argument wrong type
        let arg1 = Arc::new(42i32);
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_as_quad_second_type_mismatch() {
        // Test as_quad() with second argument wrong type
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new(42i32);
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_as_quad_third_type_mismatch() {
        // Test as_quad() with third argument wrong type
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new(42i32);
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_as_quad_fourth_type_mismatch() {
        // Test as_quad() with fourth argument wrong type
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new(42i32);
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_as_multiple_error() {
        // Test as_multiple() returns error for Quad
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<Vec<String>> = args.as_multiple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_debug() {
        // Test Debug format for Quad
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let debug_str = format!("{:?}", args);
        assert_eq!(debug_str, "Quad(<value>, <value>, <value>, <value>)");
    }

    // ==================== MigrationArguments::Multiple Tests ====================

    #[test]
    fn test_migration_arguments_multiple_empty() {
        // Test Multiple variant with empty vector
        let args = MigrationArguments::Multiple(vec![]);
        assert_eq!(args.arg_count(), 0);
    }

    #[test]
    fn test_migration_arguments_multiple_single_item() {
        // Test Multiple variant with one item
        let arg = Arc::new("value".to_string());
        let args = MigrationArguments::Multiple(vec![arg]);
        assert_eq!(args.arg_count(), 1);
    }

    #[test]
    fn test_migration_arguments_multiple_multiple_items() {
        // Test Multiple variant with multiple items
        let args_vec = vec![
            Arc::new("first".to_string()) as Arc<dyn Any + Send + Sync>,
            Arc::new("second".to_string()) as Arc<dyn Any + Send + Sync>,
            Arc::new("third".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        assert_eq!(args.arg_count(), 3);
    }

    #[test]
    fn test_migration_arguments_multiple_as_multiple_success() {
        // Test successful as_multiple()
        let args_vec = vec![
            Arc::new("first".to_string()) as Arc<dyn Any + Send + Sync>,
            Arc::new("second".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let result: NitriteResult<Vec<String>> = args.as_multiple();
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], "first".to_string());
        assert_eq!(values[1], "second".to_string());
    }

    #[test]
    fn test_migration_arguments_multiple_as_multiple_type_mismatch() {
        // Test as_multiple() with wrong type
        let args_vec = vec![
            Arc::new(42i32) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let result: NitriteResult<Vec<String>> = args.as_multiple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_multiple_as_single_error() {
        // Test as_single() returns error for Multiple
        let args_vec = vec![
            Arc::new("value".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let result: NitriteResult<String> = args.as_single();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_multiple_debug() {
        // Test Debug format for Multiple
        let args_vec = vec![
            Arc::new("first".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let debug_str = format!("{:?}", args);
        assert_eq!(debug_str, "Multiple(<values>)");
    }

    #[test]
    fn test_migration_arguments_multiple_as_any_multiple_success() {
        // Test successful as_any_multiple()
        let args_vec = vec![
            Arc::new("first".to_string()) as Arc<dyn Any + Send + Sync>,
            Arc::new("second".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let result = args.as_any_multiple();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    // ==================== Migration Tests ====================

    #[test]
    fn test_migration_new() {
        // Test Migration creation with new()
        let migration = Migration::new(1, 2, |_| Ok(()));
        assert_eq!(migration.from_version(), 1);
        assert_eq!(migration.to_version(), 2);
    }

    #[test]
    fn test_migration_from_version() {
        // Test from_version() getter
        let migration = Migration::new(5, 10, |_| Ok(()));
        assert_eq!(migration.from_version(), 5);
    }

    #[test]
    fn test_migration_to_version() {
        // Test to_version() getter
        let migration = Migration::new(3, 7, |_| Ok(()));
        assert_eq!(migration.to_version(), 7);
    }

    #[test]
    fn test_migration_add_step() {
        // Test add_step() adds steps to migration
        let migration = Migration::new(1, 2, |_| Ok(()));
        
        let step = MigrationStep {
            instruction_type: InstructionType::AddUser,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };

        let result = migration.add_step(step);
        assert!(result.is_ok());
    }

    #[test]
    fn test_migration_get_all_steps() {
        // Test get_all_steps() returns steps
        let migration = Migration::new(1, 2, |_| Ok(()));
        
        let step = MigrationStep {
            instruction_type: InstructionType::ChangePassword,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };

        migration.add_step(step).unwrap();
        let steps = migration.get_all_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::ChangePassword);
    }

    #[test]
    fn test_migration_get_all_steps_multiple() {
        // Test get_all_steps() with multiple steps
        let migration = Migration::new(1, 2, |_| Ok(()));
        
        for i in 0..3 {
            let step = MigrationStep {
                instruction_type: if i == 0 { InstructionType::AddUser } else { InstructionType::ChangePassword },
                collection_name: None,
                entity_name: None,
                key: None,
                arguments: MigrationArguments::None,
            };
            migration.add_step(step).unwrap();
        }

        let steps = migration.get_all_steps().unwrap();
        assert_eq!(steps.len(), 3);
    }

    #[test]
    fn test_migration_execute_success() {
        // Test execute() successfully runs migration
        let migration = Migration::new(1, 2, |_| Ok(()));
        let result = migration.execute();
        assert!(result.is_ok());
    }

    #[test]
    fn test_migration_execute_with_error() {
        // Test execute() propagates error from migration function
        let migration = Migration::new(1, 2, |_| {
            Err(NitriteError::new("Test error", ErrorKind::ValidationError))
        });
        let result = migration.execute();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_steps_lazy_execution() {
        // Test steps() triggers lazy execution on first call
        let executed = Arc::new(Mutex::new(false));
        let executed_clone = executed.clone();

        let migration = Migration::new(1, 2, move |_| {
            *executed_clone.lock().unwrap() = true;
            Ok(())
        });

        let result = migration.steps();
        assert!(result.is_ok());
        assert!(*executed.lock().unwrap());
    }

    #[test]
    fn test_migration_steps_cached() {
        // Test steps() doesn't re-execute on subsequent calls
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let migration = Migration::new(1, 2, move |_| {
            *call_count_clone.lock().unwrap() += 1;
            Ok(())
        });

        let _ = migration.steps();
        let _ = migration.steps();
        
        // Should only be called once due to caching
        assert_eq!(*call_count.lock().unwrap(), 1);
    }

    #[test]
    fn test_migration_steps_with_steps() {
        // Test steps() returns added steps
        let migration = Migration::new(1, 2, |_| Ok(()));
        
        let step = MigrationStep {
            instruction_type: InstructionType::DropCollection,
            collection_name: Some("users".to_string()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };

        migration.add_step(step).unwrap();
        let steps = migration.steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropCollection);
        assert_eq!(steps[0].collection_name, Some("users".to_string()));
    }

    #[test]
    fn test_migration_steps_returns_error() {
        // Test steps() returns error from execute()
        let migration = Migration::new(1, 2, |_| {
            Err(NitriteError::new("Exec error", ErrorKind::ValidationError))
        });
        let result = migration.steps();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_clone() {
        // Test Migration can be cloned (via Arc)
        let migration = Migration::new(1, 2, |_| Ok(()));
        let cloned = Migration {
            inner: Arc::clone(&migration.inner),
        };
        
        assert_eq!(cloned.from_version(), 1);
        assert_eq!(cloned.to_version(), 2);
    }

    #[test]
    fn test_migration_debug() {
        // Test Debug implementation for Migration
        let migration = Migration::new(1, 2, |_| Ok(()));
        let debug_str = format!("{:?}", migration);
        assert!(debug_str.contains("Migration"));
    }

    #[test]
    fn test_migration_inner_debug() {
        // Test Debug implementation for MigrationInner
        let migration = Migration::new(3, 4, |_| Ok(()));
        let debug_str = format!("{:?}", migration);
        assert!(debug_str.contains("3") || debug_str.contains("4"));
    }

    // ==================== Mixed Type Tests ====================

    #[test]
    fn test_migration_arguments_single_with_integer() {
        // Test Single variant with integer type
        let value = Arc::new(42i32);
        let args = MigrationArguments::Single(value);
        let result: NitriteResult<i32> = args.as_single();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_migration_arguments_double_with_mixed_types() {
        // Test Double variant with different types
        let arg1 = Arc::new("text".to_string());
        let arg2 = Arc::new(100i32);
        let args = MigrationArguments::Double(arg1, arg2);
        let result: NitriteResult<(String, i32)> = args.as_double();
        assert!(result.is_ok());
        let (text, num) = result.unwrap();
        assert_eq!(text, "text".to_string());
        assert_eq!(num, 100);
    }

    #[test]
    fn test_migration_arguments_clone() {
        // Test clone for all variants
        let none_args = MigrationArguments::None;
        let cloned_none = none_args.clone();
        assert_eq!(cloned_none.arg_count(), 0);

        let single_args = MigrationArguments::Single(Arc::new("test".to_string()));
        let cloned_single = single_args.clone();
        assert_eq!(cloned_single.arg_count(), 1);
    }

    #[test]
    fn test_migration_step_with_all_fields() {
        // Test MigrationStep with all fields populated
        let step = MigrationStep {
            instruction_type: InstructionType::CustomInstruction,
            collection_name: Some("col".to_string()),
            entity_name: Some("Entity".to_string()),
            key: Some("id".to_string()),
            arguments: MigrationArguments::Double(
                Arc::new("arg1".to_string()),
                Arc::new("arg2".to_string()),
            ),
        };

        assert_eq!(step.collection_name, Some("col".to_string()));
        assert_eq!(step.entity_name, Some("Entity".to_string()));
        assert_eq!(step.key, Some("id".to_string()));
        assert_eq!(step.arguments.arg_count(), 2);
    }

    #[test]
    fn test_migration_arguments_single_as_triple_error() {
        // Test as_triple() returns error for Single
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_single_as_quad_error() {
        // Test as_quad() returns error for Single
        let value = Arc::new("test".to_string());
        let args = MigrationArguments::Single(value);
        let result: NitriteResult<(String, String, String, String)> = args.as_quad();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_double_as_single_error() {
        // Test as_single() returns error for Double
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let args = MigrationArguments::Double(arg1, arg2);
        let result: NitriteResult<String> = args.as_single();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_triple_as_double_error() {
        // Test as_double() returns error for Triple
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let args = MigrationArguments::Triple(arg1, arg2, arg3);
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_as_single_error() {
        // Test as_single() returns error for Quad
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<String> = args.as_single();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_quad_as_triple_error() {
        // Test as_triple() returns error for Quad
        let arg1 = Arc::new("first".to_string());
        let arg2 = Arc::new("second".to_string());
        let arg3 = Arc::new("third".to_string());
        let arg4 = Arc::new("fourth".to_string());
        let args = MigrationArguments::Quad(arg1, arg2, arg3, arg4);
        let result: NitriteResult<(String, String, String)> = args.as_triple();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_multiple_as_double_error() {
        // Test as_double() returns error for Multiple
        let args_vec = vec![
            Arc::new("first".to_string()) as Arc<dyn Any + Send + Sync>,
            Arc::new("second".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let result: NitriteResult<(String, String)> = args.as_double();
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_arguments_multiple_partial_match() {
        // Test as_multiple() with partial type match (stops at first mismatch)
        let args_vec = vec![
            Arc::new("first".to_string()) as Arc<dyn Any + Send + Sync>,
            Arc::new(42i32) as Arc<dyn Any + Send + Sync>,
            Arc::new("third".to_string()) as Arc<dyn Any + Send + Sync>,
        ];
        let args = MigrationArguments::Multiple(args_vec);
        let result: NitriteResult<Vec<String>> = args.as_multiple();
        assert!(result.is_err());
    }
}
