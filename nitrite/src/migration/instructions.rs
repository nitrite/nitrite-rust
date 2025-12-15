use super::migration::{MigrationArguments, MigrationStep};
use crate::{
    collection::Document,
    common::Value,
    errors::{ErrorKind, NitriteError, NitriteResult},
    nitrite::Nitrite,
};
use std::{
    any::Any
    ,
    sync::{Arc, Mutex},
};

/// Unified wrapper enum for all migration function closures that can be type-erased and downcasted.
///
/// MigrationFn consolidates three types of migration functions (custom instructions, value converters,
/// and field generators) into a single enum that can be stored in MigrationArguments and called
/// appropriately based on the instruction type.
///
/// # Purpose
/// Enables type-safe storage and execution of different function types in a unified migration system
/// without requiring complex lifetime or generic parameters.
///
/// # Characteristics
/// - **Type-erased**: Each variant wraps a function in Arc for shared ownership
/// - **Thread-safe**: All closures require Send + Sync
/// - **Cloneable**: Arc enables inexpensive cloning across migration steps
/// - **Fallible**: All functions return NitriteResult for error handling
///
/// # Variants
/// - **CustomInstruction**: Takes Nitrite, performs operations, returns ()
/// - **ValueConverter**: Takes Value, transforms it, returns transformed Value
/// - **FieldGenerator**: Takes Document, generates value from document, returns Value
///
/// # Usage
///
/// Create from function and execute later:
/// ```ignore
/// let fn_wrapper = MigrationFn::value_converter(|val| Ok(val));
/// let result = fn_wrapper.call_value_converter(Value::from(42))?;
/// ```
///
/// Used internally by builders:
/// ```ignore
/// instruction.for_repository("User", None)
///     .change_data_type("age", |val| {
///         // Convert string to integer
///         Ok(Value::from(42))
///     });
/// ```
#[derive(Clone)]
pub enum MigrationFn {
    /// Custom instruction: `Fn(Nitrite) -> NitriteResult<()>`
    CustomInstruction(Arc<dyn Fn(Nitrite) -> NitriteResult<()> + Send + Sync + 'static>),
    /// Value converter: `Fn([Value]) -> NitriteResult<[Value]>`
    ValueConverter(Arc<dyn Fn(Value) -> NitriteResult<Value> + Send + Sync + 'static>),
    /// Field generator: `Fn([Document]) -> NitriteResult<[Value]>`
    FieldGenerator(Arc<dyn Fn(Document) -> NitriteResult<Value> + Send + Sync + 'static>),
}

impl MigrationFn {
    /// Create a custom instruction function wrapper
    ///
    /// # Arguments
    /// * `f` - Closure taking Nitrite and performing database operations
    ///
    /// # Returns
    /// MigrationFn::CustomInstruction variant wrapping the closure
    ///
    /// # Behavior
    /// Wraps the closure in Arc for shared ownership and thread-safe access.
    /// Used for database-level operations in migrations.
    pub fn custom_instruction<F>(f: F) -> Self
    where
        F: Fn(Nitrite) -> NitriteResult<()> + Send + Sync + 'static,
    {
        MigrationFn::CustomInstruction(Arc::new(f))
    }

    /// Create a value converter function wrapper
    ///
    /// # Arguments
    /// * `f` - Closure taking a Value and returning a transformed Value
    ///
    /// # Returns
    /// MigrationFn::ValueConverter variant wrapping the closure
    ///
    /// # Behavior
    /// Wraps the closure in Arc for shared ownership and thread-safe access.
    /// Used for data type conversions during field migration.
    /// Example: Converting string field to integer field.
    pub fn value_converter<F>(f: F) -> Self
    where
        F: Fn(Value) -> NitriteResult<Value> + Send + Sync + 'static,
    {
        MigrationFn::ValueConverter(Arc::new(f))
    }

    /// Create a field generator function wrapper
    ///
    /// # Arguments
    /// * `f` - Closure taking a Document and returning a generated Value
    ///
    /// # Returns
    /// MigrationFn::FieldGenerator variant wrapping the closure
    ///
    /// # Behavior
    /// Wraps the closure in Arc for shared ownership and thread-safe access.
    /// Used to generate values for newly added fields.
    /// Example: Creating a timestamp field populated from document data.
    pub fn field_generator<F>(f: F) -> Self
    where
        F: Fn(Document) -> NitriteResult<Value> + Send + Sync + 'static,
    {
        MigrationFn::FieldGenerator(Arc::new(f))
    }

    /// Call as custom instruction, returns error if wrong variant
    ///
    /// # Arguments
    /// * `nitrite` - Database instance to perform operations on
    ///
    /// # Returns
    /// Ok(()) on success, Err if wrong MigrationFn variant
    ///
    /// # Errors
    /// Returns ValidationError if called on non-CustomInstruction variant
    pub fn call_custom_instruction(&self, nitrite: Nitrite) -> NitriteResult<()> {
        match self {
            MigrationFn::CustomInstruction(f) => f(nitrite),
            _ => Err(NitriteError::new(
                "Expected CustomInstruction function",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Call as value converter, returns error if wrong variant
    ///
    /// # Arguments
    /// * `value` - Value to convert/transform
    ///
    /// # Returns
    /// Converted Value on success, Err if wrong MigrationFn variant
    ///
    /// # Errors
    /// Returns ValidationError if called on non-ValueConverter variant
    pub fn call_value_converter(&self, value: Value) -> NitriteResult<Value> {
        match self {
            MigrationFn::ValueConverter(f) => f(value),
            _ => Err(NitriteError::new(
                "Expected ValueConverter function",
                ErrorKind::ValidationError,
            )),
        }
    }

    /// Call as field generator, returns error if wrong variant
    ///
    /// # Arguments
    /// * `doc` - Document to generate field value from
    ///
    /// # Returns
    /// Generated Value on success, Err if wrong MigrationFn variant
    ///
    /// # Errors
    /// Returns ValidationError if called on non-FieldGenerator variant
    pub fn call_field_generator(&self, doc: Document) -> NitriteResult<Value> {
        match self {
            MigrationFn::FieldGenerator(f) => f(doc),
            _ => Err(NitriteError::new(
                "Expected FieldGenerator function",
                ErrorKind::ValidationError,
            )),
        }
    }
}

/// Default implementation of InstructionSet
///
/// InstructionSet serves as the root builder for creating migration instructions.
/// It provides factory methods to create builders for database, collection, and repository
/// level instructions, all sharing the same underlying migration steps list.
///
/// # Purpose
/// InstructionSet coordinates all migration instructions and tracks them in a shared,
/// thread-safe list. It provides convenient entry points for building different types
/// of migration instructions.
///
/// # Characteristics
/// - **Shared state**: Uses Arc<Mutex<Vec<MigrationStep>>> for thread-safe shared access
/// - **Factory**: Provides builder factory methods for different instruction scopes
/// - **Cloneable**: Can be cloned to share the same instruction list across builders
/// - **Thread-safe**: All access protected by Mutex
///
/// # Usage
///
/// Create instruction set and build migrations:
/// ```ignore
/// let instruction = InstructionSet::new(vec![]);
/// instruction.for_database().add_user("admin", "password");
/// instruction.for_collection("users").rename("customers");
/// instruction.for_repository("books", None).delete_field("price");
/// ```
#[derive(Clone)]
pub struct InstructionSet {
    migration_steps: Arc<Mutex<Vec<MigrationStep>>>,
}

impl InstructionSet {
    /// Creates a new InstructionSet with optional initial steps.
    ///
    /// # Arguments
    /// * `steps` - Initial migration steps (usually empty)
    ///
    /// # Returns
    /// New InstructionSet with shared mutable step list
    ///
    /// # Usage
    /// ```ignore
    /// let instruction = InstructionSet::new(vec![]);
    /// ```
    pub fn new(steps: Vec<MigrationStep>) -> Self {
        InstructionSet {
            migration_steps: Arc::new(Mutex::new(steps)),
        }
    }

    /// Retrieves all accumulated migration steps.
    ///
    /// # Returns
    /// Vec of all MigrationSteps added via builders
    ///
    /// # Errors
    /// Returns error if mutex is poisoned
    pub fn get_steps(&self) -> NitriteResult<Vec<MigrationStep>> {
        let steps = self.migration_steps.lock().unwrap();
        Ok(steps.clone())
    }

    /// Creates a database-level instruction builder.
    ///
    /// # Returns
    /// DatabaseInstructionBuilder for adding database operations
    ///
    /// # Usage
    /// Add user or manage collections:
    /// ```ignore
    /// instruction.for_database()
    ///     .add_user("admin", "password")
    ///     .drop_collection("temp_data");
    /// ```
    pub fn for_database(&self) -> DatabaseInstructionBuilder {
        DatabaseInstructionBuilder::new(self.migration_steps.clone())
    }

    /// Creates a collection-level instruction builder.
    ///
    /// # Arguments
    /// * `name` - Name of the collection to operate on
    ///
    /// # Returns
    /// CollectionInstructionBuilder for adding collection operations
    ///
    /// # Usage
    /// Rename collection or modify fields:
    /// ```ignore
    /// instruction.for_collection("users")
    ///     .rename("customers")
    ///     .add_field("version", Some(Value::from(1)), None);
    /// ```
    pub fn for_collection(&self, name: &str) -> CollectionInstructionBuilder {
        CollectionInstructionBuilder::new(name.to_string(), self.migration_steps.clone())
    }

    /// Creates a repository-level instruction builder.
    ///
    /// # Arguments
    /// * `entity_name` - Entity type name (e.g., "User", "Product")
    /// * `key` - Optional key field name for repository
    ///
    /// # Returns
    /// RepositoryInstructionBuilder for adding repository operations
    ///
    /// # Usage
    /// Modify repository structure:
    /// ```ignore
    /// instruction.for_repository("User", Some("id"))
    ///     .change_data_type("age", |val| Ok(val))
    ///     .change_id_field(&["id"], &["userId"]);
    /// ```
    pub fn for_repository(
        &self,
        entity_name: &str,
        key: Option<&str>,
    ) -> RepositoryInstructionBuilder {
        RepositoryInstructionBuilder::new(
            entity_name.to_string(),
            key.map(|s| s.to_string()),
            self.migration_steps.clone(),
        )
    }
}

/// All instruction types supported by the migration system.
///
/// InstructionType defines the complete set of database schema evolution operations
/// supported by Nitrite migrations. Operations are organized by scope level:
/// database (user/auth), collection (schema), and repository (entity).
///
/// # Purpose
/// Identifies the type of migration operation being performed to determine
/// which parameters are required and how the operation is executed.
///
/// # Characteristics
/// - **Scoped**: Operations grouped by database, collection, or repository level
/// - **Comprehensive**: Covers all common schema evolution patterns
/// - **Type-safe**: Enum prevents invalid instruction type combinations
/// - **Copy/Debug**: Lightweight value type suitable for metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionType {
    // Database Level (5)
    /// Add database user with username and password
    AddUser,
    /// Change user password with old and new passwords
    ChangePassword,
    /// Drop entire collection from database
    DropCollection,
    /// Drop repository from database
    DropRepository,
    /// Execute custom database operation
    CustomInstruction,

    // Collection Level (7)
    /// Rename collection to new name
    CollectionRename,
    /// Add new field to collection documents
    AddField,
    /// Rename field in collection documents
    RenameField,
    /// Delete field from collection documents
    DeleteField,
    /// Drop specific index on fields
    DropIndex,
    /// Drop all indexes on collection
    DropAllIndices,
    /// Create new index on fields with type
    CreateIndex,

    // Repository Level (10)
    /// Rename repository entity type
    RepositoryRename,
    /// Add field to repository entity
    RepositoryAddField,
    /// Rename field in repository entity
    RepositoryRenameField,
    /// Delete field from repository entity
    RepositoryDeleteField,
    /// Convert field value from one type to another
    RepositoryChangeDataType,
    /// Change the ID field(s) of repository
    RepositoryChangeIdField,
    /// Drop specific index on repository fields
    RepositoryDropIndex,
    /// Drop all indexes on repository
    RepositoryDropAllIndices,
    /// Create new index on repository fields
    RepositoryCreateIndex,
}

/// Base trait for all migration instructions.
///
/// Instruction is the common interface for all migration operations, providing
/// the core method to identify operation type.
///
/// # Characteristics
/// - **Thread-safe**: Requires Send + Sync for concurrent execution
/// - **Polymorphic**: Enables trait objects for heterogeneous instruction collections
pub trait Instruction: Send + Sync {
    /// Returns the type of this instruction.
    ///
    /// # Returns
    /// InstructionType identifying the operation
    fn instruction_type(&self) -> InstructionType;
}

/// Database-level migration instructions.
///
/// DatabaseInstruction extends Instruction for operations scoped to the entire database,
/// such as user management and collection lifecycle.
///
/// # Characteristics
/// - **Database scope**: Operations affect entire database
/// - **Examples**: Add user, drop collection, custom database operations
pub trait DatabaseInstruction: Instruction {
    /// Returns migration steps for this instruction
    ///
    /// # Returns
    /// Vector of MigrationSteps to execute
    fn steps(&self) -> NitriteResult<Vec<MigrationStep>>;
}

/// Collection-level migration instructions.
///
/// CollectionInstruction extends Instruction for operations on specific collections,
/// such as renaming, field manipulation, and indexing.
///
/// # Characteristics
/// - **Collection scope**: Operations affect specific collection
/// - **Examples**: Rename, add/delete fields, manage indexes
pub trait CollectionInstruction: Instruction {
    /// Returns migration steps for this instruction
    ///
    /// # Returns
    /// Vector of MigrationSteps to execute
    fn steps(&self) -> NitriteResult<Vec<MigrationStep>>;

    /// Returns the name of the collection this instruction operates on
    ///
    /// # Returns
    /// Collection name string reference
    fn collection_name(&self) -> &str;
}

/// Repository-level migration instructions.
///
/// RepositoryInstruction extends Instruction for operations on specific repositories,
/// such as entity renaming, field mutation, and data type conversion.
///
/// # Characteristics
/// - **Repository scope**: Operations affect specific entity repository
/// - **Examples**: Change data type, modify ID fields, entity renaming
pub trait RepositoryInstruction: Instruction {
    /// Returns migration steps for this instruction
    ///
    /// # Returns
    /// Vector of MigrationSteps to execute
    fn steps(&self) -> NitriteResult<Vec<MigrationStep>>;

    /// Returns the entity type name this instruction operates on
    ///
    /// # Returns
    /// Entity name string reference
    fn entity_name(&self) -> &str;

    /// Returns the key field for this repository, if set
    ///
    /// # Returns
    /// Some(key) if explicitly set, None otherwise
    fn key(&self) -> Option<&str>;
}

/// Builder for database-level instructions.
///
/// DatabaseInstructionBuilder provides methods to construct database-level migration
/// operations such as user management and collection drops. Uses fluent builder pattern
/// for chaining operations.
///
/// # Purpose
/// Enables type-safe, ergonomic construction of database migration operations
/// with method chaining support.
///
/// # Characteristics
/// - **Fluent**: Returns &mut Self for method chaining
/// - **Shared state**: All builders share same Arc<Mutex<Vec<MigrationStep>>>
/// - **Additive**: Each method adds a step to the shared list
///
/// # Usage
///
/// ```ignore
/// let instruction = InstructionSet::new(vec![]);
/// instruction.for_database()
///     .add_user("admin", "password")
///     .drop_collection("temporary_data");
/// ```
pub struct DatabaseInstructionBuilder {
    steps: Arc<Mutex<Vec<MigrationStep>>>,
}

impl DatabaseInstructionBuilder {
    /// Creates new builder with shared steps list
    ///
    /// # Arguments
    /// * `steps` - Arc<Mutex<Vec<MigrationStep>>> shared with other builders
    pub fn new(steps: Arc<Mutex<Vec<MigrationStep>>>) -> Self {
        DatabaseInstructionBuilder { steps }
    }

    fn add_step(&mut self, step: MigrationStep) {
        let mut steps = self.steps.lock().unwrap();
        steps.push(step);
    }

    /// Adds a new user to the database.
    ///
    /// # Arguments
    /// * `username` - Username for the new user
    /// * `password` - Password for the new user
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_database().add_user("admin", "secure_password");
    /// ```
    pub fn add_user(&mut self, username: &str, password: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::AddUser,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Double(
                Arc::new(username.to_string()),
                Arc::new(password.to_string()),
            ),
        };
        self.add_step(step);
        self
    }

    /// Changes password for an existing user.
    ///
    /// # Arguments
    /// * `username` - Username whose password to change
    /// * `old_pw` - Current password
    /// * `new_pw` - New password
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn change_password(&mut self, username: &str, old_pw: &str, new_pw: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::ChangePassword,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Triple(
                Arc::new(username.to_string()),
                Arc::new(old_pw.to_string()),
                Arc::new(new_pw.to_string()),
            ),
        };
        self.add_step(step);
        self
    }

    /// Drops an entire collection from the database.
    ///
    /// # Arguments
    /// * `collection_name` - Name of collection to drop
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_database().drop_collection("temporary_data");
    /// ```
    pub fn drop_collection(&mut self, collection_name: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::DropCollection,
            collection_name: Some(collection_name.to_string()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Single(Arc::new(collection_name.to_string())),
        };
        self.add_step(step);
        self
    }

    /// Drops a repository from the database.
    ///
    /// # Arguments
    /// * `entity_name` - Entity type name (e.g., "User")
    /// * `key` - Optional key field name
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn drop_repository(&mut self, entity_name: &str, key: Option<&str>) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::DropRepository,
            collection_name: None,
            entity_name: Some(entity_name.to_string()),
            key: key.map(|k| k.to_string()),
            arguments: if let Some(k) = key {
                MigrationArguments::Double(
                    Arc::new(entity_name.to_string()),
                    Arc::new(k.to_string()),
                )
            } else {
                MigrationArguments::Single(Arc::new(entity_name.to_string()))
            },
        };
        self.add_step(step);
        self
    }

    /// Executes custom database-level operation.
    ///
    /// # Arguments
    /// * `instruction` - Closure taking Nitrite and performing operations
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_database().custom_instruction(|db| {
    ///     // Custom database operations
    ///     Ok(())
    /// });
    /// ```
    pub fn custom_instruction<F>(&mut self, instruction: F) -> &mut Self
    where
        F: Fn(Nitrite) -> NitriteResult<()> + Send + Sync + 'static,
    {
        let step = MigrationStep {
            instruction_type: InstructionType::CustomInstruction,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Single(Arc::new(MigrationFn::custom_instruction(instruction))),
        };
        self.add_step(step);
        self
    }
}

/// Builder for collection-level instructions.
///
/// CollectionInstructionBuilder provides methods to construct collection-level migration
/// operations such as renaming, field management, and indexing. All operations are scoped
/// to a specific collection and use fluent builder pattern.
///
/// # Purpose
/// Enables type-safe, ergonomic construction of collection migration operations
/// with method chaining support.
///
/// # Characteristics
/// - **Fluent**: Returns &mut Self for method chaining
/// - **Collection-scoped**: All operations apply to specific collection
/// - **Shared state**: Shares Arc<Mutex<Vec<MigrationStep>>> with other builders
///
/// # Usage
///
/// ```ignore
/// let instruction = InstructionSet::new(vec![]);
/// instruction.for_collection("users")
///     .rename("customers")
///     .add_field("version", Some(Value::from(1)), None);
/// ```
pub struct CollectionInstructionBuilder {
    collection_name: String,
    steps: Arc<Mutex<Vec<MigrationStep>>>,
}

impl CollectionInstructionBuilder {
    /// Creates new builder for specific collection
    ///
    /// # Arguments
    /// * `collection_name` - Name of collection to operate on
    /// * `steps` - Arc<Mutex<Vec<MigrationStep>>> shared with other builders
    pub fn new(collection_name: String, steps: Arc<Mutex<Vec<MigrationStep>>>) -> Self {
        CollectionInstructionBuilder {
            collection_name,
            steps,
        }
    }

    fn add_step(&mut self, step: MigrationStep) {
        let mut steps = self.steps.lock().unwrap();
        steps.push(step);
    }

    /// Renames the collection.
    ///
    /// # Arguments
    /// * `name` - New collection name
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_collection("users").rename("customers");
    /// ```
    pub fn rename(&mut self, name: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::CollectionRename,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Single(Arc::new(name.to_string())),
        };
        self.add_step(step);
        self
    }

    /// Adds a new field to all documents in the collection.
    ///
    /// # Arguments
    /// * `field_name` - Name of field to add
    /// * `default_value` - Optional default value for existing documents
    /// * `generator` - Optional closure to generate field value from document
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Behavior
    /// If both default_value and generator are provided, default_value takes precedence.
    /// If neither is provided, field added without value.
    ///
    /// # Usage
    /// ```ignore
    /// // With default value
    /// instruction.for_collection("users")
    ///     .add_field("version", Some(Value::from(1)), None);
    ///
    /// // With generator
    /// instruction.for_collection("users")
    ///     .add_field("id", None, Some(|doc| Ok(Value::from("generated"))));
    /// ```
    pub fn add_field(
        &mut self,
        field_name: &str,
        default_value: Option<Value>,
        generator: Option<impl Fn(Document) -> NitriteResult<Value> + Send + Sync + 'static>,
    ) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::AddField,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: if let Some(val) = default_value {
                MigrationArguments::Double(Arc::new(field_name.to_string()), Arc::new(val))
            } else if let Some(gen) = generator {
                MigrationArguments::Double(Arc::new(field_name.to_string()), Arc::new(MigrationFn::field_generator(gen)))
            } else {
                MigrationArguments::Single(Arc::new(field_name.to_string()))
            },
        };
        self.add_step(step);
        self
    }

    /// Renames a field in all documents.
    ///
    /// # Arguments
    /// * `old_name` - Current field name
    /// * `new_name` - New field name
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn rename_field(&mut self, old_name: &str, new_name: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RenameField,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Double(
                Arc::new(old_name.to_string()),
                Arc::new(new_name.to_string()),
            ),
        };
        self.add_step(step);
        self
    }

    /// Deletes a field from all documents.
    ///
    /// # Arguments
    /// * `field_name` - Name of field to delete
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn delete_field(&mut self, field_name: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::DeleteField,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Single(Arc::new(field_name.to_string())),
        };
        self.add_step(step);
        self
    }

    /// Drops index on specific fields.
    ///
    /// # Arguments
    /// * `field_names` - Slice of field names that were indexed
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn drop_index(&mut self, field_names: &[&str]) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::DropIndex,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Single(Arc::new(
                field_names
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )),
        };
        self.add_step(step);
        self
    }

    /// Drops all indexes on the collection.
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn drop_all_indices(&mut self) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::DropAllIndices,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::None,
        };
        self.add_step(step);
        self
    }

    /// Creates index on specific fields.
    ///
    /// # Arguments
    /// * `index_type` - Index type (e.g., "UNIQUE", "NON_UNIQUE")
    /// * `field_names` - Slice of field names to index
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_collection("users")
    ///     .create_index("UNIQUE", &["email"])
    ///     .create_index("NON_UNIQUE", &["age"]);
    /// ```
    pub fn create_index(&mut self, index_type: &str, field_names: &[&str]) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::CreateIndex,
            collection_name: Some(self.collection_name.clone()),
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Double(
                Arc::new(index_type.to_string()),
                Arc::new(
                    field_names
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>(),
                ),
            ),
        };
        self.add_step(step);
        self
    }

    /// Returns the name of the collection
    ///
    /// # Returns
    /// Collection name string reference
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

/// Builder for repository-level instructions.
///
/// RepositoryInstructionBuilder provides methods to construct repository-level migration
/// operations on entity types, such as field management, data type conversion, and ID field changes.
/// All operations are scoped to a specific repository and use fluent builder pattern.
///
/// # Purpose
/// Enables type-safe, ergonomic construction of repository migration operations
/// with method chaining support.
///
/// # Characteristics
/// - **Fluent**: Returns &mut Self for method chaining
/// - **Repository-scoped**: All operations apply to specific entity repository
/// - **Shared state**: Shares Arc<Mutex<Vec<MigrationStep>>> with other builders
/// - **Entity-aware**: Tracks entity name and optional key field
///
/// # Usage
///
/// Builder for repository-level migration instructions.
///
/// # Purpose
/// Provides fluent API to define migration operations on all entities of a specific
/// repository type (e.g., "User" entity). Operations include renaming, field modifications,
/// index management, and data type conversions.
///
/// # Characteristics
/// - Fluent builder pattern: all mutating methods return &mut Self for method chaining
/// - Shares Arc<Mutex<Vec<MigrationStep>>> with sibling builders
/// - Entity type name and key field are immutable (set at construction)
/// - Operations are queued as MigrationStep entries for later execution
/// - Thread-safe: builder can be shared across threads via Arc
///
/// # Usage
/// ```ignore
/// let migration = Migration::new(1, 2, |instruction| {
///     instruction.for_repository("User", Some("id"))
///         .rename_repository("Account", Some("account_id"))
///         .add_field("created_at", Some(Value::from(Utc::now().to_rfc3339())), None)
///         .create_index("UNIQUE", &["email"])
///         .change_data_type("age", |val| {
///             // Convert string to integer if needed
///             Ok(Value::from(42))
///         });
///     Ok(())
/// });
/// ```
pub struct RepositoryInstructionBuilder {
    entity_name: String,
    key: Option<String>,
    steps: Arc<Mutex<Vec<MigrationStep>>>,
}

impl RepositoryInstructionBuilder {
    /// Creates new builder for specific repository
    ///
    /// # Arguments
    /// * `entity_name` - Entity type name (e.g., "User")
    /// * `key` - Optional key field name
    /// * `steps` - Arc<Mutex<Vec<MigrationStep>>> shared with other builders
    pub fn new(
        entity_name: String,
        key: Option<String>,
        steps: Arc<Mutex<Vec<MigrationStep>>>,
    ) -> Self {
        RepositoryInstructionBuilder {
            entity_name,
            key,
            steps,
        }
    }

    fn add_step(&mut self, step: MigrationStep) {
        let mut steps = self.steps.lock().unwrap();
        steps.push(step);
    }

    /// Renames the repository entity type and optionally its key field.
    ///
    /// # Arguments
    /// * `new_entity_name` - New entity type name
    /// * `new_key` - Optional new key field name
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn rename_repository(&mut self, new_entity_name: &str, new_key: Option<&str>) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryRename,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: {
                MigrationArguments::Double(
                    Arc::new(new_entity_name.to_string()),
                    Arc::new(new_key.map(|s| s.to_string())),
                )
            },
        };
        self.add_step(step);
        self
    }

    /// Adds a new field to all entities in the repository.
    ///
    /// # Arguments
    /// * `field_name` - Name of field to add
    /// * `default_value` - Optional default value for existing entities
    /// * `generator` - Optional closure to generate field value from document
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Behavior
    /// If both default_value and generator are provided, default_value takes precedence.
    pub fn add_field(
        &mut self,
        field_name: &str,
        default_value: Option<Value>,
        generator: Option<impl Fn(Document) -> NitriteResult<Value> + Send + Sync + 'static>,
    ) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryAddField,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: if let Some(val) = default_value {
                MigrationArguments::Double(Arc::new(field_name.to_string()), Arc::new(val))
            } else if let Some(gen) = generator {
                MigrationArguments::Double(Arc::new(field_name.to_string()), Arc::new(MigrationFn::field_generator(gen)))
            } else {
                MigrationArguments::Single(Arc::new(field_name.to_string()))
            },
        };
        self.add_step(step);
        self
    }

    /// Renames a field in all entities.
    ///
    /// # Arguments
    /// * `old_name` - Current field name
    /// * `new_name` - New field name
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn rename_field(&mut self, old_name: &str, new_name: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryRenameField,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::Double(
                Arc::new(old_name.to_string()),
                Arc::new(new_name.to_string()),
            ),
        };
        self.add_step(step);
        self
    }

    /// Deletes a field from all entities.
    ///
    /// # Arguments
    /// * `field_name` - Name of field to delete
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn delete_field(&mut self, field_name: &str) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryDeleteField,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::Single(Arc::new(field_name.to_string())),
        };
        self.add_step(step);
        self
    }

    /// Converts field values from one type to another.
    ///
    /// # Arguments
    /// * `field_name` - Name of field to convert
    /// * `converter` - Closure taking Value and returning converted Value
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_repository("User", Some("id"))
    ///     .change_data_type("age", |val| {
    ///         // Convert string to integer, etc.
    ///         Ok(Value::from(42))
    ///     });
    /// ```
    pub fn change_data_type(
        &mut self,
        field_name: &str,
        converter: impl Fn(Value) -> NitriteResult<Value> + Send + Sync + 'static,
    ) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryChangeDataType,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::Double(
                Arc::new(field_name.to_string()),
                Arc::new(MigrationFn::value_converter(converter)),
            ),
        };
        self.add_step(step);
        self
    }

    /// Changes the ID field(s) of the repository.
    ///
    /// # Arguments
    /// * `old_field_names` - Slice of current ID field names
    /// * `new_field_names` - Slice of new ID field names
    ///
    /// # Returns
    /// &mut Self for method chaining
    ///
    /// # Usage
    /// ```ignore
    /// instruction.for_repository("User", Some("id"))
    ///     .change_id_field(&["id"], &["userId"]);
    /// ```
    pub fn change_id_field(
        &mut self,
        old_field_names: &[&str],
        new_field_names: &[&str],
    ) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryChangeIdField,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::Double(
                Arc::new(
                    old_field_names
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                ),
                Arc::new(
                    new_field_names
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                ),
            ),
        };
        self.add_step(step);
        self
    }

    /// Drops index on specific fields.
    ///
    /// # Arguments
    /// * `field_names` - Slice of field names that were indexed
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn drop_index(&mut self, field_names: &[&str]) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryDropIndex,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::Single(Arc::new(
                field_names
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>(),
            )),
        };
        self.add_step(step);
        self
    }

    /// Drops all indexes on the repository.
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn drop_all_indices(&mut self) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryDropAllIndices,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::None,
        };
        self.add_step(step);
        self
    }

    /// Creates index on specific fields.
    ///
    /// # Arguments
    /// * `index_type` - Index type (e.g., "UNIQUE", "NON_UNIQUE")
    /// * `field_names` - Slice of field names to index
    ///
    /// # Returns
    /// &mut Self for method chaining
    pub fn create_index(&mut self, index_type: &str, field_names: &[&str]) -> &mut Self {
        let step = MigrationStep {
            instruction_type: InstructionType::RepositoryCreateIndex,
            collection_name: None,
            entity_name: Some(self.entity_name.clone()),
            key: self.key.clone(),
            arguments: MigrationArguments::Double(
                Arc::new(index_type.to_string()),
                Arc::new(
                    field_names
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>(),
                ),
            ),
        };
        self.add_step(step);
        self
    }

    /// Returns the entity type name
    ///
    /// # Returns
    /// Entity name string reference
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Returns the key field name, if set
    ///
    /// # Returns
    /// Some(key) if explicitly set, None otherwise
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== InstructionSet Tests ====================

    #[test]
    fn test_instruction_set_new_empty() {
        let set = InstructionSet::new(vec![]);
        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 0);
    }

    #[test]
    fn test_instruction_set_new_with_steps() {
        // Create a minimal step for testing
        let step = MigrationStep {
            instruction_type: InstructionType::AddUser,
            collection_name: None,
            entity_name: None,
            key: None,
            arguments: MigrationArguments::Double(
                Arc::new("test_user".to_string()),
                Arc::new("password".to_string()),
            ),
        };
        let set = InstructionSet::new(vec![step]);
        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::AddUser);
    }

    #[test]
    fn test_instruction_set_for_database() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_database();
        assert!(builder.steps.lock().is_ok());
    }

    #[test]
    fn test_instruction_set_for_collection() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_collection("test_collection");
        assert_eq!(builder.collection_name, "test_collection");
    }

    #[test]
    fn test_instruction_set_for_repository_with_key() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_repository("TestEntity", Some("id"));
        assert_eq!(builder.entity_name, "TestEntity");
        assert_eq!(builder.key, Some("id".to_string()));
    }

    #[test]
    fn test_instruction_set_for_repository_without_key() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_repository("TestEntity", None);
        assert_eq!(builder.entity_name, "TestEntity");
        assert_eq!(builder.key, None);
    }

    // ==================== InstructionType Tests ====================

    #[test]
    fn test_instruction_type_equality() {
        assert_eq!(InstructionType::AddUser, InstructionType::AddUser);
        assert_ne!(InstructionType::AddUser, InstructionType::ChangePassword);
    }

    #[test]
    fn test_instruction_type_debug() {
        let it = InstructionType::AddUser;
        let debug_str = format!("{:?}", it);
        assert!(debug_str.contains("AddUser"));
    }

    // ==================== DatabaseInstructionBuilder Tests ====================

    #[test]
    fn test_database_builder_add_user() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder.add_user("admin", "secret");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::AddUser);
        assert_eq!(steps[0].collection_name, None);
        assert_eq!(steps[0].entity_name, None);
    }

    #[test]
    fn test_database_builder_add_user_chainable() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder
            .add_user("user1", "pass1")
            .add_user("user2", "pass2");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].instruction_type, InstructionType::AddUser);
        assert_eq!(steps[1].instruction_type, InstructionType::AddUser);
    }

    #[test]
    fn test_database_builder_change_password() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder.change_password("admin", "old", "new");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::ChangePassword);
    }

    #[test]
    fn test_database_builder_drop_collection() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder.drop_collection("users");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropCollection);
        assert_eq!(steps[0].collection_name, Some("users".to_string()));
    }

    #[test]
    fn test_database_builder_drop_repository_with_key() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder.drop_repository("UserRepository", Some("userId"));

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropRepository);
        assert_eq!(steps[0].entity_name, Some("UserRepository".to_string()));
    }

    #[test]
    fn test_database_builder_drop_repository_without_key() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder.drop_repository("UserRepository", None);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropRepository);
    }

    #[test]
    fn test_database_builder_custom_instruction() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder.custom_instruction(|_db| Ok(()));

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::CustomInstruction);
    }

    #[test]
    fn test_database_builder_multiple_operations() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        builder
            .add_user("admin", "pass")
            .change_password("admin", "pass", "newpass")
            .drop_collection("temp")
            .custom_instruction(|_| Ok(()));

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].instruction_type, InstructionType::AddUser);
        assert_eq!(steps[1].instruction_type, InstructionType::ChangePassword);
        assert_eq!(steps[2].instruction_type, InstructionType::DropCollection);
        assert_eq!(steps[3].instruction_type, InstructionType::CustomInstruction);
    }

    // ==================== CollectionInstructionBuilder Tests ====================

    #[test]
    fn test_collection_builder_new() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_collection("test_col");
        assert_eq!(builder.collection_name(), "test_col");
    }

    #[test]
    fn test_collection_builder_rename() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("old_name");
        builder.rename("new_name");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::CollectionRename);
        assert_eq!(steps[0].collection_name, Some("old_name".to_string()));
    }

    #[test]
    fn test_collection_builder_add_field_with_default() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.add_field("age", Some(Value::from(0)), None::<fn(Document) -> NitriteResult<Value>>);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::AddField);
        assert_eq!(steps[0].collection_name, Some("users".to_string()));
    }

    #[test]
    fn test_collection_builder_add_field_without_default() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.add_field("email", None, None::<fn(Document) -> NitriteResult<Value>>);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::AddField);
    }

    #[test]
    fn test_collection_builder_add_field_with_generator() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.add_field(
            "id",
            None,
            Some(|_doc| Ok(Value::from("generated"))),
        );

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::AddField);
    }

    #[test]
    fn test_collection_builder_rename_field() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.rename_field("old_field", "new_field");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RenameField);
    }

    #[test]
    fn test_collection_builder_delete_field() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.delete_field("unused_field");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DeleteField);
    }

    #[test]
    fn test_collection_builder_drop_index_specific() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.drop_index(&["email", "name"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropIndex);
    }

    #[test]
    fn test_collection_builder_drop_index_empty() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.drop_index(&[]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropIndex);
    }

    #[test]
    fn test_collection_builder_drop_all_indices() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.drop_all_indices();

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::DropAllIndices);
    }

    #[test]
    fn test_collection_builder_create_index() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.create_index("UNIQUE", &["email"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::CreateIndex);
    }

    #[test]
    fn test_collection_builder_create_index_multiple_fields() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder.create_index("COMPOUND", &["firstName", "lastName"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::CreateIndex);
    }

    #[test]
    fn test_collection_builder_chained_operations() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_collection("users");
        builder
            .add_field("age", Some(Value::from(0)), None::<fn(Document) -> NitriteResult<Value>>)
            .rename_field("email_addr", "email")
            .create_index("UNIQUE", &["email"])
            .delete_field("temp");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].instruction_type, InstructionType::AddField);
        assert_eq!(steps[1].instruction_type, InstructionType::RenameField);
        assert_eq!(steps[2].instruction_type, InstructionType::CreateIndex);
        assert_eq!(steps[3].instruction_type, InstructionType::DeleteField);
    }

    // ==================== RepositoryInstructionBuilder Tests ====================

    #[test]
    fn test_repository_builder_new_with_key() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_repository("UserRepo", Some("id"));
        assert_eq!(builder.entity_name(), "UserRepo");
        assert_eq!(builder.key(), Some("id"));
    }

    #[test]
    fn test_repository_builder_new_without_key() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_repository("UserRepo", None);
        assert_eq!(builder.entity_name(), "UserRepo");
        assert_eq!(builder.key(), None);
    }

    #[test]
    fn test_repository_builder_rename_with_new_key() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("OldRepo", Some("oldKey"));
        builder.rename_repository("NewRepo", Some("newKey"));

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryRename);
        assert_eq!(steps[0].entity_name, Some("OldRepo".to_string()));
    }

    #[test]
    fn test_repository_builder_rename_without_key() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("OldRepo", None);
        builder.rename_repository("NewRepo", None);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryRename);
    }

    #[test]
    fn test_repository_builder_add_field_with_default() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.add_field("status", Some(Value::from("active")), None::<fn(Document) -> NitriteResult<Value>>);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryAddField);
    }

    #[test]
    fn test_repository_builder_add_field_with_generator() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.add_field(
            "createdAt",
            None,
            Some(|_doc| Ok(Value::from("2024-01-01"))),
        );

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryAddField);
    }

    #[test]
    fn test_repository_builder_rename_field() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.rename_field("usr_name", "username");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryRenameField);
    }

    #[test]
    fn test_repository_builder_delete_field() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.delete_field("deprecatedField");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryDeleteField);
    }

    #[test]
    fn test_repository_builder_change_data_type() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.change_data_type("age", Ok);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryChangeDataType);
    }

    #[test]
    fn test_repository_builder_change_id_field() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.change_id_field(&["oldId"], &["newId"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryChangeIdField);
    }

    #[test]
    fn test_repository_builder_change_id_field_multiple() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.change_id_field(&["firstName", "lastName"], &["fullName"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryChangeIdField);
    }

    #[test]
    fn test_repository_builder_drop_index() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.drop_index(&["email"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryDropIndex);
    }

    #[test]
    fn test_repository_builder_drop_all_indices() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.drop_all_indices();

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryDropAllIndices);
    }

    #[test]
    fn test_repository_builder_create_index() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder.create_index("UNIQUE", &["email"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryCreateIndex);
    }

    #[test]
    fn test_repository_builder_chained_operations() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder
            .add_field("age", Some(Value::from(0)), None::<fn(Document) -> NitriteResult<Value>>)
            .rename_field("usr_name", "username")
            .create_index("UNIQUE", &["username"])
            .delete_field("temp");

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].instruction_type, InstructionType::RepositoryAddField);
        assert_eq!(steps[1].instruction_type, InstructionType::RepositoryRenameField);
        assert_eq!(steps[2].instruction_type, InstructionType::RepositoryCreateIndex);
        assert_eq!(steps[3].instruction_type, InstructionType::RepositoryDeleteField);
    }

    #[test]
    fn test_repository_builder_full_workflow() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_repository("UserRepo", Some("id"));
        builder
            .rename_repository("ModernUserRepo", Some("userId"))
            .add_field("createdAt", Some(Value::from("2024-01-01")), None::<fn(Document) -> NitriteResult<Value>>)
            .change_id_field(&["id"], &["userId"])
            .create_index("UNIQUE", &["email"])
            .change_data_type("age", Ok)
            .drop_index(&["oldIndex"]);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 6);
    }

    // ==================== Edge Cases and Integration Tests ====================

    #[test]
    fn test_instruction_set_shared_state() {
        let set = InstructionSet::new(vec![]);
        let mut db_builder = set.for_database();
        let mut col_builder = set.for_collection("col1");
        let mut repo_builder = set.for_repository("Repo1", None);

        db_builder.add_user("admin", "pass");
        col_builder.add_field("field1", None, None::<fn(Document) -> NitriteResult<Value>>);
        repo_builder.add_field("field2", Some(Value::from(1)), None::<fn(Document) -> NitriteResult<Value>>);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 3);
    }

    #[test]
    fn test_multiple_collections_same_set() {
        let set = InstructionSet::new(vec![]);
        let mut col1 = set.for_collection("users");
        let mut col2 = set.for_collection("products");

        col1.add_field("email", None, None::<fn(Document) -> NitriteResult<Value>>);
        col2.add_field("price", None, None::<fn(Document) -> NitriteResult<Value>>);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].collection_name, Some("users".to_string()));
        assert_eq!(steps[1].collection_name, Some("products".to_string()));
    }

    #[test]
    fn test_complex_migration_scenario() {
        let set = InstructionSet::new(vec![]);
        
        // Database operations
        let mut db = set.for_database();
        db.add_user("admin", "secure_pass");
        
        // Collection operations
        let mut col = set.for_collection("legacy_users");
        col.rename("users")
            .add_field("version", Some(Value::from(1)), None::<fn(Document) -> NitriteResult<Value>>)
            .create_index("UNIQUE", &["email"]);
        
        // Repository operations
        let mut repo = set.for_repository("OldUserRepo", Some("id"));
        repo.rename_repository("UserRepository", Some("userId"))
            .change_id_field(&["id"], &["userId"])
            .add_field("migrated", Some(Value::from(true)), None::<fn(Document) -> NitriteResult<Value>>);

        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 7);
        
        // Verify order
        assert_eq!(steps[0].instruction_type, InstructionType::AddUser);
        assert_eq!(steps[1].instruction_type, InstructionType::CollectionRename);
        assert_eq!(steps[2].instruction_type, InstructionType::AddField);
        assert_eq!(steps[3].instruction_type, InstructionType::CreateIndex);
        assert_eq!(steps[4].instruction_type, InstructionType::RepositoryRename);
        assert_eq!(steps[5].instruction_type, InstructionType::RepositoryChangeIdField);
        assert_eq!(steps[6].instruction_type, InstructionType::RepositoryAddField);
    }

    #[test]
    fn test_collection_name_with_special_chars() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_collection("user_data_v2.0");
        assert_eq!(builder.collection_name(), "user_data_v2.0");
    }

    #[test]
    fn test_repository_entity_name_with_namespace() {
        let set = InstructionSet::new(vec![]);
        let builder = set.for_repository("com.example.User", Some("userId"));
        assert_eq!(builder.entity_name(), "com.example.User");
    }

    #[test]
    fn test_builder_returns_mutable_reference() {
        let set = InstructionSet::new(vec![]);
        let mut builder = set.for_database();
        let returned = builder.add_user("user", "pass");
        // Verify it returns a mutable reference to self
        let _ = returned.add_user("user2", "pass2");
        
        let steps = set.get_steps().unwrap();
        assert_eq!(steps.len(), 2);
    }
}
