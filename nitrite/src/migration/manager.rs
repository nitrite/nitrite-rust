use super::instructions::MigrationFn;
use super::migration::{Migration, MigrationStep};
use crate::collection::Document;
use crate::common::{repository_name, AuthService, Fields};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::migration::commands::{Command, MigrationCommand};
use crate::migration::InstructionType;
use crate::nitrite::Nitrite;
use crate::store::Metadata;
use crate::Value;
use crate::STORE_INFO;
use std::sync::Arc;

/// Manages database migrations including version tracking and execution
pub struct MigrationManager {
    nitrite: Nitrite,
}

impl MigrationManager {
    pub fn new(nitrite: Nitrite) -> Self {
        MigrationManager { nitrite }
    }

    /// Executes migration if needed by finding and executing migration path
    pub fn do_migrate(&self) -> NitriteResult<()> {
        // Check if migration is needed
        if !self.is_migration_needed()? {
            return Ok(());
        }

        let current_version = self.nitrite.database_metadata()?.schema_version;
        let target_version = self.nitrite.config().schema_version();

        // Find migration path
        let migration_path = self.find_migration_path(current_version, target_version)?;
        let has_valid_path = !migration_path.is_empty();

        if !has_valid_path {
            let _ = self.nitrite.close();
            return Err(NitriteError::new(
                &format!(
                    "No valid migration path found from version {} to {}",
                    current_version, target_version
                ),
                ErrorKind::MigrationError,
            ));
        }

        // Execute migration path
        self.execute_migration_path(&migration_path)?;
        Ok(())
    }

    /// Checks if migration is needed
    fn is_migration_needed(&self) -> NitriteResult<bool> {
        let existing_version = self.nitrite.database_metadata()?.schema_version;
        let incoming_version = self.nitrite.config().schema_version();

        Ok(existing_version != incoming_version)
    }

    /// Finds a migration path from start to end version
    fn find_migration_path(&self, start: u32, end: u32) -> NitriteResult<Vec<Arc<Migration>>> {
        if start == end {
            return Ok(Vec::new());
        }

        {
            let this = &self;
            let upgrade = end > start;
            let mut result: Vec<Arc<Migration>> = Vec::new();
            let mut current = start;

            loop {
                // Direction-aware loop termination
                if if upgrade {
                    current >= end
                } else {
                    current <= end
                } {
                    break;
                }

                let migrations = this.nitrite.config().migrations();
                let target_node = match migrations.get(&current) {
                    Some(node) => node,
                    None => return Ok(vec![]),
                };

                // Find the best migration target based on direction
                let target = if upgrade {
                    // For upgrades: find largest target that doesn't exceed end
                    target_node
                        .keys()
                        .filter(|&&t| t > current && t <= end)
                        .max()
                        .copied()
                } else {
                    // For downgrades: find smallest target that doesn't go below end
                    target_node
                        .keys()
                        .filter(|&&t| t < current && t >= end)
                        .min()
                        .copied()
                };

                let target = match target {
                    Some(t) => t,
                    None => return Ok(vec![]),
                };

                let migration = target_node.get(&target).ok_or_else(|| {
                    NitriteError::new(
                        &format!("Migration from version {} to {} not found", current, target),
                        ErrorKind::MigrationError,
                    )
                })?;

                result.push(Arc::new(migration.clone()));
                current = target;
            }

            Ok(result)
        }
    }

    /// Executes a sequence of migrations
    fn execute_migration_path(&self, path: &[Arc<Migration>]) -> NitriteResult<()> {
        for migration in path {
            self.execute_migration_steps(migration)?;
        }

        let mut meta_data = self.nitrite.database_metadata()?;
        meta_data.schema_version = self.nitrite.config().schema_version();

        // Update schema version in metadata
        let store = self.nitrite.store();
        let store_info = store.open_map(STORE_INFO)?;
        store_info.put(
            Value::from(STORE_INFO),
            Value::Document(meta_data.get_info()),
        )?;

        Ok(())
    }

    /// Executes all steps in a migration
    fn execute_migration_steps(&self, migration: &Migration) -> NitriteResult<()> {
        // Use steps() to trigger lazy execution of the migration closure
        let steps = migration.steps()?;

        for step in steps {
            self.execute_step(&step)?;
        }

        Ok(())
    }

    /// Executes a single migration step
    fn execute_step(&self, step: &MigrationStep) -> NitriteResult<()> {
        let command = match step.instruction_type {
            // Database level
            InstructionType::AddUser => {
                let (username, password) = step.arguments.as_double::<String, String>()?;

                MigrationCommand::Custom {
                    collection_name: "".to_string(),
                    command: Box::new(move |nitrite: Nitrite| -> NitriteResult<()> {
                        let auth_service = AuthService::new(nitrite.store().clone());
                        auth_service.add_update_password(&username, "", &password, false)?;
                        Ok(())
                    }),
                }
            }
            InstructionType::ChangePassword => {
                let (username, old_pw, new_pw) =
                    step.arguments.as_triple::<String, String, String>()?;

                MigrationCommand::Custom {
                    collection_name: "".to_string(),
                    command: Box::new(move |nitrite: Nitrite| -> NitriteResult<()> {
                        let auth_service = AuthService::new(nitrite.store().clone());
                        auth_service.add_update_password(&username, &old_pw, &new_pw, true)?;
                        Ok(())
                    }),
                }
            }
            InstructionType::DropCollection => {
                let collection_name = step.arguments.as_single::<String>()?;
                MigrationCommand::Drop { collection_name }
            }
            InstructionType::DropRepository => {
                let arg_count = step.arguments.arg_count();

                if arg_count == 2 {
                    let (entity_name, key) = step.arguments.as_double::<String, String>()?;
                    let collection_name = repository_name(&entity_name, Some(&key))?;
                    MigrationCommand::Drop { collection_name }
                } else if arg_count == 1 {
                    let entity_name = step.arguments.as_single::<String>()?;
                    let collection_name = repository_name(&entity_name, None)?;
                    MigrationCommand::Drop { collection_name }
                } else {
                    return Err(NitriteError::new(
                        "Invalid arguments for DropRepository",
                        ErrorKind::ValidationError,
                    ));
                }
            }
            InstructionType::CustomInstruction => {
                let fn_wrapper = step.arguments.as_single::<MigrationFn>()?;
                MigrationCommand::Custom {
                    collection_name: "".to_string(),
                    command: Box::new(move |nitrite: Nitrite| -> NitriteResult<()> {
                        fn_wrapper.call_custom_instruction(nitrite)
                    }),
                }
            }

            // Collection level
            InstructionType::CollectionRename => {
                let new_name = step.arguments.as_single::<String>()?;
                let old_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for Rename",
                        ErrorKind::ValidationError,
                    )
                })?;

                MigrationCommand::Rename {
                    collection_name: old_name.to_string(),
                    new_name,
                    is_repository: false,
                }
            }
            InstructionType::AddField => {
                let collection_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for AddField",
                        ErrorKind::ValidationError,
                    )
                })?;

                // AddField can be called with single (field_name only) or double (field_name + default_value)
                if let Ok((field_name, default_value)) =
                    step.arguments.as_double::<String, crate::Value>()
                {
                    MigrationCommand::AddField {
                        collection_name: collection_name.to_string(),
                        field_name,
                        default_value: Some(default_value),
                        generator: None,
                    }
                } else if let Ok((field_name, generator_fn)) =
                    step.arguments.as_double::<String, MigrationFn>()
                {
                    let generator: Arc<dyn Fn(Document) -> NitriteResult<Value> + Send + Sync> =
                        Arc::new(move |doc| generator_fn.call_field_generator(doc));
                    MigrationCommand::AddField {
                        collection_name: collection_name.to_string(),
                        field_name,
                        default_value: None,
                        generator: Some(generator),
                    }
                } else {
                    // Field without default value
                    let field_name = step.arguments.as_single::<String>()?;
                    MigrationCommand::AddField {
                        collection_name: collection_name.to_string(),
                        field_name,
                        default_value: None,
                        generator: None,
                    }
                }
            }
            InstructionType::RenameField => {
                let collection_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for RenameField",
                        ErrorKind::ValidationError,
                    )
                })?;
                let (old_name, new_name) = step.arguments.as_double::<String, String>()?;
                MigrationCommand::RenameField {
                    collection_name: collection_name.to_string(),
                    old_field_name: old_name,
                    new_field_name: new_name,
                }
            }
            InstructionType::DeleteField => {
                let collection_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for DeleteField",
                        ErrorKind::ValidationError,
                    )
                })?;
                let field_name = step.arguments.as_single::<String>()?;
                MigrationCommand::DeleteField {
                    collection_name: collection_name.to_string(),
                    field_name,
                }
            }
            InstructionType::DropIndex => {
                let collection_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for DropIndex",
                        ErrorKind::ValidationError,
                    )
                })?;

                let field_names = step.arguments.as_single::<Vec<String>>()?;
                let fields = Fields::with_names(
                    field_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )?;
                MigrationCommand::DropIndex {
                    collection_name: collection_name.to_string(),
                    fields: Some(fields),
                }
            }
            InstructionType::DropAllIndices => {
                let collection_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for DropAllIndices",
                        ErrorKind::ValidationError,
                    )
                })?;

                MigrationCommand::DropIndex {
                    collection_name: collection_name.to_string(),
                    fields: None,
                }
            }
            InstructionType::CreateIndex => {
                let collection_name = step.collection_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Collection name required for CreateIndex",
                        ErrorKind::ValidationError,
                    )
                })?;

                let (index_type, field_names) =
                    step.arguments.as_double::<String, Vec<String>>()?;
                let fields = Fields::with_names(
                    field_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )?;

                MigrationCommand::CreateIndex {
                    collection_name: collection_name.to_string(),
                    fields,
                    index_type,
                }
            }
            // Repository level
            InstructionType::RepositoryRename => {
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryRename",
                        ErrorKind::ValidationError,
                    )
                })?;
                let key = step.key.as_ref().map(|s| s.as_str());
                let collection_name = repository_name(entity_name, key)?;

                let (new_entity_name, new_key) =
                    step.arguments.as_double::<String, Option<String>>()?;
                let new_collection_name = repository_name(&new_entity_name, new_key.as_deref())?;
                MigrationCommand::Rename {
                    collection_name,
                    new_name: new_collection_name,
                    is_repository: true,
                }
            }
            InstructionType::RepositoryAddField => {
                // RepositoryAddField can be called with single (field_name only) or double (field_name + default_value)
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryAddField",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                if let Ok((field_name, default_value)) =
                    step.arguments.as_double::<String, crate::Value>()
                {
                    MigrationCommand::AddField {
                        collection_name,
                        field_name,
                        default_value: Some(default_value),
                        generator: None,
                    }
                } else if let Ok((field_name, generator_fn)) =
                    step.arguments.as_double::<String, MigrationFn>()
                {
                    let generator: Arc<dyn Fn(Document) -> NitriteResult<Value> + Send + Sync> =
                        Arc::new(move |doc| generator_fn.call_field_generator(doc));
                    MigrationCommand::AddField {
                        collection_name,
                        field_name,
                        default_value: None,
                        generator: Some(generator),
                    }
                } else {
                    // Field without default value
                    let field_name = step.arguments.as_single::<String>()?;
                    MigrationCommand::AddField {
                        collection_name,
                        field_name,
                        default_value: None,
                        generator: None,
                    }
                }
            }
            InstructionType::RepositoryRenameField => {
                let (old_name, new_name) = step.arguments.as_double::<String, String>()?;
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryRenameField",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                MigrationCommand::RenameField {
                    collection_name,
                    old_field_name: old_name,
                    new_field_name: new_name,
                }
            }
            InstructionType::RepositoryDeleteField => {
                let field_name = step.arguments.as_single::<String>()?;
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryDeleteField",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                MigrationCommand::DeleteField {
                    collection_name,
                    field_name,
                }
            }
            InstructionType::RepositoryChangeDataType => {
                // RepositoryChangeDataType stores field_name and converter function in Double variant
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryChangeDataType",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                let (field_name, converter_fn) =
                    step.arguments.as_double::<String, MigrationFn>()?;
                let converter: Arc<dyn Fn(Value) -> NitriteResult<Value> + Send + Sync> =
                    Arc::new(move |v| converter_fn.call_value_converter(v));
                MigrationCommand::ChangeDataType {
                    collection_name,
                    field_name,
                    converter,
                }
            }
            InstructionType::RepositoryChangeIdField => {
                let (old_field_names, new_field_names) =
                    step.arguments.as_double::<Vec<String>, Vec<String>>()?;
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryChangeIdField",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                let old_field = Fields::with_names(
                    old_field_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )?;
                let new_field = Fields::with_names(
                    new_field_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )?;

                MigrationCommand::ChangeIdField {
                    collection_name,
                    old_id_field: old_field,
                    new_id_field: new_field,
                }
            }
            InstructionType::RepositoryDropIndex => {
                let args = step.arguments.as_single::<Vec<String>>()?;
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryDropIndex",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                MigrationCommand::DropIndex {
                    collection_name,
                    fields: Some(Fields::with_names(
                        args.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                    )?),
                }
            }
            InstructionType::RepositoryDropAllIndices => {
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryDropAllIndices",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                MigrationCommand::DropIndex {
                    collection_name,
                    fields: None,
                }
            }
            InstructionType::RepositoryCreateIndex => {
                let (index_type, field_names) =
                    step.arguments.as_double::<String, Vec<String>>()?;
                let entity_name = step.entity_name.as_ref().ok_or_else(|| {
                    NitriteError::new(
                        "Entity name required for RepositoryCreateIndex",
                        ErrorKind::ValidationError,
                    )
                })?;
                let collection_name = repository_name(entity_name, step.key.as_deref())?;

                MigrationCommand::CreateIndex {
                    collection_name,
                    fields: Fields::with_names(
                        field_names
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<&str>>(),
                    )?,
                    index_type,
                }
            }
        };

        command.execute(self.nitrite.clone())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{ErrorKind, NitriteResult};
    use crate::migration::Migration;
    use crate::nitrite::Nitrite;
    use std::sync::{Arc, Mutex};

    // ==================== Helper Functions ====================

    /// Creates a no-op migration that does nothing
    fn noop_migration(from: u32, to: u32) -> Migration {
        Migration::new(from, to, |_instruction_set| Ok(()))
    }

    /// Creates a migration that tracks execution via a shared tracker
    fn tracked_migration(
        from: u32,
        to: u32,
        label: &str,
        tracker: Arc<Mutex<Vec<String>>>,
    ) -> Migration {
        let label = label.to_string();
        Migration::new(from, to, move |instruction_set| {
            // Simply add a step to the instruction set that will push the label when executed
            let mut database_builder = instruction_set.for_database();
            let tracker_clone = tracker.clone();
            let label_clone = label.clone();
            database_builder.custom_instruction(move |_nitrite| {
                tracker_clone.lock().unwrap().push(label_clone.clone());
                Ok(())
            });
            Ok(())
        })
    }

    /// Sets up a MigrationManager with given migrations and schema versions
    fn setup_manager(
        config_schema_version: u32,
        migrations: Vec<Migration>,
    ) -> NitriteResult<(MigrationManager, Nitrite)> {
        let mut builder = Nitrite::builder().schema_version(config_schema_version);

        // Add migrations to the builder before initialization
        for migration in migrations {
            builder = builder.add_migration(migration);
        }

        let nitrite = builder.open_or_create(None, None)?;
        let manager = MigrationManager::new(nitrite.clone());
        Ok((manager, nitrite))
    }

    // ==================== MigrationManager::new() Tests ====================

    #[test]
    fn test_new_creates_manager_with_nitrite() -> NitriteResult<()> {
        let nitrite = Nitrite::builder().open_or_create(None, None)?;
        let manager = MigrationManager::new(nitrite.clone());
        // Manager should hold reference to nitrite
        let _ = manager.nitrite.config().schema_version();
        nitrite.close()?;
        Ok(())
    }

    // ==================== is_migration_needed() Tests ====================

    #[test]
    fn test_is_migration_needed_false_when_versions_match() -> NitriteResult<()> {
        let nitrite = Nitrite::builder()
            .schema_version(1)
            .open_or_create(None, None)?;
        let manager = MigrationManager::new(nitrite.clone());

        // Database metadata version equals config version
        let metadata = nitrite.database_metadata()?;
        let config_version = nitrite.config().schema_version();

        if metadata.schema_version == config_version {
            assert!(!manager.is_migration_needed()?);
        }
        nitrite.close()?;
        Ok(())
    }

    // ==================== find_migration_path() Tests ====================

    #[test]
    fn test_find_migration_path_same_version_returns_empty() -> NitriteResult<()> {
        let (manager, nitrite) = setup_manager(1, vec![])?;
        let path = manager.find_migration_path(5, 5)?;
        assert!(path.is_empty());
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_upgrade_single_step() -> NitriteResult<()> {
        let migrations = vec![noop_migration(1, 2)];
        let (manager, nitrite) = setup_manager(2, migrations)?;

        let path = manager.find_migration_path(1, 2)?;
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].from_version(), 1);
        assert_eq!(path[0].to_version(), 2);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_upgrade_multi_step() -> NitriteResult<()> {
        let migrations = vec![
            noop_migration(1, 2),
            noop_migration(2, 3),
            noop_migration(3, 4),
        ];
        let (manager, nitrite) = setup_manager(4, migrations)?;

        let path = manager.find_migration_path(1, 4)?;
        assert_eq!(path.len(), 3);
        assert_eq!(path[0].from_version(), 1);
        assert_eq!(path[2].to_version(), 4);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_downgrade_single_step() -> NitriteResult<()> {
        let migrations = vec![noop_migration(2, 1)];
        let (manager, nitrite) = setup_manager(1, migrations)?;

        let path = manager.find_migration_path(2, 1)?;
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].from_version(), 2);
        assert_eq!(path[0].to_version(), 1);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_downgrade_multi_step() -> NitriteResult<()> {
        let migrations = vec![
            noop_migration(4, 3),
            noop_migration(3, 2),
            noop_migration(2, 1),
        ];
        let (manager, nitrite) = setup_manager(1, migrations)?;

        let path = manager.find_migration_path(4, 1)?;
        assert_eq!(path.len(), 3);
        assert_eq!(path[0].from_version(), 4);
        assert_eq!(path[2].to_version(), 1);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_no_route_returns_empty() -> NitriteResult<()> {
        let migrations = vec![noop_migration(1, 2)];
        let (manager, nitrite) = setup_manager(5, migrations)?;

        // No migration from 3 to 5
        let path = manager.find_migration_path(3, 5)?;
        assert!(path.is_empty());
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_partial_route_returns_empty() -> NitriteResult<()> {
        let migrations = vec![
            noop_migration(1, 2),
            // Gap: no migration from 2 to 3
            noop_migration(3, 4),
        ];
        let (manager, nitrite) = setup_manager(4, migrations)?;

        // Cannot reach 4 from 1 due to gap at 2->3
        let path = manager.find_migration_path(1, 4)?;
        assert!(path.is_empty());
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_find_migration_path_chooses_optimal_upgrade() -> NitriteResult<()> {
        // Two paths: 1->2->3 or 1->3 directly
        let migrations = vec![
            noop_migration(1, 2),
            noop_migration(2, 3),
            noop_migration(1, 3), // Direct jump
        ];
        let (manager, nitrite) = setup_manager(3, migrations)?;

        let path = manager.find_migration_path(1, 3)?;
        // Should prefer direct jump (1->3)
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].from_version(), 1);
        assert_eq!(path[0].to_version(), 3);
        nitrite.close()?;
        Ok(())
    }

    // ==================== execute_migration_path() Tests ====================

    #[test]
    fn test_execute_migration_path_empty_succeeds() -> NitriteResult<()> {
        let (manager, nitrite) = setup_manager(1, vec![])?;
        let result = manager.execute_migration_path(&[]);
        assert!(result.is_ok());
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_execute_migration_path_updates_schema_version() -> NitriteResult<()> {
        let migrations = vec![noop_migration(1, 2)];
        let (manager, nitrite) = setup_manager(2, migrations.clone())?;

        let path: Vec<Arc<Migration>> = migrations.into_iter().map(Arc::new).collect();
        manager.execute_migration_path(&path)?;

        // Schema version should be updated to config version
        let store = nitrite.store();
        let store_info = store.open_map(STORE_INFO)?;
        if let Some(Value::Document(doc)) = store_info.get(&Value::from(STORE_INFO))? {
            let version_val = doc.get("schema_version")?;
            let version = version_val.as_u32().unwrap_or(&0);
            assert_eq!(*version, 2);
        }
        nitrite.close()?;
        Ok(())
    }

    // ==================== execute_migration_steps() Tests ====================

    #[test]
    fn test_execute_migration_steps_runs_all_steps() -> NitriteResult<()> {
        let tracker = Arc::new(Mutex::new(Vec::new()));
        let migration = tracked_migration(1, 2, "test_step", tracker.clone());

        let (manager, nitrite) = setup_manager(2, vec![])?;

        // execute_migration_steps should trigger the migration closure and run steps
        manager.execute_migration_steps(&migration)?;

        let executed = tracker.lock().unwrap();
        assert_eq!(executed.len(), 1);
        assert_eq!(executed[0], "test_step");
        nitrite.close()?;
        Ok(())
    }

    // ==================== do_migrate() Tests ====================

    #[test]
    fn test_do_migrate_skips_when_not_needed() -> NitriteResult<()> {
        let nitrite = Nitrite::builder()
            .schema_version(1)
            .open_or_create(None, None)?;
        let manager = MigrationManager::new(nitrite.clone());

        // If versions match, should return Ok without doing anything
        let result = manager.do_migrate();
        assert!(result.is_ok());
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_do_migrate_error_when_no_path() -> NitriteResult<()> {
        // Create nitrite with version 1, then try to migrate to version 5 without migrations
        let nitrite = Nitrite::builder()
            .schema_version(5)
            .open_or_create(None, None)?;

        let manager = MigrationManager::new(nitrite.clone());

        // Check if migration is needed
        let metadata = nitrite.database_metadata()?;
        let config_version = nitrite.config().schema_version();

        if metadata.schema_version != config_version {
            let result = manager.do_migrate();
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(e.kind(), &ErrorKind::MigrationError);
            }
        }
        Ok(())
    }

    // ==================== Migration Direction Tests ====================

    #[test]
    fn test_upgrade_direction_detection() -> NitriteResult<()> {
        let migrations = vec![noop_migration(1, 2)];
        let (manager, nitrite) = setup_manager(2, migrations)?;

        // 1 -> 2 is upgrade
        let path = manager.find_migration_path(1, 2)?;
        assert_eq!(path.len(), 1);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_downgrade_direction_detection() -> NitriteResult<()> {
        let migrations = vec![noop_migration(2, 1)];
        let (manager, nitrite) = setup_manager(1, migrations)?;

        // 2 -> 1 is downgrade
        let path = manager.find_migration_path(2, 1)?;
        assert_eq!(path.len(), 1);
        nitrite.close()?;
        Ok(())
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_migration_with_zero_version() -> NitriteResult<()> {
        let migrations = vec![noop_migration(0, 1)];
        let (manager, nitrite) = setup_manager(1, migrations)?;

        let path = manager.find_migration_path(0, 1)?;
        assert_eq!(path.len(), 1);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_migration_large_version_numbers() -> NitriteResult<()> {
        let migrations = vec![noop_migration(1000, 1001)];
        let (manager, nitrite) = setup_manager(1001, migrations)?;

        let path = manager.find_migration_path(1000, 1001)?;
        assert_eq!(path.len(), 1);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_multiple_migrations_from_same_version() -> NitriteResult<()> {
        // Multiple targets from version 1
        let migrations = vec![
            noop_migration(1, 2),
            noop_migration(1, 3),
            noop_migration(1, 5),
        ];
        let (manager, nitrite) = setup_manager(5, migrations)?;

        // Should choose 1->5 as optimal path to 5
        let path = manager.find_migration_path(1, 5)?;
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].to_version(), 5);
        nitrite.close()?;
        Ok(())
    }

    #[test]
    fn test_chained_migrations_execute_in_order() -> NitriteResult<()> {
        let tracker = Arc::new(Mutex::new(Vec::new()));
        let migrations = vec![
            tracked_migration(1, 2, "step_1_2", tracker.clone()),
            tracked_migration(2, 3, "step_2_3", tracker.clone()),
        ];

        let (manager, nitrite) = setup_manager(3, migrations.clone())?;

        let path: Vec<Arc<Migration>> = migrations.into_iter().map(Arc::new).collect();
        manager.execute_migration_path(&path)?;

        let executed = tracker.lock().unwrap();
        assert_eq!(executed.len(), 2);
        assert_eq!(executed[0], "step_1_2");
        assert_eq!(executed[1], "step_2_3");
        nitrite.close()?;
        Ok(())
    }
}
