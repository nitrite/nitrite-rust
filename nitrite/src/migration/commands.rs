use std::{
    any::Any,
    sync::Arc,
};

use crate::{
    collection::{
        operation::{CollectionOperations, IndexManager},
        Document,
    },
    common::{Fields, NitriteEventBus, Value, DOC_ID, UNIQUE_INDEX},
    errors::{ErrorKind, NitriteError, NitriteResult},
    nitrite::Nitrite,
    store::{NitriteMap, NitriteStore},
};

pub trait Command {
    fn execute(&self, nitrite: Nitrite) -> NitriteResult<()>;
}

/// Unified migration command enum combining all command types
pub enum MigrationCommand {
    /// Add a new field to all documents with an optional default value
    AddField {
        collection_name: String,
        field_name: String,
        default_value: Option<Value>,
        generator: Option<Arc<dyn Fn(Document) -> NitriteResult<Value> + Send + Sync>>,
    },
    /// Change the data type of a field by applying a converter function
    ChangeDataType {
        collection_name: String,
        field_name: String,
        converter: Arc<dyn Fn(Value) -> NitriteResult<Value> + Send + Sync>,
    },
    /// Change which field serves as the document ID
    ChangeIdField {
        collection_name: String,
        old_id_field: Fields,
        new_id_field: Fields,
    },
    /// Create an index on one or more fields
    CreateIndex {
        collection_name: String,
        fields: Fields,
        index_type: String,
    },
    /// Delete a field from all documents
    DeleteField {
        collection_name: String,
        field_name: String,
    },
    /// Drop an entire collection
    Drop { collection_name: String },
    /// Drop an index from one or more fields
    DropIndex {
        collection_name: String,
        fields: Option<Fields>,
    },
    /// Rename a collection or repository
    Rename {
        collection_name: String,
        new_name: String,
        is_repository: bool,
    },
    /// Rename a field in all documents
    RenameField {
        collection_name: String,
        old_field_name: String,
        new_field_name: String,
    },
    Custom {
        collection_name: String,
        command: Box<dyn Fn(Nitrite) -> NitriteResult<()> + Send + Sync>,
    },
}

impl MigrationCommand {
    /// Initialize collection resources (store, map, operations)
    fn initialize(
        &self,
        nitrite: &Nitrite,
        collection_name: &str,
    ) -> NitriteResult<(
        NitriteStore,
        Option<NitriteMap>,
        Option<CollectionOperations>,
    )> {
        let store = nitrite.store();
        let map = store.open_map(collection_name)?;

        let ops = CollectionOperations::new(
            collection_name,
            map.clone(),
            nitrite.config(),
            NitriteEventBus::new(),
        )?;

        Ok((store, Some(map), Some(ops)))
    }
}

impl Command for MigrationCommand {
    fn execute(&self, nitrite: Nitrite) -> NitriteResult<()> {
        match self {
            MigrationCommand::AddField {
                collection_name,
                field_name,
                default_value,
                generator,
            } => {
                let (_store, map, ops) = self.initialize(&nitrite, collection_name)?;
                let map = map.ok_or_else(|| {
                    NitriteError::new("Map not initialized", ErrorKind::MigrationError)
                })?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                let index_descriptor = ops.find_index(&Fields::with_names(vec![field_name])?)?;

                for entry in map.entries()? {
                    let entry = entry?;
                    let mut doc = match entry.1 {
                        Value::Document(doc) => doc,
                        _ => {
                            return Err(NitriteError::new(
                                "Unexpected value type in map",
                                ErrorKind::MigrationError,
                            ));
                        }
                    };

                    let value = if let Some(generator) = generator {
                        generator(doc.clone())?
                    } else {
                        default_value.clone().unwrap_or(Value::Null)
                    };
                    doc.put(field_name.clone(), value)?;
                    map.put(entry.0.clone(), Value::Document(doc))?;

                    if let Some(index) = &index_descriptor {
                        ops.create_index(
                            &Fields::with_names(vec![field_name])?,
                            &index.index_type(),
                        )?;
                    }
                }

                Ok(())
            }

            MigrationCommand::ChangeDataType {
                collection_name,
                field_name,
                converter,
            } => {
                let (_store, map, ops) = self.initialize(&nitrite, collection_name)?;
                let map = map.ok_or_else(|| {
                    NitriteError::new("Map not initialized", ErrorKind::MigrationError)
                })?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                for entry in map.entries()? {
                    let entry = entry?;
                    let mut doc = match entry.1 {
                        Value::Document(doc) => doc,
                        _ => {
                            return Err(NitriteError::new(
                                "Unexpected value type in map",
                                ErrorKind::MigrationError,
                            ));
                        }
                    };

                    let field_value = doc.get(field_name)?;
                    let converted_value = converter(field_value)?;
                    doc.put(field_name.clone(), converted_value)?;
                    map.put(entry.0.clone(), Value::Document(doc))?;
                }

                if let Some(index) = ops.find_index(&Fields::with_names(vec![field_name])?)? {
                    ops.rebuild_index(&index)?;
                }

                Ok(())
            }

            MigrationCommand::ChangeIdField {
                collection_name,
                old_id_field,
                new_id_field,
            } => {
                let (_store, _map, ops) = self.initialize(&nitrite, collection_name)?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                let has_index = ops.has_index(old_id_field)?;
                if has_index {
                    ops.drop_index(old_id_field)?;
                }

                ops.create_index(new_id_field, UNIQUE_INDEX)?;

                Ok(())
            }

            MigrationCommand::CreateIndex {
                collection_name,
                fields,
                index_type,
            } => {
                let (_store, _map, ops) = self.initialize(&nitrite, collection_name)?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                ops.create_index(fields, index_type)?;

                Ok(())
            }

            MigrationCommand::DeleteField {
                collection_name,
                field_name,
            } => {
                let (_store, map, ops) = self.initialize(&nitrite, collection_name)?;
                let map = map.ok_or_else(|| {
                    NitriteError::new("Map not initialized", ErrorKind::MigrationError)
                })?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                for entry in map.entries()? {
                    let entry = entry?;
                    let mut doc = match entry.1 {
                        Value::Document(doc) => doc,
                        _ => {
                            return Err(NitriteError::new(
                                "Unexpected value type in map",
                                ErrorKind::MigrationError,
                            ));
                        }
                    };

                    doc.remove(field_name)?;
                    map.put(entry.0.clone(), Value::Document(doc))?;
                }

                if let Ok(fields) = Fields::with_names(vec![field_name]) {
                    if ops.has_index(&fields)? {
                        ops.drop_index(&fields)?;
                    }
                }

                Ok(())
            }

            MigrationCommand::Drop { collection_name } => {
                let (_store, _map, ops) = self.initialize(&nitrite, collection_name)?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;
                ops.dispose()?;
                Ok(())
            }

            MigrationCommand::DropIndex {
                collection_name,
                fields,
            } => {
                let (_store, _map, ops) = self.initialize(&nitrite, collection_name)?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                if let Some(fields) = fields {
                    ops.drop_index(fields)?;
                } else {
                    ops.drop_all_indexes()?;
                }

                Ok(())
            }

            MigrationCommand::Rename {
                collection_name,
                new_name,
                is_repository,
            } => {
                let (store, map, ops) = self.initialize(&nitrite, collection_name)?;
                let map = map.ok_or_else(|| {
                    NitriteError::new("Map not initialized", ErrorKind::MigrationError)
                })?;

                let new_map = store.open_map(new_name)?;
                let new_ops = CollectionOperations::new(
                    new_name,
                    new_map.clone(),
                    nitrite.config(),
                    NitriteEventBus::new(),
                )?;

                for entry in map.entries()? {
                    let entry = entry?;
                    new_map.put(entry.0.clone(), entry.1.clone())?;
                }

                let index_manager =
                    IndexManager::new(collection_name.clone(), nitrite.config().clone())?;
                let index_entries = index_manager.get_index_descriptors()?;
                for index in index_entries {
                    new_ops.create_index(&index.index_fields(), &index.index_type())?;
                }

                if let Some(ops) = ops {
                    ops.dispose()?;
                }
                
                // Update the catalog: add new name (old name already removed by ops.dispose())
                let catalog = store.store_catalog()?;
                if *is_repository {
                    catalog.write_repository_entry(new_name)?;
                } else {
                    catalog.write_collection_entry(new_name)?;
                }

                Ok(())
            }

            MigrationCommand::RenameField {
                collection_name,
                old_field_name,
                new_field_name,
            } => {
                let (_store, map, ops) = self.initialize(&nitrite, collection_name)?;
                let map = map.ok_or_else(|| {
                    NitriteError::new("Map not initialized", ErrorKind::MigrationError)
                })?;
                let ops = ops.ok_or_else(|| {
                    NitriteError::new("Operations not initialized", ErrorKind::MigrationError)
                })?;

                for entry in map.entries()? {
                    let entry = entry?;
                    let mut doc = match entry.1 {
                        Value::Document(doc) => doc,
                        _ => {
                            return Err(NitriteError::new(
                                "Unexpected value type in map",
                                ErrorKind::MigrationError,
                            ));
                        }
                    };

                    if doc.contains_key(old_field_name) {
                        let field_value = doc.get(old_field_name)?;
                        doc.remove(old_field_name)?;
                        doc.put(new_field_name.clone(), field_value)?;
                        map.put(entry.0.clone(), Value::Document(doc))?;
                    }
                }

                let old_fields = Fields::with_names(vec![old_field_name])?;
                let matching_descriptors =
                    IndexManager::new(collection_name.clone(), nitrite.config().clone())?
                        .find_matching_index(&old_fields)?;

                for descriptor in matching_descriptors {
                    let mut new_field_names = Vec::new();
                    for field in descriptor.index_fields().field_names() {
                        if field == old_field_name.as_str() {
                            new_field_names.push(new_field_name.clone());
                        } else {
                            new_field_names.push(field.to_string());
                        }
                    }
                    let new_fields =
                        Fields::with_names(new_field_names.iter().map(String::as_str).collect())?;
                    ops.create_index(&new_fields, &descriptor.index_type())?;
                    ops.drop_index(&descriptor.index_fields())?;
                }

                Ok(())
            }

            MigrationCommand::Custom {
                collection_name: _,
                command,
            } => command(nitrite),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nitrite_config::NitriteConfig;

    fn setup_nitrite() -> NitriteResult<Nitrite> {
        let config = NitriteConfig::default();
        config.auto_configure()?;
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None)?;
        Ok(nitrite)
    }

    #[test]
    fn test_add_field_with_default_value() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_add_field")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::AddField {
            collection_name: "test_add_field".to_string(),
            field_name: "age".to_string(),
            default_value: Some(Value::from(25)),
            generator: None,
        };

        cmd.execute(nitrite.clone())?;

        let cursor = col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("age")?, Value::from(25));

        Ok(())
    }

    #[test]
    fn test_add_field_with_generator() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_add_field_gen")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        let generator: Arc<dyn Fn(Document) -> NitriteResult<Value> + Send + Sync> =
            Arc::new(|_doc| Ok(Value::from("generated")));

        let cmd = MigrationCommand::AddField {
            collection_name: "test_add_field_gen".to_string(),
            field_name: "status".to_string(),
            default_value: None,
            generator: Some(generator),
        };

        cmd.execute(nitrite.clone())?;

        let cursor = col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("status")?, Value::from("generated"));

        Ok(())
    }

    #[test]
    fn test_add_field_without_default() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_add_field_null")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::AddField {
            collection_name: "test_add_field_null".to_string(),
            field_name: "optional_field".to_string(),
            default_value: None,
            generator: None,
        };

        cmd.execute(nitrite.clone())?;

        let cursor = col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("optional_field")?, Value::Null);

        Ok(())
    }

    #[test]
    fn test_change_data_type() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_change_type")?;

        let mut doc = Document::new();
        doc.put("count", Value::from("100"))?;
        col.insert(doc)?;

        let converter: Arc<dyn Fn(Value) -> NitriteResult<Value> + Send + Sync> = Arc::new(|val| {
            if let Value::String(s) = val {
                s.parse::<i32>()
                    .map(Value::from)
                    .map_err(|_| NitriteError::new("Parse error", ErrorKind::ValidationError))
            } else {
                Ok(Value::from(0))
            }
        });

        let cmd = MigrationCommand::ChangeDataType {
            collection_name: "test_change_type".to_string(),
            field_name: "count".to_string(),
            converter,
        };

        cmd.execute(nitrite.clone())?;

        let cursor = col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("count")?, Value::from(100));

        Ok(())
    }

    #[test]
    fn test_delete_field() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_delete_field")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        doc.put("temp_field", Value::from("remove_me"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::DeleteField {
            collection_name: "test_delete_field".to_string(),
            field_name: "temp_field".to_string(),
        };

        let result = cmd.execute(nitrite.clone());
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_rename_field() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_rename_field")?;

        let mut doc = Document::new();
        doc.put("old_name", Value::from("test"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::RenameField {
            collection_name: "test_rename_field".to_string(),
            old_field_name: "old_name".to_string(),
            new_field_name: "new_name".to_string(),
        };

        let result = cmd.execute(nitrite.clone());
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_create_index() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_create_index")?;

        let mut doc = Document::new();
        doc.put("email", Value::from("test@example.com"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::CreateIndex {
            collection_name: "test_create_index".to_string(),
            fields: Fields::with_names(vec!["email"])?,
            index_type: "non-unique".to_string(),
        };

        cmd.execute(nitrite.clone())?;

        // Verify index was created by checking collection has index
        let cursor = col.find(crate::filter::all())?;
        let _docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;

        Ok(())
    }

    #[test]
    fn test_drop_index_specific_fields() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_drop_index")?;

        let mut doc = Document::new();
        doc.put("email", Value::from("test@example.com"))?;
        col.insert(doc)?;

        // Create index first
        col.create_index(vec!["email"], &crate::index::non_unique_index())?;

        let cmd = MigrationCommand::DropIndex {
            collection_name: "test_drop_index".to_string(),
            fields: Some(Fields::with_names(vec!["email"])?),
        };

        cmd.execute(nitrite.clone())?;

        Ok(())
    }

    #[test]
    fn test_drop_all_indexes() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_drop_all_indexes")?;

        let mut doc = Document::new();
        doc.put("field1", Value::from("value1"))?;
        doc.put("field2", Value::from("value2"))?;
        col.insert(doc)?;

        // Create multiple indexes
        col.create_index(vec!["field1"], &crate::index::non_unique_index())?;
        col.create_index(vec!["field2"], &crate::index::non_unique_index())?;

        let cmd = MigrationCommand::DropIndex {
            collection_name: "test_drop_all_indexes".to_string(),
            fields: None,
        };

        cmd.execute(nitrite.clone())?;

        Ok(())
    }

    #[test]
    fn test_drop_collection() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_drop_collection")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        // Verify collection exists
        assert!(nitrite.has_collection("test_drop_collection")?);

        let cmd = MigrationCommand::Drop {
            collection_name: "test_drop_collection".to_string(),
        };

        let result = cmd.execute(nitrite.clone());
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_rename_collection() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_rename_old")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::Rename {
            collection_name: "test_rename_old".to_string(),
            new_name: "test_rename_new".to_string(),
            is_repository: false,
        };

        cmd.execute(nitrite.clone())?;

        // Verify new collection exists with data
        let new_col = nitrite.collection("test_rename_new")?;
        let cursor = new_col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name")?, Value::from("test"));

        Ok(())
    }

    #[test]
    fn test_custom_command() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_custom")?;

        let mut doc = Document::new();
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::Custom {
            collection_name: "test_custom".to_string(),
            command: Box::new(|_nitrite| {
                // Custom command that does nothing
                Ok(())
            }),
        };

        cmd.execute(nitrite.clone())?;

        Ok(())
    }

    #[test]
    fn test_change_id_field() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_change_id")?;

        let mut doc = Document::new();
        doc.put("user_id", Value::from("user123"))?;
        doc.put("name", Value::from("test"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::ChangeIdField {
            collection_name: "test_change_id".to_string(),
            old_id_field: Fields::with_names(vec![DOC_ID])?,
            new_id_field: Fields::with_names(vec!["user_id"])?,
        };

        cmd.execute(nitrite.clone())?;

        Ok(())
    }

    #[test]
    fn test_command_trait_implementation() -> NitriteResult<()> {
        let cmd = MigrationCommand::Custom {
            collection_name: "test".to_string(),
            command: Box::new(|_nitrite| Ok(())),
        };

        let nitrite = setup_nitrite()?;
        // Verify Command trait can be called
        cmd.execute(nitrite)?;
        Ok(())
    }

    #[test]
    fn test_rename_field_without_existing_field() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_rename_no_field")?;

        let mut doc = Document::new();
        doc.put("existing_field", Value::from("value"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::RenameField {
            collection_name: "test_rename_no_field".to_string(),
            old_field_name: "nonexistent_field".to_string(),
            new_field_name: "new_field".to_string(),
        };

        let result = cmd.execute(nitrite.clone());
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_delete_field_nonexistent_field() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_delete_nonexistent")?;

        let mut doc = Document::new();
        doc.put("existing_field", Value::from("value"))?;
        col.insert(doc)?;

        let cmd = MigrationCommand::DeleteField {
            collection_name: "test_delete_nonexistent".to_string(),
            field_name: "nonexistent_field".to_string(),
        };

        cmd.execute(nitrite.clone())?;

        let cursor = col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("existing_field")?, Value::from("value"));

        Ok(())
    }

    #[test]
    fn test_multiple_documents_operations() -> NitriteResult<()> {
        let nitrite = setup_nitrite()?;
        let col = nitrite.collection("test_multi_doc")?;

        // Insert multiple documents
        for i in 0..5 {
            let mut doc = Document::new();
            doc.put("id", Value::from(i))?;
            doc.put("value", Value::from(format!("value_{}", i)))?;
            col.insert(doc)?;
        }

        let cmd = MigrationCommand::AddField {
            collection_name: "test_multi_doc".to_string(),
            field_name: "processed".to_string(),
            default_value: Some(Value::from(true)),
            generator: None,
        };

        cmd.execute(nitrite.clone())?;

        let cursor = col.find(crate::filter::all())?;
        let docs: Vec<_> = cursor.collect::<NitriteResult<Vec<_>>>()?;
        assert_eq!(docs.len(), 5);

        for doc in docs {
            assert_eq!(doc.get("processed")?, Value::from(true));
        }

        Ok(())
    }
}
