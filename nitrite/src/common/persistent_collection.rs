use crate::{errors::NitriteResult, index::{IndexDescriptor, IndexOptions}, store::NitriteStore};

use super::{AttributeAware, EventAware, Processor};

pub trait PersistentCollection: EventAware + AttributeAware + Send + Sync {
    fn add_processor(&self, processor: Processor) -> NitriteResult<()>;

    fn create_index(&self, field_names: Vec<&str>, index_options: &IndexOptions) -> NitriteResult<()>;

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()>;

    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>>;

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool>;

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool>;

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()>;

    fn drop_all_indexes(&self) -> NitriteResult<()>;

    fn clear(&self) -> NitriteResult<()>;

    fn dispose(&self) -> NitriteResult<()>;

    fn is_dropped(&self) -> NitriteResult<bool>;

    fn is_open(&self) -> NitriteResult<bool>;

    fn size(&self) -> NitriteResult<u64>;

    fn close(&self) -> NitriteResult<()>;

    fn store(&self) -> NitriteResult<NitriteStore>;
}
