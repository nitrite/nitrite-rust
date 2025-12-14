// doc constants
pub const DOC_REVISION: &str = "_revision";
pub const DOC_MODIFIED: &str = "_modified";
pub const DOC_SOURCE: &str = "_source";
pub const DOC_ID: &str = "_id";
pub const TYPE_NAME: &str = "_type";
pub const RESERVED_FIELDS: [&str; 4] = [DOC_ID, DOC_REVISION, DOC_MODIFIED, DOC_SOURCE];

// Compile-time assertion for reserved fields count
const _: () = {
    const RESERVED_FIELDS_COUNT: usize = 4;
    const ACTUAL_COUNT: usize = RESERVED_FIELDS.len();
    const _: [(); 1] = [(); (ACTUAL_COUNT == RESERVED_FIELDS_COUNT) as usize];
};

// attributes constants
pub const CREATED_TIME: &str = "created_at";
pub const LAST_MODIFIED_TIME: &str = "last_modified_at";
pub const OWNER: &str = "owner";
pub const UNIQUE_ID: &str = "uuid";

// store constants
pub const META_MAP_NAME: &str = "$nitrite_meta_map";
pub const COLLECTION_CATALOG: &str = "$nitrite_catalog";
pub const TAG_MAP_METADATA: &str = "mapNames";
pub const TAG_COLLECTION: &str = "collection";
pub const TAG_REPOSITORIES: &str = "repositories";
pub const TAG_KEYED_REPOSITORIES: &str = "keyed-repositories";
pub const KEY_OBJ_SEPARATOR: &str = "+";
pub const USER_MAP: &str = "$nitrite_users";
pub const NAME_SEPARATOR: &str = "|";

// event constants
pub const NITRITE_EVENT: &str = "nitrite_event";

// index constants
pub const UNIQUE_INDEX: &str = "unique";
pub const NON_UNIQUE_INDEX: &str = "non-unique";
pub const FULL_TEXT_INDEX: &str = "full-text";

// nitrite constants
pub const INTERNAL_NAME_SEPARATOR: &str = "|";
pub const INDEX_PREFIX: &str = "$nitrite_index";
pub const INDEX_META_PREFIX: &str = "$nitrite_index_meta";
pub const INITIAL_SCHEMA_VERSION: u32 = 1;
pub const NO2: &str = "NO\u{2082}";
pub const REPLICATOR: &str = "Replicator.NO\u{2082}";
pub const OBJECT_STORE_NAME_SEPARATOR: &str = ":";
pub const STORE_INFO: &str = "$nitrite_store_info";
pub const RESERVED_NAMES: [&str; 9] = [
    INDEX_META_PREFIX,
    INDEX_PREFIX,
    INTERNAL_NAME_SEPARATOR,
    USER_MAP,
    OBJECT_STORE_NAME_SEPARATOR,
    META_MAP_NAME,
    STORE_INFO,
    COLLECTION_CATALOG,
    KEY_OBJ_SEPARATOR,
];

// Compile-time assertion for reserved names count
const _RESERVED_NAMES_CHECK: () = {
    const RESERVED_NAMES_COUNT: usize = 9;
    const ACTUAL_RESERVED_NAMES: usize = RESERVED_NAMES.len();
    const _: [(); 1] = [(); (ACTUAL_RESERVED_NAMES == RESERVED_NAMES_COUNT) as usize];
};

pub const NITRITE_VERSION: &str = env!("CARGO_PKG_VERSION");