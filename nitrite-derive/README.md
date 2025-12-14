# Nitrite Derive

Procedural macros for Nitrite database entity definitions.

## Macros

### `#[derive(Convertible)]`

Enables automatic conversion between Rust structs and Nitrite documents.

```rust
use nitrite_derive::Convertible;

#[derive(Convertible)]
pub struct User {
    name: String,
    age: i32,
}
```

### `#[derive(NitriteEntity)]`

Defines a Nitrite entity with optional ID configuration and indexes.

```rust
use nitrite_derive::NitriteEntity;

#[derive(NitriteEntity, Default)]
pub struct Book {
    id: i32,
    name: String,
}
```

## Entity Attributes

### Custom Entity Name

```rust
#[derive(NitriteEntity, Default)]
#[entity(name = "MyBook")]
pub struct Book {
    id: i32,
    name: String,
}
```

### Entity ID Field

```rust
#[derive(NitriteEntity, Default)]
#[entity(id(field = "id"))]
pub struct Book {
    id: i32,
    name: String,
}
```

### Embedded ID Fields

```rust
use nitrite_derive::{Convertible, NitriteEntity};

#[derive(NitriteEntity, Default)]
#[entity(id(field = "book_id", embedded_fields = "author, isbn"))]
pub struct Book {
    book_id: BookId,
    name: String,
}

#[derive(Default, Convertible)]
pub struct BookId {
    author: String,
    isbn: String,
}
```

### Entity Indexes

```rust
#[derive(NitriteEntity, Default)]
#[entity(
    index(type = "unique", fields = "name"),
    index(type = "non-unique", fields = "name, publisher")
)]
pub struct Book {
    name: String,
    publisher: String,
}
```

## NitriteId Support

Use `NitriteId` for auto-generated unique identifiers:

```rust
use nitrite::collection::NitriteId;
use nitrite_derive::NitriteEntity;

#[derive(NitriteEntity, Default)]
#[entity(id(field = "id"))]
pub struct Book {
    id: NitriteId,
    name: String,
}
```

## License

Apache License 2.0
