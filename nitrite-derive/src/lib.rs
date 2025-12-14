#![recursion_limit = "128"]
//! # Nitrite Derive Macros
//!
//! This crate provides procedural macros for deriving Nitrite traits automatically.
//!
//! ## Macros
//!
//! ### `Convertible`
//!
//! Derives the `Convertible` trait for structs and enums, enabling automatic conversion
//! between Rust types and Nitrite's `Document` representation.
//!
//! - **Supported for**: Structs with named fields and enums with data
//! - **Field attribute**: `#[converter(serialize = "...", deserialize = "...")]`
//!
//! # Examples
//!
//! ```rust,ignore
//! use nitrite_derive::Convertible;
//!
//! #[derive(Convertible)]
//! pub struct User {
//!     pub name: String,
//!     pub age: u32,
//!     pub email: String,
//! }
//!
//! #[derive(Convertible)]
//! pub enum Status {
//!     Active,
//!     Inactive,
//! }
//! ```
//!
//! ### `NitriteEntity`
//!
//! Derives the `NitriteEntity` trait for structs, marking them as persistable entities
//! in the database. Works together with `Convertible`.
//!
//! - **Supported for**: Structs with named fields only
//! - **Field attribute**: `#[entity(id)]` marks a field as the entity ID
//!
//! # Examples
//!
//! ```rust,ignore
//! use nitrite_derive::{NitriteEntity, Convertible};
//! use nitrite::collection::NitriteId;
//!
//! #[derive(NitriteEntity, Convertible)]
//! pub struct Product {
//!     #[entity(id)]
//!     pub id: Option<NitriteId>,
//!     pub name: String,
//!     pub price: f64,
//! }
//! ```
//!
//! ## Error Messages
//!
//! The macros provide detailed error messages when derivation fails:
//!
//! - **Convertible**: Ensure all fields implement `Convertible`
//! - **NitriteEntity**: Only structs with named fields are supported
//!
//! For enums, use `#[derive(Convertible)]` only; `NitriteEntity` doesn't support enums.

extern crate proc_macro;
mod convertible;
mod nitrite_entity;

use crate::convertible::{generate_convertible_for_enum, generate_convertible_for_struct};
use crate::nitrite_entity::generate_entity_for_struct;
use proc_macro::TokenStream;
use syn::{Data, DeriveInput};

/// Derives the `Convertible` trait for automatic type conversion.
///
/// This macro enables automatic conversion between Rust types and Nitrite's
/// `Document` representation. It supports both structs and enums.
///
/// # Attributes
///
/// - `#[converter(serialize = "fn_name", deserialize = "fn_name")]` - Custom conversion functions
///
/// # Errors
///
/// Returns a compile error if:
/// - Any field doesn't implement `Convertible`
/// - The type is a union (unions are not supported)
///
/// # Examples
///
/// ```rust,ignore
/// #[derive(Convertible)]
/// pub struct User {
///     pub name: String,
///     pub age: u32,
/// }
/// ```
#[proc_macro_derive(Convertible, attributes(converter))]
pub fn derive_convert(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    match ast.data {
        Data::Struct(ref data) => {
            let result = generate_convertible_for_struct(&ast, data);
            match result {
                Ok(token_stream) => token_stream,
                Err(e) => {
                    let error = syn::Error::new_spanned(
                        &ast,
                        format!(
                            "Failed to derive Convertible for struct '{}': {}.\n\
                             Make sure all fields implement Convertible trait.",
                            ast.ident, e
                        ),
                    );
                    error.to_compile_error().into()
                }
            }
        },
        Data::Enum(ref data) => {
            let result = generate_convertible_for_enum(&ast, data);
            match result {
                Ok(token_stream) => token_stream,
                Err(e) => {
                    let error = syn::Error::new_spanned(
                        &ast,
                        format!(
                            "Failed to derive Convertible for enum '{}': {}.\n\
                             Ensure all enum variants have supported types.",
                            ast.ident, e
                        ),
                    );
                    error.to_compile_error().into()
                }
            }
        },
        Data::Union(_) => {
            let error = syn::Error::new_spanned(
                &ast,
                "Cannot derive Convertible for unions. Unions are not supported by the Convertible derive macro.",
            );
            error.to_compile_error().into()
        }
    }
}

/// Derives the `NitriteEntity` trait for entity persistence.
///
/// This macro marks a struct as a persistable entity that can be stored in
/// object repositories. Must be used with `#[derive(Convertible)]`.
///
/// # Supported Types
///
/// - Structs with named fields only
/// - Enums and unions are not supported
///
/// # Attributes
///
/// - `#[entity(id)]` - Marks a field as the primary key (optional)
///
/// # Errors
///
/// Returns a compile error if:
/// - Applied to an enum or union
/// - Used on tuple structs or unit structs
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite_derive::{NitriteEntity, Convertible};
///
/// #[derive(NitriteEntity, Convertible)]
/// pub struct User {
///     pub name: String,
///     pub age: u32,
/// }
/// ```
#[proc_macro_derive(NitriteEntity, attributes(entity))]
pub fn derive_nitrite_entity(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    match ast.data {
        Data::Struct(ref data) => {
            let result = generate_entity_for_struct(&ast, data);
            match result {
                Ok(token_stream) => token_stream,
                Err(e) => {
                    let error = syn::Error::new_spanned(
                        &ast,
                        format!(
                            "Failed to derive NitriteEntity for struct '{}': {}.\n\
                             Only structs with named fields are supported.\n\
                             Example: #[derive(NitriteEntity)] pub struct MyEntity {{ field: Type }}",
                            ast.ident, e
                        ),
                    );
                    error.to_compile_error().into()
                }
            }
        },
        Data::Enum(_) => {
            let error = syn::Error::new_spanned(
                &ast,
                "Cannot derive NitriteEntity for enums. Only structs are supported.",
            );
            error.to_compile_error().into()
        },
        Data::Union(_) => {
            let error = syn::Error::new_spanned(
                &ast,
                "Cannot derive NitriteEntity for unions. Only structs are supported.",
            );
            error.to_compile_error().into()
        }
    }
}
