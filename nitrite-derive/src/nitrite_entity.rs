use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{DataStruct, DeriveInput, LitStr, Result};

pub(crate) fn generate_entity_for_struct(
    ast: &DeriveInput,
    data: &DataStruct,
) -> Result<TokenStream> {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let mut entity_name = name.to_string();
    let mut entity_id: Option<String> = None;
    let mut embedded_fields: Option<Vec<String>> = None;
    let mut indexes = Vec::new();
    let mut id_found = false;
    let mut is_nitrite_id = false;
    let mut id_type: Option<proc_macro2::TokenStream> = None;

    for attr in &ast.attrs {
        if attr.path().is_ident("entity") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    entity_name = s.value();
                    Ok(())
                } else if meta.path.is_ident("id") {
                    if id_found {
                        return Err(meta.error("Multiple id attributes are not allowed"));
                    }

                    meta.parse_nested_meta(|meta| {
                        id_found = true;
                        if meta.path.is_ident("field") {
                            let value = meta.value()?;
                            let s: LitStr = value.parse()?;
                            entity_id = Some(s.value());
                            Ok(())
                        } else if meta.path.is_ident("embedded_fields") {
                            let value = meta.value()?;
                            let s: LitStr = value.parse()?;
                            // Pre-allocate and collect embedded fields efficiently
                            let field_str = s.value();
                            let field_list: Vec<&str> = field_str.split(',').collect();
                            let mut fields = Vec::with_capacity(field_list.len());
                            for field in field_list {
                                fields.push(field.trim().to_string());
                            }
                            embedded_fields = Some(fields);
                            Ok(())
                        } else {
                            Err(meta.error("Unknown id attribute"))
                        }
                    })
                } else if meta.path.is_ident("index") {
                    let mut index_type: Option<String> = None;
                    let mut index_fields: Option<Vec<String>> = None;

                    meta.parse_nested_meta(|meta| {
                        if meta.path.is_ident("type") {
                            let value = meta.value()?;
                            let s: LitStr = value.parse()?;
                            index_type = Some(s.value());
                            Ok(())
                        } else if meta.path.is_ident("fields") {
                            let value = meta.value()?;
                            let s: LitStr = value.parse()?;
                            // Pre-allocate for index fields
                            let field_str = s.value();
                            let field_list: Vec<&str> = field_str.split(',').collect();
                            let mut fields = Vec::with_capacity(field_list.len());
                            for field in field_list {
                                fields.push(field.trim().to_string());
                            }
                            index_fields = Some(fields);
                            Ok(())
                        } else {
                            Err(meta.error("Unknown index attribute"))
                        }
                    })
                    .and_then(|_| {
                        if let (Some(idx_type), Some(idx_fields)) = (index_type, index_fields) {
                            indexes.push((idx_type, idx_fields));
                            Ok(())
                        } else {
                            Err(meta.error("Index type and fields are required"))
                        }
                    })
                } else {
                    Err(meta.error("Unknown nitrite attribute"))
                }
            })?
        }
    }

    // Find id field and check if it exists - cache the lookup result
    if let Some(ref id_field_name) = entity_id {
        if let Some(id_field) = data.fields.iter().find(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident == id_field_name)
        }) {
            // Set id type and check if it's NitriteId
            id_type = Some(id_field.ty.to_token_stream());

            let no_embedded_fields =
                embedded_fields.is_none() || embedded_fields.as_ref().unwrap().is_empty();
            if id_field
                .ty
                .to_token_stream()
                .to_string()
                .contains("NitriteId")
                && no_embedded_fields
            {
                is_nitrite_id = true;
            }
        } else {
            return Err(syn::Error::new_spanned(
                ast,
                format!("Field {} not found in struct", id_field_name),
            ));
        }
    }

    // Generate entity name code
    let entity_name_code = quote! {
        fn entity_name(&self) -> String {
            #entity_name.to_string()
        }
    };

    // Generate associated type code
    let id_type_code = if let Some(id_type) = id_type {
        quote! {
            type Id = #id_type;
        }
    } else {
        quote! {
            type Id = ();
        }
    };

    // Generate entity id with embedded fields code
    let entity_id_code = if let Some(entity_id) = entity_id {
        if let Some(embedded_fields_list) = &embedded_fields {
            if embedded_fields_list.is_empty() {
                quote! {
                    fn entity_id(&self) -> Option<nitrite::repository::EntityId> {
                        Some(nitrite::repository::EntityId::new(#entity_id, Some(#is_nitrite_id), None))
                    }
                }
            } else {
                let embedded_fields_code = embedded_fields_list.iter().map(|field| quote!(#field));
                quote! {
                    fn entity_id(&self) -> Option<nitrite::repository::EntityId> {
                        Some(nitrite::repository::EntityId::new(#entity_id, Some(#is_nitrite_id), Some(vec![#(#embedded_fields_code),*])))
                    }
                }
            }
        } else {
            quote! {
                fn entity_id(&self) -> Option<nitrite::repository::EntityId> {
                    Some(nitrite::repository::EntityId::new(#entity_id, Some(#is_nitrite_id), None))
                }
            }
        }
    } else {
        quote! {
            fn entity_id(&self) -> Option<nitrite::repository::EntityId> {
                None
            }
        }
    };

    // Generate entity indexes code - only create vec if there are indexes
    let entity_indexes_code = if indexes.is_empty() {
        quote! {
            fn entity_indexes(&self) -> Option<Vec<nitrite::repository::EntityIndex>> {
                None
            }
        }
    } else {
        // Pre-allocate indexes with exact capacity
        let indexes_code: Vec<_> = indexes.iter().map(|(index_type, fields)| {
            let fields_code = fields.iter().map(|field| quote!(#field));
            quote! {
                nitrite::repository::EntityIndex::new(vec![#(#fields_code),*], Some(#index_type))
            }
        }).collect();

        quote! {
            fn entity_indexes(&self) -> Option<Vec<nitrite::repository::EntityIndex>> {
                Some(vec![#(#indexes_code),*])
            }
        }
    };

    let gen = quote! {
        impl #impl_generics nitrite::repository::NitriteEntity for #name #ty_generics #where_clause {
            #id_type_code
            #entity_name_code
            #entity_id_code
            #entity_indexes_code
        }
    };

    Ok(TokenStream::from(gen))
}
