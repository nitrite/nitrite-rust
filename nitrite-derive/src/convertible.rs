use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{DataEnum, DataStruct, DeriveInput, Field, LitStr, Result, Type};

pub(crate) fn generate_convertible_for_struct(ast: &DeriveInput, data: &DataStruct) -> Result<TokenStream> {
    let mut ignored_fields: Vec<String> = vec![];
    
    // Check if the struct has a `converter` attribute - use if-let for clarity
    for attr in &ast.attrs {
        if attr.path().is_ident("converter") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("ignored") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    // Pre-allocate with_capacity to reduce reallocations during extend
                    let field_str = s.value();
                    let field_list: Vec<&str> = field_str.split(',').collect();
                    ignored_fields.reserve(field_list.len());
                    for field in field_list {
                        ignored_fields.push(field.trim().to_string());
                    }
                }
                Ok(())
            })?
        }
    }
    
    // Parse out all the fields from the struct
    let fields: Vec<&Field> = match &data.fields {
        syn::Fields::Named(ref fields) => fields.named.iter().collect(),
        _ => panic!("Only structs with named fields are supported"),
    };

    // Filter fields efficiently - avoid double iteration
    let filtered_fields: Vec<&Field> = fields
        .iter()
        .filter(|f| {
            f.ident.as_ref()
                .map_or(true, |ident| !ignored_fields.contains(&ident.to_string()))
        })
        .copied()
        .collect();

    // Extract identifiers and names in a single pass
    let filtered_idents: Vec<&Ident> = filtered_fields
        .iter()
        .filter_map(|f| f.ident.as_ref())
        .collect();

    let filtered_field_names: Vec<String> = filtered_idents
        .iter()
        .map(|i| i.to_string())
        .collect();

    // Get all fields for from_value - avoid duplicate field collection
    let all_idents: Vec<&Ident> = fields
        .iter()
        .filter_map(|f| f.ident.as_ref())
        .collect();

    let all_field_names: Vec<String> = all_idents
        .iter()
        .map(|i| i.to_string())
        .collect();

    let all_types: Vec<Type> = fields
        .iter()
        .map(|field| field.ty.clone())
        .collect();

    // Get default value implementations - cache ignored field checks
    let default_initializers: Vec<proc_macro2::TokenStream> = all_idents.iter()
        .zip(all_field_names.iter())
        .zip(all_types.iter())
        .map(|((ident, name), ty)| {
            if ignored_fields.contains(name) {
                quote! { #ident: Default::default() }
            } else {
                quote! { #ident: nitrite::common::from_value::<#ty>(&doc.get(#name)?)? }
            }
        })
        .collect();

    // Get the name identifier of the struct
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    // Generate the implementation of the trait
    let gen = quote! {
        impl #impl_generics nitrite::common::Convertible for #name #ty_generics #where_clause {
            type Output = Self;

            fn to_value(&self) -> nitrite::errors::NitriteResult<nitrite::common::Value> {
                let mut doc = nitrite::collection::Document::new();
                #(doc.put(#filtered_field_names, self.#filtered_idents.to_value()?)?;)*
                Ok(nitrite::common::Value::Document(doc))
            }

            fn from_value(value: &nitrite::common::Value) -> nitrite::errors::NitriteResult<Self::Output> {
                match value {
                    nitrite::common::Value::Document(doc) => {
                        Ok(#name {
                            #(#default_initializers,)*
                        })
                    },
                    _ => {
                        return Err(nitrite::errors::NitriteError::new(
                            "Value is not a document",
                            nitrite::errors::ErrorKind::ObjectMappingError,
                        ))
                    },
                }
            }
        }
    };

    Ok(TokenStream::from(gen))
}

pub(crate) fn generate_convertible_for_enum(ast: &DeriveInput, data: &DataEnum) -> Result<TokenStream> {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let mut ignored_fields: Vec<String> = vec![];
    
    // Check if the enum has a `converter` attribute with ignored fields
    for attr in &ast.attrs {
        if attr.path().is_ident("converter") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("ignored") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    // Pre-allocate capacity for ignored fields
                    let field_str = s.value();
                    let field_list: Vec<&str> = field_str.split(',').collect();
                    ignored_fields.reserve(field_list.len());
                    for field in field_list {
                        ignored_fields.push(field.trim().to_string());
                    }
                }
                Ok(())
            })?;
        }
    }

    // Pre-allocate for variants to reduce reallocations
    let variant_count = data.variants.len();
    let mut to_value_variants = Vec::with_capacity(variant_count);
    let mut from_value_variants = Vec::with_capacity(variant_count);

    for variant in &data.variants {
        let variant_ident = &variant.ident;
        let variant_name = variant_ident.to_string();
        
        match &variant.fields {
            syn::Fields::Named(fields) => {
                // Filter fields efficiently in single pass
                let field_vec: Vec<_> = fields.named.iter().collect();
                let filtered_fields: Vec<_> = field_vec
                    .iter()
                    .filter(|f| {
                        f.ident.as_ref()
                            .map_or(false, |ident| !ignored_fields.contains(&ident.to_string()))
                    })
                    .copied()
                    .collect();

                let field_idents: Vec<_> = filtered_fields
                    .iter()
                    .filter_map(|f| f.ident.as_ref())
                    .collect();
                
                let field_names: Vec<String> = field_idents
                    .iter()
                    .map(|i| i.to_string())
                    .collect();

                // Get all fields for pattern matching
                let all_field_idents: Vec<_> = fields
                    .named
                    .iter()
                    .filter_map(|f| f.ident.as_ref())
                    .collect();

                to_value_variants.push(quote! {
                    #name::#variant_ident { #(ref #all_field_idents),* } => {
                        let mut doc = nitrite::collection::Document::new();
                        #(doc.put(#field_names, #field_idents.to_value()?)?;)*
                        let document = doc!{
                            "variant": (#variant_name),
                            "value": (nitrite::common::Value::Document(doc)),
                        };
                        Ok(nitrite::common::Value::from(document))
                    }
                });

                // from_value variant handling
                let from_field_idents: Vec<_> = fields
                    .named
                    .iter()
                    .filter_map(|f| f.ident.as_ref())
                    .collect();
                let from_field_names: Vec<String> = from_field_idents
                    .iter()
                    .map(|i| i.to_string())
                    .collect();
                let from_field_types: Vec<Type> = fields
                    .named
                    .iter()
                    .map(|f| f.ty.clone())
                    .collect();

                let field_initializers: Vec<_> = from_field_idents.iter()
                    .zip(from_field_names.iter())
                    .zip(from_field_types.iter())
                    .map(|((ident, name), ty)| {
                        if ignored_fields.contains(name) {
                            quote! { #ident: Default::default() }
                        } else {
                            quote! { #ident: nitrite::common::from_value::<#ty>(&data.get(#name)?)? }
                        }
                    })
                    .collect();

                from_value_variants.push(quote! {
                    #variant_name => {
                        let data = doc.get("value")?;
                        let data = data.as_document().expect("value is not a document");
                        Ok(#name::#variant_ident {
                            #(#field_initializers,)*
                        })
                    }
                });
            }
            syn::Fields::Unnamed(fields) => {
                let field_count = fields.unnamed.len();
                let field_idents: Vec<_> = (0..field_count)
                    .map(|i| Ident::new(&format!("field_{}", i), Span::call_site()))
                    .collect();
                
                to_value_variants.push(quote! {
                    #name::#variant_ident(#(#field_idents),*) => {
                        let mut array = Vec::with_capacity(#field_count);
                        #(array.push(#field_idents.to_value()?);)*
                        let document = doc!{
                            "variant": (#variant_name),
                            "value": (nitrite::common::Value::Array(array)),
                        };
                        Ok(nitrite::common::Value::from(document))
                    }
                });

                let field_indices: Vec<_> = (0..field_count).collect();
                let field_types: Vec<Type> = fields
                    .unnamed
                    .iter()
                    .map(|f| f.ty.clone())
                    .collect();
                
                from_value_variants.push(quote! {
                    #variant_name => {
                        let data = doc.get("value")?;
                        let data = data.as_array().expect("value is not an array");
                        Ok(#name::#variant_ident(
                            #(nitrite::common::from_value::<#field_types>(&data[#field_indices])?,)*
                        ))
                    }
                });
            }
            syn::Fields::Unit => {
                to_value_variants.push(quote! {
                    #name::#variant_ident => {
                        let document = doc!{
                            "variant": (#variant_name),
                            "value": (nitrite::common::Value::Null),
                        };
                        Ok(nitrite::common::Value::from(document))
                    }
                });

                from_value_variants.push(quote! {
                    #variant_name => Ok(#name::#variant_ident)
                });
            }
        }
    }

    let gen = quote! {
        impl #impl_generics nitrite::common::Convertible for #name #ty_generics #where_clause {
            type Output = Self;

            fn to_value(&self) -> nitrite::errors::NitriteResult<nitrite::common::Value> {
                match self {
                    #(#to_value_variants),*
                }
            }

            fn from_value(value: &nitrite::common::Value) -> nitrite::errors::NitriteResult<Self::Output> {
                match value {
                    nitrite::common::Value::Document(doc) => {
                        let variant = doc.get("variant")?;
                        match variant {
                            nitrite::common::Value::String(variant) => {
                                match variant.as_str() {
                                    #(#from_value_variants),*,
                                    _ => {
                                        Err(nitrite::errors::NitriteError::new(
                                            "Value is not a valid enum variant",
                                            nitrite::errors::ErrorKind::ObjectMappingError,
                                        ))
                                    }
                                }
                            },
                            _ => {
                                Err(nitrite::errors::NitriteError::new(
                                    "Value is not a valid enum variant",
                                    nitrite::errors::ErrorKind::ObjectMappingError,
                                ))
                            }
                        }
                    },
                    _ => {
                        return Err(nitrite::errors::NitriteError::new(
                            "Value is not a document",
                            nitrite::errors::ErrorKind::ObjectMappingError,
                        ))
                    }
                }
            }
        }
    };

    Ok(TokenStream::from(gen))
}