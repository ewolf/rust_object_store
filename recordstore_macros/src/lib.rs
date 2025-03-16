use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};
use proc_macro2::Ident;
use quote::ToTokens;

fn capitalize_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

#[proc_macro]
pub fn init_objects(_input: TokenStream) -> TokenStream {
    r#"
     use serde::{Serialize, Deserialize};
     use bincode;

     use std::any::Any;
     //use std::sync::RwLock;

     // Trait for objects that can be stored dynamically
     pub trait ObjectType {
         pub fn as_any(&self) -> &dyn Any; // Allows downcasting if needed
         pub fn to_bytes(&self) -> Vec<u8>;

         // was object type factory?
         pub fn name() -> String;
         pub fn create_from_bytes(bytes: &[u8]) -> Box<Self>;
     }

     // Separate trait for serialization/deserialization
     pub trait Serializable: Serialize + for<'de> Deserialize<'de> {}

     // Struct that stores a `Box<dyn ObjectType>`
     pub struct Obj<T: ObjectType> {
         pub id: u64,

         pub saved: bool,   // true if this object has ever been saved to the data store
         pub dirty: bool,   // true if this object needs to be saved to the data store

         pub data: Box<T>,
     }

     impl<T: ObjectType> Obj<T> {
         // Creates an Obj with any ObjectType implementation
         fn new(data_obj: T) -> Self {
             Obj { 
                 id: 0,
                 saved: false,
                 dirty: true,
                 data: Box::new(data_obj),
             }
         }

         // Creates an Obj with any ObjectType implementation
         fn new_from_boxed(data_boxed: Box<T>) -> Self {
             Obj { 
                 id: 0,
                 saved: false,
                 dirty: true,
                 data: data_boxed,
             }
         }

         // Creates an Obj from bytes
         fn from_bytes(bytes: &[u8], id: u64) -> Self {
             Obj::<T> { id, saved: false, dirty: true, data: T::create_from_bytes(bytes) }
         }

         fn to_bytes(&self) -> Vec<u8> {
             self.data.to_bytes()
         }
     }
    "#.parse().unwrap()
}


#[proc_macro_derive(Getters)]
pub fn derive_getters(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident; // Struct name

    // Ensure it's a struct with named fields
    let fields = if let syn::Data::Struct(ref data) = input.data {
        if let syn::Fields::Named(ref fields) = data.fields {
            &fields.named
        } else {
            return syn::Error::new_spanned(struct_name, "Only named fields are supported")
                .to_compile_error()
                .into();
        }
    } else {
        return syn::Error::new_spanned(struct_name, "Only structs are supported")
            .to_compile_error()
            .into();
    };

    // generate lookup_field method, first with ifs  if x == 'foo' {}, if {}
    let ifs = fields.iter().filter_map(|field| {
        if let Some(field_name) = &field.ident {
            let field_type = &field.ty;
            let cap_type = capitalize_first(&field_type.into_token_stream().to_string());
            if cap_type == "Bool" || cap_type == "I32" || cap_type == "I64" || cap_type == "U32" || cap_type == "U64" || cap_type == "F32" || cap_type == "F64" {
                let field_name_str = format!( "{}", field_name );
                let type_ident = Ident::new(&cap_type, proc_macro2::Span::call_site());
                if cap_type == "String" {
                    Some(quote! {
                        if name == #field_name_str {
                            return Some(ObjectTypeOption::#type_ident(String::from(&self.data.#field_name)));
                        }
                    })
                } else {
                    Some(quote! {
                        if name == #field_name_str {
                            return Some(ObjectTypeOption::#type_ident(self.data.#field_name));
                        }
                    })
                }
            } else {
                None
            }
        } else {
            None
        }
    });
    let lookup_field_fun = quote! {
        pub fn lookup_field(&self,name: String) -> Option<ObjectTypeOption> {
            #(#ifs)*
            None
        }
    };

    // Generate getter methods
    let getters = fields.iter().filter_map(|field| {
        if let Some(field_name) = &field.ident {
            let field_type = &field.ty;
            let getter_name = syn::Ident::new(&format!("get_{}", field_name), field_name.span());

            Some(quote! {
                pub fn #getter_name(&self) -> &#field_type {
                    &self.data.#field_name
                }
            })
        } else {
            None
        }
    });

    let struct_name_str = format!( "{}", struct_name );

    // Generate the `impl` block
    let expanded = quote! {
        impl Obj<#struct_name> {
            #(#getters)*
            #lookup_field_fun
        }
        impl ObjectType for #struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn to_bytes(&self) -> Vec<u8> {
                bincode::serialize(self).expect("Failed to Serialize")
            }

            fn name() -> String { #struct_name_str.to_string() }

            fn create_from_bytes(bytes: &[u8]) -> Box<Self> {
                let deserialized: Self = bincode::deserialize(bytes).expect("Failed to deserialize");
                Box::new(deserialized)
            }
        }

        impl Serializable for #struct_name {}

    };

    TokenStream::from(expanded)
}
