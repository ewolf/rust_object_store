use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro]
pub fn init_objects(_input: TokenStream) -> TokenStream {
    r#"
     use serde::{Serialize, Deserialize};
     use bincode;

     use std::any::Any;
     use std::sync::RwLock;

     // Trait for objects that can be stored dynamically
     pub trait ObjectType {
         fn as_any(&self) -> &dyn Any; // Allows downcasting if needed
         fn to_bytes(&self) -> Vec<u8>;
     }

     // Separate trait for serialization/deserialization
     pub trait Serializable: Serialize + for<'de> Deserialize<'de> {}

     // Factory trait for creating objects dynamically
     pub trait ObjectTypeFactory {
         fn name() -> String;
         fn create_from_bytes(bytes: &[u8]) -> Box<dyn ObjectType>;
     }


     // Struct that stores a `Box<dyn ObjectType>`
     pub struct Obj {
         pub id: u64,

         saved: bool,   // true if this object has ever been saved to the data store
         dirty: bool,   // true if this object needs to be saved to the data store

         pub data: Box<dyn ObjectType>,
     }

     impl Obj {
         // Creates an Obj with any ObjectType implementation
         fn new<T: ObjectType + ObjectTypeFactory + 'static>(data_obj: T) -> Self {
             Obj { 
                 id: 0,
                 saved: false,
                 dirty: true,
                 data: Box::new(data_obj),
             }
         }

         // Creates an Obj from bytes
         fn from_bytes<T: ObjectType + ObjectTypeFactory + 'static>(bytes: &[u8], id: u64) -> Self {
             Obj { id, saved: false, dirty: true, data: T::create_from_bytes(bytes) }
         }

         fn to_bytes<T: ObjectTypeFactory>(&self) -> Vec<u8> {
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

    // Generate getter methods
    let getters = fields.iter().filter_map(|field| {
        if let Some(field_name) = &field.ident {
            let field_type = &field.ty;
            let getter_name = syn::Ident::new(&format!("get_{}", field_name), field_name.span());

            Some(quote! {
                fn #getter_name(&self) -> &#field_type {
                    &self.#field_name
                }
            })
        } else {
            None
        }
    });

    // Generate the `impl` block
    let expanded = quote! {
        impl #struct_name {
            #(#getters)*
        }
        impl ObjectType for #struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn to_bytes(&self) -> Vec<u8> {
                bincode::serialize(self).expect("Failed to Serialize")
            }
        }

        impl Serializable for #struct_name {}

        impl ObjectTypeFactory for #struct_name {
            fn name() -> String { "#struct_name".to_string() }
            fn create_from_bytes(bytes: &[u8]) -> Box<dyn ObjectType> {
                let deserialized: #struct_name = bincode::deserialize(bytes).expect("Failed to deserialize");
                Box::new(deserialized)
            }
        }
    };

    TokenStream::from(expanded)
}
