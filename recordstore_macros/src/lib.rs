use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};
use proc_macro2::{Ident, Span};
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
            if cap_type == "Bool" || 
                cap_type == "I32" || 
                cap_type == "I64" || 
                cap_type == "U32" || 
                cap_type == "U64" || 
                cap_type == "F32" || 
                cap_type == "F64" || 
                cap_type == "Reference" || 
                cap_type == "String" 
            {
                let field_name_str = format!( "{}", field_name );
                let type_ident = Ident::new(&cap_type, Span::call_site());
                if cap_type == "String" {
                    Some(quote! {
                        if name == #field_name_str {
                            return Some(ObjTypeOption::#type_ident(String::from(&data.#field_name)));
                        }
                    })
                } else {
                    Some(quote! {
                        if name == #field_name_str {
                            return Some(ObjTypeOption::#type_ident(data.#field_name));
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
        fn lookup_field(&self,name: String) -> Option<ObjTypeOption> {
            let data:& #struct_name = self.data.as_any().downcast_ref::<#struct_name>().expect("unable to downcast");
            #(#ifs)*
            None
        }
    };

    // Generate getter methods
    let getters = fields.iter().filter_map(|field| {
        if let Some(field_name) = &field.ident {
            let field_type = &field.ty;
            let getter_name = syn::Ident::new(&format!("get_{}", field_name), Span::call_site());
            let mutgetter_name = syn::Ident::new(&format!("getmut_{}", field_name), Span::call_site());
            let setter_name = syn::Ident::new(&format!("set_{}", field_name), Span::call_site());

            Some(quote! {
                fn #getter_name(&self) -> &#field_type {
                    let data:& #struct_name = self.data.as_any().downcast_ref::<#struct_name>().expect("unable to downcast");
                    &data.#field_name
                }
                fn #mutgetter_name(&mut self) -> & mut #field_type {
                    &mut self.data.#field_name
                }
                fn #setter_name(&mut self,val: #field_type) {
                    self.dirty = true;
                    self.data.#field_name = val;
                }
            })
        } else {
            None
        }
    });

    // generate trait ext method prototypes
    let exts = fields.iter().filter_map(|field| {
        if let Some(field_name) = &field.ident {
            let field_type = &field.ty;
            let getter_name = syn::Ident::new(&format!("get_{}", field_name), Span::call_site());
            let mutgetter_name = syn::Ident::new(&format!("getmut_{}", field_name), Span::call_site());
            let setter_name = syn::Ident::new(&format!("set_{}", field_name), Span::call_site());

            Some(quote! {
                fn #getter_name(&self) -> &#field_type;
                fn #mutgetter_name(&mut self) -> &mut #field_type;
                fn #setter_name(&mut self,val: #field_type);
            })
        } else {
            None
        }
    });

    let struct_name_str = format!( "{}", struct_name );
    let ext_name = syn::Ident::new(&format!( "{}Ext", struct_name ), Span::call_site());
    

    // Generate the `impl` block
    let expanded = quote! {
        trait #ext_name {
            #(#exts)*
            fn lookup_field(&self, name: String) -> Option<ObjTypeOption>;
        }
        impl #ext_name for Obj<#struct_name> {
            #(#getters)*
            #lookup_field_fun
        }
        impl ObjType for #struct_name {
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
