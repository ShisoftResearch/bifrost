use syn::{Ident, Type, parse_macro_input, punctuated::Punctuated, Token, TypeTuple, TypeReference, parse::{ParseStream, ParseBuffer, Parse}, Result, FnArg, Pat, PatType, ItemTrait, TraitItem, TraitItemFn, Lifetime};
use quote::quote;
use proc_macro::TokenStream;

struct Args {
    args: Punctuated<FnArg, Token![,]>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> Result<Self> {
        let args = Punctuated::parse_terminated(input)?;
        Ok(Args { args })
    }
}

#[proc_macro]
pub fn adjust_caller_identifiers(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as Args);
    let output = input.args.into_iter().map(|arg| {
        match arg {
            FnArg::Typed(pat_type) => {
                let pat = &*pat_type.pat;
                let ty = &*pat_type.ty;

                match (&pat, ty) {
                    (Pat::Ident(pat_ident), Type::Reference(_)) => {
                        let ident = &pat_ident.ident;
                        quote! { ref #ident }
                    },
                    (Pat::Ident(pat_ident), Type::Group(group)) => {
                        let ident = &pat_ident.ident;
                        if let Type::Reference(_) = &*group.elem {
                            quote! { ref #ident }
                        } else {
                            quote! { #ident }
                        }
                    },
                    (Pat::Ident(pat_ident), _) => {
                        let ident = &pat_ident.ident;
                        quote! { #ident }
                    },
                    _ => panic!("Unsupported pattern!"),
                }
            },
            _ => panic!("Variadic arguments are not supported!"),
        }
    }).collect::<Vec<_>>();

    quote! {
        ( #(#output),* )
    }.into()
}

#[proc_macro]
pub fn adjust_function_signature(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as TraitItemFn);
    let mut output_trait_fn = input.clone();
    let sig = &mut output_trait_fn.sig;
    // eprintln!("Adjust {:?}", sig);
    for input in &mut sig.inputs {
        match input {
            FnArg::Typed(pat_type) => {
                // eprintln!("Checking lifetime {:?}", pat_type);
                match *pat_type.ty {
                    Type::Reference(ref mut ref_type) => {
                        if ref_type.lifetime.is_none() {
                            ref_type.lifetime = Some(Lifetime::new("'a", proc_macro2::Span::call_site()));
                            //eprintln!("Assigning lifetime {:?}", ref_type);
                        }
                    },
                    Type::Group(ref mut group) => {
                        if let Type::Reference(ref mut ref_type) = &mut *group.elem {
                            if ref_type.lifetime.is_none() {
                                ref_type.lifetime = Some(Lifetime::new("'a", proc_macro2::Span::call_site()));
                                //eprintln!("Assigning lifetime {:?}", ref_type);
                            } 
                        }
                    }
                    _ => {}
                }
            },
            FnArg::Receiver(ref mut receiver) => {
                if let &mut Some((_, ref mut lifetime)) = &mut receiver.reference {
                    if lifetime.is_none() {
                        *lifetime = Some(Lifetime::new("'a", proc_macro2::Span::call_site()));
                    }
                }
            }
            _ => {}
        }
    }

    quote!(#output_trait_fn).into()
}


#[proc_macro]
pub fn deref_tuple_types(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as TypeTuple);

    let transformed_types: Vec<_> = input.elems.into_iter().map(|ty| {
        match ty {
            Type::Reference(TypeReference { elem, .. }) => *elem,
            Type::Group(group) => {
                if let Type::Reference(TypeReference { elem, .. }) = *group.elem {
                    *elem
                } else {
                    Type::Group(group)
                }
            },
            other => other,
        }
    }).collect();

    let tokens = quote! { (#(#transformed_types),*) };
    tokens.into()
}
