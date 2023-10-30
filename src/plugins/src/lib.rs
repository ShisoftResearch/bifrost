extern crate proc_macro;
extern crate bifrost_hasher;
extern crate syn;

use proc_macro::TokenStream;
use bifrost_hasher::hash_str;
use proc_macro::TokenTree;
use syn::{parse_macro_input, LitStr};

#[proc_macro]
pub fn hash_ident(item: TokenStream) -> TokenStream {
    let item_clone = item.clone();
    let tokens: Vec<_> = item.into_iter().collect();
    if tokens.len() != 1 {
        panic!("argument should be a single identifier, but got {} arguments {:?}",
               tokens.len(), tokens);
    }
    let text = match tokens[0] {
        TokenTree::Ident(ref ident) => ident.to_string(),
        TokenTree::Literal(_) => parse_macro_input!(item_clone as LitStr).value(),
        _ => panic!("argument only support ident or string literal, found '{:?}', parsing {:?}", tokens, tokens[0])
    };
    let text = &*text;
    let str = String::from(text);
    format!("{}", hash_str(&str)).parse().unwrap()
}