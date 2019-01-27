extern crate proc_macro;
extern crate bifrost_hasher;

use proc_macro::TokenStream;
use bifrost_hasher::hash_str;
use proc_macro::TokenTree;
use proc_macro::TokenTree::Ident;

#[proc_macro]
pub fn hash_ident(item: TokenStream) -> TokenStream {
    let tokens: Vec<_> = item.into_iter().collect();
    if tokens.len() != 1 {
        panic!("argument should be a single identifier, but got {} arguments {:?}",
               tokens.len(), tokens);
    }
    let text = match tokens[0] {
        TokenTree::Ident(ref ident) => ident.to_string(),
        _ => tokens[0].to_string().trim().to_owned()
    };
    let text = &*text;
    let str = String::from(text);
    format!("{}", hash_str(&str)).parse().unwrap()
}