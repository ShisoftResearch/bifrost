#![feature(plugin_registrar, rustc_private)]

extern crate rustc;
extern crate rustc_plugin;
extern crate syntax;

use syntax::parse::token;
use syntax::tokenstream::TokenTree;
use syntax::ext::base::{ExtCtxt, MacResult, DummyResult, MacEager};
use syntax::ext::build::AstBuilder;  // trait for expr_usize
use syntax::ext::quote::rt::Span;
use syntax::ast::Ident;
use rustc_plugin::Registry;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

fn hash_ident (cx: &mut ExtCtxt, sp: Span, args: &[TokenTree]) -> Box<MacResult + 'static> {
    if args.len() != 1 {
        cx.span_err(
            sp,
            &format!("argument should be a single identifier, but got {} arguments", args.len()));
        return DummyResult::any(sp);
    }

    let text = match args[0] {
        TokenTree::Token(_, token::Ident(s)) => s.to_string(),
        _ => {
            cx.span_err(sp, "argument should be a single identifier");
            return DummyResult::any(sp);
        }
    };
    let text = &*text;
    let mut hasher = DefaultHasher::default();
    let text_bytes = String::from(text).into_bytes();
    let text_bytes = text_bytes.as_slice();
    hasher.write(&text_bytes);
    MacEager::expr(cx.expr_usize(sp, hasher.finish() as usize))
}

#[plugin_registrar]
pub fn plugin_registrar(reg: &mut Registry) {
    reg.register_macro("hash_ident", hash_ident);
}