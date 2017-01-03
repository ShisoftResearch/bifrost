#![feature(plugin_registrar, rustc_private)]

extern crate rustc;
extern crate rustc_plugin;
extern crate syntax;
extern crate bifrost_hasher;

use syntax::tokenstream::TokenTree;
use syntax::ext::base::{ExtCtxt, MacResult, DummyResult, MacEager};
use syntax::ext::build::AstBuilder;  // trait for expr_usize
use syntax::ext::quote::rt::Span;
use syntax::ast::{self, Ident, TraitRef, Ty, TyKind};
use syntax::ast::LitKind::Str;
use syntax::ast::MetaItemKind::NameValue;
use syntax::parse::{self, token, PResult};
use syntax::parse::parser::{Parser, PathStyle};
use syntax::ptr::P;
use syntax::util::small_vector::SmallVector;
use rustc_plugin::Registry;
use bifrost_hasher::hash_str;

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
    let str = String::from(text);
    MacEager::expr(cx.expr_usize(sp, hash_str(str) as usize))
}

#[plugin_registrar]
pub fn plugin_registrar(reg: &mut Registry) {
    reg.register_macro("hash_ident", hash_ident);
}