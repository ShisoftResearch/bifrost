#![feature(plugin_registrar, rustc_private)]

extern crate rustc;
extern crate rustc_plugin;
extern crate syntax;

use syntax::tokenstream::TokenTree;
use syntax::ext::base::{ExtCtxt, MacResult, DummyResult, MacEager};
use syntax::ext::build::AstBuilder;  // trait for expr_usize
use syntax::ext::quote::rt::Span;
use syntax::ast::{self, Ident, TraitRef, Ty, TyKind};
use syntax::ast::LitKind::Str;
use syntax::ast::MetaItemKind::NameValue;
use syntax::parse::{self, token, PResult};
use syntax::parse::parser::{Parser, PathStyle};
use syntax::parse::token::intern_and_get_ident;
use syntax::ptr::P;
use syntax::util::small_vector::SmallVector;
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

fn fn_args_ty(cx: &mut ExtCtxt, sp: Span, tts: &[TokenTree]) -> Box<MacResult + 'static> {
    let mut parser = parse::new_parser_from_tts(cx.parse_sess(), tts.into());
    let mut item = match parser.parse_trait_item() {
        Ok(s) => s,
        Err(mut diagnostic) => {
            diagnostic.emit();
            return DummyResult::any(sp);
        }
    };

    if let Err(mut diagnostic) = parser.expect(&token::Eof) {
        diagnostic.emit();
        return DummyResult::any(sp);
    }
    convert_args(&mut item.ident);
    MacEager::trait_items(SmallVector::one(item))
}

fn fn_args_ident(cx: &mut ExtCtxt, sp: Span, tts: &[TokenTree]) -> Box<MacResult + 'static> {
    match tts[0] {
        TokenTree::Token(_, token::Ident(t)) => {
            let mut t = t.clone();
            convert_args(&mut t);
            MacEager::expr(cx.expr_ident(sp, t))
        },
        _ => DummyResult::any(sp)
    }
}

fn convert_args(ident: &mut Ident) {
    let ident_str = ident.to_string();
    let mut nty = String::from("__");
    nty.push_str(&ident_str);
    nty.push_str("__ARGS__");
    *ident = Ident::with_empty_ctxt(token::intern(&nty));
}

#[plugin_registrar]
pub fn plugin_registrar(reg: &mut Registry) {
    reg.register_macro("hash_ident", hash_ident);
    reg.register_macro("fn_args_ty", fn_args_ty);
}