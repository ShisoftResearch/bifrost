#![crate_type = "lib"]
#![feature(plugin)]
#![feature(integer_atomics)]
#![feature(proc_macro)]
#![feature(box_syntax)]
#![feature(use_extern_macros)]
#![feature(proc_macro_hygiene)]
#![feature(trait_alias)]

// #[cfg(disable_shortcut)]
pub static DISABLE_SHORTCUT: bool = true;
//
// #[cfg(not(disable_shortcut))]
// pub static DISABLE_SHORTCUT: bool = false;

#[macro_use]
pub mod utils;
pub mod tcp;
#[macro_use]
pub mod rpc;
#[macro_use]
pub mod raft;
pub mod conshash;
pub mod durable_queue;
pub mod membership;
pub mod store;
pub mod vector_clock;

#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;
