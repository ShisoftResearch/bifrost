#![crate_type = "lib"]
#![feature(plugin)]
#![feature(integer_atomics)]
#![feature(proc_macro)]
#![feature(fnbox)]
#![feature(conservative_impl_trait, generators)]
#![feature(box_syntax)]
#![feature(use_extern_macros)]
#![feature(proc_macro_hygiene)]

#[cfg(disable_shortcut)]
pub static DISABLE_SHORTCUT: bool = true;

#[cfg(not(disable_shortcut))]
pub static DISABLE_SHORTCUT: bool = false;

#[macro_use]
pub mod utils;
pub mod tcp;
#[macro_use]
pub mod rpc;
#[macro_use]
pub mod raft;
pub mod conshash;
pub mod membership;
pub mod store;
pub mod vector_clock;
pub mod durable_queue;

extern crate byteorder;

extern crate futures_await as futures;
extern crate futures_cpupool;
extern crate parking_lot;
extern crate thread_id;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_middleware;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_timer;

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate num_cpus;
extern crate rand;
extern crate threadpool;

#[macro_use]
extern crate lazy_static;

extern crate bifrost_hasher;
extern crate bifrost_plugins;

extern crate crc32fast;
