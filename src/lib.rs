#![plugin(bifrost_plugins)]
#![crate_type = "lib"]

#![feature(plugin)]
#![feature(integer_atomics)]
#![feature(collections_bound)]
#![feature(proc_macro)]
#![feature(btree_range)]

pub mod tcp;
#[macro_use]
pub mod rpc;
#[macro_use]
pub mod raft;
pub mod store;

mod utils;

extern crate mio;
extern crate byteorder;

extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_middleware;
extern crate tokio_timer;
extern crate futures;

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate rand;
extern crate time;
extern crate threadpool;
extern crate num_cpus;

#[macro_use]
extern crate lazy_static;

extern crate bifrost_plugins;
extern crate bifrost_hasher;
