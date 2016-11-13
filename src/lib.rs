#![feature(plugin)]
#![plugin(bifrost_plugins)]
#![crate_type = "lib"]

pub mod rpc;

extern crate mio;
extern crate byteorder;
extern crate slab;

#[macro_use]
extern crate log;
extern crate env_logger;