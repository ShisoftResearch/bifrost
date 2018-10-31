#![feature(plugin)]
#![plugin(bifrost_plugins)]
#![feature(proc_macro)]
#![feature(box_syntax)]
#![feature(conservative_impl_trait)]

#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;
extern crate bincode;
extern crate byteorder;
extern crate futures_await as futures;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate lazy_static;
extern crate parking_lot;

#[macro_use]
extern crate log;
extern crate env_logger;

mod conshash;
mod membership;
mod raft;
#[cfg(disable_shortcut)]
mod rpc;
mod store;
mod utils;
mod vector_clock;

mod mutex {
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[test]
    fn mutable() {
        let l = Mutex::new(HashMap::new());
        {
            let mut m = l.lock().unwrap();
            m.insert(1, 2);
        }
        {
            let mut m = l.lock().unwrap();
            assert_eq!(m.get(&1), Some(&2));
        }
    }

    //    #[test]
    //    fn reentering_lock() { //FAIL
    //        let l = Mutex::new(false);
    //        {
    //            println!("Locking A");
    //            let a = l.lock().unwrap();
    //            println!("Locked A");
    //            {
    //                println!("Locking B");
    //                let b = l.lock().unwrap();
    //                println!("Locked B");
    //            }
    //            println!("Unlocked B");
    //        }
    //        println!("Unlocked A");
    //    }
}

mod hasher {

    use bifrost_hasher::hash_bytes_secondary;

    #[test]
    fn secondary() {
        println!("{}", hash_bytes_secondary(&[1u8, 10]));
        println!("{}", hash_bytes_secondary(&[2u8, 20]));
    }
}
