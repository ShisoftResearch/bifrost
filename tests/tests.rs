#![feature(plugin)]
#![plugin(bifrost_plugins)]

#![feature(proc_macro)]

#[macro_use]
extern crate bifrost;
extern crate byteorder;
extern crate bincode;
extern crate futures;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate lazy_static;
extern crate parking_lot;


#[cfg(disable_shortcut)]
mod rpc;
mod raft;
mod store;
mod membership;
mod utils;
mod conshash;
mod vector_clock;

mod mutex {
    use std::sync::Mutex;
    use std::collections::{HashMap};

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