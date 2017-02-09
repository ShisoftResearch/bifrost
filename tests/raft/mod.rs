use std::{thread, time};

mod primary;
mod callback;

pub fn wait() {
    thread::sleep(time::Duration::from_secs(2))
}