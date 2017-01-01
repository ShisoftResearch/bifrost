use std::{thread, time};

mod primary;

pub fn wait() {
    thread::sleep(time::Duration::from_secs(5))
}