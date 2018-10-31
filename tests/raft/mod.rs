use std::{thread, time};

mod callback;
mod primary;

pub fn wait() {
    thread::sleep(time::Duration::from_secs(5))
}
