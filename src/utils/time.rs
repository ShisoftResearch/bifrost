use std::time::{SystemTime, Duration, UNIX_EPOCH};

pub fn get_time() -> u64 {
    //Get current time
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    //Calculate milliseconds
    since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000
}

pub fn duration_to_ms(duration: Duration) -> u64 {
    let nanos = duration.subsec_nanos() as u64;
    (1000*1000*1000 * duration.as_secs() + nanos)/(1000 * 1000)
}
