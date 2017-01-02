use time;
use std::time::Duration;

pub fn get_time() -> i64 {
    //Get current time
    let current_time = time::get_time();
    //Calculate milliseconds
    (current_time.sec as i64 * 1000) + (current_time.nsec as i64 / 1000 / 1000)
}

pub fn duration_to_ms(duration: Duration) -> u64 {
    let nanos = duration.subsec_nanos() as u64;
    (1000*1000*1000 * duration.as_secs() + nanos)/(1000 * 1000)
}
