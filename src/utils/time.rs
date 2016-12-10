use time;
use std::time::Duration;

pub fn get_time() -> u64 {
    let timespec = time::get_time();
    let mills: f64 = ((1000*1000*1000) as f64 * timespec.sec as f64 + timespec.nsec as f64)/(1000 * 1000) as f64;
    mills as u64
}

pub fn duration_to_ms(duration: Duration) -> u64 {
    let nanos = duration.subsec_nanos() as u64;
    (1000*1000*1000 * duration.as_secs() + nanos)/(1000 * 1000)
}
