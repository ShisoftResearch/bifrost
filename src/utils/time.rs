use std::time::Duration;
use std::time::SystemTime;
use tokio::time::delay_for;

pub fn get_time() -> i64 {
    //Get current time
    let current_time = SystemTime::now();
    let duration = current_time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    //Calculate milliseconds
    return duration_to_ms(duration) as i64;
}

pub fn duration_to_ms(duration: Duration) -> u64 {
    let nanos = duration.subsec_nanos() as u64;
    (1000 * 1000 * 1000 * duration.as_secs() + nanos) / (1000 * 1000)
}

pub async fn async_wait(duration: Duration) {
    delay_for(duration).await;
}

pub async fn async_wait_5_secs() {
    async_wait(Duration::from_secs(2)).await;
}
