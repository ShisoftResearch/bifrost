use tokio::time::delay_for;
use std::time::Duration;

mod callback;
mod primary;

pub async fn wait() {
    delay_for(Duration::from_secs(5)).await
}
