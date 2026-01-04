use async_trait::async_trait;
use std::error::Error;

#[async_trait]
pub trait EventProducer: Send + Sync {
    async fn send(&self, key: &str, payload: &str) -> Result<(), Box<dyn Error>>;
}
