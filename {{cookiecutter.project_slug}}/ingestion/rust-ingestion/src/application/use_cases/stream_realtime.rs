use crate::application::{Config, State};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Provider error: {0}")]
    Provider(String),
    #[error("Kafka not configured")]
    KafkaNotConfigured,
}

pub async fn execute(state: State, _config: Config) -> Result<(), StreamError> {
    println!("🔴 Starting real-time data streaming from Binance...");

    state
        .provider
        .fetch_real_time_data(state.kafka.clone())
        .await
        .map_err(|e| StreamError::Provider(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{Config, ProviderConfig, State};
    use crate::domain::provider::DataProvider;
    use crate::infrastructure::providers::binance::VoidProvider;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_stream_with_void_provider() {
        // Arrange
        let config = Config::default();
        let provider_config = ProviderConfig::default();
        let void_provider = VoidProvider::new(&provider_config);

        let state = State {
            config: config.clone(),
            provider: Box::new(void_provider),
            storage: None,
            kafka: None,
        };

        // Act
        let result = execute(state, config).await;

        // Assert
        assert!(result.is_ok());
    }
}
