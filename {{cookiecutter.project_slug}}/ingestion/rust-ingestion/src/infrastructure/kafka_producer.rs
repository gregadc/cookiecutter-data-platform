use crate::domain::event::EventProducer;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::error::Error;
use std::time::Duration;

pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaProducer {
    pub fn new(brokers: &str, topic: &str) -> Result<Self, Box<dyn Error>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl EventProducer for KafkaProducer {
    async fn send(&self, key: &str, payload: &str) -> Result<(), Box<dyn Error>> {
        let record = FutureRecord::to(&self.topic).payload(payload).key(key);

        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|(e, _)| e)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use std::sync::Arc;

    // Mock EventProducer for testing
    mock! {
        pub TestEventProducer {}

        #[async_trait]
        impl EventProducer for TestEventProducer {
            async fn send(&self, key: &str, payload: &str) -> Result<(), Box<dyn Error>>;
        }
    }

    #[tokio::test]
    async fn test_mock_event_producer_success() {
        let mut mock_producer = MockTestEventProducer::new();

        mock_producer
            .expect_send()
            .times(1)
            .withf(|key: &str, payload: &str| {
                key == "BTCUSDT" && payload.contains("45000")
            })
            .returning(|_, _| Ok(()));

        let result = mock_producer.send("BTCUSDT", r#"{"price": 45000}"#).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mock_event_producer_error() {
        let mut mock_producer = MockTestEventProducer::new();

        mock_producer
            .expect_send()
            .times(1)
            .returning(|_, _| Err("Connection failed".into()));

        let result = mock_producer.send("BTCUSDT", r#"{"price": 45000}"#).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_event_producer_multiple_calls() {
        let mut mock_producer = MockTestEventProducer::new();

        mock_producer
            .expect_send()
            .times(3)
            .returning(|_, _| Ok(()));

        assert!(mock_producer.send("BTCUSDT", r#"{"price": 45000}"#).await.is_ok());
        assert!(mock_producer.send("ETHUSDT", r#"{"price": 3000}"#).await.is_ok());
        assert!(mock_producer.send("DOTUSDT", r#"{"price": 25}"#).await.is_ok());
    }

    #[test]
    fn test_kafka_producer_with_arc() {
        // Test that KafkaProducer can be wrapped in Arc for thread safety
        let producer = KafkaProducer {
            producer: rdkafka::config::ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .create()
                .unwrap(),
            topic: "test-topic".to_string(),
        };

        let arc_producer: Arc<KafkaProducer> = Arc::new(producer);
        assert_eq!(arc_producer.topic, "test-topic");
    }

    #[test]
    fn test_kafka_producer_creation_valid() {
        // Test successful creation with valid configuration
        let result = KafkaProducer::new("localhost:9092", "test-topic");
        assert!(result.is_ok());

        let producer = result.unwrap();
        assert_eq!(producer.topic, "test-topic");
    }
}
