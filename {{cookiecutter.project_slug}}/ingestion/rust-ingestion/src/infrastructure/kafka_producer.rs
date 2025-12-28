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

    pub async fn send(&self, key: &str, payload: &str) -> Result<(), Box<dyn Error>> {
        let record = FutureRecord::to(&self.topic).payload(payload).key(key);

        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|(e, _)| e)?;

        Ok(())
    }
}
