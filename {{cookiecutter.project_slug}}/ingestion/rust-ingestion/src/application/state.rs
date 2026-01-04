use std::sync::Arc;

use crate::application::Config;
use crate::domain::event::EventProducer;
use crate::domain::provider::DataProvider;
use crate::infrastructure::kafka_producer::KafkaProducer;
use crate::infrastructure::providers::binance::{BinanceProvider, VoidProvider};
use crate::infrastructure::storage::redis::RedisManager;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("Kafka error: {0}")]
    Kafka(String),
}

pub struct State {
    pub config: Config,
    pub provider: Box<dyn DataProvider>,
    pub storage: Option<Arc<RedisManager>>,
    pub kafka: Option<Arc<dyn EventProducer>>,
}

pub async fn get_state(config: &Config) -> Result<State, StateError> {
    let kafka: Option<Arc<dyn EventProducer>> = if config.kafka.enabled {
        match KafkaProducer::new(&config.kafka.brokers, &config.kafka.topic) {
            Ok(producer) => Some(Arc::new(producer) as Arc<dyn EventProducer>),
            Err(e) => return Err(StateError::Kafka(e.to_string())),
        }
    } else {
        None
    };

    /*let data = &config.provider {
        BinanceProvider::new(conf_provider.clone()).boxed()
    } else {
        BinanceProvider::new().boxed()
    };*/

    let provider: Box<dyn DataProvider> = if config.provider.enabled {
        Box::new(BinanceProvider::new(config.provider.clone()))
    } else {
        println!(" VoidProvider connection");
        Box::new(VoidProvider::new(&config.provider.clone()))
    };

    let storage = match RedisManager::new(&config.redis.clone()) {
        Ok(manager) => Some(Arc::new(manager)),
        Err(_) => {
            eprintln!("❌ Error initializing Redis.");
            None
        }
    };

    let config = config.clone();

    Ok(State {
        config,
        provider,
        storage,
        kafka,
    })
}
