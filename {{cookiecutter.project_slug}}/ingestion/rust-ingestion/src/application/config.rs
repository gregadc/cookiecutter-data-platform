use config::ConfigError;

use crate::application::configs::get_datasource_config;
use crate::application::configs::get_kafka_config;
use crate::application::configs::get_provider_config;
use crate::application::configs::get_redis_config;
use crate::application::configs::DataSourceConfig;
use crate::application::configs::KafkaConfig;
use crate::application::configs::ProviderConfig;
use crate::application::configs::RedisConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub provider: ProviderConfig,
    pub redis: RedisConfig,
    pub data_source: DataSourceConfig,
}

pub fn get_config() -> Result<Config, ConfigError> {
    let kafka = get_kafka_config("KAFKA")?;
    let provider: ProviderConfig = get_provider_config("PROVIDER")?;
    let redis: RedisConfig = get_redis_config("STORAGE")?;
    let data_source = get_datasource_config("DATASOURCE")?;

    Ok(Config {
        kafka,
        provider,
        redis,
        data_source,
    })
}
