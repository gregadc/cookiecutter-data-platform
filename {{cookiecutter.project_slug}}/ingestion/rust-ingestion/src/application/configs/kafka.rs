use config::Config;
use config::ConfigError;
use config::Environment;
use serde::Deserialize;
use serde::Deserializer;

fn default_enabled() -> bool {
    true
}

fn default_brokers() -> String {
    String::from("localhost:9092")
}

fn default_certs_location() -> Option<String> {
    None
}

fn default_topic() -> String {
    String::from("crypto-prices")
}

fn deserialize_certs_location<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let certs: Option<String> = Option::deserialize(deserializer)?;
    Ok(certs.filter(|s| !s.is_empty()))
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default = "default_brokers")]
    pub brokers: String,

    #[serde(default = "default_certs_location")]
    #[serde(deserialize_with = "deserialize_certs_location")]
    pub certs_location: Option<String>,

    #[serde(default = "default_topic")]
    pub topic: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            certs_location: default_certs_location(),
            enabled: default_enabled(),
            brokers: default_brokers(),
            topic: default_topic(),
        }
    }
}

pub fn get_kafka_config(prefix: &str) -> Result<KafkaConfig, ConfigError> {
    let source = Environment::with_prefix(prefix)
        .try_parsing(true)
        .prefix_separator("__");
    let config = match Config::builder().add_source(source).build() {
        Err(error) => return Err(error),
        Ok(value) => value,
    };
    let config: KafkaConfig = match config.try_deserialize() {
        Err(error) => return Err(error),
        Ok(value) => value,
    };
    Ok(config)
}
