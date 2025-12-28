
use config::Config;
use config::ConfigError;
use config::Environment;
use serde::Deserialize;

fn default_enabled() -> bool {
    true
}

fn default_uri() -> String {
    String::from("redis://localhost:6379/0")
}

fn default_channel() -> String {
    String::from("channel")
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default = "default_uri")]
    pub uri: String,

    #[serde(default = "default_channel")]
    pub channel: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            channel: default_channel(),
            enabled: default_enabled(),
            uri: default_uri(),
        }
    }
}

pub fn get_redis_config(prefix: &str) -> Result<RedisConfig, ConfigError> {
    let source = Environment::with_prefix(prefix)
        .try_parsing(true)
        .prefix_separator("__");
    let config = Config::builder().add_source(source).build()?;
    let config: RedisConfig = config.try_deserialize()?;
    Ok(config)
}
