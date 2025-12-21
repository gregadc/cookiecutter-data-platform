use config::{Config as ConfigRS, ConfigError, Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub binance: BinanceConfig,
    pub yahoo: YahooConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BinanceConfig {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct YahooConfig {
    pub api_key: Option<String>,
}

impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        let config_dir = std::env::var("CONFIG_DIR").unwrap_or_else(|_| "config".to_string());
        
        let builder = ConfigRS::builder()
            .add_source(File::from(Path::new(&config_dir).join("default.yaml")).required(false))
            .add_source(
                File::from(
                    Path::new(&config_dir)
                        .join(format!("{}.yaml", std::env::var("RUN_ENV").unwrap_or_else(|_| "development".to_string())))
                )
                .required(false)
            )
            .add_source(Environment::with_prefix("APP").separator("_"));

        builder.build()?.try_deserialize()
    }
}
