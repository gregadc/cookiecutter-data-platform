use config::{Config, ConfigError, Environment};
use serde::Deserialize;

// Defaults pour DataSource
fn default_source_type() -> String {
    String::from("binance") // ou "local_csv"
}

fn default_csv_path() -> String {
    String::from("data/orders.csv")
}

fn default_s3_uri() -> String {
    String::from("s3://my-bucket/data/orders.csv")
}

fn default_use_realtime() -> bool {
    false
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataSourceConfig {
    #[serde(default = "default_source_type")]
    pub source_type: String, // "local_csv", "s3_csv", or "binance"
    
    #[serde(default = "default_csv_path")]
    pub csv_path: String,
    
    #[serde(default = "default_s3_uri")]
    pub s3_uri: String,

    #[serde(default = "default_use_realtime")]
    pub use_realtime: bool,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            source_type: default_source_type(),
            csv_path: default_csv_path(),
            s3_uri: default_s3_uri(),
            use_realtime: default_use_realtime(),
        }
    }
}

pub fn get_datasource_config(prefix: &str) -> Result<DataSourceConfig, ConfigError> {
    let source = Environment::with_prefix(prefix)
        .try_parsing(true)
        .prefix_separator("__");
    
    let config = Config::builder()
        .add_source(source)
        .build()?;
    
    config.try_deserialize()
}