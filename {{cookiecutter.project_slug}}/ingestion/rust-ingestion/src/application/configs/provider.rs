use chrono::{DateTime, Duration, NaiveDate, Utc};
use config::Config;
use config::ConfigError;
use config::Environment;
use serde::Deserialize;
use thiserror::Error;

use crate::api::provider::Interval;

#[derive(Debug, Error)]
pub enum DateValidationError {
    #[error("start_date ({start}) must be before end_date ({end})")]
    StartAfterEnd {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },

    #[error("date range too large: maximum allowed is {max_days} days, got {actual_days} days")]
    RangeTooLarge { max_days: i64, actual_days: i64 },
}

fn default_url() -> String {
    "https://api.binance.com/api/v3/klines".to_string()
}

fn default_enabled() -> bool {
    true
}

fn default_interval() -> Interval {
    Interval::Minute1
}

fn default_end_date() -> DateTime<Utc> {
    Utc::now()
}

fn default_start_date() -> DateTime<Utc> {
    default_end_date() - Duration::days(1)
}

fn default_coin() -> String {
    "BTCUSDT".to_string()
}

fn default_real_time() -> bool {
    false
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let date_str = String::deserialize(deserializer)?;
    if !date_str.chars().all(|c| c.is_digit(10) || c == '-') {
        return Err(serde::de::Error::custom(format!(
            "Invalid date: '{}'. Expected format YYYY-MM-DD",
            date_str
        )));
    }

    NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
        .map_err(|e| serde::de::Error::custom(format!("Invalid date '{}': {}", date_str, e)))?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| serde::de::Error::custom("Erro conversion to DateTime"))?
        .and_local_timezone(Utc)
        .single()
        .ok_or_else(|| serde::de::Error::custom("Error conversion to UTC"))
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    pub api_key: String,

    pub api_secret: String,

    #[serde(default = "default_url")]
    pub url: String,

    #[serde(default = "default_interval")]
    pub interval: Interval,

    #[serde(default = "default_end_date")]
    #[serde(deserialize_with = "deserialize_date")]
    pub end_date: DateTime<Utc>,

    #[serde(default = "default_start_date")]
    #[serde(deserialize_with = "deserialize_date")]
    pub start_date: DateTime<Utc>,

    #[serde(default = "default_coin")]
    pub coin: String,

    #[serde(default = "default_real_time")]
    pub real_time: bool
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            api_key: "".to_string(),
            api_secret: "".to_string(),
            url: "".to_string(),
            enabled: true,
            interval: Interval::Day1,
            end_date: default_end_date(),
            start_date: default_start_date(),
            coin: default_coin(),
            real_time: default_real_time(),
        }
    }
}

impl ProviderConfig {
    pub fn new(prefix: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let source = Environment::with_prefix(prefix)
            .try_parsing(true)
            .prefix_separator("__");

        let config = match Config::builder().add_source(source).build() {
            Err(error) => return Err(Box::new(error)),
            Ok(value) => value,
        };
        let config: ProviderConfig = match config.try_deserialize() {
            Err(error) => return Err(Box::new(error)),
            Ok(value) => value,
        };

        config.validate_dates()?;

        Ok(config)
    }

    fn validate_dates(&self) -> Result<(), DateValidationError> {
        if self.start_date > self.end_date {
            return Err(DateValidationError::StartAfterEnd {
                start: self.start_date,
                end: self.end_date,
            });
        }

        let duration = self.end_date - self.start_date;
        let max_days = 365;

        if duration.num_days() > max_days {
            return Err(DateValidationError::RangeTooLarge {
                max_days,
                actual_days: duration.num_days(),
            });
        }

        Ok(())
    }
}

pub fn get_provider_config(prefix: &str) -> Result<ProviderConfig, ConfigError> {
    let config = match ProviderConfig::new(prefix) {
        Ok(config) => config,
        Err(err) => return Err(ConfigError::NotFound(err.to_string())),
    };
    Ok(config)
}
