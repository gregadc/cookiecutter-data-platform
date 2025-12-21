use std::sync::Arc;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::infrastructure::kafka_producer::KafkaProducer;

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("API request failed: {0}")]
    RequestError(String),
    #[error("Invalid data received: {0}")]
    DataError(String),
}

/*#[derive(Error, Debug)]
pub enum ProviderError {
    RequestError(String),
    DataError(String),
    AuthenticationError(String),
}*/
#[derive(Debug, Deserialize)]
pub struct Balance {
    pub asset: String,
    pub free: String,
    pub locked: String,
}

#[derive(Debug, Deserialize)]
pub struct AccountInformation {
    pub maker_commission: i32,
    pub taker_commission: i32,
    pub buyer_commission: i32,
    pub seller_commission: i32,
    pub can_trade: bool,
    pub can_withdraw: bool,
    pub can_deposit: bool,
    pub balances: Vec<Balance>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Interval {
    #[serde(rename = "1m")]
    Minute1,

    #[serde(rename = "5m")]
    Minute5,

    #[serde(rename = "1h")]
    Hour1,

    #[serde(rename = "1d")]
    Day1,

    #[serde(rename = "1M")]
    Month1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarketData {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/*impl From<serde_json::Error> for ProviderError {
    fn from(err: serde_json::Error) -> Self {
        ProviderError::DataError(err.to_string())
    }
}*/

#[async_trait::async_trait]
pub trait DataProvider {
    async fn fetch_historical_data(&self) -> Result<Vec<MarketData>, ProviderError>;

    fn get_supported_symbols(&self) -> Vec<String>;
    async fn get_information_account(&self) -> Result<AccountInformation, ProviderError>;
    async fn fetch_real_time_data(&self, kafka: Option<Arc<KafkaProducer>>) -> Result<Vec<MarketData>, ProviderError>;
}
