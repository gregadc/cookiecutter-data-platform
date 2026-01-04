use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("API request failed: {0}")]
    RequestError(String),
    #[error("Invalid data received: {0}")]
    DataError(String),
}

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

#[derive(Debug, Clone, Copy)]
pub enum Interval {
    Minute1,
    Minute5,
    Hour1,
    Day1,
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

#[async_trait::async_trait]
pub trait DataProvider {
    async fn fetch_historical_data(
        &self,
        symbol: &str,
        interval: Interval,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<MarketData>, ProviderError>;

    fn get_supported_symbols(&self) -> Vec<String>;
    async fn get_information_account(&self) -> Result<AccountInformation, ProviderError>;
}

#[derive(Debug)]
pub struct MarketDataWithSymbol {
    pub symbol: String,
    pub data: MarketData,
}
