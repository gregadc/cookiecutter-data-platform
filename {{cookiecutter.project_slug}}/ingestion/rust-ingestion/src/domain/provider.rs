use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

use crate::domain::event::EventProducer;

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
    async fn fetch_historical_data(&self) -> Result<Vec<MarketData>, ProviderError>;

    fn get_supported_symbols(&self) -> Vec<String>;
    async fn get_information_account(&self) -> Result<AccountInformation, ProviderError>;
    async fn fetch_real_time_data(
        &self,
        kafka: Option<Arc<dyn EventProducer>>,
    ) -> Result<Vec<MarketData>, ProviderError>;
}

#[derive(Debug)]
pub struct MarketDataWithSymbol {
    pub symbol: String,
    pub data: MarketData,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_market_data_serialization() {
        let market_data = MarketData {
            timestamp: Utc::now(),
            open: 45000.0,
            high: 46000.0,
            low: 44000.0,
            close: 45500.0,
            volume: 1250.5,
        };

        // Test serialization
        let json = serde_json::to_string(&market_data).unwrap();
        assert!(json.contains("open"));
        assert!(json.contains("45000"));

        // Test deserialization
        let deserialized: MarketData = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.open, 45000.0);
        assert_eq!(deserialized.high, 46000.0);
        assert_eq!(deserialized.volume, 1250.5);
    }

    #[test]
    fn test_interval_deserialization() {
        let json_1m = r#""1m""#;
        let interval: Interval = serde_json::from_str(json_1m).unwrap();
        assert!(matches!(interval, Interval::Minute1));

        let json_5m = r#""5m""#;
        let interval: Interval = serde_json::from_str(json_5m).unwrap();
        assert!(matches!(interval, Interval::Minute5));

        let json_1h = r#""1h""#;
        let interval: Interval = serde_json::from_str(json_1h).unwrap();
        assert!(matches!(interval, Interval::Hour1));

        let json_1d = r#""1d""#;
        let interval: Interval = serde_json::from_str(json_1d).unwrap();
        assert!(matches!(interval, Interval::Day1));

        let json_1M = r#""1M""#;
        let interval: Interval = serde_json::from_str(json_1M).unwrap();
        assert!(matches!(interval, Interval::Month1));
    }

    #[test]
    fn test_provider_error_display() {
        let request_error = ProviderError::RequestError("Connection failed".to_string());
        assert_eq!(request_error.to_string(), "API request failed: Connection failed");

        let data_error = ProviderError::DataError("Invalid JSON".to_string());
        assert_eq!(data_error.to_string(), "Invalid data received: Invalid JSON");
    }

    #[test]
    fn test_balance_deserialization() {
        let json = r#"{
            "asset": "BTC",
            "free": "1.5",
            "locked": "0.5"
        }"#;

        let balance: Balance = serde_json::from_str(json).unwrap();
        assert_eq!(balance.asset, "BTC");
        assert_eq!(balance.free, "1.5");
        assert_eq!(balance.locked, "0.5");
    }

    #[test]
    fn test_market_data_with_symbol() {
        let market_data = MarketData {
            timestamp: Utc::now(),
            open: 45000.0,
            high: 46000.0,
            low: 44000.0,
            close: 45500.0,
            volume: 1250.5,
        };

        let with_symbol = MarketDataWithSymbol {
            symbol: "BTCUSDT".to_string(),
            data: market_data,
        };

        assert_eq!(with_symbol.symbol, "BTCUSDT");
        assert_eq!(with_symbol.data.open, 45000.0);
    }
}
