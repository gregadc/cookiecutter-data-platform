use std::sync::Arc;

use crate::application::ProviderConfig;
use crate::domain::event::EventProducer;
use crate::domain::provider::{
    AccountInformation, DataProvider, Interval, MarketData, ProviderError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json;

use futures_util::StreamExt;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use url::Url;

#[derive(Debug)]
pub struct BinanceProvider {
    api_key: String,
    api_secret: String,
    url: String,
    client: Client,
    end_date: DateTime<Utc>,
    start_date: DateTime<Utc>,
    coin: String,
    interval: Interval,
}

pub struct VoidProvider {}

impl VoidProvider {
    pub fn new(_config: &ProviderConfig) -> Self {
        Self {}
    }
}

#[async_trait]
impl DataProvider for VoidProvider {
    async fn fetch_historical_data(&self) -> Result<Vec<MarketData>, ProviderError> {
        Ok(vec![MarketData {
            timestamp: Utc::now(),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
        }])
    }

    async fn fetch_real_time_data(
        &self,
        _kafka: Option<Arc<dyn EventProducer>>,
    ) -> Result<Vec<MarketData>, ProviderError> {
        Ok(vec![MarketData {
            timestamp: Utc::now(),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
        }])
    }

    fn get_supported_symbols(&self) -> Vec<String> {
        vec!["".to_string()]
    }
    async fn get_information_account(&self) -> Result<AccountInformation, ProviderError> {
        Ok(AccountInformation {
            maker_commission: 1,
            taker_commission: 1,
            buyer_commission: 1,
            seller_commission: 1,
            can_trade: true,
            can_withdraw: true,
            can_deposit: true,
            balances: vec![],
        })
    }
}

impl BinanceProvider {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            api_key: config.api_key,
            api_secret: config.api_secret,
            url: config.url,
            client: Client::new(),
            end_date: config.end_date,
            start_date: config.start_date,
            coin: config.coin,
            interval: config.interval,
        }
    }
}

pub struct KlineData {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub close: f64,
}

#[async_trait]
impl DataProvider for BinanceProvider {
    async fn fetch_historical_data(&self) -> Result<Vec<MarketData>, ProviderError> {
        let interval_str = match self.interval {
            Interval::Minute1 => "1m",
            Interval::Minute5 => "5m",
            Interval::Hour1 => "1h",
            Interval::Day1 => "1d",
            Interval::Month1 => "1M",
        };

        let url = format!(
            "{}?symbol={}&interval={}&startTime={}&endTime={}",
            &self.url,
            self.coin,
            interval_str,
            self.start_date.timestamp_millis(),
            self.end_date.timestamp_millis()
        );

        let response = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ProviderError::RequestError(e.to_string()))?;

        let data: Vec<Vec<serde_json::Value>> = response
            .json()
            .await
            .map_err(|e| ProviderError::DataError(e.to_string()))?;

        let market_data = data
            .into_iter()
            .map(|candle| MarketData {
                timestamp: DateTime::from_timestamp(
                    candle[0].as_i64().unwrap_or_default() / 1000,
                    0,
                )
                .unwrap_or_default(),
                open: candle[1]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default(),
                high: candle[2]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default(),
                low: candle[3]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default(),
                close: candle[4]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default(),
                volume: candle[5]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or_default(),
            })
            .collect();

        Ok(market_data)
    }

    async fn fetch_real_time_data(
        &self,
        kafka: Option<Arc<dyn EventProducer>>,
    ) -> Result<Vec<MarketData>, ProviderError> {
        let pairs = ["btcusdt", "dotusdt", "ethusdt", "adausdt", "xrpusdt"];
        let streams: Vec<String> = pairs.iter().map(|p| format!("{}@kline_1m", p)).collect();

        let stream_url = format!(
            "wss://stream.binance.com:9443/stream?streams={}",
            streams.join("/")
        );
        let url =
            Url::parse(&stream_url).map_err(|e| ProviderError::RequestError(e.to_string()))?;

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| ProviderError::RequestError(e.to_string()))?;

        let (_, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            use tokio::time::{sleep, Duration};
            sleep(Duration::from_secs(5)).await; // temporary

            let msg = msg.map_err(|e| ProviderError::DataError(e.to_string()))?;

            if msg.is_text() {
                let text = msg
                    .to_text()
                    .map_err(|e| ProviderError::DataError(e.to_string()))?;

                let v: Value = serde_json::from_str(text)
                    .map_err(|e| ProviderError::DataError(e.to_string()))?;

                let data = &v["data"]["k"];

                let symbol = data["s"].as_str().unwrap_or("UNKNOWN");
                let open = data["o"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                let high = data["h"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                let low = data["l"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                let close = data["c"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                let volume = data["v"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                let timestamp_ms = data["t"].as_i64().unwrap_or(0);

                let timestamp =
                    DateTime::from_timestamp(timestamp_ms / 1000, 0).unwrap_or_else(Utc::now);

                let payload = serde_json::json!({
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {"field": "symbol", "type": "string"},
                            {"field": "timestamp", "type": "string"},
                            {"field": "open", "type": "double"},
                            {"field": "high", "type": "double"},
                            {"field": "low", "type": "double"},
                            {"field": "close", "type": "double"},
                            {"field": "volume", "type": "double"}
                        ]
                    },
                    "payload": {
                        "symbol": symbol,
                        "timestamp": timestamp.to_rfc3339(),
                        "open": open,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume
                    }
                });

                println!(
                    "📊 {} -> open: {}, high: {}, low: {}, close: {}, volume: {}",
                    symbol, open, high, low, close, volume
                );

                if let Some(ref kafka_producer) = kafka {
                    let json_payload = serde_json::to_string(&payload)
                        .map_err(|e| ProviderError::DataError(e.to_string()))?;
                    kafka_producer
                        .send(symbol, &json_payload)
                        .await
                        .map_err(|e| ProviderError::DataError(e.to_string()))?;
                }
            }
        }

        Ok(vec![])
    }

    fn get_supported_symbols(&self) -> Vec<String> {
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    }

    async fn get_information_account(&self) -> Result<AccountInformation, ProviderError> {
        // hmac security
        //let params = "recvWindow=5000";
        //let response = self.authenticated_request("/account", params).await?;
        let url = format!("{}/account", &self.url);
        let response = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ProviderError::RequestError(e.to_string()))?;

        let account_info: AccountInformation = response
            .json()
            .await
            .map_err(|e| ProviderError::DataError(e.to_string()))?;

        Ok(account_info)
    }

    /*fn generate_signature(&self, query_string: &str) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let full_query = format!("{}&timestamp={}", query_string, timestamp);

        // create signature HMAC-SHA256
        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(full_query.as_bytes());

        mac.finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }

    async fn authenticated_request(&self, endpoint: &str, params: &str) -> Result<Response, ProviderError> {
        let signature = self.generate_signature(params);

        let full_url = format!(
            "{}{}?{}&signature={}",
            self.base_url,
            endpoint,
            params,
            signature
        );

        self.client
            .get(&full_url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ProviderError::RequestError(e.to_string()))
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_void_provider_fetch_historical_data() {
        let config = ProviderConfig {
            interval: Interval::Minute1,
            ..Default::default()
        };

        let provider = VoidProvider::new(&config);
        let result = provider.fetch_historical_data().await;

        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].open, 0.0);
        assert_eq!(data[0].close, 0.0);
    }

    #[tokio::test]
    async fn test_void_provider_fetch_real_time_data() {
        let config = ProviderConfig {
            interval: Interval::Minute1,
            ..Default::default()
        };

        let provider = VoidProvider::new(&config);
        let result = provider.fetch_real_time_data(None).await;

        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), 1);
    }

    #[tokio::test]
    async fn test_void_provider_get_supported_symbols() {
        let config = ProviderConfig {
            interval: Interval::Minute1,
            ..Default::default()
        };

        let provider = VoidProvider::new(&config);
        let symbols = provider.get_supported_symbols();

        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0], "");
    }

    #[tokio::test]
    async fn test_void_provider_get_information_account() {
        let config = ProviderConfig {
            interval: Interval::Minute1,
            ..Default::default()
        };

        let provider = VoidProvider::new(&config);
        let result = provider.get_information_account().await;

        assert!(result.is_ok());
        let account = result.unwrap();
        assert_eq!(account.maker_commission, 1);
        assert_eq!(account.taker_commission, 1);
        assert!(account.can_trade);
        assert_eq!(account.balances.len(), 0);
    }

    #[test]
    fn test_binance_provider_creation() {
        let config = ProviderConfig {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            url: "https://api.binance.com".to_string(),
            coin: "BTCUSDT".to_string(),
            interval: Interval::Minute1,
            ..Default::default()
        };

        let provider = BinanceProvider::new(config);
        assert_eq!(provider.api_key, "test_key");
        assert_eq!(provider.api_secret, "test_secret");
        assert_eq!(provider.coin, "BTCUSDT");
    }

    #[test]
    fn test_binance_provider_get_supported_symbols() {
        let config = ProviderConfig {
            coin: "BTCUSDT".to_string(),
            interval: Interval::Minute1,
            ..Default::default()
        };

        let provider = BinanceProvider::new(config);
        let symbols = provider.get_supported_symbols();

        assert!(symbols.contains(&"BTCUSDT".to_string()));
        assert!(symbols.contains(&"ETHUSDT".to_string()));
    }
}
