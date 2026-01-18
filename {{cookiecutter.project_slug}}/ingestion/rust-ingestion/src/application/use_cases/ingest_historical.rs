use crate::application::{Config, State};
use crate::domain::provider::{Interval, MarketData};
use crate::infrastructure::readers::{read_csv_local, read_csv_s3};
use crate::infrastructure::storage::redis::OhlcRecord;
use std::collections::HashMap;
use std::time::Instant;
use thiserror::Error;

const BATCH_SIZE: usize = 100;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("Provider error: {0}")]
    Provider(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Invalid data source: {0}")]
    InvalidSource(String),
    #[error("Redis connection error: {0}")]
    RedisConnection(String),
}

pub struct IngestStats {
    pub total_processed: usize,
    pub duration_secs: f64,
}

pub async fn execute(state: State, config: Config) -> Result<IngestStats, IngestError> {
    let start_time = Instant::now();

    let storage = state.storage.as_ref().ok_or_else(|| {
        IngestError::Storage("Storage not configured".to_string())
    })?;

    let mut conn = storage.get_connection().await.map_err(|e| {
        IngestError::RedisConnection(e.to_string())
    })?;

    println!("📊 Fetching historical data from: {:?}", config.data_source.source_type);

    let date_format = get_date_format(&config.provider.interval);
    let total_processed = match config.data_source.source_type.as_str() {
        "binance" => {
            process_binance_data(&state, &storage, &mut conn, &config.provider.coin, date_format).await?
        }
        "local_csv" | "s3_csv" => {
            process_csv_data(&config, &storage, &mut conn, date_format).await?
        }
        _ => {
            return Err(IngestError::InvalidSource(
                config.data_source.source_type.clone()
            ));
        }
    };

    let duration = start_time.elapsed();
    println!("✨ Collection completed in {:.2?}", duration);

    Ok(IngestStats {
        total_processed,
        duration_secs: duration.as_secs_f64(),
    })
}

async fn process_binance_data(
    state: &State,
    storage: &std::sync::Arc<crate::infrastructure::storage::redis::RedisManager>,
    conn: &mut deadpool_redis::Connection,
    coin: &str,
    date_format: &str,
) -> Result<usize, IngestError> {
    let historical_data = state
        .provider
        .fetch_historical_data()
        .await
        .map_err(|e| IngestError::Provider(e.to_string()))?;

    let batches = batch_market_data(&historical_data, date_format);
    let total = batches.iter().map(|b| b.len()).sum();

    for (idx, batch) in batches.iter().enumerate() {
        storage
            .set_batch_ohlc_records(conn, coin, batch)
            .await
            .map_err(|e| IngestError::Storage(e.to_string()))?;

        println!(
            "✅ Processed batch {} for {} ({} items)",
            idx + 1,
            coin,
            batch.len()
        );
    }

    println!("📈 Total processed: {}", total);
    Ok(total)
}

async fn process_csv_data(
    config: &Config,
    storage: &crate::infrastructure::storage::redis::RedisManager,
    conn: &mut deadpool_redis::Connection,
    date_format: &str,
) -> Result<usize, IngestError> {
    let data_with_symbols = if config.data_source.source_type == "local_csv" {
        read_csv_local(&config.data_source.csv_path)
            .map_err(|e| IngestError::Provider(e.to_string()))?
    } else {
        read_csv_s3(&config.data_source.s3_uri)
            .await
            .map_err(|e| IngestError::Provider(e.to_string()))?
    };

    let mut batches_by_symbol: HashMap<String, Vec<OhlcRecord>> = HashMap::new();

    for item in data_with_symbols {
        let record = market_data_to_ohlc_record(&item.data, date_format);
        batches_by_symbol
            .entry(item.symbol.clone())
            .or_default()
            .push(record);
    }

    let mut total_processed = 0;
    for (symbol, records) in batches_by_symbol {
        println!("📝 Processing {} records for {}", records.len(), symbol);

        for chunk in records.chunks(BATCH_SIZE) {
            storage
                .set_batch_ohlc_records(conn, &symbol, chunk)
                .await
                .map_err(|e| IngestError::Storage(e.to_string()))?;

            total_processed += chunk.len();
        }
    }

    println!("📈 Total processed: {}", total_processed);
    Ok(total_processed)
}

fn get_date_format(interval: &Interval) -> &'static str {
    match interval {
        Interval::Minute1 | Interval::Minute5 => "%Y-%m-%dT%H:%M",
        Interval::Hour1 => "%Y-%m-%dT%H",
        Interval::Day1 => "%Y-%m-%d",
        Interval::Month1 => "%Y-%m",
    }
}

fn market_data_to_ohlc_record(data: &MarketData, date_format: &str) -> OhlcRecord {
    OhlcRecord {
        timestamp: data.timestamp.format(date_format).to_string(),
        open: data.open,
        high: data.high,
        low: data.low,
        close: data.close,
        volume: data.volume,
    }
}

fn batch_market_data(data: &[MarketData], date_format: &str) -> Vec<Vec<OhlcRecord>> {
    data.chunks(BATCH_SIZE)
        .map(|chunk| {
            chunk
                .iter()
                .map(|d| market_data_to_ohlc_record(d, date_format))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_get_date_format_minute1() {
        assert_eq!(get_date_format(&Interval::Minute1), "%Y-%m-%dT%H:%M");
    }

    #[test]
    fn test_get_date_format_day1() {
        assert_eq!(get_date_format(&Interval::Day1), "%Y-%m-%d");
    }

    #[test]
    fn test_batch_market_data_multiple_batches() {
        let data: Vec<MarketData> = (0..250)
            .map(|i| MarketData {
                timestamp: Utc::now(),
                open: i as f64,
                high: i as f64,
                low: i as f64,
                close: i as f64,
                volume: i as f64,
            })
            .collect();

        let batches = batch_market_data(&data, "%Y-%m-%d");

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 100);
        assert_eq!(batches[1].len(), 100);
        assert_eq!(batches[2].len(), 50);
    }
}
