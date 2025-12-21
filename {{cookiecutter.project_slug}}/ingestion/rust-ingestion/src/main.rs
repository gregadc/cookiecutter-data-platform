use rust_ingestion::api::provider::Interval;
use rust_ingestion::application;
use rust_ingestion::api::redis::OhlcRecord;
use rust_ingestion::infrastructure::{read_csv_local, read_csv_s3};

use futures::StreamExt;
use std::time::Instant;
use std::collections::HashMap;

const BATCH_SIZE: usize = 100;

fn get_date_format(interval: &Interval) -> &'static str {
    match interval {
        Interval::Minute1 | Interval::Minute5 => "%Y-%m-%dT%H:%M",
        Interval::Hour1 => "%Y-%m-%dT%H",
        Interval::Day1 => "%Y-%m-%d",
        Interval::Month1 => "%Y-%m",
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    println!("🚀 Starting data collection process...");

    let config = application::get_config().map_err(|error| {
        eprintln!("❌ Failed to load worker config: {}", error);
        error
    })?;

    let state = application::get_state(&config).await.map_err(|error| {
        eprintln!("❌ Failed to load worker state: {}", error);
        error
    })?;

    if config.redis.enabled {
        if let Some(storage) = state.storage {
            match storage.get_connection().await {
                Ok(mut conn) => {
                    println!("📊 Fetching historical data from: {:?}", config.data_source.source_type);
                    
                    let date_format = get_date_format(&config.provider.interval);
                    
                    match config.data_source.source_type.as_str() {
                        "binance" => {
                            let historical_data = state.provider.fetch_historical_data().await?;
                            let mut current_batch = Vec::with_capacity(BATCH_SIZE);
                            let mut total_processed = 0;

                            for data in historical_data {
                                let record = OhlcRecord {
                                    timestamp: data.timestamp.format(date_format).to_string(),
                                    open: data.open,
                                    high: data.high,
                                    low: data.low,
                                    close: data.close,
                                    volume: data.volume,
                                };
                                    
                                current_batch.push(record);

                                if current_batch.len() >= BATCH_SIZE {
                                    storage.set_batch_ohlc_records(&mut conn, &config.provider.coin, &current_batch).await?;
                                    total_processed += current_batch.len();
                                    println!("✅ Processed batch of {} items for {} (total: {})", BATCH_SIZE, config.provider.coin, total_processed);
                                    current_batch.clear();
                                }
                            }

                            if !current_batch.is_empty() {
                                storage.set_batch_ohlc_records(&mut conn, &config.provider.coin, &current_batch).await?;
                                total_processed += current_batch.len();
                                println!("✅ Processed final batch for {}", config.provider.coin);
                            }

                            println!("📈 Total processed: {}", total_processed);
                            
                        },
                        "local_csv" | "s3_csv" => {
                            let data_with_symbols = if config.data_source.source_type == "local_csv" {
                                read_csv_local(&config.data_source.csv_path)?
                            } else {
                                read_csv_s3(&config.data_source.s3_uri).await?
                            };

                            let mut batches_by_symbol: HashMap<String, Vec<OhlcRecord>> = HashMap::new();

                            for item in data_with_symbols {
                                let record = OhlcRecord {
                                    timestamp: item.data.timestamp.format(date_format).to_string(),
                                    open: item.data.open,
                                    high: item.data.high,
                                    low: item.data.low,
                                    close: item.data.close,
                                    volume: item.data.volume,
                                };

                                batches_by_symbol
                                    .entry(item.symbol.clone())
                                    .or_insert_with(Vec::new)
                                    .push(record);
                            }

                            let mut total_processed = 0;
                            for (symbol, records) in batches_by_symbol {
                                println!("📝 Processing {} records for {}", records.len(), symbol);
                                
                                for chunk in records.chunks(BATCH_SIZE) {
                                    storage.set_batch_ohlc_records(&mut conn, &symbol, chunk).await?;
                                    total_processed += chunk.len();
                                    println!("✅ Processed batch of {} items for {} (total: {})", chunk.len(), symbol, total_processed);
                                }
                            }

                            println!("📈 Total processed: {}", total_processed);
                        },
                        _ => {
                            eprintln!("❌ Unknown data source type: {}", config.data_source.source_type);
                            return Err("Invalid data source".into());
                        }
                    }

                    let duration = start_time.elapsed();
                    println!("✨ Collection completed in {:.2?}", duration);
                }
                Err(err) => eprintln!("❌ Error redis connection: {:?}", err),
            }
        }
    }
    else if config.data_source.use_realtime {
        println!("Starting real-time data streaming from Binance...");
        state.provider.fetch_real_time_data(state.kafka.clone()).await?;
        
    }
    else {
        eprintln!("❌ RedisManager not configured.");
    }
    Ok(())
}