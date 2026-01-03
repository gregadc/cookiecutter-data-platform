use crate::domain::provider::MarketData;
use crate::domain::provider::MarketDataWithSymbol;
use chrono::NaiveDateTime;
use csv::Reader;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Deserialize)]
struct CsvRecord {
    symbol: String,
    timestamp: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl From<CsvRecord> for MarketDataWithSymbol {
    fn from(record: CsvRecord) -> Self {
        let datetime = NaiveDateTime::parse_from_str(&record.timestamp, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(&record.timestamp, "%Y-%m-%dT%H:%M:%S"))
            .expect("Invalid timestamp format in CSV");

        MarketDataWithSymbol {
            symbol: record.symbol,
            data: MarketData {
                timestamp: datetime.and_utc(),
                open: record.open,
                high: record.high,
                low: record.low,
                close: record.close,
                volume: record.volume,
            },
        }
    }
}

pub async fn read_csv_s3(uri: &str) -> Result<Vec<MarketDataWithSymbol>, Box<dyn Error>> {
    println!("Reading CSV from S3: {}", uri);

    let uri_clean = uri.trim_start_matches("s3://");
    let parts: Vec<&str> = uri_clean.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err("Invalid S3 URI format. Expected: s3://bucket/key".into());
    }

    let bucket = parts[0];
    let key = parts[1];

    println!("  Bucket: {}", bucket);
    println!("  Key: {}", key);

    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let resp = client.get_object().bucket(bucket).key(key).send().await?;

    let data = resp.body.collect().await?;
    let bytes = data.into_bytes();

    let cursor = std::io::Cursor::new(bytes);
    let mut reader = Reader::from_reader(cursor);
    let mut records = Vec::new();

    for result in reader.deserialize() {
        let record: CsvRecord = result?;
        records.push(record.into());
    }

    println!("Successfully read {} records from S3", records.len());
    Ok(records)
}
