use crate::domain::provider::MarketData;
use crate::domain::models::MarketDataWithSymbol;
use chrono::NaiveDateTime;
use csv::Reader;
use serde::Deserialize;
use std::error::Error;
use std::fs;

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

pub fn read_csv_local(path: &str) -> Result<Vec<MarketDataWithSymbol>, Box<dyn Error>> {
    println!("Reading CSV from local path: {}", path);
    let mut data = Vec::new();
    let entries = fs::read_dir(path)?;

    for entry in entries {
        let entry = entry?;
        let file_path = entry.path();

        if !file_path.is_file() || file_path.extension().is_none_or(|ext| ext != "csv") {
            continue;
        }

        let mut reader = Reader::from_path(&file_path)?;
        println!("Successfully read records from file {:?}", &file_path);
        for result in reader.deserialize() {
            let record: CsvRecord = result?;
            data.push(record.into());
        }
    }
    println!("Successfully read {} records from CSV", data.len());
    Ok(data)
}
