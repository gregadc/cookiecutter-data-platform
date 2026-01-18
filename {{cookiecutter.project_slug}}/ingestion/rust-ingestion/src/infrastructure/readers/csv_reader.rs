use crate::domain::provider::MarketData;
use crate::domain::provider::MarketDataWithSymbol;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_csv_record_to_market_data_with_symbol() {
        let record = CsvRecord {
            symbol: "BTCUSDT".to_string(),
            timestamp: "2025-01-15 10:00:00".to_string(),
            open: 45000.50,
            high: 45500.75,
            low: 44800.25,
            close: 45200.00,
            volume: 1250.5,
        };

        let market_data: MarketDataWithSymbol = record.into();

        assert_eq!(market_data.symbol, "BTCUSDT");
        assert_eq!(market_data.data.open, 45000.50);
        assert_eq!(market_data.data.high, 45500.75);
        assert_eq!(market_data.data.low, 44800.25);
        assert_eq!(market_data.data.close, 45200.00);
        assert_eq!(market_data.data.volume, 1250.5);
    }

    #[test]
    fn test_csv_record_timestamp_formats() {
        // Test format: YYYY-MM-DD HH:MM:SS
        let record1 = CsvRecord {
            symbol: "BTCUSDT".to_string(),
            timestamp: "2025-01-15 10:00:00".to_string(),
            open: 45000.0,
            high: 45500.0,
            low: 44800.0,
            close: 45200.0,
            volume: 1250.0,
        };
        let data1: MarketDataWithSymbol = record1.into();
        assert_eq!(data1.symbol, "BTCUSDT");

        // Test format: YYYY-MM-DDTHH:MM:SS
        let record2 = CsvRecord {
            symbol: "ETHUSDT".to_string(),
            timestamp: "2025-01-15T10:00:00".to_string(),
            open: 3000.0,
            high: 3050.0,
            low: 2980.0,
            close: 3020.0,
            volume: 8500.0,
        };
        let data2: MarketDataWithSymbol = record2.into();
        assert_eq!(data2.symbol, "ETHUSDT");
    }

    #[test]
    fn test_read_csv_local_success() {
        // Create a temporary directory
        let temp_dir = tempdir().unwrap();
        let csv_path = temp_dir.path().join("test_data.csv");

        // Write test CSV data
        let mut file = fs::File::create(&csv_path).unwrap();
        writeln!(
            file,
            "symbol,timestamp,open,high,low,close,volume"
        )
        .unwrap();
        writeln!(
            file,
            "BTCUSDT,2025-01-15 10:00:00,45000.50,45500.75,44800.25,45200.00,1250.5"
        )
        .unwrap();
        writeln!(
            file,
            "ETHUSDT,2025-01-15 10:00:00,3000.25,3050.50,2980.00,3020.75,8500.25"
        )
        .unwrap();

        // Test reading CSV
        let result = read_csv_local(temp_dir.path().to_str().unwrap());
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.len(), 2);

        // Verify first record
        assert_eq!(data[0].symbol, "BTCUSDT");
        assert_eq!(data[0].data.open, 45000.50);
        assert_eq!(data[0].data.volume, 1250.5);

        // Verify second record
        assert_eq!(data[1].symbol, "ETHUSDT");
        assert_eq!(data[1].data.close, 3020.75);
    }

    #[test]
    fn test_read_csv_local_empty_directory() {
        let temp_dir = tempdir().unwrap();

        let result = read_csv_local(temp_dir.path().to_str().unwrap());
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn test_read_csv_local_mixed_files() {
        // Create a temporary directory with CSV and non-CSV files
        let temp_dir = tempdir().unwrap();

        // Create a CSV file
        let csv_path = temp_dir.path().join("data.csv");
        let mut csv_file = fs::File::create(&csv_path).unwrap();
        writeln!(csv_file, "symbol,timestamp,open,high,low,close,volume").unwrap();
        writeln!(
            csv_file,
            "BTCUSDT,2025-01-15 10:00:00,45000.0,45500.0,44800.0,45200.0,1250.0"
        )
        .unwrap();

        // Create a non-CSV file
        let txt_path = temp_dir.path().join("readme.txt");
        let mut txt_file = fs::File::create(&txt_path).unwrap();
        writeln!(txt_file, "This is not a CSV file").unwrap();

        // Test reading - should only read CSV files
        let result = read_csv_local(temp_dir.path().to_str().unwrap());
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].symbol, "BTCUSDT");
    }

    #[test]
    fn test_read_csv_local_invalid_path() {
        let result = read_csv_local("/nonexistent/path");
        assert!(result.is_err());
    }
}
