use crate::application::RedisConfig;
use deadpool_redis::{
    redis::{cmd, AsyncCommands},
    Config, Pool, Runtime,
};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub struct RedisInitializationError;

impl fmt::Display for RedisInitializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to initialize Redis")
    }
}

impl Error for RedisInitializationError {}

#[derive(Debug, Clone)]
pub struct OhlcRecord {
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug)]
pub struct RedisManager {
    pool: Arc<Pool>,
}

impl RedisManager {
    pub fn new(config: &RedisConfig) -> Result<Self, RedisInitializationError> {
        let cfg = Config::from_url(&config.uri);
        let pool = cfg
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .map_err(|_| RedisInitializationError)?;

        Ok(RedisManager {
            pool: Arc::new(pool),
        })
    }

    pub async fn get_connection(&self) -> Result<deadpool_redis::Connection, Box<dyn Error>> {
        let conn = self.pool.get().await?;
        Ok(conn)
    }

    pub async fn set_value(
        &self,
        conn: &mut deadpool_redis::Connection,
        key: &str,
        value: &str,
    ) -> Result<(), Box<dyn Error>> {
        cmd("SET")
            .arg(&[key, value])
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    pub async fn get_value(
        &self,
        conn: &mut deadpool_redis::Connection,
        key: &str,
    ) -> Result<(), Box<dyn Error>> {
        let value: String = cmd("GET").arg(&[key]).query_async(conn).await?;
        println!("Value: {:?}", value);
        Ok(())
    }

    pub async fn set_batch_values(
        &self,
        conn: &mut deadpool_redis::Connection,
        batch: &[(String, String)],
    ) -> Result<(), Box<dyn Error>> {
        let mut pipe = deadpool_redis::redis::pipe();

        for (key, value) in batch {
            pipe.cmd("SET").arg(key).arg(value);
        }

        // pipe.query_async(conn).await?;
        pipe.query_async::<()>(conn).await?;
        Ok(())
    }

    pub async fn set_ohlc_record(
        &self,
        conn: &mut deadpool_redis::Connection,
        symbol: &str,
        record: &OhlcRecord,
    ) -> Result<(), Box<dyn Error>> {
        let key = format!("{}-{}", symbol, record.timestamp);

        //conn.hset_multiple(&key, &[
        conn.hset_multiple::<_, _, _, ()>(
            &key,
            &[
                ("open", record.open.to_string()),
                ("high", record.high.to_string()),
                ("low", record.low.to_string()),
                ("close", record.close.to_string()),
                ("volume", record.volume.to_string()),
                ("timestamp", record.timestamp.clone()),
            ],
        )
        .await?;

        Ok(())
    }

    pub async fn set_batch_ohlc_records(
        &self,
        conn: &mut deadpool_redis::Connection,
        symbol: &str,
        records: &[OhlcRecord],
    ) -> Result<(), Box<dyn Error>> {
        let mut pipe = deadpool_redis::redis::pipe();

        for record in records {
            let key = format!("{}-{}", symbol, record.timestamp);
            pipe.hset_multiple(
                &key,
                &[
                    ("open", record.open.to_string()),
                    ("high", record.high.to_string()),
                    ("low", record.low.to_string()),
                    ("close", record.close.to_string()),
                    ("volume", record.volume.to_string()),
                    ("timestamp", record.timestamp.clone()),
                ],
            );
        }

        // pipe.query_async(conn).await?;
        pipe.query_async::<()>(conn).await?;
        Ok(())
    }

    pub async fn get_ohlc_record(
        &self,
        conn: &mut deadpool_redis::Connection,
        key: &str,
    ) -> Result<OhlcRecord, Box<dyn Error>> {
        let data: Vec<(String, String)> = conn.hgetall(key).await?;

        let mut record = OhlcRecord {
            timestamp: String::new(),
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
        };

        for (field, value) in data {
            match field.as_str() {
                "open" => record.open = value.parse()?,
                "high" => record.high = value.parse()?,
                "low" => record.low = value.parse()?,
                "close" => record.close = value.parse()?,
                "volume" => record.volume = value.parse()?,
                "timestamp" => record.timestamp = value,
                _ => {}
            }
        }

        Ok(record)
    }
}
