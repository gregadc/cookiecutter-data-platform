use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use actix_web_prometheus::PrometheusMetricsBuilder;
use actix_cors::Cors;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::info;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

mod metrics_middleware;

use prometheus::{Encoder, Registry, TextEncoder};
use metrics_middleware::MetricsMiddleware;

#[derive(Deserialize)]
struct Pagination {
    page: Option<u32>,
    limit: Option<u32>,
}

#[derive(Serialize, sqlx::FromRow)]
struct StagingOhlc {
    symbol: String,
    timestamp: DateTime<Utc>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

#[derive(Serialize, sqlx::FromRow)]
struct DailyOhlc {
    symbol: String,
    timestamp: DateTime<Utc>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

#[actix_web::get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "message": "Hello",
        "version": "0.1.0"
    }))
}

#[actix_web::get("/api/raw/count")]
async fn get_raw_count(pool: web::Data<PgPool>) -> impl Responder {
    let result = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_ohlc")
        .fetch_one(pool.get_ref())
        .await;

    match result {
        Ok(count) => HttpResponse::Ok().json(serde_json::json!({
            "count": count
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Database error: {}", e)
        })),
    }
}

#[actix_web::get("/api/staging")]
async fn get_staging(
    pool: web::Data<PgPool>,
    query: web::Query<Pagination>,
) -> impl Responder {
    let page = query.page.unwrap_or(1);
    let limit = query.limit.unwrap_or(50).min(100);
    let offset = (page - 1) * limit;

    info!("Fetching staging: page={}, limit={}", page, limit);

    let result = sqlx::query_as::<_, StagingOhlc>(
        "SELECT symbol, timestamp, open, high, low, close, volume 
         FROM stg_ohlc 
         ORDER BY timestamp DESC 
         LIMIT $1 OFFSET $2",
    )
    .bind(limit as i64)
    .bind(offset as i64)
    .fetch_all(pool.get_ref())
    .await;

    match result {
        Ok(data) => HttpResponse::Ok().json(serde_json::json!({
            "page": page,
            "limit": limit,
            "count": data.len(),
            "data": data
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Database error: {}", e)
        })),
    }
}

#[actix_web::get("/api/daily")]
async fn get_daily(
    pool: web::Data<PgPool>,
    query: web::Query<Pagination>,
) -> impl Responder {
    let page = query.page.unwrap_or(1);
    let limit = query.limit.unwrap_or(50).min(100);
    let offset = (page - 1) * limit;

    let result = sqlx::query_as::<_, DailyOhlc>(
        "SELECT symbol, day AS timestamp, open, high, low, close, volume
         FROM daily_ohlc
         ORDER BY day DESC, symbol
         LIMIT $1 OFFSET $2",
    )
    .bind(limit as i64)
    .bind(offset as i64)
    .fetch_all(pool.get_ref())
    .await;

    match result {
        Ok(data) => HttpResponse::Ok().json(serde_json::json!({
            "page": page,
            "limit": limit,
            "count": data.len(),
            "data": data
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Database error: {}", e)
        })),
    }
}

#[actix_web::post("/api/trigger")]
async fn trigger_pipeline() -> impl Responder {
    info!("Triggering Airflow pipeline");
    
    let client = reqwest::Client::new();
    let airflow_url = std::env::var("AIRFLOW_URL")
        .unwrap_or_else(|_| "http://airflow-webserver:8081".to_string());
    // let airflow_url = "http://localhost:8081/api/v1/dags/crypto_ingestion_pipeline/dagRuns";
    
    let response = client
        .post(airflow_url)
        .basic_auth("admin", Some("admin"))
        .json(&serde_json::json!({"conf": {}}))
        .send()
        .await;
    
    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                info!("Pipeline triggered successfully");
                HttpResponse::Ok().json(serde_json::json!({
                    "status": "success",
                    "message": "Pipeline triggered successfully"
                }))
            } else {
                let status = resp.status();
                let error_body = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                info!("Failed to trigger pipeline: {} - {}", status, error_body);
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Airflow error: {} - {}", status, error_body)
                }))
            }
        },
        Err(e) => {
            info!("Failed to connect to Airflow: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": format!("Connection error: {}", e)
            }))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("actix_web=info,sqlx=warn")
        .init();

    let registry = Registry::new();
    let metrics = MetricsMiddleware::new(registry.clone());

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://admin:admin@localhost:5434/admin".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect");

    info!("Connected to DB");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();

        App::new()
            .app_data(web::Data::new(pool.clone()))
            .wrap(cors)
            .wrap(metrics.clone())
            .route("/metrics", web::get().to({
                let registry = registry.clone();
                move || {
                    let registry = registry.clone();
                    async move {
                        let encoder = TextEncoder::new();
                        let mf = registry.gather();
                        let mut buffer = vec![];
                        encoder.encode(&mf, &mut buffer).unwrap();
                        HttpResponse::Ok()
                            .content_type("text/plain; charset=utf-8")
                            .body(buffer)
                    }
                }
            }))
            .service(hello)
            .service(get_raw_count)
            .service(get_staging)
            .service(get_daily)
            .service(trigger_pipeline)
    })
    .bind("0.0.0.0:8082")?
    .run()
    .await
}