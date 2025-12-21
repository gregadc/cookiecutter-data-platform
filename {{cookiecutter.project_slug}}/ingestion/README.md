# Baby-Bot 🤖

## Environment variables

| Variable | Value |
|----------|--------|
| RUST_BACKTRACE | full |
| PROVIDER__START_DATE | 2024-03-5 |
| PROVIDER__END_DATE | 2024-03-21 |
| PROVIDER__COIN | BTCUSDT |
| PROVIDER__INTERVAL | 1h |
| STORAGE__ENABLED | true |
| STORAGE__URI | redis://localhost:6379/1 |
| STORAGE__CHANNEL | channel |
| PROVIDER__ENABLED | true |
| PROVIDER__API_SECRET | <your_secret> |
| PROVIDER__API_KEY | <your_key> |
| RUSTFLAGS | -Awarnings |

### Manual test with container

```bash
docker run --rm --network cookiecutterproject_slug_data-platform \
  -e DATASOURCE__SOURCE_TYPE=local_csv \
  -e DATASOURCE__CSV_PATH=../../data/ \
  -e STORAGE__ENABLED=true \
  -e STORAGE__URI=redis://data-platform-redis:6379 \
  -e STORAGE__CHANNEL=channel \
  -e PROVIDER__API_KEY="" \
  -e PROVIDER__API_SECRET="" \
  -e PROVIDER__ENABLED=false \
  -v $(pwd)/data:/data \
  crypto-ingestion:latest
```

### Retrieve datas from files

```
DATASOURCE__CSV_PATH=../../data/ DATASOURCE__SOURCE_TYPE=local_csv cargo run --bin rust-ingestion
```

### Real time
```
kafka-topics --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

KAFKA__TOPIC=crypto-prices KAFKA__BROKERS=localhost:9092 KAFKA__ENABLED=true STORAGE__ENABLED=false DATASOURCE__USE_REALTIME=true DATASOURCE__CSV_PATH=../../data/ DATASOURCE__SOURCE_TYPE=binance cargo run --bin rust-ingestion
```

### Listening kafka
```
kafka-console-consumer --topic crypto-prices --from-beginning --bootstrap-server localhost:9092 --from-beginning
```
