# 📚 Complete Command Reference

This file contains all commands for development, debugging, and operations.

## 🐳 Docker Commands

### Start Services

```bash
# BATCH mode (default)
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d

# REALTIME mode (with Kafka)
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml --profile realtime up -d

# Build and start specific service
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d --build superset
```

### Stop Services

```bash
# Stop all services
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml down

# Stop and remove volumes
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml down -v

# Stop specific service
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml stop postgres
```

### Check Status

```bash
# List all running containers
docker ps

# Check specific services
docker ps | grep -E "zookeeper|kafka|crypto-producer"

# View logs
docker logs -f <container_name>
docker logs -f superset
docker logs crypto-producer --tail 20
docker logs kafka-connect --tail 50
```

### Restart Services

```bash
# Restart Airflow
docker restart airflow-scheduler airflow-webserver

# Restart Kafka Connect
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml --profile realtime restart kafka-connect
```

## 🦀 Rust Build Commands

### Rebuild Crypto Ingestion Image

```bash
# No cache rebuild
docker build --no-cache -t crypto-ingestion:latest ./ingestion/rust-ingestion

# Remove old images
docker rmi crypto-ingestion:latest cookiecutterproject_slug-api cookiecutterproject_slug-superset
```

### Test Rust Container Manually

```bash
docker run --rm --network cookiecutterproject_slug_data-platform \
  -e DATASOURCE__SOURCE_TYPE=local_csv \
  -e DATASOURCE__CSV_PATH=/data/ \
  -e STORAGE__ENABLED=true \
  -e STORAGE__URI=redis://data-platform-redis:6379 \
  -e STORAGE__CHANNEL=channel \
  -e PROVIDER__API_KEY="" \
  -e PROVIDER__API_SECRET="" \
  -e PROVIDER__ENABLED=false \
  -v $(pwd)/data:/data \
  crypto-ingestion:latest
```

### Clean Rust Build Artifacts

```bash
# Clean all target directories
find ~/Documents/ -name target -type d -prune -exec rm -rf {} \;
```

## 🗃️ Database Commands

### PostgreSQL

```bash
# Connect to database
docker exec -it data-platform-postgres psql -U admin -d admin

# Run SQL query
docker exec -it data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"

# View recent data
docker exec data-platform-postgres psql -U admin -d admin -c "
SELECT symbol, timestamp, close
FROM raw_ohlc
ORDER BY timestamp DESC
LIMIT 10;
"

# Connection URI for external tools
postgresql+psycopg2://admin:admin@localhost:5434/admin
# Internal URI (from containers)
postgresql+psycopg2://admin:admin@data-platform-postgres:5432/admin
```

### PostgreSQL - Reset Database

```bash
# Stop, remove container and volume, restart
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml stop postgres && \
docker rm data-platform-postgres && \
docker volume rm cookiecutterproject_slug_postgres_data && \
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d postgres

# Alternative (shorter)
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml rm -sf postgres && \
docker volume rm cookiecutterproject_slug_postgres_data && \
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d postgres
```

### Redis

```bash
# Connect to Redis CLI
docker exec -it data-platform-redis redis-cli

# Inside Redis CLI:
KEYS *
GET <key_name>
exit
```

## 📊 dbt Commands

### Run dbt Models

```bash
# Run all models
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml run --rm dbt run --profiles-dir /usr/app/dbt

# Run specific model
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml run --rm dbt run --select raw_ohlc

# Clean and rebuild raw_ohlc
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml run --rm dbt run --select raw_ohlc
```

## 🔄 Kafka Commands

### Kafka UI

Access Kafka UI at: http://localhost:8080

### Consume Messages (Local)

```bash
# Consume from beginning
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning

# Consume from internal port
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic crypto-prices \
  --max-messages 3

# Check last 5 messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic crypto-prices \
  --from-beginning \
  --max-messages 5
```

### Kafka Connect

```bash
# Start Kafka Connect
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d kafka-connect

# List connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/postgres-sink-crypto/status

# Check with pretty print
curl http://localhost:8083/connectors/postgres-sink-crypto/status | jq

# Delete connector
curl -X DELETE http://localhost:8083/connectors/postgres-sink-crypto
```

### Create Kafka Connect Sink

```bash
# Create JDBC Sink connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-sink-crypto",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max": "1",
      "topics": "crypto-prices",
      "connection.url": "jdbc:postgresql://data-platform-postgres:5432/admin",
      "connection.user": "admin",
      "connection.password": "admin",
      "auto.create": "false",
      "insert.mode": "upsert",
      "pk.mode": "record_value",
      "pk.fields": "symbol,timestamp",
      "table.name.format": "raw_ohlc",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "true",
      "transforms": "TimestampConverter",
      "transforms.TimestampConverter.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
      "transforms.TimestampConverter.field": "timestamp",
      "transforms.TimestampConverter.target.type": "Timestamp",
      "transforms.TimestampConverter.format": "yyyy-MM-dd'\''T'\''HH:mm:ssXXX"
    }
  }'
```

### Check Consumer Group Offset

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --describe \
  --group connect-postgres-sink-crypto
```

## 🔍 Real-time Mode Verification

Complete checklist for real-time data flow:

```bash
# 1. Check all Kafka services are UP
docker ps | grep -E "zookeeper|kafka|crypto-producer"

# 2. Verify crypto-producer is streaming
docker logs crypto-producer --tail 20

# 3. Consume messages from Kafka
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic crypto-prices \
  --max-messages 3

# 4. Check Kafka Connect is running
curl http://localhost:8083/connectors

# 5. Check connector status
curl http://localhost:8083/connectors/postgres-sink-crypto/status

# 6. Verify data in PostgreSQL
docker exec -it data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"

# 7. Check again after 10 seconds (count should increase!)
sleep 10
docker exec -it data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"

# 8. View latest inserted rows
docker exec data-platform-postgres psql -U admin -d admin -c "
SELECT symbol, timestamp, close
FROM raw_ohlc
ORDER BY timestamp DESC
LIMIT 10;
"
```

## 🛠️ Troubleshooting Commands

### Full Rebuild

```bash
# Complete rebuild from scratch
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml down

docker rmi crypto-ingestion:latest cookiecutterproject_slug-api cookiecutterproject_slug-superset

docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d --build
```

### Network Issues

```bash
# List Docker networks
docker network ls | grep data-platform

# Inspect network
docker network inspect cookiecutterproject_slug_data-platform
```

### Check Kafka Connect Errors

```bash
# View connector logs
docker logs kafka-connect --tail 50

# Check connector tasks
curl http://localhost:8083/connectors/postgres-sink-crypto/tasks

# Restart connector
curl -X POST http://localhost:8083/connectors/postgres-sink-crypto/restart
```

## 📈 Monitoring

### Prometheus
URL: http://localhost:9090

### Grafana
URL: http://localhost:3000

## 🎯 Airflow

### Access
- URL: http://localhost:8081
- Login: `admin`
- Password: `admin`

### Restart Airflow
```bash
docker restart airflow-scheduler airflow-webserver
```

## 🔒 Pre-commit

### Install
```bash
pip install pre-commit
pre-commit install
```

### Update hooks
```bash
pre-commit autoupdate
```

### Run manually
```bash
# On all files
pre-commit run --all-files

# On specific files
pre-commit run --files {{cookiecutter.project_slug}}/dags/test.py
```

### Skip
```bash
git commit --no-verify -m "urgent fix"
```

## 📝 Notes

### Architecture Diagrams

See README.md for:
- Overall architecture
- Medallion layers
- Batch vs Real-time flow
