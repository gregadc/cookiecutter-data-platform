# Crypto Data Platform - Guide de démarrage rapide

## 🚀 Démarrage du projet

### 1. Lancer tous les services

```bash
docker compose -f docker-compose-dev.yml up -d
```

### 2. Vérifier que les containers tournent

```bash
docker ps
```

Tu devrais voir :
- `data-platform-postgres` (port 5434)
- `postgres-airflow` (port 5433)
- `data-platform-redis` (port 6379)
- `airflow-webserver` (port 8081)
- `airflow-scheduler`

---

## 🔧 Build de l'image Rust

### Rebuild complet de l'image crypto-ingestion

```bash
docker build --no-cache -t crypto-ingestion:latest ./ingestion/rust-ingestion
```

### Test manuel du container (optionnel)

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

---

## 📊 Utilisation d'Airflow

### Accéder à l'interface Airflow

- URL : **http://localhost:8081**
- Login : `admin`
- Password : `admin`

### Lancer le DAG crypto_ingestion_pipeline

1. Va sur http://localhost:8081
2. Trouve le DAG `crypto_ingestion_pipeline`
3. Active-le (toggle à gauche)
4. Clique sur ▶️ pour le lancer

### Restart des services Airflow (si besoin)

```bash
docker restart airflow-scheduler airflow-webserver
```

---

## 🔍 Vérification des données

### Vérifier Redis

```bash
docker exec -it data-platform-redis redis-cli
KEYS *
GET <une_clé>
exit
```

### Vérifier PostgreSQL et changer l'uri
postgresql+psycopg2://admin:admin@data-platform-postgres:5432/admin
```bash
docker exec -it data-platform-postgres psql -U admin -d admin
SELECT * FROM raw_ohlc LIMIT 10;
\q
```

### Voir logs temps réel
```bash
docker compose -f docker-compose-dev.yml up -d --build superset
docker logs -f superset
```

### Supprimer PostgreSQL et volume
docker compose -f docker-compose-dev.yml stop postgres && docker rm data-platform-postgres && docker volume rm cookiecutterproject_slug_postgres_data && docker compose -f docker-compose-dev.yml up -d postgres
---

## 🛑 Arrêter tous les services

```bash
docker compose -f docker-compose-dev.yml down
```

### Arrêter ET supprimer les volumes (reset complet)

```bash
docker compose -f docker-compose-dev.yml down -v
```

---

## 📝 Architecture

**Pipeline :**
1. **Extract** : `crypto-ingestion` (Rust) lit CSV → Redis
2. **Load** : Airflow transfère Redis → PostgreSQL (`raw_ohlc`)
3. **Transform** : (à venir avec dbt)
4. **Visualize** : (à venir avec Superset)

**Stack :**
- Orchestration : Airflow
- Ingestion : Rust
- Cache : Redis
- Database : PostgreSQL
- Containerization : Docker Compose

---

## ⚠️ Troubleshooting

### Le DAG n'apparaît pas dans Airflow
Attends 30 secondes (rechargement automatique) ou restart :
```bash
docker restart airflow-scheduler airflow-webserver
```

### Erreur "network not found"
Vérifie le nom du réseau :
```bash
docker network ls | grep data-platform
```
Utilise le nom complet dans le DAG.

### Rebuild complet si besoin
```bash
docker compose -f docker-compose-dev.yml down

# docker build --no-cache -t crypto-ingestion:latest ./ingestion/rust-ingestion
docker rmi crypto-ingestion:latest cookiecutterproject_slug-api cookiecutterproject_slug-superset
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml up -d --build
```

### Clener volume postgres
```bash
docker compose -f docker-compose-dev.yml rm -sf postgres && docker volume rm cookiecutterproject_slug_postgres_data && docker compose -f docker-compose-dev.yml up -d postgres
```
---

### Clean target directories
```bash
find ~/Documents/ -name target -type d -prune -exec rm -rf {} \;
```

### Applied DBT tables
```bash
docker compose -f docker-compose-dev.yml run --rm dbt run --profiles-dir /usr/app/dbt
Clean raw_ohlc table
docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml run --rm dbt run --select raw_ohlc

```

### KAFKA URI
http://localhost:8080

## locally consumer kafka
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning


docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic crypto-prices \
  --max-messages 3
```
or
```bash
docker compose -f docker-compose-dev.yml up -d kafka-connect
```

## Create kafka connect file
# Kafka Connect - Configuration

## Create connector JDBC Sink

1. **Créer le fichier de configuration**
```bash
cat > kafka-connect-postgres-sink.json << 'EOF'
{
  "name": "postgres-sink-raw-ohlc",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "crypto-prices",
    "connection.url": "jdbc:postgresql://data-platform-postgres:5432/admin",
    "connection.user": "admin",
    "connection.password": "admin",
    "auto.create": "false",
    "insert.mode": "insert",
    "table.name.format": "raw_ohlc",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "errors.tolerance": "all",
    "transforms": "convertTimestamp",
    "transforms.convertTimestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.convertTimestamp.field": "timestamp",
    "transforms.convertTimestamp.target.type": "Timestamp",
    "transforms.convertTimestamp.format": "yyyy-MM-dd'T'HH:mm:ssXXX"
  }
}
EOF

 docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic crypto-prices --from-beginning --max-messages 5
```

2. **Envoyer la config à Kafka Connect**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @kafka-connect-postgres-sink.json
```
# Vérifie que kafka-connect est healthy
docker ps | grep kafka-connect

# Si oui, recrée le connector avec la bonne config
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

# Vérifie le statut
curl http://localhost:8083/connectors/postgres-sink-crypto/status
docker exec -it data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"

3. **Vérifier le statut**
```bash
curl http://localhost:8083/connectors/postgres-sink-raw-ohlc/status
```

## Commandes utiles
```bash
# Supprimer un connector
curl -X DELETE http://localhost:8083/connectors/postgres-sink-raw-ohlc

# Lister tous les connectors
curl http://localhost:8083/connectors
```

## 🎯 Prochaines étapes

- [ ] Ajouter dbt pour les transformations
- [ ] Ajouter Superset pour la visualisation
- [ ] Ajouter Prometheus/Grafana pour le monitoring
- [ ] Ajouter Terraform pour l'infrastructure cloud

## 🎯 Observability

Prometheus : http://localhost:9090
Grafana : http://localhost:3000



⚠️ Limites actuelles (à améliorer pour MAX impact)Ce qui manque pour être "wow" :
Production-ready features

❌ Monitoring (Prometheus/Grafana) → Ajoute ça !
❌ Tests (unitaires + intégration)
❌ CI/CD (GitHub Actions)
❌ Documentation API


MEDAILLON architecture

          ┌────────────────────────┐
          │  BRONZE : raw_ohlc     │
          │  (RAW / Argent brut)   │
          │  Données brutes CSV/API│
          └─────────┬─────────────┘
                    │
                    ▼
       ┌─────────────────────────────┐
       │ Rust ingestion              │
       │ CSV/API → Redis             │
       └─────────┬─────────────────┘
                 │
                 ▼
       ┌─────────────────────────────┐
       │ Redis → Postgres             │
       │ Remplit raw_ohlc            │
       └─────────┬─────────────────┘
                 │
                 ▼
       ┌─────────────────────────────┐
       │ SILVER : stg_ohlc           │
       │ (Staging / Or raffiné)  
       │ DBT : Transformations  
       │ Données nettoyées & normalisées │
       └─────────┬─────────────────┘
                 │
                 ▼
       ┌─────────────────────────────┐
       │ GOLD : daily_ohlc           │
       │ (Analytics / Produit fini)  │
       │ Agrégations journalières,   │
       │ dashboards, métriques       │
       └─────────────────────────────┘



# Mode BATCH (sans Kafka) - par défaut
docker compose -f docker-compose-dev.yml up -d

# Mode REALTIME (avec Kafka)
docker compose -f docker-compose-dev.yml --profile realtime up -d

# Check real time data
# 1. All Kafka services must be UP and healthy
docker ps | grep -E "zookeeper|kafka|crypto-producer"

# 2. Vérifie que crypto-producer stream bien
docker logs crypto-producer --tail 20

# 3. Check that crypto-producer stream is working correctly
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic crypto-prices \
  --max-messages 3

# 4. Check that kafka-connect is running
curl http://localhost:8083/connectors

# 5. If the connector is created, check its status.
curl http://localhost:8083/connectors/postgres-sink-crypto/status

# 6. Verify that the data is arriving in Postgres
docker exec -it data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"

# Check again 10 seconds later (the count should increase!)
sleep 10
docker exec -it data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"


                    ┌─────────────────┐
                    │  COOKIECUTTER   │
                    │ DATA PLATFORM   │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
         MODE BATCH                    MODE REALTIME
              │                             │
    ┌─────────▼─────────┐        ┌─────────▼─────────┐
    │  CSV Files (data/)│        │ Binance WebSocket │
    └─────────┬─────────┘        └─────────┬─────────┘
              │                             │
    ┌─────────▼─────────┐        ┌─────────▼─────────┐
    │ Rust Ingestion    │        │  crypto-producer  │
    └─────────┬─────────┘        └─────────┬─────────┘
              │                             │
    ┌─────────▼─────────┐        ┌─────────▼─────────┐
    │      Redis        │        │   Kafka Topic     │
    └─────────┬─────────┘        │  crypto-prices    │
              │                  └─────────┬─────────┘
              │                            │
              │                  ┌─────────▼─────────┐
              │                  │  Kafka Connect    │
              │                  │   JDBC Sink       │
              │                  └─────────┬─────────┘
              │                            │
              └────────────┬───────────────┘
                           │
                  ┌────────▼────────┐
                  │  Postgres DB    │
                  │   raw_ohlc      │
                  └────────┬────────┘
                           │
                  ┌────────▼────────┐
                  │   dbt Models    │
                  │  stg_ohlc       │
                  │  daily_ohlc     │
                  └────────┬────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
    ┌─────────▼─────────┐    ┌─────────▼─────────┐
    │   Superset        │    │   Grafana         │
    │  Dashboards       │    │   Monitoring      │
    └───────────────────┘    └───────────────────┘



### Checker les erreurs
# 1. Vérifie que crypto-producer stream bien
docker logs crypto-producer --tail 10

# 2. Vérifie que Kafka reçoit les messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic crypto-prices \
  --max-messages 5

# 3. Status du connector (IMPORTANT)
curl http://localhost:8083/connectors/postgres-sink-crypto/status | jq

# 4. Logs de kafka-connect (cherche les erreurs)
docker logs kafka-connect --tail 50

# 5. Compte en DB
docker exec data-platform-postgres psql -U admin -d admin -c "SELECT COUNT(*) FROM raw_ohlc;"

# 6. Vérifie les dernières lignes insérées (timestamp)
docker exec data-platform-postgres psql -U admin -d admin -c "
SELECT symbol, timestamp, close 
FROM raw_ohlc 
ORDER BY timestamp DESC 
LIMIT 10;
"

# 7. Vérifie le consumer group offset (est-ce qu'il avance ?)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --describe \
  --group connect-postgres-sink-crypto

docker compose -f {{cookiecutter.project_slug}}/docker-compose-dev.yml --profile realtime restart kafka-connect



RAG ? ou n8S ?
┌─────────────────────────────────────────────────┐
│           Ton projet actuel                     │
│  Kafka → Postgres → dbt → Superset              │
└─────────────────┬───────────────────────────────┘
                  │
                  ↓
         ┌────────────────────┐
         │  Vector Database   │
         │  (ChromaDB/Qdrant) │
         └────────┬───────────┘
                  │
         ┌────────▼───────────┐
         │   Embeddings       │
         │  (OpenAI/HuggingFace)
         └────────┬───────────┘
                  │
         ┌────────▼───────────┐
         │    RAG Engine      │
         │  (LangChain/Llama) │
         └────────┬───────────┘
                  │
         ┌────────▼───────────┐
         │   Chat Interface   │
         │    (Streamlit)     │
         └────────────────────┘
