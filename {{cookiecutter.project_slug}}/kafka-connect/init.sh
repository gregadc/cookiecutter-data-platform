#!/bin/bash
echo "Waiting for Kafka Connect..."
until curl -f http://kafka-connect:8083/; do sleep 5; done

# Create kafka connector
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/config/postgres-sink-config.json

echo "Done!"