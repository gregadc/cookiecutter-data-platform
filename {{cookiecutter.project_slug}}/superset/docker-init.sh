#!/bin/bash
set -e

#source /app/.venv/bin/activate
pip install --upgrade pip
pip install psycopg2-binary

# Lance Superset normalement
superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@superset.com --password admin || true
superset init
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger