import os
import psycopg2
from langchain_ollama import OllamaEmbeddings

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "admin",
    "user": "admin",
    "password": "admin",
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_embeddings_model():
    return OllamaEmbeddings(model="nomic-embed-text")

def fetch_crypto_data(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, open, high, low, close, volume, timestamp
            FROM raw_ohlc
            LIMIT 100
        """)
        return cur.fetchall()

def row_to_text(row):
    symbol, open_, high, low, close, volume, timestamp = row
    return (
        f"{symbol} le {timestamp}: "
        f"ouverture {open_}, "
        f"plus haut {high}, "
        f"plus bas {low}, "
        f"clôture {close}, "
        f"volume {volume}"
    )

def ingest():
    conn = get_connection()
    embeddings_model = get_embeddings_model()

    rows = fetch_crypto_data(conn)
    if not rows:
        return

    for row in rows:
        text = row_to_text(row)
        embedding = embeddings_model.embed_query(text)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO embeddings (content, metadata, embedding) VALUES (%s, %s, %s)",
                (text, None, embedding)
            )
        conn.commit()

    conn.close()

if __name__ == "__main__":
    ingest()
