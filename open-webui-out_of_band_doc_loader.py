#!/usr/bin/env python3
"""
Standalone ingestion pipeline:
- Reads settings from settings.ini
- Walks a directory for documents
- Uses Apache Tika (REST) to extract text
- Calls Ollama to generate embeddings
- Inserts content + embedding into PostgreSQL with pgvector
"""

import os
import logging
import requests
import psycopg2
import configparser
from tika import parser

# --- Load configuration ---
config = configparser.ConfigParser()
config.read('settings.ini')

TIKA_URL      = config['tika']['url']
OLLAMA_URL    = config['ollama']['url']
OLLAMA_MODEL  = config['ollama']['model']
PG_CONN       = config['database']['dsn']
TABLE_NAME    = config['database']['table']
VECTOR_DIM    = int(config['pgvector']['dim'])
DOCS_DIR      = config['pipeline']['docs_dir']
LOG_LEVEL     = config['pipeline'].get('log_level', 'INFO').upper()

# --- Logging setup ---
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Helpers ---

def extract_text(filepath: str) -> str:
    """
    Extract plain text via Tika REST.
    """
    parsed = parser.from_file(filepath, server_url=TIKA_URL)
    return parsed.get('content', '').strip()


def embed_text(text: str) -> list[float]:
    """
    Generate embeddings via Ollama REST.
    """
    resp = requests.post(
        OLLAMA_URL,
        json={"model": OLLAMA_MODEL, "prompt": text}
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("embeddings") or data.get("data")


def init_db(conn_str: str):
    """
    Connect to Postgres and ensure pgvector extension + table exist.
    """
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            path TEXT UNIQUE,
            content TEXT,
            embedding VECTOR({VECTOR_DIM})
        );
    """)
    conn.commit()
    return conn


def main():
    conn = init_db(PG_CONN)
    cur = conn.cursor()

    for root, _, files in os.walk(DOCS_DIR):
        for fname in files:
            path = os.path.join(root, fname)
            logger.info(f"Processing: {path}")

            try:
                # 1. Extract
                text = extract_text(path)
                if not text:
                    logger.warning(f"No text extracted for {path}")
                    continue

                # 2. Embed
                vector = embed_text(text)
                if not vector:
                    logger.error(f"Embedding failed for {path}")
                    continue

                # 3. Insert
                cur.execute(
                    f"INSERT INTO {TABLE_NAME} (path, content, embedding) VALUES (%s, %s, %s) "
                    f"ON CONFLICT (path) DO UPDATE SET content=EXCLUDED.content, embedding=EXCLUDED.embedding;",
                    (path, text, vector)
                )
                conn.commit()
                logger.info(f"Inserted: {path}")

            except Exception as e:
                logger.error(f"Error on {path}: {e}")

    cur.close()
    conn.close()
    logger.info("Ingestion complete.")


if __name__ == "__main__":
    main()
