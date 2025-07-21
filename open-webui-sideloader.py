#!/usr/bin/env python3
"""
OpenWebUI Knowledge Collection Sideloader
Loads documents directly into OpenWebUI knowledge collections via database.

This version integrates with OpenWebUI's collection schema rather than creating
a separate table. It reads from the OpenWebUI database to find existing collections
and inserts documents properly linked to those collections.
"""

import os
import logging
import requests
import psycopg2
import psycopg2.extras
import configparser
from tika import parser
import json
import hashlib
from datetime import datetime
import uuid
from pathlib import Path

# --- Load configuration ---
config = configparser.ConfigParser()
config.read('settings.ini')

# Load from existing sections or create sideloader section
TIKA_URL = config.get('tika', 'url', fallback='http://localhost:9998/tika')
OLLAMA_URL = config.get('ollama', 'url', fallback='http://localhost:11434/api/embeddings')
OLLAMA_MODEL = config.get('ollama', 'model', fallback='nomic-embed-text')
PG_CONN = config.get('database', 'dsn', fallback='postgresql://webui:webui@localhost:5432/open-webui')
DOCS_DIR = config.get('sideloader', 'docs_dir', fallback='./documents')
LOG_LEVEL = config.get('sideloader', 'log_level', fallback='INFO').upper()
COLLECTION_NAME = config.get('sideloader', 'collection_name', fallback=None)

# --- Logging setup ---
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Helpers ---

def extract_text(filepath: str) -> str:
    """
    Extract plain text via Tika REST.
    """
    try:
        parsed = parser.from_file(filepath, serverEndpoint=TIKA_URL)
        return parsed.get('content', '').strip()
    except Exception as e:
        logger.error(f"Tika extraction failed for {filepath}: {e}")
        return ""


def embed_text(text: str) -> list[float]:
    """
    Generate embeddings via Ollama REST API.
    """
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": text}
        )
        resp.raise_for_status()
        data = resp.json()
        
        # Handle different response formats
        embedding = data.get("embedding") or data.get("embeddings") or data.get("data")
        if embedding and isinstance(embedding, list) and len(embedding) > 0:
            return embedding
        else:
            logger.error(f"Unexpected embedding response format: {data}")
            return None
    except Exception as e:
        logger.error(f"Ollama embedding failed: {e}")
        return None


def get_file_hash(filepath: str) -> str:
    """Generate a hash of the file content for deduplication."""
    with open(filepath, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()


def list_knowledge_collections(conn) -> dict:
    """
    List all knowledge collections from OpenWebUI database.
    Returns dict of {name: id}
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Query the knowledge table for collections
        cur.execute("""
            SELECT id, name, description 
            FROM knowledge 
            WHERE name IS NOT NULL
            ORDER BY name
        """)
        
        collections = {}
        for row in cur.fetchall():
            collections[row['name']] = row['id']
            logger.info(f"Found collection: {row['name']} (ID: {row['id']}, Description: {row['description']})")
        
        return collections
    except Exception as e:
        logger.error(f"Failed to list collections: {e}")
        return {}
    finally:
        cur.close()


def find_or_select_collection(conn, preferred_name: str = None) -> str:
    """
    Find a collection by name or prompt user to select one.
    Returns collection ID.
    """
    collections = list_knowledge_collections(conn)
    
    if not collections:
        logger.error("No knowledge collections found in OpenWebUI!")
        logger.error("Please create a collection in OpenWebUI first.")
        return None
    
    # If preferred name specified and exists, use it
    if preferred_name and preferred_name in collections:
        collection_id = collections[preferred_name]
        logger.info(f"Using collection '{preferred_name}' (ID: {collection_id})")
        return collection_id
    
    # Otherwise, let user select
    logger.info("\nAvailable collections:")
    collection_list = list(collections.items())
    for i, (name, cid) in enumerate(collection_list):
        print(f"{i+1}. {name} (ID: {cid})")
    
    while True:
        try:
            choice = input("\nSelect collection number (or 'q' to quit): ")
            if choice.lower() == 'q':
                return None
            
            idx = int(choice) - 1
            if 0 <= idx < len(collection_list):
                name, collection_id = collection_list[idx]
                logger.info(f"Selected collection '{name}' (ID: {collection_id})")
                return collection_id
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a number or 'q' to quit.")


def insert_document_to_collection(conn, collection_id: str, filepath: str, 
                                text: str, embedding: list[float]) -> bool:
    """
    Insert a document into OpenWebUI's knowledge collection system.
    This mimics how OpenWebUI stores documents internally.
    """
    cur = conn.cursor()
    try:
        # Generate IDs and metadata
        file_id = str(uuid.uuid4())
        filename = os.path.basename(filepath)
        file_hash = get_file_hash(filepath)
        
        # First, insert into files table (OpenWebUI's file storage)
        cur.execute("""
            INSERT INTO files (id, user_id, filename, meta, created_at, updated_at, hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hash) DO UPDATE
            SET updated_at = EXCLUDED.updated_at
            RETURNING id
        """, (
            file_id,
            'sideloader',  # System user
            filename,
            json.dumps({
                'source': 'sideloader',
                'path': filepath,
                'size': os.path.getsize(filepath),
                'content_type': 'text/plain'
            }),
            datetime.utcnow(),
            datetime.utcnow(),
            file_hash
        ))
        
        # Get the file ID (in case of update)
        result = cur.fetchone()
        if result:
            file_id = result[0]
        
        # Create a data entry for the file content with embedding
        data_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO file_data (id, file_id, content, embedding)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_id) DO UPDATE
            SET content = EXCLUDED.content,
                embedding = EXCLUDED.embedding
        """, (
            data_id,
            file_id,
            text,
            embedding
        ))
        
        # Link file to knowledge collection
        cur.execute("""
            INSERT INTO knowledge_files (knowledge_id, file_id)
            VALUES (%s, %s)
            ON CONFLICT (knowledge_id, file_id) DO NOTHING
        """, (
            collection_id,
            file_id
        ))
        
        conn.commit()
        logger.info(f"Successfully added '{filename}' to collection")
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert document: {e}")
        return False
    finally:
        cur.close()


def init_db(conn_str: str):
    """
    Connect to OpenWebUI's PostgreSQL database.
    Assumes OpenWebUI schema already exists.
    """
    try:
        conn = psycopg2.connect(conn_str)
        
        # Verify we can access OpenWebUI tables
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name IN ('knowledge', 'files', 'file_data', 'knowledge_files')
        """)
        count = cur.fetchone()[0]
        cur.close()
        
        if count < 4:
            logger.error("OpenWebUI tables not found! Is this the correct database?")
            return None
            
        logger.info("Connected to OpenWebUI database")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None


def main():
    """Main processing loop"""
    conn = init_db(PG_CONN)
    if not conn:
        logger.error("Failed to connect to database")
        return
    
    # Select collection
    collection_id = find_or_select_collection(conn, COLLECTION_NAME)
    if not collection_id:
        logger.error("No collection selected")
        conn.close()
        return
    
    # Process documents
    if not os.path.exists(DOCS_DIR):
        logger.error(f"Documents directory not found: {DOCS_DIR}")
        conn.close()
        return
    
    processed = 0
    errors = 0
    
    for root, _, files in os.walk(DOCS_DIR):
        for fname in files:
            path = os.path.join(root, fname)
            logger.info(f"Processing: {path}")
            
            try:
                # 1. Extract text
                text = extract_text(path)
                if not text:
                    logger.warning(f"No text extracted for {path}")
                    errors += 1
                    continue
                
                # 2. Generate embedding
                embedding = embed_text(text)
                if not embedding:
                    logger.error(f"Embedding generation failed for {path}")
                    errors += 1
                    continue
                
                # 3. Insert into collection
                if insert_document_to_collection(conn, collection_id, path, text, embedding):
                    processed += 1
                else:
                    errors += 1
                    
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                errors += 1
    
    conn.close()
    
    logger.info(f"Sideload complete: {processed} documents processed, {errors} errors")
    
    if processed > 0:
        logger.info("Documents have been added to the collection.")
        logger.info("You may need to restart OpenWebUI or refresh the UI to see the new documents.")


if __name__ == "__main__":
    main()