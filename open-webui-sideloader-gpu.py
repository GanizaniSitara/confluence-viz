#!/usr/bin/env python3
"""
Pilot script to side-load plaintext Confluence pages into OpenWebUI knowledge base (v0.6.18),
using Nomic embedding model via local Ollama for GPU acceleration, then bulk inserting into PostgreSQL+PGVector.

Edit the DEFAULT_* constants below for quick overrides in IDE (e.g. PyCharm).
"""

import argparse
import json
import uuid
import time
import hashlib
import pickle
import os
from pathlib import Path

import psycopg2
import psycopg2.extras
from pgvector.psycopg2 import register_vector
import ollama
from tqdm import tqdm

from utils.html_cleaner import clean_confluence_html

# ------ DEFAULT CONFIGURATION ------
DEFAULT_PICKLE_DIR = "./temp"  # e.g. /mnt/data/pickles
DEFAULT_OLLAMA_HOST = "http://localhost:11434"  # e.g. http://192.168.1.100:11434
DEFAULT_KNOWLEDGE_ID = "d357bd23-5eee-46c1-b09a-488cd90e4ba2"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "openwebui"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = ""
DEFAULT_CHUNK_SIZE = 500
DEFAULT_OVERLAP = 50
DEFAULT_BATCH_SIZE = 64
DEFAULT_DEVICE = "cuda:0"  # e.g. 'cuda:0', 'cuda:1' or 'cpu'
DEFAULT_EMBED_MODEL = "nomic-embed-text:v1.5"
DEFAULT_PAD_EMBEDDINGS = True  # Pad embeddings to match DB dimension if needed
CHECKPOINT_FILE = 'gpu_sideloader_checkpoint.json'


def save_checkpoint(pickle_file: str, page_idx: int):
    """Save checkpoint to track progress within pickle files"""
    checkpoint = {
        'current_pickle': pickle_file,
        'last_page_index': page_idx,
        'timestamp': time.time()
    }
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save checkpoint: {e}")


def load_checkpoint():
    """Load checkpoint if it exists"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load checkpoint: {e}")
    return None


def clear_checkpoint():
    """Clear checkpoint after successful completion"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print("Checkpoint cleared - processing completed successfully")
        except Exception as e:
            print(f"Warning: Failed to clear checkpoint: {e}")


def chunk_text(text, chunk_size=DEFAULT_CHUNK_SIZE, overlap=DEFAULT_OVERLAP):
    """
    Split text into chunks of approximately chunk_size characters with overlap.
    """
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks


def pad_or_truncate_embedding(embedding, target_dim):
    """
    Pad or truncate embedding to match target dimension.
    """
    current_dim = len(embedding)
    if current_dim == target_dim:
        return embedding
    elif current_dim < target_dim:
        # Pad with zeros
        padded = embedding + [0.0] * (target_dim - current_dim)
        return padded
    else:
        # Truncate
        return embedding[:target_dim]


def process_page_content(page, space_key, space_name):
    """
    Convert a Confluence page dict to cleaned plaintext for embedding.
    """
    title = page.get('title', 'Untitled')
    body = page.get('body', '')
    storage_body = body if isinstance(body, str) else ''
    cleaned = clean_confluence_html(storage_body) if storage_body else ''
    
    # Build hierarchical path like in the parallel script
    ancestors = page.get('ancestors', [])
    path_parts = [space_name]
    for ancestor in ancestors:
        path_parts.append(ancestor.get('title', 'Unknown'))
    path_parts.append(title)
    breadcrumb = ' > '.join(path_parts)
    
    # Format content with space and path info at the top
    content = f"{title}\n{'='*len(title)}\n\n"
    content += f"Space: {space_name}\n\n"
    content += f"Path: {breadcrumb}\n"
    content += "\n" + "-" * 60 + "\n\n"
    content += cleaned
    
    return content, breadcrumb


def connect_db(args):
    """
    Connect to PostgreSQL and register PGVector adapter.
    """
    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password
    )
    register_vector(conn)
    return conn


def get_db_vector_dim(conn):
    """
    Query the PGVector 'vector' column dimension of document_chunk.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT atttypmod
          FROM pg_catalog.pg_attribute
         WHERE attrelid = 'document_chunk'::regclass
           AND attname = 'vector'
        """
    )
    result = cur.fetchone()
    if not result:
        raise RuntimeError("document_chunk.vector column not found in database.")
    # PGVector stores dimension as atttypmod, not atttypmod - 4
    return result[0]


def insert_file(conn, file_id, title, content, user_id, space_info, page, breadcrumb):
    """
    Insert a file record into the `file` table with complete metadata.
    """
    cur = conn.cursor()
    now_ms = int(time.time() * 1000)
    
    # Create rich metadata like in the other scripts
    meta = {
        "name": title,
        "source": f"confluence:{space_info['name']}",
        "space_key": space_info["key"],
        "space_name": space_info["name"],
        "page_id": page.get("id", ""),
        "confluence_url": page.get("url", ""),
        "title": title,
        "path": breadcrumb
    }
    
    data = {"content": content}
    hash_value = hashlib.sha256(content.encode('utf-8')).hexdigest()
    cur.execute(
        """
        INSERT INTO file (id, filename, user_id, created_at, updated_at, meta, data, hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (str(file_id), title, user_id, now_ms, now_ms,
         json.dumps(meta), json.dumps(data), hash_value)
    )
    conn.commit()


def insert_chunks(conn, knowledge_id, file_id, chunks, embeddings):
    """
    Bulk insert document chunks and their vectors into `document_chunk`.
    """
    cur = conn.cursor()
    records = []
    for idx, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        chunk_id = uuid.uuid4()
        vmetadata = {"file_id": str(file_id), "chunk_index": idx}
        records.append((
            str(chunk_id), knowledge_id, chunk,
            json.dumps(vmetadata), emb
        ))
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO document_chunk (id, collection_name, text, vmetadata, vector)
        VALUES %s
        """,
        records,
        template="(%s, %s, %s, %s, %s)"
    )
    conn.commit()


def update_knowledge(conn, knowledge_id, file_id):
    """
    Read and update the `file_ids` array in the knowledge base's data JSON.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT data FROM knowledge WHERE id = %s", (knowledge_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Knowledge base {knowledge_id} not found")
    
    kb_data = row['data']
    if kb_data is None:
        # Initialize empty data structure if NULL
        kb_data = {}
    
    file_ids = kb_data.get('file_ids', [])
    file_ids.append(str(file_id))
    kb_data['file_ids'] = file_ids
    
    cur.execute("UPDATE knowledge SET data = %s WHERE id = %s",
                (json.dumps(kb_data), knowledge_id))
    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Pilot side-load with Ollama/Nomic embeddings"
    )
    parser.add_argument("--pickle-dir", default=DEFAULT_PICKLE_DIR,
                        help="Directory of Confluence pickle files")
    parser.add_argument("--knowledge-id", default=DEFAULT_KNOWLEDGE_ID,
                        help="Target knowledge base UUID")
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST)
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--db-user", default=DEFAULT_DB_USER)
    parser.add_argument("--db-password", default=DEFAULT_DB_PASSWORD)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--overlap", type=int, default=DEFAULT_OVERLAP)
    parser.add_argument("--embed-model", default=DEFAULT_EMBED_MODEL,
                        help="Ollama model name for embedding (e.g. nomic-embed-text)")
    parser.add_argument("--pad-embeddings", type=bool, default=DEFAULT_PAD_EMBEDDINGS,
                        help="Pad embeddings with zeros if dimension mismatch")
    parser.add_argument("--ollama-host", default=DEFAULT_OLLAMA_HOST,
                        help=f"Ollama API host URL (default: {DEFAULT_OLLAMA_HOST})")
    parser.add_argument("--clear-checkpoint", action="store_true",
                        help="Clear checkpoint and start fresh")
    args = parser.parse_args()
    
    # Clear checkpoint if requested
    if args.clear_checkpoint:
        clear_checkpoint()
        print("Checkpoint cleared - starting fresh")

    # Connect to DB and check vector dimension
    conn = connect_db(args)
    db_dim = get_db_vector_dim(conn)
    print(f"PGVector expects {db_dim}-dim vectors.")

    # Configure Ollama client with the specified host
    ollama_client = ollama.Client(host=args.ollama_host)
    
    # Check if Ollama model is available
    print(f"Checking Ollama model '{args.embed_model}' at {args.ollama_host}...")
    try:
        # Test with a simple embedding
        test_resp = ollama_client.embeddings(model=args.embed_model, prompt="test")
        
        # Handle EmbeddingsResponse object
        test_vec = None
        if hasattr(test_resp, 'embeddings'):
            # EmbeddingsResponse object with embeddings attribute
            test_vec = test_resp.embeddings
        elif hasattr(test_resp, 'embedding'):
            # Alternative attribute name
            test_vec = test_resp.embedding
        elif isinstance(test_resp, list):
            # Direct list response
            test_vec = test_resp
        elif isinstance(test_resp, dict):
            # Dictionary response - try different keys
            test_vec = test_resp.get('embeddings') or test_resp.get('embedding') or test_resp.get('vector')
            
        if test_vec is None:
            print(f"ERROR: Model '{args.embed_model}' returned unexpected format")
            print(f"Response type: {type(test_resp)}")
            print(f"Response attributes: {dir(test_resp) if hasattr(test_resp, '__dir__') else 'N/A'}")
            return
        else:
            print(f"Model '{args.embed_model}' OK, generates {len(test_vec)}-dim vectors")
            if len(test_vec) != db_dim:
                print(f"WARNING: Model vector dim {len(test_vec)} != DB dim {db_dim}")
                if args.pad_embeddings:
                    print(f"Embeddings will be padded from {len(test_vec)} to {db_dim} dimensions")
                else:
                    print("ERROR: Dimension mismatch will cause failures. Use --pad-embeddings=True to enable padding")
    except Exception as e:
        print(f"ERROR: Cannot connect to Ollama or model '{args.embed_model}' not available")
        print(f"Error: {e}")
        print("Make sure Ollama is running and the model is pulled:")
        print(f"  ollama pull {args.embed_model}")
        return

    user_id = args.db_user
    pickle_files = sorted(Path(args.pickle_dir).glob("*.pkl"))
    print(f"Found {len(pickle_files)} pickle files to process.")
    
    # Load checkpoint if exists
    checkpoint = load_checkpoint()
    start_pickle_idx = 0
    start_page_idx = 0
    
    if checkpoint:
        # Find which pickle file to resume from
        checkpoint_pickle = checkpoint.get('current_pickle')
        if checkpoint_pickle:
            for idx, pkl in enumerate(pickle_files):
                if str(pkl) == checkpoint_pickle:
                    start_pickle_idx = idx
                    start_page_idx = checkpoint.get('last_page_index', 0)
                    print(f"Resuming from pickle {pkl.name}, page index {start_page_idx}")
                    break

    total_pages_processed = 0
    for pkl_idx, pkl in enumerate(pickle_files[start_pickle_idx:], start_pickle_idx + 1):
        data = pickle.load(open(pkl, "rb"))
        pages = data.get("sampled_pages", [])
        
        # Get space information from pickle data
        space_info = data.get("space_info", {})
        if not space_info:
            # Fallback - try to extract from filename
            space_key = pkl.stem.replace("_sampled", "").upper()
            space_name = space_key
            space_info = {"key": space_key, "name": space_name}
        
        print(f"\n[{pkl_idx}/{len(pickle_files)}] {pkl.name}: {len(pages)} pages from space {space_info.get('name', 'Unknown')}")
        
        # Skip to the right page if resuming
        pages_to_process = pages
        if pkl_idx == start_pickle_idx + 1 and start_page_idx > 0:
            pages_to_process = pages[start_page_idx:]
            print(f"Skipping first {start_page_idx} pages (already processed)")
        
        for page_enum_idx, page in enumerate(tqdm(pages_to_process, desc=f"Processing {pkl.name}"), 1):
            # Calculate actual page index for checkpoint
            actual_page_idx = page_enum_idx if pkl_idx > start_pickle_idx + 1 else page_enum_idx + start_page_idx
            page_title = page.get("title", "Untitled")
            page_id = page.get("id", "unknown")
            space_key = space_info.get("key", "")
            
            # Use consistent naming format like in open-webui-parallel.py
            filename = f"{space_key}-{page_id}-TEXT"
            
            text, breadcrumb = process_page_content(page, space_key, space_info.get("name", ""))
            file_id = uuid.uuid4()
            
            # Insert file record with full metadata
            insert_file(conn, file_id, filename, text, user_id, space_info, page, breadcrumb)
            
            # Chunk text
            chunks = chunk_text(text, args.chunk_size, args.overlap)
            if actual_page_idx == 1 or actual_page_idx % 10 == 0:
                print(f"  Page {actual_page_idx}: {page_title[:50]}... ({len(chunks)} chunks)")
            
            # Generate embeddings via Ollama
            embeddings = []
            for chunk_idx, chunk in enumerate(chunks):
                try:
                    resp = ollama_client.embeddings(model=args.embed_model, prompt=chunk)
                    
                    # Handle EmbeddingsResponse object
                    vec = None
                    if hasattr(resp, 'embeddings'):
                        # EmbeddingsResponse object with embeddings attribute
                        vec = resp.embeddings
                    elif hasattr(resp, 'embedding'):
                        # Alternative attribute name
                        vec = resp.embedding
                    elif isinstance(resp, list):
                        # Direct list response
                        vec = resp
                    elif isinstance(resp, dict):
                        # Dictionary response - try different keys
                        vec = resp.get('embeddings') or resp.get('embedding') or resp.get('vector')
                    
                    if vec is None:
                        print(f"Error: Unexpected Ollama response format")
                        print(f"Response type: {type(resp)}")
                        print(f"Response attributes: {dir(resp) if hasattr(resp, '__dir__') else 'N/A'}")
                        raise RuntimeError(f"Ollama embedding failed for chunk {chunk_idx}")
                    
                    if len(vec) != db_dim:
                        if args.pad_embeddings:
                            vec = pad_or_truncate_embedding(vec, db_dim)
                        else:
                            raise RuntimeError(
                                f"Embedding dimension {len(vec)} does not match DB {db_dim}"
                            )
                    embeddings.append(vec)
                except Exception as e:
                    print(f"Error generating embedding for chunk {chunk_idx}: {e}")
                    print(f"Chunk text (first 100 chars): {chunk[:100]}...")
                    raise
            insert_chunks(conn, args.knowledge_id, file_id, chunks, embeddings)
            update_knowledge(conn, args.knowledge_id, file_id)
            total_pages_processed += 1
            
            # Save checkpoint after each page
            save_checkpoint(str(pkl), actual_page_idx)
    
    print(f"\n✓ Processing complete! Total pages: {total_pages_processed}")
    clear_checkpoint()
    conn.close()

if __name__ == "__main__":
    main()
