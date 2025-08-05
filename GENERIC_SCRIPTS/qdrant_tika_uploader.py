#!/usr/bin/env python3
"""
Process and upload non-text documents to OpenWebUI via Qdrant with full integration.
Uses Apache Tika for text extraction and ensures files are visible in OpenWebUI's interface by:
1. Registering files in PostgreSQL
2. Uploading vectors to Qdrant
3. Updating knowledge.data with file references

Processes: .doc, .docx, .ppt, .pptx, .xls, .xlsx, .odt, .rtf, .txt, .csv, .msg, .eml
Skips: PDFs (should be handled by OCR script) and markdown files

This is a generic script that can be used with any document collection.
"""
import argparse
import configparser
import json
import uuid
import time
import hashlib
import os
import psycopg2
import requests
from pathlib import Path
from typing import List, Dict, Any, Set
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import ollama
from tqdm import tqdm
from datetime import datetime

# Detect if we're in WSL
is_wsl = os.path.exists('/proc/version') and 'microsoft' in open('/proc/version').read().lower()

# Default configuration
DEFAULT_INPUT_DIR = "./documents"
DEFAULT_OLLAMA_HOST = "http://localhost:11434"
DEFAULT_QDRANT_HOST = "localhost"
DEFAULT_QDRANT_PORT = 6333
DEFAULT_KNOWLEDGE_ID = "00000000-0000-0000-0000-000000000000"  # Replace with your knowledge ID
DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000000"  # Replace with your user ID
DEFAULT_CHUNK_SIZE = 500
DEFAULT_OVERLAP = 50
DEFAULT_EMBED_MODEL = "nomic-embed-text:v1.5"
DEFAULT_VECTOR_SIZE = 768
DEFAULT_TIKA_URL = "http://localhost:9998"
CHECKPOINT_FILE = 'qdrant_tika_checkpoint.json'
PROCESSED_FILES = '.tika_processed.json'

# OpenWebUI collection names
FILES_COLLECTION = "open-webui_files"
KNOWLEDGE_COLLECTION = "open-webui_knowledge"

# Batch size for knowledge.data updates
KNOWLEDGE_UPDATE_BATCH = 100

# File extensions to process (excluding PDFs which should be handled by OCR)
SUPPORTED_EXTENSIONS = {
    '.doc', '.docx',      # Word documents
    '.ppt', '.pptx',      # PowerPoint
    '.xls', '.xlsx',      # Excel
    '.odt', '.ods',       # OpenDocument
    '.rtf',               # Rich Text
    '.txt',               # Plain text
    '.csv',               # CSV
    '.msg',               # Outlook messages
    '.eml',               # Email files
}

# Extensions to explicitly skip
SKIP_EXTENSIONS = {
    '.pdf',               # PDFs are handled by OCR script
    '.md',                # Markdown files are handled by markdown uploader
}

def load_config():
    """Load configuration from settings.ini"""
    config = configparser.ConfigParser()
    config_file = 'settings.ini'
    
    # Set defaults
    defaults = {
        'input_dir': DEFAULT_INPUT_DIR,
        'ollama_host': DEFAULT_OLLAMA_HOST,
        'qdrant_host': DEFAULT_QDRANT_HOST,
        'qdrant_port': str(DEFAULT_QDRANT_PORT),
        'knowledge_id': DEFAULT_KNOWLEDGE_ID,
        'user_id': DEFAULT_USER_ID,
        'chunk_size': str(DEFAULT_CHUNK_SIZE),
        'overlap': str(DEFAULT_OVERLAP),
        'embed_model': DEFAULT_EMBED_MODEL,
        'vector_size': str(DEFAULT_VECTOR_SIZE),
        'tika_url': DEFAULT_TIKA_URL,
        'checkpoint_file': CHECKPOINT_FILE,
        'processed_file': PROCESSED_FILES,
        'batch_points': '30',
        # PostgreSQL settings
        'db_host': '172.17.112.1' if is_wsl else 'localhost',
        'db_port': '5432',
        'db_name': 'openwebui',
        'db_user': 'postgres',
        'db_password': 'password'
    }
    
    if os.path.exists(config_file):
        config.read(config_file)
        if 'tika_uploader' in config:
            cfg = config['tika_uploader']
            for key in defaults:
                if key in cfg:
                    defaults[key] = cfg[key]
                    
        # Also check qdrant_uploader section for Qdrant-specific settings
        if 'qdrant_uploader' in config and 'qdrant_host' in config['qdrant_uploader']:
            defaults['qdrant_host'] = config['qdrant_uploader']['qdrant_host']
    
    # Convert types
    defaults['qdrant_port'] = int(defaults['qdrant_port'])
    defaults['chunk_size'] = int(defaults['chunk_size'])
    defaults['overlap'] = int(defaults['overlap'])
    defaults['vector_size'] = int(defaults['vector_size'])
    defaults['batch_points'] = int(defaults['batch_points'])
    defaults['db_port'] = int(defaults['db_port'])
    
    return defaults

def extract_text_with_tika(file_path: Path, tika_url: str) -> str:
    """Extract text from file using Apache Tika"""
    try:
        headers = {'Accept': 'text/plain'}
        with open(file_path, 'rb') as f:
            response = requests.put(f"{tika_url}/tika", data=f, headers=headers)
            
        if response.status_code == 200:
            return response.text.strip()
        else:
            print(f"  Tika error {response.status_code}: {response.text}")
            return ""
    except Exception as e:
        print(f"  Tika extraction failed: {e}")
        return ""

def save_checkpoint(file_path: str, checkpoint_file: str, uploaded_files: List[Dict]):
    """Save checkpoint to track progress including uploaded files list"""
    checkpoint = {
        'last_file': file_path,
        'timestamp': time.time(),
        'uploaded_files': uploaded_files
    }
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def load_checkpoint(checkpoint_file: str):
    """Load checkpoint if it exists"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                print(f"Loading checkpoint from: {checkpoint_file}")
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load checkpoint: {e}")
    return None

def clear_checkpoint(checkpoint_file: str):
    """Clear checkpoint after successful completion"""
    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print(f"Checkpoint cleared: {checkpoint_file}")
        except Exception as e:
            print(f"Warning: Failed to clear checkpoint: {e}")

def load_processed_files(processed_file: str) -> Set[str]:
    """Load set of already processed files"""
    if os.path.exists(processed_file):
        try:
            with open(processed_file, 'r') as f:
                data = json.load(f)
                return set(data.get('processed', []))
        except Exception as e:
            print(f"Warning: Failed to load processed files: {e}")
    return set()

def save_processed_files(processed_files: Set[str], processed_file: str):
    """Save set of processed files"""
    try:
        with open(processed_file, 'w') as f:
            json.dump({'processed': list(processed_files), 'count': len(processed_files)}, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save processed files: {e}")

def chunk_text(text, chunk_size=500, overlap=50):
    """Split text into chunks"""
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks

def compute_file_hash(content: str) -> str:
    """Compute SHA256 hash of file content"""
    return hashlib.sha256(content.encode()).hexdigest()

def register_file_in_postgres(conn, file_id: str, filename: str, content: str, 
                            user_id: str, knowledge_id: str) -> bool:
    """Register file in PostgreSQL database"""
    try:
        cur = conn.cursor()
        
        # Check if file already exists
        cur.execute("SELECT id FROM file WHERE id = %s", (file_id,))
        if cur.fetchone():
            print(f"  File already registered in PostgreSQL")
            return True
        
        # Insert file record
        file_hash = compute_file_hash(content)
        current_time = int(time.time() * 1000)  # milliseconds
        
        cur.execute("""
            INSERT INTO file (id, user_id, filename, meta, created_at, updated_at, hash, data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            file_id,
            user_id,
            filename,
            json.dumps({
                'name': filename,
                'content_type': 'text/plain',
                'size': len(content),
                'knowledge_id': knowledge_id,
                'source': 'tika_extraction'
            }),
            current_time,
            current_time,
            file_hash,
            json.dumps({})
        ))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"  Warning: Failed to register in PostgreSQL: {e}")
        conn.rollback()
        return False

def update_knowledge_data(conn, knowledge_id: str, uploaded_files: List[Dict]) -> bool:
    """Update knowledge.data with file references to make them visible in UI"""
    try:
        cur = conn.cursor()
        
        # Get current knowledge data
        cur.execute("""
            SELECT data 
            FROM knowledge 
            WHERE id = %s
        """, (knowledge_id,))
        
        result = cur.fetchone()
        if not result:
            print("Warning: Knowledge collection not found!")
            return False
        
        current_data = result[0] if result[0] else {}
        
        # Update with new file list
        current_data['files'] = uploaded_files
        
        # Also update file_ids if it exists
        if 'file_ids' in current_data or len(uploaded_files) > 0:
            current_data['file_ids'] = [f['id'] for f in uploaded_files]
        
        # Update the knowledge record
        cur.execute("""
            UPDATE knowledge 
            SET data = %s,
                updated_at = %s,
                meta = jsonb_set(
                    COALESCE(meta, '{}'::jsonb),
                    '{file_count}',
                    %s::jsonb
                )
            WHERE id = %s
        """, (
            json.dumps(current_data),
            int(time.time() * 1000),
            str(len(uploaded_files)),
            knowledge_id
        ))
        
        conn.commit()
        print(f"  [OK] Updated knowledge.data with {len(uploaded_files)} files")
        return True
        
    except Exception as e:
        print(f"  Warning: Failed to update knowledge.data: {e}")
        conn.rollback()
        return False

def ensure_collection_exists(client: QdrantClient, collection_name: str, vector_size: int):
    """Ensure the collection exists with proper configuration"""
    try:
        collections = client.get_collections().collections
        exists = any(col.name == collection_name for col in collections)
        
        if not exists:
            print(f"Creating collection '{collection_name}' with vector size {vector_size}")
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
            )
        else:
            collection_info = client.get_collection(collection_name)
            current_size = collection_info.config.params.vectors.size
            if current_size != vector_size:
                print(f"WARNING: Collection exists with vector size {current_size}, expecting {vector_size}")
    except Exception as e:
        print(f"Error checking/creating collection: {e}")
        raise

def insert_chunks_to_qdrant(client: QdrantClient, chunks: List[str], 
                           embeddings: List[List[float]], file_id: str, 
                           filename: str, user_id: str, knowledge_id: str,
                           batch_size: int = 30) -> bool:
    """Insert chunks into both Qdrant collections"""
    
    # Prepare points for both collections
    files_points = []
    knowledge_points = []
    
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        # Common metadata
        metadata = {
            "source": filename,
            "name": filename,
            "created_by": user_id,
            "file_id": file_id,
            "start_index": idx * (DEFAULT_CHUNK_SIZE - DEFAULT_OVERLAP),
            "hash": hashlib.sha256(chunk.encode()).hexdigest(),
            "embedding_config": json.dumps({
                "engine": "ollama",
                "model": DEFAULT_EMBED_MODEL
            }),
            "extraction_method": "tika"
        }
        
        # Files collection point
        files_points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=embedding,
                payload={
                    "text": chunk,
                    "metadata": metadata,
                    "tenant_id": f"file-{file_id}"
                }
            )
        )
        
        # Knowledge collection point
        knowledge_points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=embedding,
                payload={
                    "text": chunk,
                    "metadata": metadata,
                    "tenant_id": knowledge_id
                }
            )
        )
    
    try:
        # Batch upsert to both collections
        for i in range(0, len(files_points), batch_size):
            batch_files = files_points[i:i+batch_size]
            batch_knowledge = knowledge_points[i:i+batch_size]
            
            client.upsert(
                collection_name=FILES_COLLECTION,
                points=batch_files
            )
            
            client.upsert(
                collection_name=KNOWLEDGE_COLLECTION,
                points=batch_knowledge
            )
            
            if i + batch_size < len(files_points):
                time.sleep(0.1)
        
        return True
        
    except Exception as e:
        print(f"ERROR during Qdrant upsert: {e}")
        return False

def collect_files(input_dir: str) -> List[Path]:
    """Recursively collect all supported files"""
    files = []
    for root, dirs, filenames in os.walk(input_dir):
        for filename in filenames:
            file_path = Path(root) / filename
            if file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                files.append(file_path)
    return sorted(files)

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(
        description="Upload documents to OpenWebUI via Tika and Qdrant with full integration"
    )
    parser.add_argument("--input-dir", default=config['input_dir'])
    parser.add_argument("--knowledge-id", default=config['knowledge_id'])
    parser.add_argument("--user-id", default=config['user_id'])
    parser.add_argument("--qdrant-host", default=config['qdrant_host'])
    parser.add_argument("--qdrant-port", type=int, default=config['qdrant_port'])
    parser.add_argument("--chunk-size", type=int, default=config['chunk_size'])
    parser.add_argument("--overlap", type=int, default=config['overlap'])
    parser.add_argument("--embed-model", default=config['embed_model'])
    parser.add_argument("--ollama-host", default=config['ollama_host'])
    parser.add_argument("--vector-size", type=int, default=config['vector_size'])
    parser.add_argument("--tika-url", default=config['tika_url'])
    parser.add_argument("--batch-points", type=int, default=config['batch_points'])
    parser.add_argument("--clear-checkpoint", action="store_true")
    
    args = parser.parse_args()
    
    if args.clear_checkpoint:
        clear_checkpoint(config['checkpoint_file'])
        print("Checkpoint cleared")
    
    # Test Tika connection
    print(f"Testing Tika connection at {args.tika_url}...")
    try:
        response = requests.get(f"{args.tika_url}/tika")
        if response.status_code == 200:
            print("Tika server is running")
        else:
            print(f"Warning: Tika returned status {response.status_code}")
    except Exception as e:
        print(f"ERROR: Cannot connect to Tika at {args.tika_url}: {e}")
        print("Please start Tika server with: docker run -p 9998:9998 apache/tika:latest")
        return
    
    # Connect to PostgreSQL
    print(f"\nConnecting to PostgreSQL at {config['db_host']}:{config['db_port']}")
    try:
        pg_conn = psycopg2.connect(
            host=config['db_host'],
            port=config['db_port'],
            database=config['db_name'],
            user=config['db_user'],
            password=config['db_password']
        )
        print("PostgreSQL connected successfully")
    except Exception as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}")
        print("Files will be uploaded to Qdrant but won't appear in OpenWebUI!")
        pg_conn = None
    
    # Connect to Qdrant
    print(f"\nConnecting to Qdrant at {args.qdrant_host}:{args.qdrant_port}")
    qdrant_client = QdrantClient(host=args.qdrant_host, port=args.qdrant_port)
    
    try:
        collections = qdrant_client.get_collections()
        print(f"Successfully connected to Qdrant. Found {len(collections.collections)} collections")
    except Exception as e:
        print(f"ERROR: Cannot connect to Qdrant: {e}")
        return
    
    # Configure Ollama
    ollama_client = ollama.Client(host=args.ollama_host)
    
    # Test embedding model
    print(f"\nChecking embedding model '{args.embed_model}'...")
    try:
        test_resp = ollama_client.embeddings(model=args.embed_model, prompt="test")
        test_vec = None
        if hasattr(test_resp, 'embeddings'):
            test_vec = test_resp.embeddings
        elif hasattr(test_resp, 'embedding'):
            test_vec = test_resp.embedding
        elif isinstance(test_resp, list):
            test_vec = test_resp
        elif isinstance(test_resp, dict):
            test_vec = test_resp.get('embeddings') or test_resp.get('embedding')
        
        if test_vec:
            actual_vector_size = len(test_vec)
            print(f"Model OK, generates {actual_vector_size}-dim vectors")
            args.vector_size = actual_vector_size
        else:
            print("ERROR: Model returned unexpected format")
            return
    except Exception as e:
        print(f"ERROR: Cannot connect to Ollama: {e}")
        return
    
    # Ensure collections exist
    ensure_collection_exists(qdrant_client, FILES_COLLECTION, args.vector_size)
    ensure_collection_exists(qdrant_client, KNOWLEDGE_COLLECTION, args.vector_size)
    
    # Load processed files
    processed_files = load_processed_files(config['processed_file'])
    print(f"\nAlready processed {len(processed_files)} files (will skip)")
    
    # Collect files
    print(f"\nLooking for files in: {args.input_dir}")
    all_files = collect_files(args.input_dir)
    
    # Filter out already processed files and skip extensions
    files = []
    skipped_pdfs = 0
    skipped_other = 0
    for file_path in all_files:
        # Skip PDFs and markdown files
        if file_path.suffix.lower() in SKIP_EXTENSIONS:
            if file_path.suffix.lower() == '.pdf':
                skipped_pdfs += 1
            else:
                skipped_other += 1
            continue
            
        # Skip if already processed
        if str(file_path) in processed_files:
            continue
        
        files.append(file_path)
    
    print(f"Found {len(all_files)} total files")
    print(f"Skipping {skipped_pdfs} PDF files (handle with OCR script)")
    print(f"Skipping {skipped_other} other excluded files")
    print(f"Skipping {len(processed_files)} already processed")
    print(f"Will process {len(files)} new files")
    
    if not files:
        print("No new files to process!")
        return
    
    # Load checkpoint and uploaded files list
    checkpoint = load_checkpoint(config['checkpoint_file'])
    start_idx = 0
    uploaded_files = []
    
    if checkpoint:
        last_file = checkpoint.get('last_file')
        uploaded_files = checkpoint.get('uploaded_files', [])
        if last_file:
            for idx, file_path in enumerate(files):
                if str(file_path) == last_file:
                    start_idx = idx + 1
                    print(f"Resuming from file {start_idx}/{len(files)}")
                    print(f"Already uploaded: {len(uploaded_files)} files")
                    break
    
    total_processed = len(uploaded_files)
    failed_files = []
    
    # Process each file
    for idx, file_path in enumerate(files[start_idx:], start_idx):
        try:
            # Get relative path for display
            rel_path = file_path.relative_to(args.input_dir)
            filename = str(rel_path).replace('\\', '/')
            
            print(f"\n[{idx+1}/{len(files)}] Processing: {filename}")
            print(f"  Extension: {file_path.suffix}")
            
            # Extract text with Tika
            print("  Extracting text with Tika...")
            text = extract_text_with_tika(file_path, args.tika_url)
            
            if not text.strip():
                print(f"  No text extracted, skipping")
                processed_files.add(str(file_path))
                save_processed_files(processed_files, config['processed_file'])
                save_checkpoint(str(file_path), config['checkpoint_file'], uploaded_files)
                continue
            
            print(f"  Extracted {len(text)} characters")
            
            file_id = str(uuid.uuid4())
            
            # Register in PostgreSQL FIRST
            if pg_conn:
                if not register_file_in_postgres(pg_conn, file_id, filename, text, 
                                               args.user_id, args.knowledge_id):
                    print("  Warning: PostgreSQL registration failed, file won't show in UI")
            
            # Chunk text
            chunks = chunk_text(text, args.chunk_size, args.overlap)
            print(f"  Created {len(chunks)} chunks")
            
            # Generate embeddings
            embeddings = []
            for chunk in tqdm(chunks, desc="Generating embeddings", leave=False):
                resp = ollama_client.embeddings(model=args.embed_model, prompt=chunk)
                
                vec = None
                if hasattr(resp, 'embeddings'):
                    vec = resp.embeddings
                elif hasattr(resp, 'embedding'):
                    vec = resp.embedding
                elif isinstance(resp, list):
                    vec = resp
                elif isinstance(resp, dict):
                    vec = resp.get('embeddings') or resp.get('embedding')
                
                if vec is None:
                    raise RuntimeError("Failed to extract embedding")
                
                embeddings.append(vec)
            
            # Insert into Qdrant
            success = insert_chunks_to_qdrant(
                qdrant_client,
                chunks,
                embeddings,
                file_id,
                filename,
                args.user_id,
                args.knowledge_id,
                args.batch_points
            )
            
            if not success:
                print(f"  ERROR: Failed to insert chunks to Qdrant")
                failed_files.append(str(file_path))
                continue
            
            # Add to uploaded files list
            uploaded_files.append({
                "id": file_id,
                "filename": filename,
                "name": filename,
                "created_at": int(time.time() * 1000)
            })
            
            # Mark as processed
            processed_files.add(str(file_path))
            save_processed_files(processed_files, config['processed_file'])
            
            total_processed += 1
            save_checkpoint(str(file_path), config['checkpoint_file'], uploaded_files)
            
            # Update knowledge.data periodically
            if total_processed % KNOWLEDGE_UPDATE_BATCH == 0 and pg_conn:
                print(f"\n[Progress] Updating knowledge.data with {len(uploaded_files)} files...")
                update_knowledge_data(pg_conn, args.knowledge_id, uploaded_files)
            
            # Progress check
            if total_processed % 10 == 0:
                print(f"\n[Progress] {total_processed} files processed successfully")
            
            # Small delay between files
            if total_processed % 5 == 0:
                time.sleep(0.5)
            
        except Exception as e:
            print(f"ERROR processing {file_path}: {e}")
            failed_files.append(str(file_path))
    
    # Final knowledge.data update
    if pg_conn and uploaded_files:
        print(f"\n[Final Update] Updating knowledge.data with all {len(uploaded_files)} files...")
        update_knowledge_data(pg_conn, args.knowledge_id, uploaded_files)
    
    # Final report
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Total files processed: {total_processed}")
    print(f"Failed files: {len(failed_files)}")
    
    if failed_files:
        print("\nFailed files:")
        for f in failed_files[:10]:
            print(f"  - {f}")
        if len(failed_files) > 10:
            print(f"  ... and {len(failed_files) - 10} more")
    
    # Show collection statistics
    try:
        files_info = qdrant_client.get_collection(FILES_COLLECTION)
        knowledge_info = qdrant_client.get_collection(KNOWLEDGE_COLLECTION)
        print(f"\nQdrant Collections:")
        print(f"  {FILES_COLLECTION}: {files_info.points_count} points")
        print(f"  {KNOWLEDGE_COLLECTION}: {knowledge_info.points_count} points")
    except Exception as e:
        print(f"Could not get collection info: {e}")
    
    # Close PostgreSQL connection
    if pg_conn:
        pg_conn.close()
    
    clear_checkpoint(config['checkpoint_file'])
    
    print("\n[OK] Files uploaded with full OpenWebUI integration!")
    print("All files should be immediately visible in OpenWebUI.")

if __name__ == "__main__":
    main()