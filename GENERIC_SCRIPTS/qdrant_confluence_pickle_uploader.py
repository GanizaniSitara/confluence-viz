#!/usr/bin/env python3
"""
Upload Confluence pickle files to OpenWebUI via Qdrant with full integration.
This ensures Confluence pages are visible in OpenWebUI's interface by:
1. Registering files in PostgreSQL
2. Uploading vectors to Qdrant
3. Updating knowledge.data with file references

This script reads pickle files created by sample_and_pickle_spaces.py
and processes them page by page into OpenWebUI.
"""
import argparse
import configparser
import json
import uuid
import time
import hashlib
import os
import pickle
import psycopg2
from pathlib import Path
from typing import List, Dict, Any, Tuple
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import ollama
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup
import html2text

# Detect if we're in WSL
is_wsl = os.path.exists('/proc/version') and 'microsoft' in open('/proc/version').read().lower()

# OpenWebUI collection names
FILES_COLLECTION = "open-webui_files"
KNOWLEDGE_COLLECTION = "open-webui_knowledge"

# Batch size for knowledge.data updates
KNOWLEDGE_UPDATE_BATCH = 100  # Update knowledge.data every 100 files

def load_config():
    """Load configuration from settings.ini"""
    config = configparser.ConfigParser()
    config_file = 'settings.ini'
    
    # Initialize empty settings
    settings = {}
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found. Please create it from settings.example.ini")
    
    config.read(config_file)
    
    # Load settings from qdrant_uploader section (common settings)
    if 'qdrant_uploader' in config:
        for key, value in config['qdrant_uploader'].items():
            settings[key] = value
    else:
        raise ValueError("Missing [qdrant_uploader] section in settings.ini")
    
    # Add settings specific to confluence_pickle_uploader
    if 'confluence_pickle_uploader' in config:
        for key, value in config['confluence_pickle_uploader'].items():
            settings[key] = value
    
    # Required settings check
    required_settings = [
        'knowledge_id', 'user_id', 'ollama_host', 'embed_model',
        'qdrant_host', 'qdrant_port', 'chunk_size', 'overlap',
        'db_host', 'db_port', 'db_name', 'db_user', 'db_password',
        'pickle_dir'
    ]
    
    missing_settings = []
    for setting in required_settings:
        if setting not in settings:
            missing_settings.append(setting)
    
    if missing_settings:
        raise ValueError(f"Missing required settings: {', '.join(missing_settings)}")
    
    # Convert types for numeric settings
    if 'qdrant_port' in settings:
        settings['qdrant_port'] = int(settings['qdrant_port'])
    if 'chunk_size' in settings:
        settings['chunk_size'] = int(settings['chunk_size'])
    if 'overlap' in settings:
        settings['overlap'] = int(settings['overlap'])
    if 'vector_size' in settings:
        settings['vector_size'] = int(settings.get('vector_size', '768'))
    else:
        settings['vector_size'] = 768  # Default if not specified
    if 'batch_points' in settings:
        settings['batch_points'] = int(settings.get('batch_points', '30'))
    else:
        settings['batch_points'] = 30
    if 'db_port' in settings:
        settings['db_port'] = int(settings['db_port'])
    
    # Convert boolean settings
    settings['process_all_spaces'] = settings.get('process_all_spaces', 'false').lower() == 'true'
    settings['html_to_markdown'] = settings.get('html_to_markdown', 'true').lower() == 'true'
    
    # Set defaults for optional settings
    if 'checkpoint_file' not in settings:
        settings['checkpoint_file'] = 'qdrant_confluence_pickle_checkpoint.json'
    if 'base_url' not in settings:
        settings['base_url'] = 'https://confluence.example.com'
    if 'space_keys' not in settings:
        settings['space_keys'] = ''
    
    return settings

def save_checkpoint(checkpoint_data: Dict, checkpoint_file: str):
    """Save checkpoint to track progress"""
    checkpoint_data['timestamp'] = time.time()
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)

def load_checkpoint(checkpoint_file: str):
    """Load checkpoint if it exists"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                print(f"Loading checkpoint from: {checkpoint_file}")
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load checkpoint: {e}")
    return {
        'processed_spaces': {},  # space_key -> {'pages': [page_ids], 'completed': bool}
        'uploaded_files': []     # List of file metadata for knowledge.data
    }

def clear_checkpoint(checkpoint_file: str):
    """Clear checkpoint after successful completion"""
    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print(f"Checkpoint cleared: {checkpoint_file}")
        except Exception as e:
            print(f"Warning: Failed to clear checkpoint: {e}")

def html_to_markdown_text(html_content: str) -> str:
    """Convert Confluence HTML to markdown for better readability"""
    if not html_content:
        return ""
    
    # Configure html2text
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = False
    h.body_width = 0  # Don't wrap lines
    h.single_line_break = True
    
    try:
        # Parse and clean HTML first
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Convert to markdown
        markdown = h.handle(str(soup))
        
        # Clean up excessive whitespace
        lines = markdown.split('\n')
        cleaned_lines = []
        prev_empty = False
        
        for line in lines:
            line = line.rstrip()
            if line:
                cleaned_lines.append(line)
                prev_empty = False
            elif not prev_empty:
                cleaned_lines.append('')
                prev_empty = True
        
        return '\n'.join(cleaned_lines).strip()
    except Exception as e:
        print(f"Warning: Failed to convert HTML to markdown: {e}")
        # Fallback to BeautifulSoup text extraction
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup.get_text(separator='\n', strip=True)
        except:
            return html_content

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

def create_page_filename(space_key: str, page_title: str, page_id: str) -> str:
    """Create a meaningful filename for a Confluence page"""
    # Clean the title for use in filename
    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in page_title)
    safe_title = safe_title.strip()[:100]  # Limit length
    
    # Format: SPACEKEY_PageTitle_pageID.md
    return f"{space_key}_{safe_title}_{page_id}.md"

def register_file_in_postgres(conn, file_id: str, filename: str, content: str, 
                            user_id: str, knowledge_id: str, metadata: Dict) -> bool:
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
        
        file_meta = {
            'name': filename,
            'content_type': 'text/markdown' if metadata.get('html_to_markdown') else 'text/html',
            'size': len(content),
            'knowledge_id': knowledge_id,
            'source': 'confluence',
            'space_key': metadata.get('space_key', ''),
            'page_id': metadata.get('page_id', ''),
            'page_title': metadata.get('page_title', ''),
            'confluence_url': metadata.get('confluence_url', ''),
            'last_updated': metadata.get('last_updated', '')
        }
        
        cur.execute("""
            INSERT INTO file (id, user_id, filename, meta, created_at, updated_at, hash, data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            file_id,
            user_id,
            filename,
            json.dumps(file_meta),
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
                meta = json_build_object(
                    'file_count', %s::int
                )
            WHERE id = %s
        """, (
            json.dumps(current_data),
            int(time.time() * 1000),
            len(uploaded_files),
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
                           metadata: Dict, batch_size: int = 30, 
                           chunk_size: int = 500, overlap: int = 50) -> bool:
    """Insert chunks into both Qdrant collections"""
    
    # Prepare points for both collections
    files_points = []
    knowledge_points = []
    
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        # Common metadata
        chunk_metadata = {
            "source": filename,
            "name": filename,
            "created_by": user_id,
            "file_id": file_id,
            "start_index": idx * (chunk_size - overlap),
            "hash": hashlib.sha256(chunk.encode()).hexdigest(),
            "embedding_config": json.dumps({
                "engine": "ollama",
                "model": metadata.get('embed_model', 'nomic-embed-text:v1.5')
            }),
            "space_key": metadata.get('space_key', ''),
            "page_id": metadata.get('page_id', ''),
            "page_title": metadata.get('page_title', ''),
            "confluence_source": True
        }
        
        # Files collection point
        files_points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=embedding,
                payload={
                    "text": chunk,
                    "metadata": chunk_metadata,
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
                    "metadata": chunk_metadata,
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

def process_confluence_page(page_data: Dict, space_key: str, space_name: str,
                          config: Dict, pg_conn, qdrant_client, ollama_client,
                          base_confluence_url: str, all_pages_lookup: Dict = None) -> Tuple[bool, Dict]:
    """Process a single Confluence page and upload to OpenWebUI"""
    
    page_id = page_data.get('id', '')
    page_title = page_data.get('title', 'Untitled')
    page_body = page_data.get('body', '')
    last_updated = page_data.get('updated', '')
    parent_id = page_data.get('parent_id')
    level = page_data.get('level', 0)
    
    if not page_id or not page_body:
        return False, None
    
    print(f"    Processing page: {page_title} (ID: {page_id})")
    
    # Build page hierarchy path
    page_path = []
    if all_pages_lookup and parent_id:
        current_parent_id = parent_id
        max_depth = 10  # Prevent infinite loops
        depth = 0
        while current_parent_id and depth < max_depth:
            parent_page = all_pages_lookup.get(current_parent_id)
            if parent_page:
                page_path.insert(0, parent_page.get('title', 'Unknown'))
                current_parent_id = parent_page.get('parent_id')
            else:
                break
            depth += 1
    
    # Build the content with metadata header
    confluence_url = f"{base_confluence_url}/pages/viewpage.action?pageId={page_id}"
    
    # Create metadata header
    metadata_header = f"---\n"
    metadata_header += f"Source: Confluence - {space_name} ({space_key})\n"
    metadata_header += f"Title: {page_title}\n"
    metadata_header += f"URL: {confluence_url}\n"
    if page_path:
        metadata_header += f"Path: {space_name} > {' > '.join(page_path)} > {page_title}\n"
    else:
        metadata_header += f"Path: {space_name} > {page_title}\n"
    metadata_header += f"Last Updated: {last_updated}\n"
    metadata_header += f"---\n\n"
    
    # Convert HTML to markdown if enabled
    if config['html_to_markdown']:
        page_content = html_to_markdown_text(page_body)
    else:
        page_content = page_body
    
    if not page_content.strip():
        print(f"    Warning: No content after processing, skipping page")
        return False, None
    
    # Combine metadata header with content
    content = metadata_header + page_content
    
    # Create filename
    filename = create_page_filename(space_key, page_title, page_id)
    file_id = str(uuid.uuid4())
    
    # Page metadata
    page_metadata = {
        'space_key': space_key,
        'space_name': space_name,
        'page_id': page_id,
        'page_title': page_title,
        'last_updated': last_updated,
        'confluence_url': f"{base_confluence_url}/pages/viewpage.action?pageId={page_id}",
        'html_to_markdown': config['html_to_markdown'],
        'embed_model': config['embed_model']
    }
    
    # Register in PostgreSQL
    if pg_conn:
        if not register_file_in_postgres(pg_conn, file_id, filename, content, 
                                       config['user_id'], config['knowledge_id'], 
                                       page_metadata):
            print("    Warning: PostgreSQL registration failed, file won't show in UI")
    
    # Chunk text
    chunks = chunk_text(content, config['chunk_size'], config['overlap'])
    print(f"    Created {len(chunks)} chunks")
    
    # Generate embeddings
    embeddings = []
    for chunk in chunks:
        try:
            resp = ollama_client.embeddings(model=config['embed_model'], prompt=chunk)
            
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
        except Exception as e:
            print(f"    Error generating embedding: {e}")
            return False, None
    
    # Insert into Qdrant
    success = insert_chunks_to_qdrant(
        qdrant_client,
        chunks,
        embeddings,
        file_id,
        filename,
        config['user_id'],
        config['knowledge_id'],
        page_metadata,
        config['batch_points'],
        config['chunk_size'],
        config['overlap']
    )
    
    if not success:
        print(f"    ERROR: Failed to insert chunks to Qdrant")
        return False, None
    
    # Return file metadata for knowledge.data update
    file_info = {
        "id": file_id,
        "filename": filename,
        "name": filename,
        "created_at": int(time.time() * 1000)
    }
    
    return True, file_info

def get_pickle_files(pickle_dir: str, space_keys: List[str] = None) -> List[Path]:
    """Get list of pickle files to process"""
    pickle_path = Path(pickle_dir)
    
    if not pickle_path.exists():
        print(f"Error: Pickle directory not found: {pickle_dir}")
        return []
    
    all_pickles = list(pickle_path.glob("*.pkl"))
    
    if space_keys:
        # Filter to specific space keys
        filtered_pickles = []
        for pkl in all_pickles:
            base_name = pkl.stem
            # Handle both regular and _full suffixed files
            if base_name.endswith('_full'):
                base_name = base_name[:-5]
            
            if base_name in space_keys:
                filtered_pickles.append(pkl)
        return sorted(filtered_pickles)
    else:
        # Return all non-personal space pickles
        return sorted([p for p in all_pickles if not p.stem.startswith('~')])

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(
        description="Upload Confluence pickle files to OpenWebUI with full integration"
    )
    parser.add_argument("--pickle-dir", default=config['pickle_dir'],
                       help="Directory containing Confluence pickle files")
    parser.add_argument("--knowledge-id", default=config['knowledge_id'])
    parser.add_argument("--user-id", default=config['user_id'])
    parser.add_argument("--qdrant-host", default=config['qdrant_host'])
    parser.add_argument("--qdrant-port", type=int, default=config['qdrant_port'])
    parser.add_argument("--chunk-size", type=int, default=config['chunk_size'])
    parser.add_argument("--overlap", type=int, default=config['overlap'])
    parser.add_argument("--embed-model", default=config['embed_model'])
    parser.add_argument("--ollama-host", default=config['ollama_host'])
    parser.add_argument("--vector-size", type=int, default=config['vector_size'])
    parser.add_argument("--batch-points", type=int, default=config['batch_points'])
    parser.add_argument("--space-keys", nargs='+', 
                       help="Specific space keys to process (default: all)")
    parser.add_argument("--all-spaces", action="store_true",
                       help="Process all space pickle files")
    parser.add_argument("--no-markdown", action="store_true",
                       help="Keep original HTML instead of converting to markdown")
    parser.add_argument("--clear-checkpoint", action="store_true",
                       help="Clear checkpoint and start fresh")
    parser.add_argument("--base-url", default="https://confluence.example.com",
                       help="Base URL of Confluence instance for generating links")
    
    args = parser.parse_args()
    
    # Override config with command line args
    if args.no_markdown:
        config['html_to_markdown'] = False
    
    if args.clear_checkpoint:
        clear_checkpoint(config['checkpoint_file'])
        print("Checkpoint cleared")
    
    # Load checkpoint
    checkpoint = load_checkpoint(config['checkpoint_file'])
    uploaded_files = checkpoint.get('uploaded_files', [])
    processed_spaces = checkpoint.get('processed_spaces', {})
    
    # Connect to PostgreSQL
    print(f"Connecting to PostgreSQL at {config['db_host']}:{config['db_port']}")
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
    
    # Get pickle files to process
    space_keys = args.space_keys if args.space_keys else (config.get('space_keys', '').split(',') if config.get('space_keys') else None)
    if not args.all_spaces and not space_keys:
        print("\nNo spaces specified. Use --all-spaces or --space-keys to specify which spaces to process.")
        return
    
    pickle_files = get_pickle_files(args.pickle_dir, space_keys if not args.all_spaces else None)
    
    print(f"\nFound {len(pickle_files)} pickle files to process")
    if not pickle_files:
        print("No pickle files found!")
        return
    
    total_pages_processed = 0
    total_spaces_processed = 0
    failed_pages = 0
    
    # Process each pickle file
    for pickle_file in pickle_files:
        space_key = pickle_file.stem
        if space_key.endswith('_full'):
            space_key = space_key[:-5]
        
        print(f"\n[Space {total_spaces_processed + 1}/{len(pickle_files)}] Processing space: {space_key}")
        
        # Check if space already processed
        if space_key in processed_spaces and processed_spaces[space_key].get('completed', False):
            print(f"  Space already fully processed, skipping")
            continue
        
        try:
            # Load pickle file
            with open(pickle_file, 'rb') as f:
                pickle_data = pickle.load(f)
            
            space_name = pickle_data.get('name', space_key)
            pages = pickle_data.get('sampled_pages', [])
            total_pages = pickle_data.get('total_pages_in_space', len(pages))
            
            print(f"  Space name: {space_name}")
            print(f"  Pages in pickle: {len(pages)} (Total in space: {total_pages})")
            
            # Get already processed pages for this space
            processed_page_ids = set(processed_spaces.get(space_key, {}).get('pages', []))
            
            # Create a lookup dictionary for all pages (for building hierarchy paths)
            all_pages_lookup = {p.get('id'): p for p in pages if p.get('id')}
            
            # Process each page
            space_page_count = 0
            space_failed_count = 0
            
            for idx, page in enumerate(pages):
                page_id = page.get('id', '')
                
                # Skip if already processed
                if page_id in processed_page_ids:
                    continue
                
                # Process page
                success, file_info = process_confluence_page(
                    page, space_key, space_name, config, pg_conn, 
                    qdrant_client, ollama_client, args.base_url, all_pages_lookup
                )
                
                if success and file_info:
                    uploaded_files.append(file_info)
                    
                    # Update checkpoint
                    if space_key not in processed_spaces:
                        processed_spaces[space_key] = {'pages': [], 'completed': False}
                    processed_spaces[space_key]['pages'].append(page_id)
                    
                    checkpoint['processed_spaces'] = processed_spaces
                    checkpoint['uploaded_files'] = uploaded_files
                    save_checkpoint(checkpoint, config['checkpoint_file'])
                    
                    space_page_count += 1
                    total_pages_processed += 1
                    
                    # Update knowledge.data periodically
                    if total_pages_processed % KNOWLEDGE_UPDATE_BATCH == 0 and pg_conn:
                        print(f"\n[Progress] Updating knowledge.data with {len(uploaded_files)} files...")
                        update_knowledge_data(pg_conn, args.knowledge_id, uploaded_files)
                else:
                    space_failed_count += 1
                    failed_pages += 1
                
                # Progress indicator
                if (idx + 1) % 10 == 0 or (idx + 1) == len(pages):
                    print(f"    Progress: {idx + 1}/{len(pages)} pages processed")
            
            # Mark space as completed
            processed_spaces[space_key]['completed'] = True
            checkpoint['processed_spaces'] = processed_spaces
            save_checkpoint(checkpoint, config['checkpoint_file'])
            
            print(f"  Space complete: {space_page_count} pages uploaded, {space_failed_count} failed")
            total_spaces_processed += 1
            
            # Small delay between spaces
            if total_spaces_processed < len(pickle_files):
                time.sleep(0.5)
            
        except Exception as e:
            print(f"  ERROR processing space {space_key}: {e}")
            continue
    
    # Final knowledge.data update
    if pg_conn and uploaded_files:
        print(f"\n[Final Update] Updating knowledge.data with all {len(uploaded_files)} files...")
        update_knowledge_data(pg_conn, args.knowledge_id, uploaded_files)
    
    # Final report
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Total spaces processed: {total_spaces_processed}")
    print(f"Total pages uploaded: {total_pages_processed}")
    print(f"Failed pages: {failed_pages}")
    
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
    
    print("\n[OK] Confluence pages uploaded with full OpenWebUI integration!")
    print("All pages should be immediately visible in OpenWebUI.")

if __name__ == "__main__":
    main()