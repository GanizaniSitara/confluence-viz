#!/usr/bin/env python3
"""
Upload Confluence pickle files to OpenWebUI via Qdrant WITHOUT Ollama dependency.
Uses sentence-transformers directly for much faster and more reliable embedding generation.
Shares the same checkpoint file with the Ollama version for seamless continuation.

This ensures Confluence pages are visible in OpenWebUI's interface by:
1. Registering files in PostgreSQL
2. Uploading vectors to Qdrant
3. Updating knowledge.data with file references
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
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer
import torch
import numpy as np
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup
import html2text

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from utils.html_cleaner import clean_confluence_html
    USE_CONFLUENCE_CLEANER = True
except ImportError:
    print("Warning: Could not import clean_confluence_html from utils, using fallback")
    USE_CONFLUENCE_CLEANER = False

# Detect if we're in WSL
is_wsl = os.path.exists('/proc/version') and 'microsoft' in open('/proc/version').read().lower()

# OpenWebUI collection names
FILES_COLLECTION = "open-webui_files"
KNOWLEDGE_COLLECTION = "open-webui_knowledge"

# Batch size for knowledge.data updates
KNOWLEDGE_UPDATE_BATCH = 100  # Update knowledge.data every 100 files

# Nomic model info
NOMIC_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
NOMIC_VECTOR_SIZE = 768

def html_to_markdown_text(html_content: str) -> str:
    """Convert Confluence HTML to markdown for better readability"""
    if not html_content:
        return ""
    
    # Use the Confluence cleaner if available
    if USE_CONFLUENCE_CLEANER:
        html_content = clean_confluence_html(html_content)
    
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

def create_metadata_header(space_key: str, page_title: str, page_id: str, 
                          last_updated: str, confluence_url: str) -> str:
    """
    Create a metadata header to prepend to Confluence pages.
    Uses double line breaks for better display in OpenWebUI.
    """
    header_lines = []
    
    # Use smaller, less prominent formatting for the title
    header_lines.append(f"### {page_title}")
    header_lines.append("")
    header_lines.append(f"**Space:** {space_key}")
    header_lines.append(f"**Page ID:** {page_id}")
    header_lines.append(f"**Last Updated:** {last_updated}")
    header_lines.append(f"**URL:** {confluence_url}")
    header_lines.append("")
    header_lines.append("---")
    header_lines.append("")
    
    return "\n".join(header_lines)

def chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
    """Split text into overlapping chunks."""
    if not text:
        return []
    
    words = text.split()
    chunks = []
    
    for i in range(0, len(words), chunk_size - overlap):
        chunk = ' '.join(words[i:i + chunk_size])
        if chunk:
            chunks.append(chunk)
    
    if not chunks and text:
        chunks = [text]
    
    return chunks

def create_page_filename(space_key: str, page_title: str, page_id: str) -> str:
    """Create a meaningful filename for a Confluence page"""
    # Clean the title for use in filename
    safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in page_title)
    safe_title = safe_title.strip()[:100]  # Limit length
    
    # Format: SPACEKEY_PageTitle_pageID.md
    return f"{space_key}_{safe_title}_{page_id}.md"

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
    
    # Add settings specific to confluence_pickle_uploader_direct
    if 'confluence_pickle_uploader_direct' in config:
        for key, value in config['confluence_pickle_uploader_direct'].items():
            settings[key] = value
    
    # Required settings check
    required_settings = [
        'knowledge_id', 'user_id',
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
    for key in ['qdrant_port', 'chunk_size', 'overlap', 'batch_points',
                'db_port', 'knowledge_update_batch', 'embedding_batch_size']:
        if key in settings:
            settings[key] = int(settings[key])
    
    # Set defaults
    settings['vector_size'] = NOMIC_VECTOR_SIZE
    settings['batch_points'] = settings.get('batch_points', 30)
    settings['knowledge_update_batch'] = settings.get('knowledge_update_batch', KNOWLEDGE_UPDATE_BATCH)
    settings['embedding_batch_size'] = settings.get('embedding_batch_size', 32)
    settings['device'] = settings.get('device', 'cuda' if torch.cuda.is_available() else 'cpu')
    settings['checkpoint_file'] = settings.get('checkpoint_file', 'qdrant_confluence_pickle_checkpoint.json')
    settings['base_url'] = settings.get('base_url', 'https://confluence.example.com')
    settings['files_collection'] = settings.get('files_collection', FILES_COLLECTION)
    settings['knowledge_collection'] = settings.get('knowledge_collection', KNOWLEDGE_COLLECTION)
    
    # Convert boolean settings
    settings['process_all_spaces'] = settings.get('process_all_spaces', 'false').lower() == 'true'
    settings['html_to_markdown'] = settings.get('html_to_markdown', 'true').lower() == 'true'
    
    # Handle WSL-specific settings
    if is_wsl:
        if 'db_host_wsl' in settings:
            settings['db_host'] = settings['db_host_wsl']
    
    return settings

def ensure_collection_exists(client: QdrantClient, collection_name: str, vector_size: int):
    """Ensure Qdrant collection exists."""
    try:
        client.get_collection(collection_name)
    except:
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
        )
        print(f"Created collection: {collection_name}")

def get_db_connection(config: dict):
    """Create PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=config['db_host'],
            port=config['db_port'],
            database=config['db_name'],
            user=config['db_user'],
            password=config['db_password']
        )
        return conn
    except Exception as e:
        print(f"Warning: Could not connect to PostgreSQL: {e}")
        return None

def register_file_in_postgres(conn, file_id: str, filename: str, content: str, 
                            user_id: str, knowledge_id: str, metadata: Dict) -> bool:
    """Register file in PostgreSQL database"""
    print(f"    Registering in PostgreSQL - content length: {len(content)}, first 100 chars: {repr(content[:100])}...")
    
    try:
        cur = conn.cursor()
        
        # Check if file already exists
        cur.execute("SELECT id FROM file WHERE id = %s", (file_id,))
        if cur.fetchone():
            print(f"  File already registered in PostgreSQL")
            return True
        
        # Insert file record
        file_hash = hashlib.sha256(content.encode()).hexdigest()
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
        
        print(f"    File metadata: size={file_meta['size']}, content_type={file_meta['content_type']}")
        
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
            json.dumps({'content': content})
        ))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"  Warning: Failed to register in PostgreSQL: {e}")
        conn.rollback()
        return False

def update_knowledge_data(conn, knowledge_id: str, uploaded_files: List[Dict]) -> bool:
    """Update knowledge.data with file references."""
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT data FROM knowledge WHERE id = %s", (knowledge_id,))
        result = cursor.fetchone()
        
        if result:
            current_data = result[0] if result[0] else {"file_ids": []}
        else:
            current_data = {"file_ids": []}
        
        if "file_ids" not in current_data:
            current_data["file_ids"] = []
        
        # Add new file IDs
        for file_info in uploaded_files:
            if file_info['file_id'] not in current_data["file_ids"]:
                current_data["file_ids"].append(file_info['file_id'])
        
        # Update knowledge
        cursor.execute("""
            UPDATE knowledge 
            SET data = %s::jsonb,
                updated_at = %s,
                meta = COALESCE(meta, '{}'::json)
            WHERE id = %s
        """, (json.dumps(current_data), int(time.time()), knowledge_id))
        
        conn.commit()
        cursor.close()
        print(f"Updated knowledge.data with {len(uploaded_files)} files")
        return True
    except Exception as e:
        print(f"Error updating knowledge data: {e}")
        conn.rollback()
        return False

def upload_vectors_to_qdrant(client: QdrantClient, chunks: List[str], 
                            embeddings, file_id: str,  # Can be numpy array or list
                            filename: str, user_id: str, knowledge_id: str,
                            metadata: Dict, batch_size: int,
                            files_collection: str, knowledge_collection: str) -> bool:
    """Upload chunks with embeddings to Qdrant collections"""
    try:
        # Convert numpy array to list only when needed
        if hasattr(embeddings, 'tolist'):
            embeddings_list = embeddings.tolist()
        else:
            embeddings_list = embeddings
            
        # Insert into files collection
        file_points = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings_list)):
            point_id = str(uuid.uuid4())
            file_points.append(PointStruct(
                id=point_id,
                vector=embedding,
                payload={
                    "file_id": file_id,
                    "chunk_index": i,
                    "content": chunk,
                    "source": filename,
                    "space_key": metadata.get('space_key', ''),
                    "page_id": metadata.get('page_id', ''),
                    "page_title": metadata.get('page_title', '')
                }
            ))
        
        # Batch upload to files collection
        for i in range(0, len(file_points), batch_size):
            batch = file_points[i:i + batch_size]
            client.upsert(collection_name=files_collection, points=batch)
        
        # Insert into knowledge collection
        knowledge_points = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings_list)):
            point_id = str(uuid.uuid4())
            knowledge_points.append(PointStruct(
                id=point_id,
                vector=embedding,
                payload={
                    "knowledge_id": knowledge_id,
                    "file_id": file_id,
                    "user_id": user_id,
                    "chunk_index": i,
                    "content": chunk,
                    "source": filename,
                    "space_key": metadata.get('space_key', ''),
                    "page_id": metadata.get('page_id', ''),
                    "page_title": metadata.get('page_title', '')
                }
            ))
        
        # Batch upload to knowledge collection
        for i in range(0, len(knowledge_points), batch_size):
            batch = knowledge_points[i:i + batch_size]
            client.upsert(collection_name=knowledge_collection, points=batch)
        
        return True
    except Exception as e:
        print(f"Error uploading to Qdrant: {e}")
        return False

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

def load_pickle_files(pickle_dir: str, process_all_spaces: bool = False, 
                     space_keys: List[str] = None) -> Dict[str, List]:
    """Load Confluence data from pickle files"""
    pickle_path = Path(pickle_dir)
    
    if not pickle_path.exists():
        raise FileNotFoundError(f"Pickle directory not found: {pickle_dir}")
    
    # Load pages
    pages_file = pickle_path / "confluence_pages.pkl"
    if not pages_file.exists():
        raise FileNotFoundError(f"Pages file not found: {pages_file}")
    
    with open(pages_file, 'rb') as f:
        all_pages = pickle.load(f)
    
    print(f"Loaded {len(all_pages)} total pages from pickle")
    
    # Group pages by space
    pages_by_space = {}
    for page in all_pages:
        space_key = page.get('space', {}).get('key', 'UNKNOWN')
        if space_key not in pages_by_space:
            pages_by_space[space_key] = []
        pages_by_space[space_key].append(page)
    
    # Filter spaces based on configuration
    if not process_all_spaces and space_keys:
        filtered_spaces = {}
        for key in space_keys:
            if key in pages_by_space:
                filtered_spaces[key] = pages_by_space[key]
            else:
                print(f"Warning: Space key '{key}' not found in pickle data")
        pages_by_space = filtered_spaces
    
    print(f"Processing {len(pages_by_space)} spaces")
    for space_key, pages in pages_by_space.items():
        print(f"  {space_key}: {len(pages)} pages")
    
    return pages_by_space

def process_confluence_pages(pages_by_space: Dict[str, List], config: Dict, 
                            model: SentenceTransformer, qdrant_client: QdrantClient, 
                            pg_conn, checkpoint: Dict) -> int:
    """Process Confluence pages and upload to Qdrant/PostgreSQL"""
    total_processed = 0
    uploaded_files = checkpoint.get('uploaded_files', [])
    
    for space_key, pages in pages_by_space.items():
        # Check if space already processed
        if space_key in checkpoint['processed_spaces'] and checkpoint['processed_spaces'][space_key].get('completed'):
            print(f"\nSpace {space_key} already completed, skipping...")
            continue
        
        # Get list of already processed pages for this space
        processed_pages = set(checkpoint['processed_spaces'].get(space_key, {}).get('pages', []))
        
        print(f"\nProcessing space: {space_key}")
        print(f"  Already processed: {len(processed_pages)} pages")
        
        space_pages = 0
        for page in tqdm(pages, desc=f"Space {space_key}"):
            page_id = page.get('id', '')
            
            # Skip if already processed
            if page_id in processed_pages:
                continue
            
            try:
                # Extract page details
                page_title = page.get('title', 'Untitled')
                last_updated = page.get('history', {}).get('lastUpdated', {}).get('when', '')
                
                # Get page body
                body = page.get('body', {})
                storage = body.get('storage', body.get('view', {}))
                html_content = storage.get('value', '')
                
                if not html_content:
                    print(f"  Skipping empty page: {page_title}")
                    processed_pages.add(page_id)
                    continue
                
                # Convert HTML to markdown if configured
                if config.get('html_to_markdown', True):
                    content = html_to_markdown_text(html_content)
                else:
                    content = html_content
                
                # Create metadata header
                confluence_url = f"{config['base_url']}/pages/viewpage.action?pageId={page_id}"
                header = create_metadata_header(space_key, page_title, page_id, 
                                               last_updated, confluence_url)
                
                # Combine header and content
                full_content = header + content
                
                # Create chunks
                chunks = chunk_text(full_content, config['chunk_size'], config['overlap'])
                
                if not chunks:
                    print(f"  No chunks created for: {page_title}")
                    processed_pages.add(page_id)
                    continue
                
                # Generate embeddings in batches
                embeddings = []
                for i in range(0, len(chunks), config['embedding_batch_size']):
                    batch = chunks[i:i + config['embedding_batch_size']]
                    # Add prefixes as per Nomic documentation
                    batch_with_prefix = [f"search_document: {chunk}" for chunk in batch]
                    batch_embeddings = model.encode(batch_with_prefix, 
                                                   show_progress_bar=False,
                                                   convert_to_numpy=True)
                    if len(embeddings) == 0:
                        embeddings = batch_embeddings
                    else:
                        embeddings = np.vstack([embeddings, batch_embeddings])
                
                # Generate file ID and filename
                file_id = str(uuid.uuid4())
                filename = create_page_filename(space_key, page_title, page_id)
                
                # Prepare metadata
                metadata = {
                    'space_key': space_key,
                    'page_id': page_id,
                    'page_title': page_title,
                    'confluence_url': confluence_url,
                    'last_updated': last_updated,
                    'html_to_markdown': config.get('html_to_markdown', True)
                }
                
                # Store in PostgreSQL
                if pg_conn:
                    success = register_file_in_postgres(
                        pg_conn, file_id, filename, full_content,
                        config['user_id'], config['knowledge_id'], metadata
                    )
                    if not success:
                        print(f"  Failed to register in PostgreSQL: {page_title}")
                        continue
                
                # Upload to Qdrant
                success = upload_vectors_to_qdrant(
                    qdrant_client, chunks, embeddings, file_id, filename,
                    config['user_id'], config['knowledge_id'], metadata,
                    config['batch_points'], config['files_collection'], 
                    config['knowledge_collection']
                )
                
                if success:
                    uploaded_files.append({
                        'file_id': file_id,
                        'filename': filename,
                        'chunks': len(chunks)
                    })
                    processed_pages.add(page_id)
                    space_pages += 1
                    total_processed += 1
                    
                    # Update checkpoint
                    if space_key not in checkpoint['processed_spaces']:
                        checkpoint['processed_spaces'][space_key] = {'pages': [], 'completed': False}
                    checkpoint['processed_spaces'][space_key]['pages'] = list(processed_pages)
                    checkpoint['uploaded_files'] = uploaded_files
                    
                    # Update knowledge.data periodically
                    if total_processed % config['knowledge_update_batch'] == 0 and pg_conn:
                        if uploaded_files:
                            update_knowledge_data(pg_conn, config['knowledge_id'], uploaded_files)
                            uploaded_files = []  # Clear after successful update
                    
                    # Save checkpoint periodically
                    if total_processed % 10 == 0:
                        save_checkpoint(checkpoint, config['checkpoint_file'])
                
            except Exception as e:
                print(f"\n  Error processing page {page_id}: {e}")
                continue
        
        # Mark space as completed
        if space_key not in checkpoint['processed_spaces']:
            checkpoint['processed_spaces'][space_key] = {'pages': [], 'completed': False}
        checkpoint['processed_spaces'][space_key]['completed'] = True
        checkpoint['processed_spaces'][space_key]['pages'] = list(processed_pages)
        save_checkpoint(checkpoint, config['checkpoint_file'])
        
        print(f"  Completed space {space_key}: {space_pages} pages processed")
    
    # Final knowledge update
    if pg_conn and uploaded_files:
        print(f"\n[Final Update] Updating knowledge.data with {len(uploaded_files)} remaining files...")
        update_knowledge_data(pg_conn, config['knowledge_id'], uploaded_files)
    
    return total_processed

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(
        description="Upload Confluence pickle files to OpenWebUI via Qdrant (direct embedding, no Ollama)"
    )
    parser.add_argument("--clear-checkpoint", action="store_true", 
                       help="Clear checkpoint and start fresh")
    parser.add_argument("--test-mode", action="store_true", 
                       help="Process only first 10 pages for testing")
    parser.add_argument("--space", type=str, 
                       help="Process only this specific space key")
    
    args = parser.parse_args()
    
    
    if args.clear_checkpoint:
        clear_checkpoint(config['checkpoint_file'])
        print("Checkpoint cleared")
    
    # Initialize components
    print(f"\n=== Confluence Pickle Uploader (Direct Embedding) ===")
    print(f"Using device: {config['device']}")
    print(f"Pickle directory: {config['pickle_dir']}")
    print(f"Knowledge ID: {config['knowledge_id']}")
    
    # Start timing
    start_time = time.time()
    
    # Load the model directly
    print("\nLoading Nomic model directly (this may take a moment on first run)...")
    try:
        model = SentenceTransformer(NOMIC_MODEL_NAME, trust_remote_code=True)
        model.to(config['device'])
        print(f"✓ Model loaded on {config['device']}")
    except Exception as e:
        print(f"ERROR: Cannot load Nomic model: {e}")
        print("You may need to install: pip install sentence-transformers torch")
        return
    
    # Connect to services
    qdrant_client = QdrantClient(host=config['qdrant_host'], port=config['qdrant_port'])
    pg_conn = get_db_connection(config)
    
    # Ensure collections exist
    ensure_collection_exists(qdrant_client, config['files_collection'], config['vector_size'])
    ensure_collection_exists(qdrant_client, config['knowledge_collection'], config['vector_size'])
    
    # Load checkpoint
    checkpoint = load_checkpoint(config['checkpoint_file'])
    
    # Determine which spaces to process
    space_keys = None
    if args.space:
        space_keys = [args.space]
        print(f"Processing single space: {args.space}")
    elif config.get('space_keys'):
        space_keys = [s.strip() for s in config['space_keys'].split(',') if s.strip()]
        print(f"Processing configured spaces: {', '.join(space_keys)}")
    
    # Load pickle files
    try:
        pages_by_space = load_pickle_files(
            config['pickle_dir'],
            config.get('process_all_spaces', False),
            space_keys
        )
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    
    if not pages_by_space:
        print("No pages to process!")
        return
    
    # Apply test mode limit if requested
    if args.test_mode:
        print("TEST MODE: Limiting to first 10 pages total")
        limited_pages = {}
        total_count = 0
        for space_key, pages in pages_by_space.items():
            if total_count >= 10:
                break
            pages_to_take = min(10 - total_count, len(pages))
            limited_pages[space_key] = pages[:pages_to_take]
            total_count += pages_to_take
        pages_by_space = limited_pages
    
    # Process pages
    total_processed = process_confluence_pages(
        pages_by_space, config, model, qdrant_client, pg_conn, checkpoint
    )
    
    # Final report
    elapsed_time = time.time() - start_time
    print(f"\n=== Processing Complete ===")
    print(f"Pages processed: {total_processed}")
    print(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    if total_processed > 0:
        print(f"Average speed: {total_processed/elapsed_time:.2f} pages/second")
        print(f"Average time per page: {elapsed_time/total_processed:.2f} seconds")
    
    # Clear checkpoint if all spaces completed
    all_completed = all(
        space_data.get('completed', False) 
        for space_data in checkpoint['processed_spaces'].values()
    )
    if all_completed and checkpoint['processed_spaces']:
        print("\nAll spaces completed successfully!")
        clear_checkpoint(config['checkpoint_file'])
    
    if pg_conn:
        pg_conn.close()

if __name__ == "__main__":
    main()