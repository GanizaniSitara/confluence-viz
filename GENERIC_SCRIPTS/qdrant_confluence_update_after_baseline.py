#!/usr/bin/env python3
"""
Sync Confluence updates to Qdrant after baseline ingestion is complete.
Uses PostgreSQL as the source of truth to determine what's already in Qdrant.

This script:
1. Queries PostgreSQL to get all existing Confluence pages
2. Compares with current pickle files to find new/updated pages
3. Deletes old versions and inserts new ones
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
import psycopg2.extras
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue, FilterSelector
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

# Nomic model info
NOMIC_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
NOMIC_VECTOR_SIZE = 768

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
    
    # Add settings specific to update sync
    if 'update_sync' in config:
        for key, value in config['update_sync'].items():
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
    for key in ['qdrant_port', 'chunk_size', 'overlap', 'batch_size',
                'db_port', 'embedding_batch_size']:
        if key in settings:
            settings[key] = int(settings[key])
    
    # Set defaults
    settings['vector_size'] = NOMIC_VECTOR_SIZE
    settings['batch_size'] = int(settings.get('batch_size', 1000))
    settings['embedding_batch_size'] = int(settings.get('embedding_batch_size', 32))
    settings['device'] = settings.get('device', 'cuda' if torch.cuda.is_available() else 'cpu')
    settings['files_collection'] = settings.get('files_collection', FILES_COLLECTION)
    settings['knowledge_collection'] = settings.get('knowledge_collection', KNOWLEDGE_COLLECTION)
    settings['base_url'] = settings.get('base_url', 'https://confluence.example.com')
    settings['pickle_dir'] = settings.get('pickle_dir', '../temp')
    
    # Convert boolean settings
    settings['delete_old_versions'] = settings.get('delete_old_versions', 'true').lower() == 'true'
    settings['verify_counts'] = settings.get('verify_counts', 'true').lower() == 'true'
    settings['html_to_markdown'] = settings.get('html_to_markdown', 'true').lower() == 'true'
    
    # Handle WSL-specific settings
    if is_wsl:
        if 'db_host_wsl' in settings:
            settings['db_host'] = settings['db_host_wsl']
    
    return settings

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
        print(f"Error: Could not connect to PostgreSQL: {e}")
        return None

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

def is_baseline_complete(config: dict) -> bool:
    """Check if baseline ingestion is complete by looking at the checkpoint file."""
    checkpoint_file = config.get('checkpoint_file', 'qdrant_confluence_pickle_checkpoint.json')
    
    if not os.path.exists(checkpoint_file):
        print(f"Checkpoint file not found: {checkpoint_file}")
        return False
    
    try:
        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)
        
        # Check if there are processed spaces
        processed_spaces = checkpoint.get('processed_spaces', {})
        if not processed_spaces:
            print("No processed spaces found in checkpoint")
            return False
        
        # Check if all spaces are marked as completed
        incomplete_spaces = [
            space for space, data in processed_spaces.items()
            if not data.get('completed', False)
        ]
        
        if incomplete_spaces:
            print(f"Baseline incomplete. Spaces still processing: {', '.join(incomplete_spaces)}")
            return False
        
        print(f"Baseline complete! {len(processed_spaces)} spaces processed.")
        return True
        
    except Exception as e:
        print(f"Error reading checkpoint file: {e}")
        return False

def get_pages_from_postgres(conn, knowledge_id: str) -> Dict[str, Dict]:
    """Get all Confluence pages currently in PostgreSQL."""
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    query = """
        SELECT 
            id as file_id,
            meta->>'page_id' as page_id,
            meta->>'last_updated' as last_updated,
            meta->>'space_key' as space_key,
            hash
        FROM file 
        WHERE meta->>'source' = 'confluence'
        AND meta->>'knowledge_id' = %s
    """
    
    cursor.execute(query, (knowledge_id,))
    
    pages = {}
    for row in cursor.fetchall():
        page_id = row['page_id']
        if page_id:
            pages[page_id] = {
                'file_id': row['file_id'],
                'last_updated': row['last_updated'],
                'space_key': row['space_key'],
                'hash': row['hash']
            }
    
    cursor.close()
    return pages

def load_pickle_files(pickle_dir: str) -> Dict[str, List]:
    """Load all Confluence data from pickle files."""
    pickle_path = Path(pickle_dir)
    
    if not pickle_path.exists():
        raise FileNotFoundError(f"Pickle directory not found: {pickle_dir}")
    
    all_pages = {}
    
    # Look for individual space pickle files (SPACENAME.pkl or SPACENAME_full.pkl)
    pickle_files = list(pickle_path.glob("*.pkl"))
    
    # Also check for the combined confluence_pages.pkl file
    combined_file = pickle_path / "confluence_pages.pkl"
    if combined_file.exists():
        print(f"Loading combined pickle file: {combined_file}")
        with open(combined_file, 'rb') as f:
            pages_data = pickle.load(f)
            
        # Group by space if it's a flat list
        if isinstance(pages_data, list):
            for page in pages_data:
                space_key = page.get('space', {}).get('key', 'UNKNOWN')
                if space_key not in all_pages:
                    all_pages[space_key] = []
                all_pages[space_key].append(page)
        else:
            all_pages.update(pages_data)
    
    # Load individual space files
    for pickle_file in pickle_files:
        if pickle_file.name == 'confluence_pages.pkl':
            continue  # Already processed
        
        space_key = pickle_file.stem
        if space_key.endswith('_full'):
            space_key = space_key[:-5]
        
        # Skip personal spaces
        if space_key.startswith('~'):
            continue
        
        try:
            with open(pickle_file, 'rb') as f:
                space_data = pickle.load(f)
            
            # Extract pages from the space data
            if isinstance(space_data, dict):
                pages = space_data.get('sampled_pages', [])
            else:
                pages = space_data
            
            if pages:
                all_pages[space_key] = pages
                
        except Exception as e:
            print(f"Warning: Could not load {pickle_file}: {e}")
    
    return all_pages

def html_to_markdown_text(html_content: str) -> str:
    """Convert Confluence HTML to markdown for better readability."""
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
    """Create a metadata header to prepend to Confluence pages."""
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

def delete_from_qdrant_and_postgres(conn, qdrant_client: QdrantClient, file_id: str, 
                                   files_collection: str, knowledge_collection: str) -> bool:
    """Delete a file from both PostgreSQL and Qdrant."""
    try:
        # Delete from Qdrant files collection
        qdrant_client.delete(
            collection_name=files_collection,
            points_selector=FilterSelector(
                filter=Filter(
                    must=[FieldCondition(
                        key="file_id",
                        match=MatchValue(value=file_id)
                    )]
                )
            )
        )
        
        # Delete from Qdrant knowledge collection
        qdrant_client.delete(
            collection_name=knowledge_collection,
            points_selector=FilterSelector(
                filter=Filter(
                    must=[FieldCondition(
                        key="file_id",
                        match=MatchValue(value=file_id)
                    )]
                )
            )
        )
        
        # Delete from PostgreSQL
        cursor = conn.cursor()
        cursor.execute("DELETE FROM file WHERE id = %s", (file_id,))
        conn.commit()
        cursor.close()
        
        return True
    except Exception as e:
        print(f"Error deleting file {file_id}: {e}")
        conn.rollback()
        return False

def process_page_to_qdrant(page: Dict, config: Dict, model: SentenceTransformer,
                          qdrant_client: QdrantClient, conn) -> bool:
    """Process a single page and insert it into Qdrant and PostgreSQL."""
    try:
        # Extract page details
        page_id = page.get('id', '')
        page_title = page.get('title', 'Untitled')
        space_key = page.get('space_key') or page.get('space', {}).get('key', 'UNKNOWN')
        last_updated = page.get('updated') or page.get('history', {}).get('lastUpdated', {}).get('when', '')
        
        # Get page body
        body = page.get('body', '')
        if isinstance(body, dict):
            storage = body.get('storage', body.get('view', {}))
            html_content = storage.get('value', '')
        else:
            html_content = body
        
        if not html_content:
            print(f"  Skipping empty page: {page_title}")
            return False
        
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
            return False
        
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
        safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in page_title)
        safe_title = safe_title.strip()[:100]
        filename = f"{space_key}_{safe_title}_{page_id}.md"
        
        # Register in PostgreSQL
        cur = conn.cursor()
        file_hash = hashlib.sha256(full_content.encode()).hexdigest()
        current_time = int(time.time() * 1000)  # milliseconds
        
        file_meta = {
            'name': filename,
            'content_type': 'text/markdown' if config.get('html_to_markdown', True) else 'text/html',
            'size': len(full_content),
            'knowledge_id': config['knowledge_id'],
            'source': 'confluence',
            'space_key': space_key,
            'page_id': page_id,
            'page_title': page_title,
            'confluence_url': confluence_url,
            'last_updated': last_updated
        }
        
        cur.execute("""
            INSERT INTO file (id, user_id, filename, meta, created_at, updated_at, hash, data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            file_id,
            config['user_id'],
            filename,
            json.dumps(file_meta),
            current_time,
            current_time,
            file_hash,
            json.dumps({'content': full_content})
        ))
        
        conn.commit()
        
        # Upload to Qdrant
        # Convert numpy array to list for Qdrant
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
                    "space_key": space_key,
                    "page_id": page_id,
                    "page_title": page_title
                }
            ))
        
        # Batch upload to files collection
        for i in range(0, len(file_points), config['batch_size']):
            batch = file_points[i:i + config['batch_size']]
            qdrant_client.upsert(collection_name=config['files_collection'], points=batch)
        
        # Insert into knowledge collection
        knowledge_points = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings_list)):
            point_id = str(uuid.uuid4())
            knowledge_points.append(PointStruct(
                id=point_id,
                vector=embedding,
                payload={
                    "knowledge_id": config['knowledge_id'],
                    "file_id": file_id,
                    "user_id": config['user_id'],
                    "chunk_index": i,
                    "content": chunk,
                    "source": filename,
                    "space_key": space_key,
                    "page_id": page_id,
                    "page_title": page_title
                }
            ))
        
        # Batch upload to knowledge collection
        for i in range(0, len(knowledge_points), config['batch_size']):
            batch = knowledge_points[i:i + config['batch_size']]
            qdrant_client.upsert(collection_name=config['knowledge_collection'], points=batch)
        
        return True
        
    except Exception as e:
        print(f"Error processing page {page.get('id', 'unknown')}: {e}")
        conn.rollback()
        return False

def update_knowledge_data(conn, knowledge_id: str, file_ids: List[str]) -> bool:
    """Update knowledge.data with file references to make them visible in UI."""
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
        
        current_data = result[0] if result[0] else {"file_ids": []}
        
        if "file_ids" not in current_data:
            current_data["file_ids"] = []
        
        # Add new file IDs
        for file_id in file_ids:
            if file_id not in current_data["file_ids"]:
                current_data["file_ids"].append(file_id)
        
        # Update knowledge
        cur.execute("""
            UPDATE knowledge 
            SET data = %s::jsonb,
                updated_at = %s
            WHERE id = %s
        """, (json.dumps(current_data), int(time.time()), knowledge_id))
        
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"Error updating knowledge data: {e}")
        conn.rollback()
        return False

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(
        description="Sync Confluence updates to Qdrant after baseline is complete"
    )
    parser.add_argument("--force", action="store_true", 
                       help="Force sync even if baseline is not marked as complete")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be updated without making changes")
    parser.add_argument("--space", type=str,
                       help="Process only this specific space key")
    parser.add_argument("--limit", type=int,
                       help="Limit number of updates to process")
    
    args = parser.parse_args()
    
    print(f"\n=== Confluence Update Sync ===")
    print(f"Using device: {config['device']}")
    print(f"Pickle directory: {config['pickle_dir']}")
    print(f"Knowledge ID: {config['knowledge_id']}")
    
    # Check if baseline is complete
    if not args.force and not is_baseline_complete(config):
        print("\nBaseline ingestion is not complete. Use --force to sync anyway.")
        return
    
    # Start timing
    start_time = time.time()
    
    # Connect to services
    print("\nConnecting to services...")
    pg_conn = get_db_connection(config)
    if not pg_conn:
        print("Failed to connect to PostgreSQL")
        return
    
    qdrant_client = QdrantClient(host=config['qdrant_host'], port=config['qdrant_port'])
    
    # Ensure collections exist
    ensure_collection_exists(qdrant_client, config['files_collection'], config['vector_size'])
    ensure_collection_exists(qdrant_client, config['knowledge_collection'], config['vector_size'])
    
    # Load the model if not doing dry run
    model = None
    if not args.dry_run:
        print("\nLoading Nomic model...")
        try:
            model = SentenceTransformer(NOMIC_MODEL_NAME, trust_remote_code=True)
            model.to(config['device'])
            print(f"✓ Model loaded on {config['device']}")
        except Exception as e:
            print(f"ERROR: Cannot load Nomic model: {e}")
            return
    
    # Get existing pages from PostgreSQL
    print("\nQuerying PostgreSQL for existing pages...")
    pg_pages = get_pages_from_postgres(pg_conn, config['knowledge_id'])
    print(f"Found {len(pg_pages)} pages in PostgreSQL/Qdrant")
    
    # Load pickle files
    print("\nLoading pickle files...")
    try:
        pickle_pages = load_pickle_files(config['pickle_dir'])
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    
    total_pickle_pages = sum(len(pages) for pages in pickle_pages.values())
    print(f"Found {total_pickle_pages} pages in {len(pickle_pages)} spaces from pickle files")
    
    # Filter by space if requested
    if args.space:
        if args.space in pickle_pages:
            pickle_pages = {args.space: pickle_pages[args.space]}
            print(f"Filtering to space: {args.space}")
        else:
            print(f"Space {args.space} not found in pickle files")
            return
    
    # Find differences
    print("\nAnalyzing differences...")
    to_insert = []
    to_update = []
    
    for space_key, pages in pickle_pages.items():
        for page in pages:
            page_id = page.get('id')
            if not page_id:
                continue
            
            # Add space_key to page data for easier processing
            page['space_key'] = space_key
            
            current_updated = page.get('updated') or page.get('history', {}).get('lastUpdated', {}).get('when', '')
            
            if page_id not in pg_pages:
                # New page not in PostgreSQL/Qdrant
                to_insert.append(page)
            elif current_updated and pg_pages[page_id]['last_updated']:
                # Compare timestamps
                if current_updated > pg_pages[page_id]['last_updated']:
                    to_update.append((page, pg_pages[page_id]['file_id']))
    
    print(f"\nFound:")
    print(f"  {len(to_insert)} new pages to insert")
    print(f"  {len(to_update)} pages to update")
    
    if args.dry_run:
        print("\nDRY RUN - No changes will be made")
        
        if to_insert[:5]:  # Show first 5
            print("\nSample of new pages to insert:")
            for page in to_insert[:5]:
                print(f"  - {page.get('space_key')}: {page.get('title')} (ID: {page.get('id')})")
        
        if to_update[:5]:  # Show first 5
            print("\nSample of pages to update:")
            for page, old_file_id in to_update[:5]:
                print(f"  - {page.get('space_key')}: {page.get('title')} (ID: {page.get('id')})")
        
        return
    
    # Apply limit if specified
    if args.limit:
        if to_update:
            to_update = to_update[:args.limit]
        if to_insert:
            remaining_limit = max(0, args.limit - len(to_update))
            to_insert = to_insert[:remaining_limit]
        print(f"\nLimited to {args.limit} total operations")
    
    # Process updates
    if to_update:
        print(f"\nProcessing {len(to_update)} updates...")
        updated_count = 0
        new_file_ids = []
        
        for page, old_file_id in tqdm(to_update, desc="Updating pages"):
            # Delete old version if configured
            if config.get('delete_old_versions', True):
                if delete_from_qdrant_and_postgres(pg_conn, qdrant_client, old_file_id,
                                                  config['files_collection'], 
                                                  config['knowledge_collection']):
                    # Insert new version
                    if process_page_to_qdrant(page, config, model, qdrant_client, pg_conn):
                        updated_count += 1
                        # Track the new file for knowledge.data update
                        # Note: We'd need to modify process_page_to_qdrant to return the file_id
                        # For now, we'll update knowledge.data separately
            else:
                # Update in place (not implemented in this version)
                print(f"Warning: Update in place not implemented, skipping page {page.get('id')}")
        
        print(f"Successfully updated {updated_count} pages")
    
    # Process new pages
    if to_insert:
        print(f"\nProcessing {len(to_insert)} new pages...")
        inserted_count = 0
        new_file_ids = []
        
        for page in tqdm(to_insert, desc="Inserting pages"):
            if process_page_to_qdrant(page, config, model, qdrant_client, pg_conn):
                inserted_count += 1
        
        print(f"Successfully inserted {inserted_count} pages")
    
    # Verify counts if configured
    if config.get('verify_counts', True):
        print("\nVerifying counts...")
        pg_pages_after = get_pages_from_postgres(pg_conn, config['knowledge_id'])
        print(f"PostgreSQL now has {len(pg_pages_after)} pages (was {len(pg_pages)})")
    
    # Final report
    elapsed_time = time.time() - start_time
    print(f"\n=== Sync Complete ===")
    print(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    
    if pg_conn:
        pg_conn.close()

if __name__ == "__main__":
    main()