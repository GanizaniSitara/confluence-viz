#!/usr/bin/env python3
"""
Open-WebUI Confluence Uploader - Parallel Processing Version
Loads Confluence pickles and uploads HTML and text versions to Open-WebUI knowledge spaces.
Uses concurrent processing for faster uploads.
"""

import sys
import os
import argparse
import pickle
import json
import configparser
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
import requests
from requests.auth import HTTPBasicAuth
import logging
from utils.html_cleaner import clean_confluence_html
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Lock
import queue

# Thread-safe counters
upload_stats = {
    'success': 0,
    'failed': 0,
    'skipped': 0,
    'total_time': 0,
    'lock': Lock()
}

# Set up logging
def setup_logging(log_file='openwebui_upload_parallel.log'):
    """Setup logging to both console and file"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(threadName)-10s] - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def safe_print(text: str):
    """Print text, replacing emojis if the terminal doesn't support UTF-8"""
    try:
        print(text)
    except UnicodeEncodeError:
        # Replace common emojis with ASCII equivalents
        replacements = {
            '✅': '[OK]',
            '❌': '[X]',
            '⚠️': '[!]',
            '📁': '[DIR]',
            '📋': '[DOC]',
            '🚀': '[>>]',
            '🔄': '[~]',
            '📊': '[=]',
            'ℹ️': '[i]',
            '⏭️': '[>>]',
            '🚪': '[DOOR]',
            '🛪': '[HUT]',
            '🏠': '[HOME]',
            '🔍': '[SEARCH]',
            '💾': '[SAVE]',
            '🧪': '[TEST]',
            '🧹': '[CLEAN]',
            '📌': '[PIN]',
            '🎉': '[PARTY]'
        }
        for emoji, ascii_text in replacements.items():
            text = text.replace(emoji, ascii_text)
        print(text.encode('ascii', 'replace').decode('ascii'))

def save_checkpoint(space_key: str, checkpoint_file: str = 'openwebui_checkpoint.txt'):
    """Save the last successfully uploaded space to checkpoint file"""
    try:
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            f.write(space_key)
        logger.info(f"Checkpoint saved: {space_key}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

def load_checkpoint(checkpoint_file: str = 'openwebui_checkpoint.txt') -> Optional[str]:
    """Load the last successfully uploaded space from checkpoint file"""
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                space_key = f.read().strip()
            logger.info(f"Checkpoint loaded: resuming after {space_key}")
            return space_key
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
    return None

class OpenWebUIClient:
    """Client for uploading documents to Open-WebUI"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Confluence-WebUI-Uploader/1.0'
        })
        self.auth_token = None
        logger.info(f"Initialized Open-WebUI client for {self.base_url}")
    
    def authenticate(self) -> bool:
        """Authenticate with Open-WebUI and get auth token"""
        if not self.username or not self.password:
            logger.info("No credentials provided, skipping authentication")
            safe_print("ℹ️  No credentials provided - proceeding without authentication")
            safe_print("   (Some Open-WebUI instances don't require authentication)")
            return True
            
        auth_url = f"{self.base_url}/api/v1/auths/signin"
        logger.debug(f"AUTH: POST {auth_url}")
        safe_print(f"🔐 Authenticating with {self.base_url}...")
        
        try:
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            }, timeout=30)
            logger.debug(f"AUTH RESPONSE [{response.status_code}]: {response.text}")
            
            if response.status_code != 200:
                logger.error(f"Authentication failed: HTTP {response.status_code}")
                safe_print(f"❌ Authentication failed: HTTP {response.status_code}")
                if response.status_code == 401:
                    safe_print("   Unauthorized - check your username/password")
                elif response.status_code == 404:
                    safe_print("   Auth endpoint not found - check the server URL")
                    safe_print(f"   Tried: {auth_url}")
                try:
                    error_data = response.json()
                    if 'detail' in error_data:
                        safe_print(f"   Server message: {error_data['detail']}")
                except:
                    pass
                return False
            
            data = response.json()
            self.auth_token = data.get("token")
            
            if not self.auth_token:
                logger.error("No auth token received")
                safe_print("❌ No auth token received from server")
                return False
            
            # Update session headers with auth token
            self.session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
            logger.info("Authentication successful")
            safe_print("✅ Authentication successful")
            return True
            
        except requests.exceptions.Timeout:
            logger.error("Authentication timeout - server did not respond within 30 seconds")
            safe_print("❌ Authentication failed: Server timeout (30s)")
            return False
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            safe_print(f"❌ Authentication failed: {str(e)}")
            return False
    
    def test_auth(self) -> bool:
        """Test if authentication is working"""
        # First authenticate if we haven't already
        if not self.auth_token and not self.authenticate():
            return False
        
        # Then test with a simple API call
        test_url = f"{self.base_url}/api/v1/auths/"
        try:
            response = self.session.get(test_url, timeout=30)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Auth test failed: {e}")
            return False
    
    def get_knowledge_collections(self) -> List[Dict[str, Any]]:
        """Get list of knowledge collections"""
        url = f"{self.base_url}/api/v1/knowledge/"
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get collections: HTTP {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error fetching collections: {e}")
            return []
    
    def create_collection(self, name: str, description: str = "") -> Optional[str]:
        """Create a new knowledge collection"""
        url = f"{self.base_url}/api/v1/knowledge/create"
        data = {
            "name": name,
            "description": description
        }
        
        try:
            response = self.session.post(url, json=data)
            if response.status_code == 200:
                result = response.json()
                return result.get('id')
            else:
                logger.error(f"Failed to create collection: HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            return None
    
    def upload_document(self, title: str, content: str, collection_id: str, 
                       is_html: bool = False) -> bool:
        """
        Upload a document to Open-WebUI knowledge base using the correct two-step process
        """
        import tempfile
        import os
        
        # Step 1: Upload file to /api/v1/files/
        upload_url = f"{self.base_url}/api/v1/files/"
        
        logger.debug(f"Uploading content as '{title}' to {upload_url}")
        
        try:
            # Determine file extension and content type
            if is_html:
                file_extension = ".html"
                content_type = "text/html"
                filename = f"{title}.html"
            else:
                file_extension = ".txt"
                content_type = "text/plain"
                filename = f"{title}.txt"
            
            # Create a temporary file with the content (specify UTF-8 encoding)
            with tempfile.NamedTemporaryFile(mode='w', suffix=file_extension, delete=False, encoding='utf-8') as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            
            # Upload the file
            with open(tmp_path, 'rb') as f:
                files = {'file': (filename, f, content_type)}
                logger.info(f"Uploading as filename='{filename}', content_type='{content_type}', is_html={is_html}")
                response = self.session.post(upload_url, files=files)
                logger.debug(f"UPLOAD [{response.status_code}]: {response.text}")
            
            # Remove the temporary file
            os.unlink(tmp_path)
            
        except Exception as e:
            logger.error(f"Exception uploading '{title}': {e}")
            return False

        if response.status_code != 200:
            logger.error(f"Failed to upload '{title}': HTTP {response.status_code}")
            return False

        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Error parsing upload JSON for '{title}': {e}")
            return False

        file_id = data.get('id')
        
        if not file_id:
            logger.error(f"No file ID returned for '{title}'")
            return False
        
        logger.info(f"File uploaded successfully. ID: {file_id}, proceeding to add to collection...")
        
        # Step 2: Add file to knowledge collection
        add_url = f"{self.base_url}/api/v1/knowledge/{collection_id}/file/add"
        add_data = {"file_id": file_id}
        
        try:
            response = self.session.post(add_url, json=add_data)
            logger.debug(f"ADD TO COLLECTION [{response.status_code}]: {response.text}")
            
            if response.status_code == 200:
                logger.info(f"Successfully added '{title}' to collection")
                return True
            else:
                logger.error(f"Failed to add '{title}' to collection: HTTP {response.status_code}")
                return False
            
        except Exception as e:
            logger.error(f"Exception adding '{title}' to collection: {e}")
            return False
    
    def find_existing_collection(self, name: str) -> Optional[str]:
        """Find an existing collection by name"""
        collections = self.get_knowledge_collections()
        for collection in collections:
            if collection.get('name') == name:
                return collection.get('id')
        return None
    
    def ensure_collection_exists(self, name: str, description: str = "") -> Optional[str]:
        """Ensure a collection exists, create if it doesn't"""
        collection_id = self.find_existing_collection(name)
        if collection_id:
            logger.info(f"Found existing collection '{name}' with ID: {collection_id}")
            return collection_id
        
        logger.info(f"Creating new collection: {name}")
        return self.create_collection(name, description)
    
    def delete_collection(self, collection_id: str) -> bool:
        """Delete a knowledge collection"""
        url = f"{self.base_url}/api/v1/knowledge/{collection_id}/delete"
        
        try:
            response = self.session.delete(url)
            if response.status_code in [200, 204]:
                logger.info(f"Deleted collection ID: {collection_id}")
                return True
            else:
                logger.error(f"Failed to delete collection: HTTP {response.status_code}")
                if response.text:
                    logger.error(f"Response: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error deleting collection: {e}")
            return False

def load_confluence_pickle(pickle_path: Path) -> Optional[Dict]:
    """Load a Confluence pickle file"""
    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        logger.info(f"Loaded pickle: {pickle_path.name}")
        return data
    except Exception as e:
        logger.error(f"Failed to load pickle {pickle_path}: {e}")
        return None

def process_confluence_page(page: Dict, space_key: str, space_name: str) -> Tuple[str, str]:
    """
    Process a Confluence page and return path and text content
    Returns: (path_content, text_content)
    """
    page_id = page.get('id', 'unknown')
    title = page.get('title', 'Untitled')
    body = page.get('body', {})
    
    # Debug logging
    logger.debug(f"Processing page {title} - body type: {type(body)}")
    logger.debug(f"Body keys: {body.keys() if isinstance(body, dict) else 'Not a dict'}")
    
    storage_body = body.get('storage', {}).get('value', '') if isinstance(body, dict) else ''
    logger.debug(f"Storage body length: {len(storage_body)}")
    logger.debug(f"Storage body preview: {storage_body[:200]}...")
    
    # Build hierarchical path
    ancestors = page.get('ancestors', [])
    path_parts = [space_name]
    for ancestor in ancestors:
        path_parts.append(ancestor.get('title', 'Unknown'))
    path_parts.append(title)
    
    # Create path information document
    path_content = f"# {title}\n\n"
    path_content += f"**Space:** {space_name}\n"
    path_content += f"**Path:** {' > '.join(path_parts)}\n"
    path_content += f"**Page ID:** {page_id}\n"
    
    # Text content - clean HTML
    text_content = f"{title}\n{'=' * len(title)}\n\n"
    text_content += f"Space: {space_name}\n"
    text_content += f"Path: {' > '.join(path_parts)}\n\n"
    
    # Add body markers
    text_content += "###BODY START###\n"
    if storage_body:
        cleaned_body = clean_confluence_html(storage_body)
        logger.debug(f"Cleaned body length: {len(cleaned_body)}")
        logger.debug(f"Cleaned body preview: {cleaned_body[:200]}...")
        text_content += cleaned_body
    else:
        text_content += "[NO BODY CONTENT FOUND]"
        logger.warning(f"No body content found for page {title}")
    text_content += "\n###BODY END###"
    
    logger.info(f"Final text_content length for {title}: {len(text_content)} chars")
    
    return path_content, text_content

def upload_page_worker(args: Tuple[Dict, str, str, str, str, str, str, Dict]) -> Dict:
    """
    Worker function to upload a single page
    Returns dict with upload results
    """
    page, space_key, space_name, text_collection_id, path_collection_id, format_choice, client_config = args
    
    # Create a new client instance for this worker thread
    client = OpenWebUIClient(
        client_config['base_url'],
        client_config['username'],
        client_config['password']
    )
    
    # Authenticate the client
    if not client.authenticate():
        return {
            'page_id': page.get('id', 'unknown'),
            'title': page.get('title', 'Untitled'),
            'success': False,
            'errors': ['Authentication failed']
        }
    
    page_id = page.get('id', 'unknown')
    title = page.get('title', 'Untitled')
    start_time = time.time()
    
    result = {
        'page_id': page_id,
        'title': title,
        'success': True,
        'errors': []
    }
    
    try:
        path_content, text_content = process_confluence_page(page, space_key, space_name)
        
        # Upload based on format choice
        if format_choice in ['txt', 'both']:
            # Upload text version
            text_title = f"{space_key}-{page_id}-TEXT"
            logger.info(f"Uploading text content for {title} (length: {len(text_content)} chars)")
            logger.debug(f"Text content preview (first 500 chars): {text_content[:500]}")
            if not client.upload_document(text_title, text_content, text_collection_id, is_html=False):
                result['success'] = False
                result['errors'].append("Failed to upload text version")
        
        if format_choice == 'path' and path_collection_id:
            # Upload path information only
            path_title = f"{space_key}-{page_id}-PATH"
            if not client.upload_document(path_title, path_content, path_collection_id, is_html=False):
                result['success'] = False
                result['errors'].append("Failed to upload path information")
        
        elapsed = time.time() - start_time
        result['elapsed'] = elapsed
        
        # Update stats
        with upload_stats['lock']:
            if result['success']:
                upload_stats['success'] += 1
            else:
                upload_stats['failed'] += 1
            upload_stats['total_time'] += elapsed
        
        logger.info(f"Processed '{title}' in {elapsed:.2f}s - Success: {result['success']}")
        
    except Exception as e:
        result['success'] = False
        result['errors'].append(str(e))
        logger.error(f"Error processing page '{title}': {e}")
        with upload_stats['lock']:
            upload_stats['failed'] += 1
    
    return result

def upload_confluence_space_parallel(client: OpenWebUIClient, pickle_data: Dict, 
                                   text_collection: str,
                                   path_collection: str = None,
                                   format_choice: str = 'txt',
                                   max_workers: int = 4) -> tuple:
    """
    Upload all pages from a Confluence space to Open-WebUI using parallel processing
    Returns tuple of (number of successfully uploaded pages, user_quit)
    """
    space_key = pickle_data.get('space_key', 'UNKNOWN')
    space_name = pickle_data.get('name', 'Unknown Space')
    sampled_pages = pickle_data.get('sampled_pages', [])
    
    if not sampled_pages:
        logger.warning(f"No pages found in space {space_key}")
        return 0, False
    
    logger.info(f"Processing {len(sampled_pages)} pages from space '{space_name}' ({space_key}) with {max_workers} workers")
    
    # Create client config to pass to workers
    client_config = {
        'base_url': client.base_url,
        'username': client.username,
        'password': client.password
    }
    
    # Prepare work items
    work_items = [
        (page, space_key, space_name, text_collection, path_collection, format_choice, client_config)
        for page in sampled_pages
    ]
    
    success_count = 0
    start_time = time.time()
    
    # Process pages in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_page = {
            executor.submit(upload_page_worker, item): item[0]
            for item in work_items
        }
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_page):
            completed += 1
            page = future_to_page[future]
            
            try:
                result = future.result()
                if result['success']:
                    success_count += 1
                
                # Progress update
                progress = (completed / len(sampled_pages)) * 100
                logger.info(f"Progress: {completed}/{len(sampled_pages)} ({progress:.1f}%) - "
                          f"Success: {upload_stats['success']}, Failed: {upload_stats['failed']}")
                
            except Exception as e:
                logger.error(f"Exception in worker thread: {e}")
    
    elapsed = time.time() - start_time
    pages_per_second = len(sampled_pages) / elapsed if elapsed > 0 else 0
    
    logger.info(f"Completed space '{space_name}' in {elapsed:.2f}s ({pages_per_second:.2f} pages/sec)")
    logger.info(f"Successfully uploaded {success_count}/{len(sampled_pages)} pages from space {space_key}")
    
    return success_count, False

def load_openwebui_settings():
    """Load Open-WebUI settings from settings.ini"""
    config = configparser.ConfigParser()
    config_path = 'settings.ini'
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found. Please copy settings.example.ini to settings.ini and configure it.")
    
    config.read(config_path)
    
    # Handle both 'openwebui' and 'OpenWebUI' section names
    if 'openwebui' in config:
        openwebui_section = config['openwebui']
    elif 'OpenWebUI' in config:
        openwebui_section = config['OpenWebUI']
    else:
        raise ValueError("No [openwebui] or [OpenWebUI] section found in settings.ini")
    
    # Create settings dict
    settings = {}
    settings['base_url'] = openwebui_section.get('base_url')
    settings['username'] = openwebui_section.get('username')
    settings['password'] = openwebui_section.get('password')
    settings['upload_dir'] = openwebui_section.get('upload_dir', 'temp')
    settings['txt_collection'] = openwebui_section.get('txt_collection', None)
    
    # Don't use placeholder values
    if settings['username'] == 'your_username' or settings['username'] == 'your_email@example.com':
        settings['username'] = None
    if settings['password'] == 'your_password':
        settings['password'] = None
    
    # Check required fields
    required_fields = ['base_url', 'username', 'password']
    for field in required_fields:
        if field not in settings or not settings[field]:
            raise ValueError(f"Missing required field '{field}' in [openwebui] section")
    
    return settings

def main():
    """Main function"""
    # Load settings first
    settings = load_openwebui_settings()
    
    # Log loaded settings (mask password)
    logger.info("Loaded settings from settings.ini:")
    logger.info(f"  base_url: {settings.get('base_url')}")
    logger.info(f"  username: {settings.get('username')}")
    logger.info(f"  password: {'*' * len(settings.get('password', '')) if settings.get('password') else 'None'}")
    logger.info(f"  upload_dir: {settings.get('upload_dir')}")
    logger.info(f"  txt_collection: {settings.get('txt_collection')}")
    
    parser = argparse.ArgumentParser(
        description="Upload Confluence spaces to Open-WebUI Knowledge Base (Parallel Version)\n\n"
                    "This parallel version can achieve 40x+ speedup compared to sequential upload\n"
                    "by processing multiple pages concurrently. Optimal worker count depends on\n"
                    "your server capacity and network bandwidth.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all spaces with default settings (4 workers)
  %(prog)s
  
  # Upload with 8 parallel workers for faster processing
  %(prog)s --workers 8
  
  # Upload only text format with 6 workers
  %(prog)s --format txt --workers 6
  
  # Test mode with limited pages (useful for benchmarking)
  %(prog)s --test-mode --test-limit 500 --workers 4
  
  # Resume from checkpoint with custom workers
  %(prog)s --resume --workers 10
  
  # Clear checkpoint and start fresh
  %(prog)s --clear-checkpoint --workers 6

Performance Tips:
  - Start with 4-8 workers and adjust based on your server's response
  - Too many workers (>20) may overload the server and reduce performance
  - Monitor server CPU/memory usage to find optimal worker count
  - Network latency affects optimal worker count (higher latency = more workers helpful)
        """
    )
    
    parser.add_argument('--format', choices=['txt', 'path'], default='txt',
                       help='Format to upload (default: txt)')
    parser.add_argument('--pickle-dir', 
                       default=settings.get('upload_dir', 'temp'), 
                       help=f"Directory containing pickle files (default: {settings.get('upload_dir', 'temp')})")
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last checkpoint')
    parser.add_argument('--clear-checkpoint', action='store_true',
                       help='Clear checkpoint and start fresh')
    parser.add_argument('--test-auth', action='store_true',
                       help='Test authentication and exit')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of parallel upload workers (default: 4, recommended: 4-8, max: 20)')
    parser.add_argument('--path-collection', 
                       help='Separate collection name for path information (used with --format path)')
    parser.add_argument('--test-mode', action='store_true',
                       help='Test mode: create temporary collection, upload TXT, delete (forces --format txt)')
    parser.add_argument('--test-limit', type=int, default=0,
                       help='Limit total pages to upload in test mode (0 = no limit)')
    
    args = parser.parse_args()
    
    # Log all parameters
    logger.info("Script parameters:")
    logger.info(f"  format: {args.format}")
    logger.info(f"  pickle_dir: {args.pickle_dir}")
    logger.info(f"  resume: {args.resume}")
    logger.info(f"  clear_checkpoint: {args.clear_checkpoint}")
    logger.info(f"  test_auth: {args.test_auth}")
    logger.info(f"  workers: {args.workers}")
    logger.info(f"  path_collection: {args.path_collection}")
    logger.info(f"  test_mode: {args.test_mode}")
    logger.info(f"  test_limit: {args.test_limit if hasattr(args, 'test_limit') else 'N/A'}")
    
    # Validate workers
    if args.workers < 1:
        safe_print("Error: --workers must be at least 1")
        return 1
    if args.workers > 20:
        safe_print("Warning: Using more than 20 workers may overload the server")
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            return 0
    
    # Clear checkpoint if requested
    if args.clear_checkpoint:
        if os.path.exists('openwebui_checkpoint.txt'):
            os.remove('openwebui_checkpoint.txt')
            safe_print("✅ Checkpoint cleared")
        else:
            safe_print("ℹ️ No checkpoint file found")
        return 0
    
    # Initialize client
    client = OpenWebUIClient(
        settings.get('base_url', 'http://localhost:8080'),
        settings.get('username'),
        settings.get('password')
    )
    
    # Authenticate
    if not client.authenticate():
        safe_print("\n❌ Authentication failed. Please check your credentials in settings.ini")
        return 1
    
    if args.test_auth:
        safe_print("\n✅ Authentication successful!")
        collections = client.get_knowledge_collections()
        safe_print(f"\nFound {len(collections)} knowledge collections:")
        for coll in collections[:10]:  # Show first 10
            safe_print(f"  - {coll.get('name', 'Unnamed')} (ID: {coll.get('id', 'Unknown')})")
        if len(collections) > 10:
            safe_print(f"  ... and {len(collections) - 10} more")
        return 0
    
    # Handle test mode
    if args.test_mode:
        args.format = 'txt'  # Force text format
        # Set default test limit if not specified
        if args.test_limit == 0:
            args.test_limit = 500
        safe_print("\n🧪 Test Mode: Creating temporary collection...")
        timestamp = int(time.time())
        test_collection_name = f"test_confluence_parallel_{timestamp}"
        test_collection_id = client.create_collection(
            test_collection_name,
            "Temporary collection for testing parallel Confluence upload"
        )
        if not test_collection_id:
            safe_print("❌ Failed to create test collection")
            return 1
        safe_print(f"✅ Created test collection: {test_collection_name}")
        
        # For test mode, use the temporary collection
        text_collection_id = test_collection_id
        path_collection_id = test_collection_id
    else:
        # Setup collections based on format
        text_collection_id = None
        path_collection_id = None
        
        if args.format in ['txt']:
            text_collection_name = settings.get('txt_collection', 'CONF-TXT')
            text_collection_id = client.ensure_collection_exists(
                text_collection_name, 
                "Confluence pages in plain text format"
            )
            if not text_collection_id:
                safe_print(f"❌ Failed to setup text collection: {text_collection_name}")
                return 1
        
        if args.format == 'path':
            if args.path_collection:
                path_collection_id = client.ensure_collection_exists(
                    args.path_collection,
                    "Confluence page navigation paths"
                )
            else:
                safe_print("❌ --path-collection is required when using --format path")
                return 1
    
    # Find pickle files
    pickle_dir = Path(args.pickle_dir)
    if not pickle_dir.exists():
        safe_print(f"❌ Pickle directory not found: {pickle_dir}")
        return 1
    
    pickle_files = sorted(pickle_dir.glob("*.pkl"))
    if not pickle_files:
        safe_print(f"❌ No pickle files found in {pickle_dir}")
        return 1
    
    safe_print(f"\n📁 Found {len(pickle_files)} pickle files in {pickle_dir}")
    
    # Handle resume
    last_processed = None
    if args.resume:
        last_processed = load_checkpoint()
    
    files_to_process = []
    if last_processed:
        # Find the position of last processed file
        found = False
        for pf in pickle_files:
            if found:
                files_to_process.append(pf)
            elif pf.stem == last_processed or pf.stem == f"{last_processed}_full":
                found = True
                safe_print(f"📌 Resuming after {last_processed}")
        
        if not found:
            safe_print(f"⚠️ Checkpoint space '{last_processed}' not found, starting from beginning")
            files_to_process = pickle_files
    else:
        files_to_process = pickle_files
    
    if not files_to_process:
        logger.info("No files to process")
        safe_print("\n✅ All spaces have already been processed!")
        return 0
    
    safe_print(f"\n✅ Ready to upload {len(files_to_process)} space(s) using {args.workers} parallel workers")
    test_limit = args.test_limit if (hasattr(args, 'test_limit') and args.test_mode) else 0
    if test_limit > 0:
        safe_print(f"🧪 Test mode: Limited to {test_limit} pages total")
    logger.info(f"🚀 Starting parallel upload: {len(files_to_process)} spaces with {args.workers} workers")
    logger.info(f"📋 Upload settings: format={args.format}, workers={args.workers}")
    
    # Reset stats
    upload_stats['success'] = 0
    upload_stats['failed'] = 0
    upload_stats['total_time'] = 0
    
    # Process files
    total_success = 0
    total_pages = 0
    pages_uploaded_so_far = 0
    overall_start = time.time()
    
    for i, pickle_file in enumerate(files_to_process, 1):
        logger.info(f"Processing space {i}/{len(files_to_process)}: {pickle_file.name}")
        
        try:
            # Load and process the pickle file
            pickle_data = load_confluence_pickle(pickle_file)
            if not pickle_data:
                logger.warning(f"⚠️ Skipping invalid pickle file: {pickle_file.name}")
                continue
            
            space_key = pickle_data.get('space_key', 'UNKNOWN')
            space_name = pickle_data.get('name', 'Unknown Space')
            page_count = len(pickle_data.get('sampled_pages', []))
            
            # Check test limit
            if test_limit > 0 and pages_uploaded_so_far >= test_limit:
                logger.info(f"Test limit reached ({test_limit} pages). Stopping.")
                safe_print(f"\n✅ Test limit reached ({test_limit} pages uploaded)")
                break
            
            # If test limit is set, adjust pickle data to only include pages up to the limit
            if test_limit > 0:
                remaining_pages = test_limit - pages_uploaded_so_far
                if remaining_pages < page_count:
                    logger.info(f"Limiting pages in this space to {remaining_pages} (test limit)")
                    pickle_data['sampled_pages'] = pickle_data['sampled_pages'][:remaining_pages]
                    page_count = remaining_pages
            
            logger.info(f"🔄 Processing space '{space_name}' ({space_key}) with {page_count} pages")
            
            success_count, user_quit = upload_confluence_space_parallel(
                client, pickle_data, text_collection_id,
                path_collection=path_collection_id,
                format_choice=args.format, max_workers=args.workers
            )
            
            pages_uploaded_so_far += success_count
            
            total_success += success_count
            total_pages += page_count
            
            # Save checkpoint after successful upload
            if success_count > 0:
                save_checkpoint(space_key)
            
            logger.info(f"✅ Completed space '{space_name}' ({space_key}): {success_count}/{page_count} pages uploaded")
            logger.info(f"📊 Overall progress: {i}/{len(files_to_process)} spaces, {total_success}/{total_pages} pages uploaded")
                
        except Exception as e:
            logger.error(f"❌ Error processing {pickle_file.name}: {e}")
            continue
    
    # Final statistics
    overall_elapsed = time.time() - overall_start
    overall_pages_per_second = total_success / overall_elapsed if overall_elapsed > 0 else 0
    
    safe_print("\n" + "="*60)
    safe_print("📊 UPLOAD COMPLETE - STATISTICS")
    safe_print("="*60)
    safe_print(f"Total spaces processed: {len(files_to_process)}")
    safe_print(f"Total pages uploaded: {total_success}/{total_pages}")
    safe_print(f"Failed uploads: {upload_stats['failed']}")
    safe_print(f"Total time: {overall_elapsed:.2f} seconds")
    safe_print(f"Average speed: {overall_pages_per_second:.2f} pages/second")
    safe_print(f"Parallel workers used: {args.workers}")
    
    # Clean up test collection if in test mode
    if args.test_mode and 'test_collection_id' in locals():
        safe_print("\n🧹 Cleaning up test collection...")
        if client.delete_collection(test_collection_id):
            safe_print("✅ Test collection deleted successfully")
        else:
            safe_print("⚠️  Failed to delete test collection - please clean up manually")
    
    if total_success < total_pages:
        logger.warning(f"⚠️ Some uploads failed. Check the log for details.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())