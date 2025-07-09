#!/usr/bin/env python3
"""
Open-WebUI Confluence Uploader
Loads Confluence pickles and uploads HTML and text versions to Open-WebUI knowledge spaces.
"""

import sys
import os
import argparse
import pickle
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
import requests
from requests.auth import HTTPBasicAuth
import logging
from utils.html_cleaner import clean_confluence_html

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OpenWebUIClient:
    """Client for interacting with Open-WebUI API"""

    def __init__(self, base_url: str, username: str = None, password: str = None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        if username and password:
            self.session.auth = HTTPBasicAuth(username, password)
        self.auth_token = None

    def authenticate(self) -> bool:
        """Authenticate with Open-WebUI if credentials provided"""
        if not self.username or not self.password:
            logger.info("No credentials provided, skipping authentication")
            return True
            
        auth_url = f"{self.base_url}/api/v1/auths/signin"
        logger.debug(f"AUTH: POST {auth_url}")
        try:
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            })
            logger.debug(f"AUTH RESPONSE [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception during authentication: {e}")
            return False

        if response.status_code != 200:
            logger.error(f"Authentication failed: HTTP {response.status_code}")
            return False

        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Failed to parse authentication JSON: {e}")
            return False

        self.auth_token = data.get("token")
        if not self.auth_token:
            logger.error("Authentication succeeded but no token returned")
            return False

        self.session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
        logger.info("Authentication successful")
        return True

    def upload_document(self, title: str, content: str, collection_name: str = "default") -> bool:
        """
        Upload a document to Open-WebUI knowledge base
        """
        upload_url = f"{self.base_url}/api/v1/documents/"
        
        payload = {
            "name": title,
            "title": title,
            "content": content,
            "collection_name": collection_name
        }
        
        logger.debug(f"Uploading document '{title}' to collection '{collection_name}'")
        try:
            response = self.session.post(upload_url, json=payload)
            logger.debug(f"UPLOAD [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception uploading document '{title}': {e}")
            return False

        if response.status_code not in [200, 201]:
            logger.error(f"Failed to upload document '{title}': HTTP {response.status_code}")
            return False

        logger.info(f"Successfully uploaded document '{title}' to collection '{collection_name}'")
        return True

    def list_collections(self) -> List[str]:
        """List available collections"""
        collections_url = f"{self.base_url}/api/v1/documents/collections"
        try:
            response = self.session.get(collections_url)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Failed to list collections: HTTP {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Exception listing collections: {e}")
            return []

def find_pickle_files(pickle_dir: str) -> List[Path]:
    """
    Find all pickle files in the given directory
    """
    pickle_path = Path(pickle_dir)
    if not pickle_path.exists():
        logger.error(f"Pickle directory does not exist: {pickle_dir}")
        return []
    
    pickle_files = []
    for file_path in pickle_path.rglob("*.pkl"):
        pickle_files.append(file_path)
    
    logger.info(f"Found {len(pickle_files)} pickle files in {pickle_dir}")
    return pickle_files

def load_confluence_pickle(pickle_path: Path) -> Optional[Dict]:
    """
    Load a Confluence pickle file and return its data
    """
    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        # Validate that this is a Confluence space pickle
        if not isinstance(data, dict) or 'sampled_pages' not in data:
            logger.warning(f"'{pickle_path.name}' doesn't appear to be a Confluence space pickle")
            return None
            
        return data
    except Exception as e:
        logger.error(f"Error loading pickle file '{pickle_path}': {e}")
        return None

def process_confluence_page(page: Dict, space_key: str, space_name: str) -> tuple[str, str]:
    """
    Process a single Confluence page and return HTML and text versions
    """
    page_id = page.get('id', 'unknown')
    title = page.get('title', 'Untitled')
    body = page.get('body', '')
    updated = page.get('updated', 'Unknown')
    
    # Create HTML version with structured content
    html_content = f"""<h1>{title}</h1>
<p><strong>Page ID:</strong> {page_id}</p>
<p><strong>Space:</strong> {space_name} ({space_key})</p>
<p><strong>Last Updated:</strong> {updated}</p>
<hr>
{body}"""
    
    # Create text version using the HTML cleaner
    text_content = f"""Title: {title}
Page ID: {page_id}
Space: {space_name} ({space_key})
Last Updated: {updated}
{'='*60}

{clean_confluence_html(body)}"""
    
    return html_content, text_content

def upload_confluence_space(client: OpenWebUIClient, pickle_data: Dict, 
                          html_collection: str, text_collection: str) -> int:
    """
    Upload all pages from a Confluence space to Open-WebUI
    Returns number of successfully uploaded pages
    """
    space_key = pickle_data.get('space_key', 'UNKNOWN')
    space_name = pickle_data.get('name', 'Unknown Space')
    sampled_pages = pickle_data.get('sampled_pages', [])
    
    if not sampled_pages:
        logger.warning(f"No pages found in space {space_key}")
        return 0
    
    logger.info(f"Processing {len(sampled_pages)} pages from space '{space_name}' ({space_key})")
    
    success_count = 0
    
    for i, page in enumerate(sampled_pages, 1):
        page_id = page.get('id', f'page_{i}')
        title = page.get('title', f'Untitled Page {i}')
        
        logger.debug(f"Processing page {i}/{len(sampled_pages)}: {title}")
        
        try:
            html_content, text_content = process_confluence_page(page, space_key, space_name)
            
            # Upload HTML version
            html_title = f"{space_key}-{page_id}-HTML"
            html_success = client.upload_document(html_title, html_content, html_collection)
            
            # Upload text version
            text_title = f"{space_key}-{page_id}-TEXT"
            text_success = client.upload_document(text_title, text_content, text_collection)
            
            if html_success and text_success:
                success_count += 1
                logger.info(f"Successfully uploaded page '{title}' ({page_id})")
            else:
                logger.warning(f"Failed to upload page '{title}' ({page_id})")
                
        except Exception as e:
            logger.error(f"Error processing page '{title}': {e}")
    
    logger.info(f"Successfully uploaded {success_count}/{len(sampled_pages)} pages from space {space_key}")
    return success_count

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Upload Confluence pickles to Open-WebUI knowledge spaces"
    )
    parser.add_argument(
        "--pickle-dir", 
        required=True,
        help="Directory containing Confluence pickle files"
    )
    parser.add_argument(
        "--ollama-server", 
        default="http://localhost:11434",
        help="Ollama server URL (default: http://localhost:11434)"
    )
    parser.add_argument(
        "--openwebui-server", 
        default="http://localhost:8080",
        help="Open-WebUI server URL (default: http://localhost:8080)"
    )
    parser.add_argument(
        "--html-collection", 
        default="CONF-HTML",
        help="Knowledge collection name for HTML versions (default: CONF-HTML)"
    )
    parser.add_argument(
        "--text-collection", 
        default="CONF-TXT",
        help="Knowledge collection name for text versions (default: CONF-TXT)"
    )
    parser.add_argument(
        "--username",
        help="Username for Open-WebUI authentication (optional)"
    )
    parser.add_argument(
        "--password",
        help="Password for Open-WebUI authentication (optional)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate pickle directory
    if not os.path.exists(args.pickle_dir):
        logger.error(f"Pickle directory does not exist: {args.pickle_dir}")
        return 1
    
    # Find all pickle files
    pickle_files = find_pickle_files(args.pickle_dir)
    if not pickle_files:
        logger.error("No pickle files found")
        return 1
    
    # Initialize Open-WebUI client
    client = OpenWebUIClient(
        args.openwebui_server,
        args.username,
        args.password
    )
    
    # Authenticate if credentials provided
    if not client.authenticate():
        logger.error("Authentication failed")
        return 1
    
    # Process each pickle file
    total_success = 0
    total_pages = 0
    
    for pickle_file in pickle_files:
        logger.info(f"Processing pickle file: {pickle_file.name}")
        
        pickle_data = load_confluence_pickle(pickle_file)
        if not pickle_data:
            logger.warning(f"Skipping invalid pickle file: {pickle_file.name}")
            continue
        
        space_key = pickle_data.get('space_key', 'UNKNOWN')
        space_name = pickle_data.get('name', 'Unknown Space')
        page_count = len(pickle_data.get('sampled_pages', []))
        
        logger.info(f"Found space '{space_name}' ({space_key}) with {page_count} pages")
        
        success_count = upload_confluence_space(
            client, pickle_data, args.html_collection, args.text_collection
        )
        
        total_success += success_count
        total_pages += page_count
    
    logger.info(f"Upload complete: {total_success}/{total_pages} pages uploaded successfully")
    
    if total_success < total_pages:
        logger.warning(f"{total_pages - total_success} pages failed to upload")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())