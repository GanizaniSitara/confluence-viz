#!/usr/bin/env python3
"""
Script to sample and pickle Confluence attachments.
Cloned from sample_and_pickle_spaces.py but modified to handle attachments.
Stores attachments in ./attachments/{spacekey}/{pageid}_attachmentname format.
"""

import os
import sys
import pickle
import json
import requests
from datetime import datetime
import argparse
import configparser
from typing import List, Dict, Tuple, Optional
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load configuration
config = configparser.ConfigParser()
config.read('settings.ini')

# Confluence settings
BASE_URL = config.get('confluence', 'base_url').rstrip('/')
USERNAME = config.get('confluence', 'username')
PASSWORD = config.get('confluence', 'password')
VERIFY_SSL = config.getboolean('confluence', 'verify_ssl', fallback=True)

# API endpoint
API_ENDPOINT = '/rest/api'

# Attachment storage settings - will use new config value
ATTACHMENTS_DIR = config.get('data', 'attachments_dir', fallback='attachments')

# Checkpoint file for tracking progress
CHECKPOINT_FILE = 'confluence_attachments_checkpoint.json'

# Logging setup
def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('attachments_pickle.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# Create session with retry strategy
def create_session():
    """Create a requests session with retry strategy"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = create_session()

def get_with_retry(url, **kwargs):
    """Wrapper for requests.get with built-in retry logic"""
    return session.get(url, **kwargs)

def load_checkpoint():
    """Load checkpoint file to resume from previous run"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
    return {
        "processed_spaces": [],
        "last_updated": None,
        "total_attachments": 0,
        "total_size_bytes": 0
    }

def save_checkpoint(checkpoint):
    """Save checkpoint to file"""
    checkpoint["last_updated"] = datetime.now().isoformat()
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

def fetch_all_spaces():
    """Fetch all spaces from Confluence"""
    spaces = []
    start = 0
    limit = 100
    
    while True:
        url = f"{BASE_URL}{API_ENDPOINT}/space"
        params = {
            "start": start,
            "limit": limit,
            "type": "global"  # Exclude personal spaces
        }
        
        try:
            response = get_with_retry(url, auth=(USERNAME, PASSWORD), params=params, verify=VERIFY_SSL)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            spaces.extend(results)
            
            if len(results) < limit:
                break
                
            start += limit
            
        except Exception as e:
            logger.error(f"Error fetching spaces: {e}")
            break
    
    return spaces

def fetch_pages_for_space(space_key):
    """Fetch all pages for a given space"""
    pages = []
    start = 0
    limit = 100
    
    while True:
        url = f"{BASE_URL}{API_ENDPOINT}/content"
        params = {
            "spaceKey": space_key,
            "start": start,
            "limit": limit,
            "expand": "children.attachment"
        }
        
        try:
            response = get_with_retry(url, auth=(USERNAME, PASSWORD), params=params, verify=VERIFY_SSL)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            pages.extend(results)
            
            if len(results) < limit:
                break
                
            start += limit
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error fetching pages for space {space_key}: {e}")
            break
    
    return pages

def sanitize_filename(filename):
    """Sanitize filename for safe storage"""
    # Replace problematic characters
    safe_name = "".join(c if c.isalnum() or c in ('.', '_', '-', ' ') else '_' for c in filename)
    # Limit length
    if len(safe_name) > 200:
        name, ext = os.path.splitext(safe_name)
        safe_name = name[:196] + ext
    return safe_name or "unnamed_attachment"

def download_and_pickle_attachment(attachment, page_id, space_key, auth_tuple):
    """Download an attachment and save metadata"""
    att_id = attachment.get('id')
    att_title = attachment.get('title', 'unnamed')
    att_download_link = attachment.get('_links', {}).get('download')
    
    if not att_download_link:
        logger.warning(f"No download link for attachment {att_title}")
        return None
    
    # Construct full download URL
    if att_download_link.startswith('/'):
        download_url = f"{BASE_URL}{att_download_link}"
    else:
        download_url = f"{BASE_URL}/{att_download_link}"
    
    # Create directory structure
    space_dir = os.path.join(ATTACHMENTS_DIR, space_key)
    os.makedirs(space_dir, exist_ok=True)
    
    # Create filename with page ID prefix
    safe_filename = sanitize_filename(att_title)
    filename = f"{page_id}_{safe_filename}"
    file_path = os.path.join(space_dir, filename)
    
    # Skip if already exists
    if os.path.exists(file_path):
        logger.debug(f"Attachment already exists: {file_path}")
        # Still return metadata even if file exists
        return {
            'id': att_id,
            'title': att_title,
            'filename': filename,
            'page_id': page_id,
            'space_key': space_key,
            'file_path': file_path,
            'size': os.path.getsize(file_path),
            'download_date': datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
            'existed': True
        }
    
    try:
        # Download the attachment
        logger.info(f"Downloading: {att_title} from page {page_id}")
        response = get_with_retry(download_url, auth=auth_tuple, verify=VERIFY_SSL, stream=True, timeout=60)
        response.raise_for_status()
        
        # Write to file
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        file_size = os.path.getsize(file_path)
        
        # Return metadata
        return {
            'id': att_id,
            'title': att_title,
            'filename': filename,
            'page_id': page_id,
            'space_key': space_key,
            'file_path': file_path,
            'size': file_size,
            'download_date': datetime.now().isoformat(),
            'media_type': attachment.get('extensions', {}).get('mediaType', 'unknown'),
            'created_date': attachment.get('version', {}).get('when'),
            'created_by': attachment.get('version', {}).get('by', {}).get('displayName'),
            'existed': False
        }
        
    except Exception as e:
        logger.error(f"Error downloading attachment {att_title}: {e}")
        return None

def process_space_attachments(space_key, space_name):
    """Process all attachments for a given space"""
    logger.info(f"Processing attachments for space: {space_key} ({space_name})")
    
    # Fetch all pages in the space
    pages = fetch_pages_for_space(space_key)
    if not pages:
        logger.warning(f"No pages found for space {space_key}")
        return []
    
    logger.info(f"Found {len(pages)} pages in space {space_key}")
    
    all_attachments_metadata = []
    total_attachments = 0
    downloaded_attachments = 0
    
    # Process each page
    for page in pages:
        page_id = page.get('id')
        page_title = page.get('title', 'Untitled')
        
        # Get attachments for this page
        attachments = page.get('children', {}).get('attachment', {}).get('results', [])
        
        if not attachments:
            continue
            
        logger.debug(f"Processing {len(attachments)} attachments from page: {page_title}")
        total_attachments += len(attachments)
        
        # Download and store each attachment
        for attachment in attachments:
            metadata = download_and_pickle_attachment(
                attachment, 
                page_id, 
                space_key, 
                (USERNAME, PASSWORD)
            )
            
            if metadata:
                all_attachments_metadata.append(metadata)
                if not metadata.get('existed', False):
                    downloaded_attachments += 1
    
    logger.info(f"Space {space_key}: Total attachments: {total_attachments}, "
                f"Downloaded: {downloaded_attachments}, "
                f"Already existed: {total_attachments - downloaded_attachments}")
    
    # Save metadata pickle for this space
    pickle_path = os.path.join(ATTACHMENTS_DIR, space_key, f"{space_key}_attachments.pkl")
    try:
        with open(pickle_path, 'wb') as f:
            pickle.dump({
                'space_key': space_key,
                'space_name': space_name,
                'pickle_date': datetime.now().isoformat(),
                'total_attachments': len(all_attachments_metadata),
                'attachments': all_attachments_metadata
            }, f)
        logger.info(f"Saved attachments metadata to: {pickle_path}")
    except Exception as e:
        logger.error(f"Error saving pickle for space {space_key}: {e}")
    
    return all_attachments_metadata

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Sample and pickle Confluence attachments")
    parser.add_argument('--space', help="Process attachments for a specific space")
    parser.add_argument('--all-spaces', action='store_true', help="Process attachments for all spaces")
    parser.add_argument('--reset', action='store_true', help="Reset checkpoint and start fresh")
    parser.add_argument('--list-spaces', action='store_true', help="List all available spaces")
    
    args = parser.parse_args()
    
    # Create attachments directory if it doesn't exist
    os.makedirs(ATTACHMENTS_DIR, exist_ok=True)
    
    # Handle list spaces
    if args.list_spaces:
        spaces = fetch_all_spaces()
        print(f"\nFound {len(spaces)} spaces:")
        for space in spaces:
            print(f"  {space['key']}: {space['name']}")
        return
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    
    # Handle reset
    if args.reset:
        logger.info("Resetting checkpoint...")
        checkpoint = {
            "processed_spaces": [],
            "last_updated": None,
            "total_attachments": 0,
            "total_size_bytes": 0
        }
        save_checkpoint(checkpoint)
    
    # Process specific space
    if args.space:
        space_key = args.space.upper()
        
        # Fetch space details
        url = f"{BASE_URL}{API_ENDPOINT}/space/{space_key}"
        try:
            response = get_with_retry(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
            response.raise_for_status()
            space_data = response.json()
            space_name = space_data.get('name', 'Unknown')
            
            # Process attachments
            attachments = process_space_attachments(space_key, space_name)
            
            # Update checkpoint
            if space_key not in checkpoint["processed_spaces"]:
                checkpoint["processed_spaces"].append(space_key)
            checkpoint["total_attachments"] += len(attachments)
            checkpoint["total_size_bytes"] += sum(a.get('size', 0) for a in attachments if not a.get('existed', False))
            save_checkpoint(checkpoint)
            
        except Exception as e:
            logger.error(f"Error processing space {space_key}: {e}")
            return
    
    # Process all spaces
    elif args.all_spaces:
        spaces = fetch_all_spaces()
        logger.info(f"Found {len(spaces)} spaces to process")
        
        processed_spaces = set(checkpoint.get("processed_spaces", []))
        
        for space in spaces:
            space_key = space['key']
            space_name = space['name']
            
            if space_key in processed_spaces:
                logger.info(f"Skipping already processed space: {space_key}")
                continue
            
            try:
                attachments = process_space_attachments(space_key, space_name)
                
                # Update checkpoint
                checkpoint["processed_spaces"].append(space_key)
                checkpoint["total_attachments"] += len(attachments)
                checkpoint["total_size_bytes"] += sum(a.get('size', 0) for a in attachments if not a.get('existed', False))
                save_checkpoint(checkpoint)
                
            except Exception as e:
                logger.error(f"Error processing space {space_key}: {e}")
                continue
        
        # Final summary
        logger.info(f"\nProcessing complete!")
        logger.info(f"Total spaces processed: {len(checkpoint['processed_spaces'])}")
        logger.info(f"Total attachments: {checkpoint['total_attachments']}")
        logger.info(f"Total size: {checkpoint['total_size_bytes'] / (1024*1024*1024):.2f} GB")
    
    else:
        print("Please specify --space SPACE_KEY or --all-spaces")
        print("Use --help for more options")

if __name__ == "__main__":
    main()