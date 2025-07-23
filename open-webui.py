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
import configparser
from pathlib import Path
from typing import List, Dict, Optional, Any
import requests
from requests.auth import HTTPBasicAuth
import logging
from utils.html_cleaner import clean_confluence_html

# Helper function to safely print text with emojis
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
            '⏭️': '[>>]'
        }
        for emoji, ascii_text in replacements.items():
            text = text.replace(emoji, ascii_text)
        print(text.encode('ascii', 'replace').decode('ascii'))

# Set up logging
def setup_logging(log_file='openwebui_upload.log'):
    """Setup logging to both console and file"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

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
        else:
            logger.info("No checkpoint file found, starting from beginning")
            return None
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        return None

def clear_checkpoint(checkpoint_file: str = 'openwebui_checkpoint.txt'):
    """Clear the checkpoint file after successful completion"""
    try:
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            logger.info("Checkpoint cleared - upload completed successfully")
    except Exception as e:
        logger.error(f"Failed to clear checkpoint: {e}")

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
            }, timeout=10)  # 10 second timeout
            logger.debug(f"AUTH RESPONSE [{response.status_code}]: {response.text}")
        except requests.exceptions.Timeout:
            logger.error(f"Authentication timeout - server did not respond within 10 seconds")
            safe_print("❌ Authentication failed: Server timeout")
            return False
        except Exception as e:
            logger.error(f"Exception during authentication: {e}")
            safe_print(f"❌ Authentication failed: {str(e)}")
            return False

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
        safe_print("✅ Authentication successful!")
        return True

    def upload_document(self, title: str, content: str, collection_name: str = "default", is_html: bool = False) -> bool:
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

        new_file_id = data.get("id")
        if not new_file_id:
            logger.error(f"No file ID returned after uploading '{title}'")
            return False

        # Step 2: Add file to knowledge collection
        knowledge_url = f"{self.base_url}/api/v1/knowledge/{collection_name}/file/add"
        knowledge_payload = {"file_id": new_file_id}
        
        logger.debug(f"Adding file '{title}' (ID: {new_file_id}) to collection '{collection_name}'")
        try:
            response = self.session.post(knowledge_url, json=knowledge_payload)
            logger.debug(f"KNOWLEDGE ADD [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception adding file '{title}' to knowledge collection: {e}")
            return False

        if response.status_code not in [200, 201]:
            logger.error(f"Failed to add file '{title}' to collection '{collection_name}': HTTP {response.status_code}")
            return False

        logger.info(f"Successfully uploaded document '{title}' to collection '{collection_name}' (file_id={new_file_id})")
        return True

    def list_knowledge_collections(self) -> Dict[str, str]:
        """List all knowledge collections and return as dict {name: id}"""
        collections_url = f"{self.base_url}/api/v1/knowledge/"
        safe_print(f"📚 Fetching knowledge collections from {self.base_url}...")
        try:
            response = self.session.get(collections_url, timeout=10)
            if response.status_code == 200:
                collections = response.json()
                collection_dict = {}
                for collection in collections:
                    name = collection.get('name', 'Unnamed')
                    collection_id = collection.get('id', None)
                    if collection_id:
                        collection_dict[name] = collection_id
                logger.info(f"Found {len(collection_dict)} existing collections: {list(collection_dict.keys())}")
                return collection_dict
            else:
                logger.warning(f"Failed to list collections: HTTP {response.status_code}")
                safe_print(f"❌ Failed to fetch collections: HTTP {response.status_code}")
                return {}
        except requests.exceptions.Timeout:
            logger.error("Timeout while fetching collections - server did not respond within 10 seconds")
            safe_print("❌ Failed to fetch collections: Server timeout")
            return {}
        except Exception as e:
            logger.error(f"Exception listing collections: {e}")
            safe_print(f"❌ Failed to fetch collections: {str(e)}")
            return {}

    def find_existing_collection(self, name: str) -> Optional[str]:
        """Find existing collection ID by name"""
        collections = self.list_knowledge_collections()
        if name in collections:
            collection_id = collections[name]
            logger.info(f"Found existing collection '{name}' (ID: {collection_id})")
            return collection_id
        else:
            logger.error(f"Collection '{name}' not found! Available collections: {list(collections.keys())}")
            return None

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

def process_confluence_page(page: Dict, space_key: str, space_name: str) -> tuple[str, str, str]:
    """
    Process a single Confluence page and return path, HTML and text versions
    """
    page_id = page.get('id', 'unknown')
    title = page.get('title', 'Untitled')
    body = page.get('body', '')
    updated = page.get('updated', 'Unknown')
    
    # Path version - just the page location info
    path_content = f"Space: {space_name} ({space_key}) > Page: {title} (ID: {page_id})"
    
    # HTML version - keep content completely INTACT, no changes whatsoever
    html_content = body
    
    # Text version - strip ALL tags (Confluence and standard HTML) using BeautifulSoup
    from bs4 import BeautifulSoup
    
    # Use BeautifulSoup to strip all HTML and XML tags
    soup = BeautifulSoup(body, 'html.parser')
    cleaned_text = soup.get_text(separator='\n', strip=True)
    
    # Create text version with metadata
    text_content = f"""Title: {title}
Page ID: {page_id}
Space: {space_name} ({space_key})
Last Updated: {updated}
{'='*60}

{cleaned_text}"""
    
    return path_content, html_content, text_content

def inspect_document(page: Dict, space_key: str, space_name: str, format_choice: str = 'both') -> None:
    """
    Display document content for inspection before upload
    """
    page_id = page.get('id', 'unknown')
    title = page.get('title', 'Untitled')
    
    path_content, html_content, text_content = process_confluence_page(page, space_key, space_name)
    
    safe_print("\n" + "="*80)
    safe_print(f"DOCUMENT INSPECTION: {title}")
    safe_print("="*80)
    safe_print(f"Page ID: {page_id}")
    safe_print(f"Space: {space_name} ({space_key})")
    safe_print("-"*80)
    
    if format_choice in ['path', 'both']:
        safe_print("\n📁 PATH VERSION:")
        safe_print(path_content)
        safe_print("-"*40)
    
    if format_choice in ['html', 'both']:
        safe_print("\n🌐 HTML VERSION (first 2000 chars):")
        safe_print(html_content[:2000])
        if len(html_content) > 2000:
            safe_print(f"... (truncated, total {len(html_content)} chars)")
        safe_print("-"*40)
    
    if format_choice in ['txt', 'both']:
        safe_print("\n📝 TEXT VERSION (first 2000 chars):")
        safe_print(text_content[:2000])
        if len(text_content) > 2000:
            safe_print(f"... (truncated, total {len(text_content)} chars)")
    
    safe_print("="*80)

def upload_confluence_space(client: OpenWebUIClient, pickle_data: Dict, 
                          html_collection: str, text_collection: str,
                          path_collection: str = None,
                          inspect: bool = False, format_choice: str = 'both',
                          interactive: bool = False) -> tuple:
    """
    Upload all pages from a Confluence space to Open-WebUI
    Returns tuple of (number of successfully uploaded pages, user_quit)
    """
    space_key = pickle_data.get('space_key', 'UNKNOWN')
    space_name = pickle_data.get('name', 'Unknown Space')
    sampled_pages = pickle_data.get('sampled_pages', [])
    
    if not sampled_pages:
        logger.warning(f"No pages found in space {space_key}")
        return 0, False
    
    logger.info(f"Processing {len(sampled_pages)} pages from space '{space_name}' ({space_key})")
    
    success_count = 0
    
    for i, page in enumerate(sampled_pages, 1):
        page_id = page.get('id', f'page_{i}')
        title = page.get('title', f'Untitled Page {i}')
        
        logger.debug(f"Processing page {i}/{len(sampled_pages)}: {title}")
        
        # If inspection is enabled, show the document
        if inspect:
            inspect_document(page, space_key, space_name, format_choice)
            
            if interactive:
                # Interactive mode - ask user what to do
                safe_print("\nOptions:")
                safe_print("  u - Upload this document")
                safe_print("  s - Skip this document")
                safe_print("  a - Upload all remaining documents without inspection")
                safe_print("  q - Quit")
                
                choice = input("\nEnter choice (u/s/a/q): ").strip().lower()
                
                if choice == 'q':
                    logger.info("User quit inspection mode")
                    return success_count, True
                elif choice == 's':
                    logger.info(f"Skipped page '{title}' by user request")
                    continue
                elif choice == 'a':
                    logger.info("User selected to upload all remaining documents")
                    inspect = False  # Turn off inspection for remaining documents
                elif choice != 'u':
                    logger.warning(f"Invalid choice '{choice}', skipping document")
                    continue
        
        try:
            path_content, html_content, text_content = process_confluence_page(page, space_key, space_name)
            
            upload_success = True
            
            # Upload based on format choice
            if format_choice in ['html', 'both']:
                # Upload HTML version
                html_title = f"{space_key}-{page_id}-HTML"
                html_success = client.upload_document(html_title, html_content, html_collection, is_html=True)
                upload_success = upload_success and html_success
            
            if format_choice in ['txt', 'both']:
                # Upload text version
                text_title = f"{space_key}-{page_id}-TEXT"
                text_success = client.upload_document(text_title, text_content, text_collection, is_html=False)
                upload_success = upload_success and text_success
            
            if format_choice == 'path':
                # Upload path version to path collection (or text collection if not specified)
                path_title = f"{space_key}-{page_id}-PATH"
                collection_to_use = path_collection if path_collection else text_collection
                path_success = client.upload_document(path_title, path_content, collection_to_use, is_html=False)
                upload_success = upload_success and path_success
            
            if upload_success:
                success_count += 1
                logger.info(f"Successfully uploaded page '{title}' ({page_id}) - format: {format_choice}")
            else:
                logger.warning(f"Failed to upload page '{title}' ({page_id})")
                
        except Exception as e:
            logger.error(f"Error processing page '{title}': {e}")
    
    logger.info(f"Successfully uploaded {success_count}/{len(sampled_pages)} pages from space {space_key}")
    return success_count, False


def load_openwebui_settings():
    """Load Open-WebUI settings from settings.ini"""
    config = configparser.ConfigParser()
    
    # Try to load settings.ini
    if not os.path.exists('settings.ini'):
        logger.warning("settings.ini not found, using command line arguments only")
        return {}
    
    config.read('settings.ini')
    
    # Get Open-WebUI settings
    if 'OpenWebUI' not in config:
        logger.warning("No [OpenWebUI] section found in settings.ini")
        return {}
    
    settings = {}
    openwebui_section = config['OpenWebUI']
    
    # Load settings with fallbacks
    settings['base_url'] = openwebui_section.get('base_url', 'http://localhost:8080')
    settings['username'] = openwebui_section.get('username', None)
    settings['password'] = openwebui_section.get('password', None)
    settings['upload_dir'] = openwebui_section.get('upload_dir', None)
    
    # Don't use placeholder values
    if settings['username'] == 'your_username':
        settings['username'] = None
    if settings['password'] == 'your_password':
        settings['password'] = None
    
    logger.info(f"Loaded Open-WebUI settings from settings.ini: {settings['base_url']}")
    if settings['username']:
        logger.info(f"Using credentials for: {settings['username']}")
    
    return settings

def main():
    """Main function"""
    # Load settings first
    settings = load_openwebui_settings()
    
    parser = argparse.ArgumentParser(
        description="Upload Confluence pickles to Open-WebUI knowledge spaces"
    )
    parser.add_argument(
        "--pickle-dir", 
        default=settings.get('upload_dir', 'temp'),
        help=f"Directory containing Confluence pickle files (default: {settings.get('upload_dir', 'temp')})"
    )
    parser.add_argument(
        "--ollama-server", 
        default="http://localhost:11434",
        help="Ollama server URL (default: http://localhost:11434)"
    )
    parser.add_argument(
        "--openwebui-server", 
        default=settings.get('base_url', "http://localhost:8080"),
        help=f"Open-WebUI server URL (default: {settings.get('base_url', 'http://localhost:8080')})"
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
        "--path-collection", 
        default=None,
        help="Knowledge collection name for path/index information (default: uses text collection)"
    )
    parser.add_argument(
        "--username",
        default=settings.get('username'),
        help=f"Username for Open-WebUI authentication (default from settings: {settings.get('username', 'None')})"
    )
    parser.add_argument(
        "--password",
        default=settings.get('password'),
        help="Password for Open-WebUI authentication (default from settings.ini)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear the checkpoint file and start from beginning"
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Inspect each document before uploading"
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive mode - choose whether to upload each document after inspection"
    )
    parser.add_argument(
        "--format",
        choices=['path', 'html', 'txt', 'both'],
        default='both',
        help="Format to upload: path (location info only), html, txt, or both (default: both)"
    )
    
    args = parser.parse_args()
    
    # Ensure path_collection attribute exists
    if not hasattr(args, 'path_collection'):
        args.path_collection = None
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # If interactive mode is requested without inspect, enable inspect automatically
    if args.interactive and not args.inspect:
        args.inspect = True
        logger.info("Enabling document inspection for interactive mode")
    
    # Show interactive menu if no specific mode is selected
    # Skip menu if format is explicitly set via command line
    format_set_explicitly = args.format != parser.get_default('format') if hasattr(parser, 'get_default') else False
    if not format_set_explicitly:
        # Check if --format was passed on command line
        format_set_explicitly = any(arg in sys.argv for arg in ['--format', '-f'])
    
    if not args.inspect and not any([args.clear_checkpoint]) and not format_set_explicitly:
        safe_print("\n" + "="*60)
        safe_print("OPEN-WEBUI CONFLUENCE UPLOADER")
        safe_print("="*60)
        safe_print("\nUpload Modes:")
        safe_print("  1. Standard upload (both HTML and text)")
        safe_print("  2. Preview & upload all (shows 2000 chars, uploads automatically)")
        safe_print("  3. Interactive upload (preview each, then choose: upload/skip/quit)")
        safe_print("  4. Upload path information only")
        safe_print("  5. Upload HTML format only")
        safe_print("  6. Upload text format only")
        safe_print("  7. Clear checkpoint and start fresh")
        safe_print("  8. Test authentication only")
        safe_print("  q. Quit")
        
        choice = input("\nSelect mode (1-8 or q): ").strip().lower()
        
        if choice == 'q':
            safe_print("Exiting...")
            return 0
        elif choice == '1':
            args.inspect = False
            args.interactive = False
            args.format = 'both'
        elif choice == '2':
            args.inspect = True
            args.interactive = False
            safe_print("\n📋 Preview Mode Selected")
            safe_print("   Each document will show 2000 characters before upload.")
            safe_print("   All documents upload automatically (no waiting for input).")
            # Ask for format
            safe_print("\nSelect format to upload:")
            safe_print("  1. Both HTML and text (default)")
            safe_print("  2. HTML only")
            safe_print("  3. Text only")
            safe_print("  4. Path information only")
            format_choice = input("\nSelect format (1-4): ").strip()
            if format_choice == '2':
                args.format = 'html'
            elif format_choice == '3':
                args.format = 'txt'
            elif format_choice == '4':
                args.format = 'path'
            else:
                args.format = 'both'
        elif choice == '3':
            args.inspect = True
            args.interactive = True
            # Ask for format
            safe_print("\nSelect format to upload:")
            safe_print("  1. Both HTML and text (default)")
            safe_print("  2. HTML only")
            safe_print("  3. Text only")
            safe_print("  4. Path information only")
            format_choice = input("\nSelect format (1-4): ").strip()
            if format_choice == '2':
                args.format = 'html'
            elif format_choice == '3':
                args.format = 'txt'
            elif format_choice == '4':
                args.format = 'path'
            else:
                args.format = 'both'
        elif choice == '4':
            args.inspect = False
            args.interactive = False
            args.format = 'path'
            safe_print("\n📁 Path Information Upload Selected")
            safe_print("   This uploads only the navigation path for each page.")
            safe_print("\n🔍 Checking available knowledge collections...")
            
            # Create a temporary client to list collections
            temp_client = OpenWebUIClient(
                args.openwebui_server,
                args.username,
                args.password
            )
            if temp_client.authenticate():
                collections = temp_client.list_knowledge_collections()
                if collections:
                    safe_print(f"\n📚 Available collections:")
                    collection_list = list(collections.keys())
                    for i, name in enumerate(collection_list, 1):
                        safe_print(f"   {i}. {name}")
                    safe_print(f"   {len(collection_list) + 1}. Create new collection (not supported yet)")
                    
                    choice_num = input(f"\nSelect collection for path index (1-{len(collection_list)}): ").strip()
                    try:
                        idx = int(choice_num) - 1
                        if 0 <= idx < len(collection_list):
                            args.path_collection = collection_list[idx]
                            safe_print(f"✅ Selected: {args.path_collection}")
                        else:
                            safe_print(f"⚠️  Invalid choice, using default text collection: {args.text_collection}")
                    except:
                        safe_print(f"⚠️  Invalid input, using default text collection: {args.text_collection}")
        elif choice == '5':
            args.inspect = False
            args.interactive = False
            args.format = 'html'
        elif choice == '6':
            args.inspect = False
            args.interactive = False
            args.format = 'txt'
        elif choice == '7':
            clear_checkpoint()
            safe_print("\n✓ Checkpoint cleared - will start from beginning")
            safe_print("Now select an upload mode:")
            safe_print("  1. Standard upload (both HTML and text)")
            safe_print("  2. Upload with document inspection")
            safe_print("  3. Interactive upload (inspect and choose per document)")
            safe_print("  4. Upload path information only")
            safe_print("  5. Upload HTML format only")
            safe_print("  6. Upload text format only")
            mode_choice = input("\nSelect upload mode (1-6): ").strip()
            if mode_choice == '2':
                args.inspect = True
                args.interactive = False
                args.format = 'both'
            elif mode_choice == '3':
                args.inspect = True
                args.interactive = True
                args.format = 'both'
            elif mode_choice == '4':
                args.format = 'path'
            elif mode_choice == '5':
                args.format = 'html'
            elif mode_choice == '6':
                args.format = 'txt'
            else:
                args.format = 'both'
        elif choice == '8':
            # Test authentication only
            safe_print("\n🔧 Testing authentication...")
            safe_print(f"   Server: {args.openwebui_server}")
            safe_print(f"   Username: {args.username or 'Not provided'}")
            safe_print(f"   Password: {'***' if args.password else 'Not provided'}")
            
            test_client = OpenWebUIClient(
                args.openwebui_server,
                args.username,
                args.password
            )
            
            if test_client.authenticate():
                safe_print("\n✅ Authentication test successful!")
                safe_print("   You can now proceed with uploads.")
                
                # Also test listing collections
                safe_print("\n📚 Testing collection access...")
                collections = test_client.list_knowledge_collections()
                if collections:
                    safe_print(f"✅ Found {len(collections)} collection(s):")
                    for name in collections.keys():
                        safe_print(f"   - {name}")
                else:
                    safe_print("❌ No collections found or unable to list collections")
            else:
                safe_print("\n❌ Authentication test failed!")
                safe_print("\nTroubleshooting tips:")
                safe_print("1. Check if you're using email (not username) for login")
                safe_print("2. Verify the server URL (should be like http://localhost:8080)")
                safe_print("3. Try logging into the web UI to confirm credentials work")
                safe_print("4. Some Open-WebUI instances may have API authentication disabled")
            
            return 0
        else:
            safe_print("Invalid choice, using standard upload mode")
            args.inspect = False
            args.interactive = False
            args.format = 'both'
        
        safe_print(f"\nSelected: inspect={args.inspect}, interactive={args.interactive}, format={args.format}")
    
    # Clear checkpoint if requested
    if args.clear_checkpoint:
        clear_checkpoint()
        logger.info("Checkpoint cleared - starting from beginning")
    
    # Validate pickle directory
    safe_print(f"\n📁 Checking pickle directory: {args.pickle_dir}")
    if not os.path.exists(args.pickle_dir):
        logger.error(f"Pickle directory does not exist: {args.pickle_dir}")
        safe_print(f"❌ ERROR: Pickle directory does not exist: {args.pickle_dir}")
        return 1
    
    # Find all pickle files
    safe_print(f"🔍 Searching for pickle files in {args.pickle_dir}...")
    pickle_files = find_pickle_files(args.pickle_dir)
    if not pickle_files:
        logger.error("No pickle files found")
        safe_print(f"❌ ERROR: No pickle files found in {args.pickle_dir}")
        return 1
    safe_print(f"✅ Found {len(pickle_files)} pickle file(s)")
    
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
    
    # Find existing knowledge collections
    logger.info("Finding existing knowledge collections...")
    safe_print(f"\n🔍 Looking for knowledge collections...")
    html_collection_id = client.find_existing_collection(args.html_collection)
    text_collection_id = client.find_existing_collection(args.text_collection)
    
    # Handle path collection if specified
    path_collection_id = None
    if args.format == 'path' and hasattr(args, 'path_collection') and args.path_collection:
        path_collection_id = client.find_existing_collection(args.path_collection)
        if not path_collection_id:
            safe_print(f"⚠️  Path collection '{args.path_collection}' not found, will use text collection")
            path_collection_id = text_collection_id
    else:
        path_collection_id = text_collection_id
    
    if not html_collection_id or not text_collection_id:
        logger.error("Required knowledge collections not found!")
        safe_print("\n❌ ERROR: Required knowledge collections not found!")
        safe_print(f"   Please ensure these collections exist in Open-WebUI:")
        safe_print(f"   - HTML collection: {args.html_collection}")
        safe_print(f"   - Text collection: {args.text_collection}")
        return 1
    
    safe_print(f"✅ Found required collections:")
    safe_print(f"   - HTML: {args.html_collection}")
    safe_print(f"   - Text: {args.text_collection}")
    if args.format == 'path' and hasattr(args, 'path_collection') and args.path_collection:
        safe_print(f"   - Path: {args.path_collection}")
    
    # Load checkpoint to resume from last successful upload
    safe_print("\n📋 Checking for checkpoint file...")
    last_uploaded_space = load_checkpoint()
    resume_mode = last_uploaded_space is not None
    if resume_mode:
        safe_print(f"✅ Found checkpoint - will resume after space: {last_uploaded_space}")
    else:
        safe_print("📝 No checkpoint found - will start from beginning")
    
    # Filter pickle files based on checkpoint
    files_to_process = []
    
    if resume_mode:
        safe_print(f"\n⏭️  Looking for checkpoint space: {last_uploaded_space}")
        # We need to find where we left off, but we'll only load files as needed
        found_checkpoint = False
        for pickle_file in pickle_files:
            # Extract space key from filename instead of loading the whole file
            # Most pickle files are named like "SPACENAME.pkl" or "SPACENAME_full.pkl"
            filename = pickle_file.stem  # Remove .pkl extension
            if filename.endswith('_full'):
                filename = filename[:-5]  # Remove _full suffix
            
            if filename == last_uploaded_space:
                safe_print(f"✅ Found checkpoint at: {pickle_file.name}")
                found_checkpoint = True
                continue  # Skip this one, start from next
            
            if found_checkpoint:
                files_to_process.append(pickle_file)
        
        if not found_checkpoint:
            safe_print(f"⚠️  Checkpoint space '{last_uploaded_space}' not found in pickle files")
            safe_print("   Starting from beginning instead...")
            files_to_process = pickle_files
    else:
        # No checkpoint, process all files
        files_to_process = pickle_files
    
    if not files_to_process:
        logger.info("No files to process")
        safe_print("\n✅ All spaces have already been processed!")
        return 0
    
    safe_print(f"\n✅ Ready to upload {len(files_to_process)} space(s)")
    logger.info(f"🚀 Starting upload: {len(files_to_process)} spaces")
    logger.info(f"📋 Upload settings: format={args.format}, inspect={args.inspect}, interactive={args.interactive}")
    
    # Process files sequentially
    total_success = 0
    total_pages = 0
    
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
            
            logger.info(f"🔄 Processing space '{space_name}' ({space_key}) with {page_count} pages")
            
            success_count, user_quit = upload_confluence_space(
                client, pickle_data, html_collection_id, text_collection_id,
                path_collection=path_collection_id,
                inspect=args.inspect, format_choice=args.format, interactive=args.interactive
            )
            
            total_success += success_count
            total_pages += page_count
            
            # Check if user quit
            if user_quit:
                logger.info("User requested quit - exiting upload process")
                safe_print("\n❌ Upload process terminated by user")
                break
            
            # Save checkpoint after successful upload
            if success_count > 0:
                save_checkpoint(space_key)
            
            logger.info(f"✅ Completed space '{space_name}' ({space_key}): {success_count}/{page_count} pages uploaded")
            logger.info(f"📊 Overall progress: {i}/{len(files_to_process)} spaces, {total_success}/{total_pages} pages uploaded")
                
        except Exception as e:
            logger.error(f"❌ Processing {pickle_file.name} failed: {e}")
    
    logger.info(f"🎉 Upload complete: {total_success}/{total_pages} pages uploaded successfully")
    
    # Clear checkpoint on successful completion
    if total_success == total_pages:
        clear_checkpoint()
    
    if total_success < total_pages:
        logger.warning(f"{total_pages - total_success} pages failed to upload")
        logger.info("Checkpoint saved - run again to resume from last successful upload")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())