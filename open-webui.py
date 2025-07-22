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

# Set up logging
def setup_logging(log_file='openwebui_upload.log'):
    """Setup logging to both console and file"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def save_checkpoint(space_key: str, checkpoint_file: str = 'openwebui_checkpoint.txt'):
    """Save the last successfully uploaded space to checkpoint file"""
    try:
        with open(checkpoint_file, 'w') as f:
            f.write(space_key)
        logger.info(f"Checkpoint saved: {space_key}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")

def load_checkpoint(checkpoint_file: str = 'openwebui_checkpoint.txt') -> Optional[str]:
    """Load the last successfully uploaded space from checkpoint file"""
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
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
            print("ℹ️  No credentials provided - proceeding without authentication")
            print("   (Some Open-WebUI instances don't require authentication)")
            return True
            
        auth_url = f"{self.base_url}/api/v1/auths/signin"
        logger.debug(f"AUTH: POST {auth_url}")
        print(f"🔐 Authenticating with {self.base_url}...")
        try:
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            }, timeout=10)  # 10 second timeout
            logger.debug(f"AUTH RESPONSE [{response.status_code}]: {response.text}")
        except requests.exceptions.Timeout:
            logger.error(f"Authentication timeout - server did not respond within 10 seconds")
            print("❌ Authentication failed: Server timeout")
            return False
        except Exception as e:
            logger.error(f"Exception during authentication: {e}")
            print(f"❌ Authentication failed: {str(e)}")
            return False

        if response.status_code != 200:
            logger.error(f"Authentication failed: HTTP {response.status_code}")
            print(f"❌ Authentication failed: HTTP {response.status_code}")
            if response.status_code == 401:
                print("   Unauthorized - check your username/password")
            elif response.status_code == 404:
                print("   Auth endpoint not found - check the server URL")
                print(f"   Tried: {auth_url}")
            try:
                error_data = response.json()
                if 'detail' in error_data:
                    print(f"   Server message: {error_data['detail']}")
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
        print("✅ Authentication successful!")
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
        print(f"📚 Fetching knowledge collections from {self.base_url}...")
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
                print(f"❌ Failed to fetch collections: HTTP {response.status_code}")
                return {}
        except requests.exceptions.Timeout:
            logger.error("Timeout while fetching collections - server did not respond within 10 seconds")
            print("❌ Failed to fetch collections: Server timeout")
            return {}
        except Exception as e:
            logger.error(f"Exception listing collections: {e}")
            print(f"❌ Failed to fetch collections: {str(e)}")
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
    
    print("\n" + "="*80)
    print(f"DOCUMENT INSPECTION: {title}")
    print("="*80)
    print(f"Page ID: {page_id}")
    print(f"Space: {space_name} ({space_key})")
    print("-"*80)
    
    if format_choice in ['path', 'both']:
        print("\n📁 PATH VERSION:")
        print(path_content)
        print("-"*40)
    
    if format_choice in ['html', 'both']:
        print("\n🌐 HTML VERSION (first 1000 chars):")
        print(html_content[:1000])
        if len(html_content) > 1000:
            print(f"... (truncated, total {len(html_content)} chars)")
        print("-"*40)
    
    if format_choice in ['txt', 'both']:
        print("\n📝 TEXT VERSION (first 1000 chars):")
        print(text_content[:1000])
        if len(text_content) > 1000:
            print(f"... (truncated, total {len(text_content)} chars)")
    
    print("="*80)

def upload_confluence_space(client: OpenWebUIClient, pickle_data: Dict, 
                          html_collection: str, text_collection: str,
                          inspect: bool = False, format_choice: str = 'both',
                          interactive: bool = False) -> int:
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
        
        # If inspection is enabled, show the document
        if inspect:
            inspect_document(page, space_key, space_name, format_choice)
            
            if interactive:
                # Interactive mode - ask user what to do
                print("\nOptions:")
                print("  u - Upload this document")
                print("  s - Skip this document")
                print("  a - Upload all remaining documents without inspection")
                print("  q - Quit")
                
                choice = input("\nEnter choice (u/s/a/q): ").strip().lower()
                
                if choice == 'q':
                    logger.info("User quit inspection mode")
                    return success_count
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
                # Upload path version as text
                path_title = f"{space_key}-{page_id}-PATH"
                path_success = client.upload_document(path_title, path_content, text_collection, is_html=False)
                upload_success = upload_success and path_success
            
            if upload_success:
                success_count += 1
                logger.info(f"Successfully uploaded page '{title}' ({page_id}) - format: {format_choice}")
            else:
                logger.warning(f"Failed to upload page '{title}' ({page_id})")
                
        except Exception as e:
            logger.error(f"Error processing page '{title}': {e}")
    
    logger.info(f"Successfully uploaded {success_count}/{len(sampled_pages)} pages from space {space_key}")
    return success_count


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
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # If interactive mode is requested without inspect, enable inspect automatically
    if args.interactive and not args.inspect:
        args.inspect = True
        logger.info("Enabling document inspection for interactive mode")
    
    # Show interactive menu if no specific mode is selected
    if not args.inspect and not any([args.clear_checkpoint]):
        print("\n" + "="*60)
        print("OPEN-WEBUI CONFLUENCE UPLOADER")
        print("="*60)
        print("\nUpload Modes:")
        print("  1. Standard upload (both HTML and text)")
        print("  2. Upload with document inspection (preview each doc before upload)")
        print("  3. Interactive upload (inspect and choose per document)")
        print("  4. Upload path information only")
        print("  5. Upload HTML format only")
        print("  6. Upload text format only")
        print("  7. Clear checkpoint and start fresh")
        print("  8. Test authentication only")
        print("  q. Quit")
        
        choice = input("\nSelect mode (1-8 or q): ").strip().lower()
        
        if choice == 'q':
            print("Exiting...")
            return 0
        elif choice == '1':
            args.inspect = False
            args.interactive = False
            args.format = 'both'
        elif choice == '2':
            args.inspect = True
            args.interactive = False
            print("\n📋 Document Inspection Mode Selected")
            print("   Each document will be displayed before upload.")
            print("   All documents will be uploaded automatically after preview.")
            # Ask for format
            print("\nSelect format to upload:")
            print("  1. Both HTML and text (default)")
            print("  2. HTML only")
            print("  3. Text only")
            print("  4. Path information only")
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
            print("\nSelect format to upload:")
            print("  1. Both HTML and text (default)")
            print("  2. HTML only")
            print("  3. Text only")
            print("  4. Path information only")
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
            print("\n✓ Checkpoint cleared - will start from beginning")
            print("Now select an upload mode:")
            print("  1. Standard upload (both HTML and text)")
            print("  2. Upload with document inspection")
            print("  3. Interactive upload (inspect and choose per document)")
            print("  4. Upload path information only")
            print("  5. Upload HTML format only")
            print("  6. Upload text format only")
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
            print("\n🔧 Testing authentication...")
            print(f"   Server: {args.openwebui_server}")
            print(f"   Username: {args.username or 'Not provided'}")
            print(f"   Password: {'***' if args.password else 'Not provided'}")
            
            test_client = OpenWebUIClient(
                args.openwebui_server,
                args.username,
                args.password
            )
            
            if test_client.authenticate():
                print("\n✅ Authentication test successful!")
                print("   You can now proceed with uploads.")
                
                # Also test listing collections
                print("\n📚 Testing collection access...")
                collections = test_client.list_knowledge_collections()
                if collections:
                    print(f"✅ Found {len(collections)} collection(s):")
                    for name in collections.keys():
                        print(f"   - {name}")
                else:
                    print("❌ No collections found or unable to list collections")
            else:
                print("\n❌ Authentication test failed!")
                print("\nTroubleshooting tips:")
                print("1. Check if you're using email (not username) for login")
                print("2. Verify the server URL (should be like http://localhost:8080)")
                print("3. Try logging into the web UI to confirm credentials work")
                print("4. Some Open-WebUI instances may have API authentication disabled")
            
            return 0
        else:
            print("Invalid choice, using standard upload mode")
            args.inspect = False
            args.interactive = False
            args.format = 'both'
        
        print(f"\nSelected: inspect={args.inspect}, interactive={args.interactive}, format={args.format}")
    
    # Clear checkpoint if requested
    if args.clear_checkpoint:
        clear_checkpoint()
        logger.info("Checkpoint cleared - starting from beginning")
    
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
    
    # Find existing knowledge collections
    logger.info("Finding existing knowledge collections...")
    print(f"\n🔍 Looking for knowledge collections...")
    html_collection_id = client.find_existing_collection(args.html_collection)
    text_collection_id = client.find_existing_collection(args.text_collection)
    
    if not html_collection_id or not text_collection_id:
        logger.error("Required knowledge collections not found!")
        print("\n❌ ERROR: Required knowledge collections not found!")
        print(f"   Please ensure these collections exist in Open-WebUI:")
        print(f"   - HTML collection: {args.html_collection}")
        print(f"   - Text collection: {args.text_collection}")
        return 1
    
    print(f"✅ Found required collections:")
    print(f"   - HTML: {args.html_collection}")
    print(f"   - Text: {args.text_collection}")
    
    # Load checkpoint to resume from last successful upload
    last_uploaded_space = load_checkpoint()
    resume_mode = last_uploaded_space is not None
    
    # Filter pickle files based on checkpoint
    files_to_process = []
    
    for pickle_file in pickle_files:
        pickle_data = load_confluence_pickle(pickle_file)
        if not pickle_data:
            logger.warning(f"Skipping invalid pickle file: {pickle_file.name}")
            continue
        
        space_key = pickle_data.get('space_key', 'UNKNOWN')
        space_name = pickle_data.get('name', 'Unknown Space')
        
        # Skip spaces until we reach the checkpoint
        if resume_mode:
            if space_key == last_uploaded_space:
                logger.info(f"Reached checkpoint space '{space_name}' ({space_key}) - resuming from next space")
                resume_mode = False
                continue
            else:
                logger.info(f"Skipping already processed space '{space_name}' ({space_key})")
                continue
        
        files_to_process.append(pickle_file)
    
    if not files_to_process:
        logger.info("No files to process")
        return 0
    
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
            
            success_count = upload_confluence_space(
                client, pickle_data, html_collection_id, text_collection_id,
                inspect=args.inspect, format_choice=args.format, interactive=args.interactive
            )
            
            total_success += success_count
            total_pages += page_count
            
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