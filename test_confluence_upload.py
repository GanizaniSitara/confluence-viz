#!/usr/bin/env python3
"""
Test script to verify Confluence content upload to Open-WebUI CONF-HTML and CONF-TXT collections
Tests the actual workflow with real Confluence content
"""

import sys
import os
import configparser
import pickle
from pathlib import Path

# We'll copy the necessary functions directly to avoid import issues
import tempfile
import requests
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
            from requests.auth import HTTPBasicAuth
            self.session.auth = HTTPBasicAuth(username, password)
        self.auth_token = None

    def authenticate(self) -> bool:
        """Authenticate with Open-WebUI if credentials provided"""
        if not self.username or not self.password:
            logger.info("No credentials provided, skipping authentication")
            return True
            
        auth_url = f"{self.base_url}/api/v1/auths/signin"
        try:
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            })
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
        """Upload a document to Open-WebUI knowledge base using the correct two-step process"""
        import tempfile
        import os
        
        # Step 1: Upload file to /api/v1/files/
        upload_url = f"{self.base_url}/api/v1/files/"
        
        logger.debug(f"Uploading content as '{title}' to {upload_url}")
        
        try:
            # Create a temporary file with the content
            with tempfile.NamedTemporaryFile(mode='w', suffix=f".{title.split('.')[-1]}", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            
            # Upload the file
            with open(tmp_path, 'rb') as f:
                files = {'file': (f"{title}.txt", f, 'text/plain')}
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

    def list_knowledge_collections(self):
        """List all knowledge collections and return as dict {name: id}"""
        collections_url = f"{self.base_url}/api/v1/knowledge/"
        try:
            response = self.session.get(collections_url)
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
                return {}
        except Exception as e:
            logger.error(f"Exception listing collections: {e}")
            return {}

    def find_existing_collection(self, name: str):
        """Find existing collection ID by name"""
        collections = self.list_knowledge_collections()
        if name in collections:
            collection_id = collections[name]
            logger.info(f"Found existing collection '{name}' (ID: {collection_id})")
            return collection_id
        else:
            logger.error(f"Collection '{name}' not found! Available collections: {list(collections.keys())}")
            return None

def load_confluence_pickle(pickle_path: Path):
    """Load a Confluence pickle file and return its data"""
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

def process_confluence_page(page, space_key: str, space_name: str):
    """Process a single Confluence page and return HTML and text versions"""
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

def load_settings():
    """Load Open-WebUI settings from settings.ini"""
    config = configparser.ConfigParser()
    
    if not os.path.exists('settings.ini'):
        print("✗ settings.ini not found")
        return None, None, None
    
    config.read('settings.ini')
    
    if 'OpenWebUI' not in config:
        print("✗ No [OpenWebUI] section found in settings.ini")
        return None, None, None
    
    openwebui = config['OpenWebUI']
    base_url = openwebui.get('base_url', 'http://localhost:8080')
    username = openwebui.get('username', None)
    password = openwebui.get('password', None)
    
    # Don't use placeholder values
    if username == 'your_username':
        username = None
    if password == 'your_password':
        password = None
    
    return base_url, username, password

def test_confluence_upload():
    """Test uploading real Confluence content to CONF-HTML and CONF-TXT collections"""
    
    print("Confluence Upload Test to CONF-HTML and CONF-TXT")
    print("=" * 50)
    
    # Load settings
    base_url, username, password = load_settings()
    if not base_url:
        return 1
    
    print(f"✓ Server: {base_url}")
    if username:
        print(f"✓ Username: {username}")
    
    # Initialize client
    client = OpenWebUIClient(base_url, username, password)
    
    # Test authentication
    if not client.authenticate():
        print("✗ Authentication failed")
        return 1
    
    # Find a pickle file to test with
    pickle_files = list(Path('temp').glob('*.pkl'))
    if not pickle_files:
        print("✗ No pickle files found in temp/ directory")
        return 1
    
    test_pickle = pickle_files[0]
    print(f"✓ Using test pickle: {test_pickle.name}")
    
    # Load the pickle
    pickle_data = load_confluence_pickle(test_pickle)
    if not pickle_data:
        print(f"✗ Failed to load pickle: {test_pickle}")
        return 1
    
    space_key = pickle_data.get('space_key', 'UNKNOWN')
    space_name = pickle_data.get('name', 'Unknown Space')
    pages = pickle_data.get('sampled_pages', [])
    
    if not pages:
        print(f"✗ No pages found in pickle")
        return 1
    
    print(f"✓ Loaded space: {space_name} ({space_key}) with {len(pages)} pages")
    
    # Find existing knowledge collections
    print("\nFinding existing knowledge collections...")
    html_collection_id = client.find_existing_collection("CONF-HTML")
    text_collection_id = client.find_existing_collection("CONF-TXT")
    
    if not html_collection_id:
        print("✗ CONF-HTML collection not found!")
        return 1
    if not text_collection_id:
        print("✗ CONF-TXT collection not found!")
        return 1
    
    # Test with first page
    test_page = pages[0]
    page_id = test_page.get('id', 'unknown')
    title = test_page.get('title', 'Untitled')
    
    print(f"\nTesting with page: {title} (ID: {page_id})")
    
    # Process the page to get HTML and text versions
    html_content, text_content = process_confluence_page(test_page, space_key, space_name)
    
    print(f"✓ HTML content length: {len(html_content)} characters")
    print(f"✓ Text content length: {len(text_content)} characters")
    
    # Show content samples
    print(f"\nHTML sample (first 200 chars):")
    print(f"'{html_content[:200]}...'")
    print(f"\nText sample (first 200 chars):")
    print(f"'{text_content[:200]}...'")
    
    # Upload HTML version
    html_title = f"{space_key}-{page_id}-HTML-TEST"
    print(f"\nUploading HTML version as '{html_title}'...")
    html_success = client.upload_document(html_title, html_content, html_collection_id)
    
    # Upload text version
    text_title = f"{space_key}-{page_id}-TEXT-TEST"
    print(f"Uploading text version as '{text_title}'...")
    text_success = client.upload_document(text_title, text_content, text_collection_id)
    
    # Results
    print(f"\nResults:")
    print(f"  HTML upload: {'✓ Success' if html_success else '✗ Failed'}")
    print(f"  Text upload: {'✓ Success' if text_success else '✗ Failed'}")
    
    if html_success and text_success:
        print(f"\n✓ SUCCESS! Check Open-WebUI collections:")
        print(f"  - CONF-HTML collection for '{html_title}'")
        print(f"  - CONF-TXT collection for '{text_title}'")
        return 0
    else:
        print(f"\n✗ Some uploads failed")
        return 1

if __name__ == "__main__":
    try:
        exit_code = test_confluence_upload()
        sys.exit(exit_code)
    except Exception as e:
        print(f"✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)