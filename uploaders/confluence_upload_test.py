#!/usr/bin/env python3
"""
Simple script to test uploading Confluence content to Open-WebUI CONF-HTML and CONF-TXT collections
"""
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import sys
import os
import configparser
import pickle
import tempfile
import requests
import logging
from pathlib import Path
from utils.html_cleaner import clean_confluence_html

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_settings():
    """Load Open-WebUI settings from settings.ini"""
    config = configparser.ConfigParser()
    
    if not os.path.exists('settings.ini'):
        print("❌ settings.ini not found")
        return None, None, None
    
    config.read('settings.ini')
    
    if 'OpenWebUI' not in config:
        print("❌ No [OpenWebUI] section found in settings.ini")
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

def authenticate(session, base_url, username, password):
    """Authenticate with Open-WebUI"""
    if not username or not password:
        print("ℹ️  No credentials provided, skipping authentication")
        return True
        
    auth_url = f"{base_url}/api/v1/auths/signin"
    try:
        response = session.post(auth_url, json={
            "email": username,
            "password": password
        })
        
        if response.status_code != 200:
            print(f"❌ Authentication failed: HTTP {response.status_code}")
            return False

        data = response.json()
        auth_token = data.get("token")
        if not auth_token:
            print("❌ Authentication succeeded but no token returned")
            return False

        session.headers.update({"Authorization": f"Bearer {auth_token}"})
        print("✅ Authentication successful")
        return True
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return False

def find_collection(session, base_url, name):
    """Find existing collection by name"""
    collections_url = f"{base_url}/api/v1/knowledge/"
    try:
        response = session.get(collections_url)
        if response.status_code == 200:
            collections = response.json()
            for collection in collections:
                if collection.get('name') == name:
                    collection_id = collection.get('id')
                    print(f"✅ Found collection '{name}' (ID: {collection_id})")
                    return collection_id
            print(f"❌ Collection '{name}' not found")
            return None
        else:
            print(f"❌ Failed to search for collection '{name}': HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Exception finding collection '{name}': {e}")
        return None

def upload_content(session, base_url, title, content, collection_id, is_html=False):
    """Upload content to Open-WebUI"""
    upload_url = f"{base_url}/api/v1/files/"
    
    # Determine file extension and content type
    if is_html:
        file_extension = ".html"
        content_type = "text/html"
        filename = f"{title}.html"
    else:
        file_extension = ".txt"
        content_type = "text/plain"
        filename = f"{title}.txt"
    
    print(f"📤 Uploading as filename='{filename}', content_type='{content_type}', is_html={is_html}")
    
    try:
        # Create a temporary file with the content
        with tempfile.NamedTemporaryFile(mode='w', suffix=file_extension, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        
        # Upload the file
        with open(tmp_path, 'rb') as f:
            files = {'file': (filename, f, content_type)}
            response = session.post(upload_url, files=files)
        
        # Remove the temporary file
        os.unlink(tmp_path)
        
        if response.status_code != 200:
            print(f"❌ Failed to upload '{title}': HTTP {response.status_code}")
            return False

        data = response.json()
        new_file_id = data.get("id")
        if not new_file_id:
            print(f"❌ No file ID returned after uploading '{title}'")
            return False

        # Add file to knowledge collection
        knowledge_url = f"{base_url}/api/v1/knowledge/{collection_id}/file/add"
        knowledge_payload = {"file_id": new_file_id}
        
        response = session.post(knowledge_url, json=knowledge_payload)
        
        if response.status_code not in [200, 201]:
            print(f"❌ Failed to add file '{title}' to collection: HTTP {response.status_code}")
            return False

        print(f"✅ Successfully uploaded '{title}' (file_id={new_file_id})")
        return True
        
    except Exception as e:
        print(f"❌ Exception uploading '{title}': {e}")
        return False

def load_confluence_pickle(pickle_path):
    """Load a Confluence pickle file"""
    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        if not isinstance(data, dict) or 'sampled_pages' not in data:
            print(f"❌ '{pickle_path.name}' doesn't appear to be a Confluence space pickle")
            return None
            
        return data
    except Exception as e:
        print(f"❌ Error loading pickle file '{pickle_path}': {e}")
        return None

def process_confluence_page(page, space_key, space_name):
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

def main():
    print("🚀 Confluence Upload Test to CONF-HTML and CONF-TXT")
    print("=" * 50)
    
    # Load settings
    base_url, username, password = load_settings()
    if not base_url:
        return 1
    
    print(f"🌐 Server: {base_url}")
    if username:
        print(f"👤 Username: {username}")
    
    # Initialize session and authenticate
    session = requests.Session()
    if not authenticate(session, base_url, username, password):
        return 1
    
    # Find a pickle file to test with
    pickle_files = list(Path('temp').glob('*.pkl'))
    if not pickle_files:
        print("❌ No pickle files found in temp/ directory")
        return 1
    
    test_pickle = pickle_files[0]
    print(f"📁 Using test pickle: {test_pickle.name}")
    
    # Load the pickle
    pickle_data = load_confluence_pickle(test_pickle)
    if not pickle_data:
        print(f"❌ Failed to load pickle: {test_pickle}")
        return 1
    
    space_key = pickle_data.get('space_key', 'UNKNOWN')
    space_name = pickle_data.get('name', 'Unknown Space')
    pages = pickle_data.get('sampled_pages', [])
    
    if not pages:
        print(f"❌ No pages found in pickle")
        return 1
    
    print(f"📄 Loaded space: {space_name} ({space_key}) with {len(pages)} pages")
    
    # Find existing knowledge collections
    print("\n🔍 Finding existing knowledge collections...")
    html_collection_id = find_collection(session, base_url, "CONF-HTML")
    text_collection_id = find_collection(session, base_url, "CONF-TXT")
    
    if not html_collection_id:
        print("❌ CONF-HTML collection not found!")
        return 1
    if not text_collection_id:
        print("❌ CONF-TXT collection not found!")
        return 1
    
    # Test with first page
    test_page = pages[0]
    page_id = test_page.get('id', 'unknown')
    title = test_page.get('title', 'Untitled')
    
    print(f"\n📋 Testing with page: {title} (ID: {page_id})")
    
    # Process the page to get HTML and text versions
    html_content, text_content = process_confluence_page(test_page, space_key, space_name)
    
    print(f"📊 HTML content length: {len(html_content)} characters")
    print(f"📊 Text content length: {len(text_content)} characters")
    
    # Show content samples
    print(f"\n📝 HTML sample (first 200 chars):")
    print(f"'{html_content[:200]}...'")
    print(f"\n📝 Text sample (first 200 chars):")
    print(f"'{text_content[:200]}...'")
    
    # Upload HTML version
    html_title = f"{space_key}-{page_id}-HTML-TEST"
    print(f"\n📤 Uploading HTML version as '{html_title}'...")
    html_success = upload_content(session, base_url, html_title, html_content, html_collection_id, is_html=True)
    
    # Upload text version
    text_title = f"{space_key}-{page_id}-TEXT-TEST"
    print(f"📤 Uploading text version as '{text_title}'...")
    text_success = upload_content(session, base_url, text_title, text_content, text_collection_id, is_html=False)
    
    # Results
    print(f"\n📊 Results:")
    print(f"  HTML upload: {'✅ Success' if html_success else '❌ Failed'}")
    print(f"  Text upload: {'✅ Success' if text_success else '❌ Failed'}")
    
    if html_success and text_success:
        print(f"\n🎉 SUCCESS! Check Open-WebUI collections:")
        print(f"  📁 CONF-HTML collection for '{html_title}.html'")
        print(f"  📁 CONF-TXT collection for '{text_title}.txt'")
        return 0
    else:
        print(f"\n❌ Some uploads failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())