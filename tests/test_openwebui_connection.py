#!/usr/bin/env python3
"""
Test script to verify Open-WebUI connectivity and API endpoints
Run this from outside WSL to test connectivity to your Open-WebUI instance
"""

import requests
import json
import tempfile
import os
import configparser
from typing import Optional

class OpenWebUITester:
    def __init__(self, base_url: str, username: str = None, password: str = None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.auth_token = None
    
    def authenticate(self) -> bool:
        """Authenticate with Open-WebUI if credentials provided"""
        if not self.username or not self.password:
            print("  No credentials provided, skipping authentication")
            return True
            
        auth_url = f"{self.base_url}/api/v1/auths/signin"
        try:
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            })
            print(f"✓ Authentication test: {response.status_code}")
            
            if response.status_code != 200:
                print(f"  Authentication failed: {response.text}")
                return False

            data = response.json()
            self.auth_token = data.get("token")
            if not self.auth_token:
                print("  Authentication succeeded but no token returned")
                return False

            self.session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
            print("  Authentication successful")
            return True
            
        except Exception as e:
            print(f"✗ Authentication failed: {e}")
            return False
        
    def test_connection(self) -> bool:
        """Test basic connectivity to Open-WebUI"""
        try:
            response = self.session.get(f"{self.base_url}/api/config")
            print(f"✓ Connection test: {response.status_code}")
            return response.status_code == 200
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    def test_file_upload(self) -> Optional[str]:
        """Test file upload to /api/v1/files/"""
        upload_url = f"{self.base_url}/api/v1/files/"
        test_content = "This is a test file for Open-WebUI upload testing."
        
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False) as tmp:
                tmp.write(test_content)
                tmp_path = tmp.name
            
            # Upload file
            with open(tmp_path, 'rb') as f:
                files = {'file': ('test_upload.txt', f, 'text/plain')}
                response = self.session.post(upload_url, files=files)
            
            # Clean up
            os.unlink(tmp_path)
            
            print(f"✓ File upload test: {response.status_code}")
            print(f"  Response: {response.text[:200]}...")
            
            if response.status_code == 200:
                data = response.json()
                file_id = data.get('id')
                print(f"  File ID: {file_id}")
                return file_id
            else:
                print(f"  Upload failed: {response.text}")
                return None
                
        except Exception as e:
            print(f"✗ File upload failed: {e}")
            return None
    
    def test_knowledge_list(self) -> bool:
        """Test listing knowledge collections"""
        knowledge_url = f"{self.base_url}/api/v1/knowledge/"
        
        try:
            response = self.session.get(knowledge_url)
            print(f"✓ Knowledge list test: {response.status_code}")
            
            if response.status_code == 200:
                collections = response.json()
                print(f"  Found {len(collections)} knowledge collections:")
                for collection in collections:
                    print(f"    - {collection.get('name', 'Unnamed')} (ID: {collection.get('id', 'No ID')})")
                return True
            else:
                print(f"  Failed to list collections: {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ Knowledge list failed: {e}")
            return False
    
    def find_existing_collection(self, name: str) -> Optional[str]:
        """Find existing collection by name"""
        collections_url = f"{self.base_url}/api/v1/knowledge/"
        try:
            response = self.session.get(collections_url)
            if response.status_code == 200:
                collections = response.json()
                for collection in collections:
                    if collection.get('name') == name:
                        collection_id = collection.get('id')
                        print(f"✓ Found collection '{name}' (ID: {collection_id})")
                        return collection_id
                print(f"✗ Collection '{name}' not found")
                return None
            else:
                print(f"✗ Failed to search for collection '{name}': HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"✗ Exception finding collection '{name}': {e}")
            return None
    
    def test_knowledge_add_file(self, collection_id: str, file_id: str) -> bool:
        """Test adding file to knowledge collection"""
        add_url = f"{self.base_url}/api/v1/knowledge/{collection_id}/file/add"
        payload = {"file_id": file_id}
        
        try:
            response = self.session.post(add_url, json=payload)
            print(f"✓ Knowledge add file test: {response.status_code}")
            print(f"  Response: {response.text[:200]}...")
            
            return response.status_code in [200, 201]
            
        except Exception as e:
            print(f"✗ Knowledge add file failed: {e}")
            return False

def load_settings():
    """Load settings from settings.ini"""
    config = configparser.ConfigParser()
    
    # Try to load settings.ini
    if os.path.exists('settings.ini'):
        config.read('settings.ini')
        print("✓ Loaded settings from settings.ini")
    else:
        print("✗ settings.ini not found, using defaults")
        return None, None, None
    
    # Get Open-WebUI settings
    if 'OpenWebUI' in config:
        base_url = config.get('OpenWebUI', 'base_url', fallback='http://localhost:8080')
        username = config.get('OpenWebUI', 'username', fallback=None)
        password = config.get('OpenWebUI', 'password', fallback=None)
        
        # Don't use placeholder values
        if username == 'your_username':
            username = None
        if password == 'your_password':
            password = None
            
        return base_url, username, password
    else:
        print("✗ No [OpenWebUI] section found in settings.ini")
        return None, None, None

def main():
    """Run all connectivity tests"""
    print("Open-WebUI Connectivity Test")
    print("=" * 40)
    
    # Load settings
    settings_url, username, password = load_settings()
    
    # Test URLs - prioritize settings.ini, then common defaults
    test_urls = []
    if settings_url:
        test_urls.append(settings_url)
    
    # Add common URLs if not already included
    common_urls = [
        "http://localhost:8080",
        "http://127.0.0.1:8080", 
        "http://192.168.1.63:8080",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ]
    for url in common_urls:
        if url not in test_urls:
            test_urls.append(url)
    
    working_url = None
    working_tester = None
    
    # Find working URL
    for url in test_urls:
        print(f"\nTesting: {url}")
        tester = OpenWebUITester(url, username, password)
        if tester.test_connection():
            if tester.authenticate():
                working_url = url
                working_tester = tester
                break
    
    if not working_url:
        print("\n✗ No working Open-WebUI URL found!")
        print("Please check that Open-WebUI is running and accessible.")
        return 1
    
    print(f"\n✓ Using working URL: {working_url}")
    if username:
        print(f"✓ Using credentials: {username}")
    
    # Run full test suite
    print("\nRunning API tests...")
    print("-" * 20)
    
    # Test knowledge collections
    working_tester.test_knowledge_list()
    
    # Test file upload
    file_id = working_tester.test_file_upload()
    
    # Find existing CONF-HTML collection
    html_collection_id = working_tester.find_existing_collection("CONF-HTML")
    
    # Test adding file to CONF-HTML collection
    if file_id and html_collection_id:
        print(f"\nTesting upload to CONF-HTML collection...")
        working_tester.test_knowledge_add_file(html_collection_id, file_id)
    
    # Find existing CONF-TXT collection  
    txt_collection_id = working_tester.find_existing_collection("CONF-TXT")
    
    # Test adding file to CONF-TXT collection
    if file_id and txt_collection_id:
        print(f"\nTesting upload to CONF-TXT collection...")
        working_tester.test_knowledge_add_file(txt_collection_id, file_id)
    
    print("\n" + "=" * 40)
    print("Test complete! Check Open-WebUI interface to see uploaded content.")
    return 0

if __name__ == "__main__":
    main()