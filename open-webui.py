"""
Open-WebUI Document Extractor
Extracts Office documents from Open-WebUI and pushes them to a DOCS collection.
Converts macro-enabled files to regular formats.
"""

import os
import sys
import json
import tempfile
import shutil
import configparser
from pathlib import Path
from typing import List, Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import python-docx and python-pptx for conversion
try:
    from docx import Document
    from pptx import Presentation
    OFFICE_LIBS_AVAILABLE = True
except ImportError:
    OFFICE_LIBS_AVAILABLE = False
    logger.warning("python-docx and python-pptx not installed. Macro-enabled files will be skipped.")
    logger.warning("Install with: pip install python-docx python-pptx")

class OpenWebUIClient:
    """Client for interacting with Open-WebUI API"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.auth_token = None
        
    def authenticate(self) -> bool:
        """Authenticate with Open-WebUI"""
        try:
            auth_url = f"{self.base_url}/api/v1/auths/signin"
            logger.debug(f"Authenticating at {auth_url} with user {self.username}")
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            })
            logger.debug(f"Auth response status: {response.status_code}")
            logger.debug(f"Auth response content: {response.text}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(f"Error decoding auth JSON: {e}")
                    return False
                self.auth_token = data.get("token")
                if self.auth_token:
                    self.session.headers.update({
                        "Authorization": f"Bearer {self.auth_token}"
                    })
                logger.info("Authentication successful")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False
    
    def get_files(self) -> List[Dict]:
        """Get all files from Open-WebUI"""
        try:
            files_url = f"{self.base_url}/api/v1/files"
            logger.debug(f"Requesting file list from {files_url}")
            response = self.session.get(files_url)
            logger.debug(f"Get files response status: {response.status_code}")
            logger.debug(f"Get files response content: {response.text}")
            
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as e:
                    logger.error(f"Error decoding files JSON: {e}")
                    return []
            else:
                logger.error(f"Failed to get files: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting files: {str(e)}")
            return []
    
    def download_file(self, file_id: str, file_name: str) -> Optional[bytes]:
        """Download a file by ID"""
        try:
            download_url = f"{self.base_url}/api/v1/files/{file_id}/content"
            logger.debug(f"Downloading {file_name} from {download_url}")
            response = self.session.get(download_url)
            logger.debug(f"Download response status for {file_name}: {response.status_code}")
            logger.debug(f"Download response headers for {file_name}: {response.headers}")
            
            if response.status_code == 200:
                return response.content
            else:
                logger.error(f"Failed to download {file_name}: {response.status_code}")
                logger.debug(f"Download response content for {file_name}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading {file_name}: {str(e)}")
            return None
    
    def create_collection(self, collection_name: str) -> Optional[str]:
        """Create a collection if it doesn't exist"""
        try:
            collections_url = f"{self.base_url}/api/v1/documents/collections"
            logger.debug(f"Checking existing collections at {collections_url}")
            response = self.session.get(collections_url)
            logger.debug(f"Get collections response status: {response.status_code}")
            logger.debug(f"Get collections response content: {response.text}")
            
            if response.status_code == 200:
                try:
                    collections = response.json()
                except ValueError as e:
                    logger.error(f"Error decoding collections JSON: {e}")
                    logger.debug(f"Raw collections response: {response.text}")
                    return None
                for collection in collections:
                    if collection.get("name") == collection_name:
                        logger.info(f"Collection '{collection_name}' already exists")
                        return collection.get("id")
            
            create_url = f"{self.base_url}/api/v1/documents/collections/create"
            logger.debug(f"Creating collection '{collection_name}' at {create_url}")
            response = self.session.post(create_url, json={
                "name": collection_name,
                "description": "Office documents collection"
            })
            logger.debug(f"Create collection response status: {response.status_code}")
            logger.debug(f"Create collection response content: {response.text}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(f"Error decoding create collection JSON: {e}")
                    logger.debug(f"Raw create collection response: {response.text}")
                    return None
                logger.info(f"Created collection '{collection_name}'")
                return data.get("id")
            else:
                logger.error(f"Failed to create collection: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error with collection: {str(e)}")
            return None
    
    def upload_to_collection(self, file_path: Path, collection_id: str) -> bool:
        """Upload a file to a collection"""
        try:
            upload_url = f"{self.base_url}/api/v1/documents/upload"
            logger.debug(f"Uploading {file_path.name} to collection {collection_id} at {upload_url}")
            
            with open(file_path, 'rb') as f:
                files = {'file': (file_path.name, f, 'application/octet-stream')}
                data = {'collection_id': collection_id}
                
                response = self.session.post(upload_url, files=files, data=data)
                logger.debug(f"Upload response status for {file_path.name}: {response.status_code}")
                logger.debug(f"Upload response content for {file_path.name}: {response.text}")
                
                if response.status_code == 200:
                    logger.info(f"Uploaded {file_path.name} to collection")
                    return True
                else:
                    logger.error(f"Failed to upload {file_path.name}: {response.status_code}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error uploading {file_path.name}: {str(e)}")
            return False

def is_office_file(filename: str) -> bool:
    """Check if file is an Office document"""
    office_extensions = {
        # PowerPoint
        '.ppt', '.pptx', '.pptm', '.ppsx', '.ppsm', '.potx', '.potm',
        # Word
        '.doc', '.docx', '.docm', '.dotx', '.dotm', '.rtf'
    }
    return any(filename.lower().endswith(ext) for ext in office_extensions)

def needs_conversion(filename: str) -> bool:
    """Check if file needs conversion (macro-enabled)"""
    macro_extensions = {'.pptm', '.ppsm', '.potm', '.docm', '.dotm'}
    return any(filename.lower().endswith(ext) for ext in macro_extensions)

def convert_macro_file(input_path: Path, output_path: Path) -> bool:
    """Convert macro-enabled file to regular format"""
    if not OFFICE_LIBS_AVAILABLE:
        logger.warning(f"Cannot convert {input_path.name} - Office libraries not installed")
        return False
    
    try:
        file_ext = input_path.suffix.lower()
        
        # PowerPoint conversions
        if file_ext in ['.pptm', '.ppsm', '.potm']:
            logger.debug(f"Converting PowerPoint macro file {input_path.name}")
            prs = Presentation(input_path)
            new_path = output_path.with_suffix('.pptx')
            prs.save(new_path)
            logger.info(f"Converted {input_path.name} to {new_path.name}")
            return True
            
        # Word conversions
        elif file_ext in ['.docm', '.dotm']:
            logger.debug(f"Converting Word macro file {input_path.name}")
            doc = Document(input_path)
            new_path = output_path.with_suffix('.docx')
            doc.save(new_path)
            logger.info(f"Converted {input_path.name} to {new_path.name}")
            return True
            
    except Exception as e:
        logger.error(f"Error converting {input_path.name}: {str(e)}")
        return False
    
    return False

def load_settings(settings_file: str = "settings.ini") -> Dict[str, str]:
    """Load settings from INI file"""
    config = configparser.ConfigParser()
    settings_path = Path(settings_file)
    
    if not settings_path.exists():
        logger.error(f"Settings file '{settings_file}' not found")
        logger.info("Creating example settings.ini file...")
        
        # Create example settings file
        example_config = configparser.ConfigParser()
        example_config['OpenWebUI'] = {
            'base_url': 'http://localhost:8080',
            'username': 'your_username',
            'password': 'your_password'
        }
        
        with open(settings_file, 'w') as f:
            example_config.write(f)
        
        logger.info(f"Created example '{settings_file}'. Please update it with your credentials.")
        sys.exit(1)
    
    config.read(settings_file)
    
    # Validate required settings
    required_settings = ['base_url', 'username', 'password']
    if 'OpenWebUI' not in config:
        logger.error("Missing [OpenWebUI] section in settings.ini")
        sys.exit(1)
    
    settings = {}
    for key in required_settings:
        if key not in config['OpenWebUI']:
            logger.error(f"Missing '{key}' in [OpenWebUI] section of settings.ini")
            sys.exit(1)
        settings[key] = config['OpenWebUI'][key]
    
    return settings

def main():
    """Main function"""
    # Load settings from INI file
    settings = load_settings()
    
    BASE_URL = settings['base_url']
    USERNAME = settings['username']
    PASSWORD = settings['password']
    COLLECTION_NAME = "DOCS"
    
    logger.info(f"Connecting to Open-WebUI at {BASE_URL}")
    
    # Initialize client
    client = OpenWebUIClient(BASE_URL, USERNAME, PASSWORD)
    
    # Authenticate
    if not client.authenticate():
        logger.error("Failed to authenticate")
        return 1
    
    # Create or get collection
    collection_id = client.create_collection(COLLECTION_NAME)
    if not collection_id:
        logger.error("Failed to create/get collection")
        return 1
    
    # Get all files
    files = client.get_files()
    if not files:
        logger.warning("No files found")
        return 0
    
    # Filter Office files
    office_files = [f for f in files if is_office_file(f.get('name', ''))]
    logger.info(f"Found {len(office_files)} Office documents")
    
    # Process files
    processed = 0
    skipped = 0
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        for file_info in office_files:
            file_id = file_info.get('id')
            file_name = file_info.get('name')
            
            if not file_id or not file_name:
                continue
            
            logger.info(f"Processing {file_name}")
            
            # Download file
            content = client.download_file(file_id, file_name)
            if not content:
                skipped += 1
                continue
            
            # Save to temp directory
            temp_file = temp_path / file_name
            temp_file.write_bytes(content)
            
            # Check if conversion needed
            if needs_conversion(file_name):
                if OFFICE_LIBS_AVAILABLE:
                    # Convert macro-enabled file
                    converted_file = temp_path / f"converted_{file_name}"
                    if convert_macro_file(temp_file, converted_file):
                        # Find the actual converted file (extension might change)
                        converted_files = list(temp_path.glob(f"converted_{Path(file_name).stem}.*"))
                        if converted_files:
                            temp_file = converted_files[0]
                    else:
                        logger.warning(f"Skipping {file_name} - conversion failed")
                        skipped += 1
                        continue
                else:
                    logger.warning(f"Skipping {file_name} - macro-enabled file")
                    skipped += 1
                    continue
            
            # Upload to collection
            if client.upload_to_collection(temp_file, collection_id):
                processed += 1
            else:
                skipped += 1
    
    logger.info(f"Processing complete: {processed} files uploaded, {skipped} files skipped")
    return 0

if __name__ == "__main__":
    sys.exit(main())
