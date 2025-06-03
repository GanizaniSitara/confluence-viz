"""
Open-WebUI Document Extractor (Revised for /api/v1/knowledge/*)
Extracts Office documents from Open-WebUI and pushes them into a "DOCS" knowledge base.
Converts macro-enabled files to standard formats when needed.
"""

import os
import sys
import tempfile
import configparser
from pathlib import Path
from typing import List, Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
import logging

# Configure logging at DEBUG level for detailed troubleshooting
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import python-docx and python-pptx for macro conversion
try:
    from docx import Document
    from pptx import Presentation
    OFFICE_LIBS_AVAILABLE = True
except ImportError:
    OFFICE_LIBS_AVAILABLE = False
    logger.warning("python-docx and python-pptx not installed. Macro-enabled files will be skipped.")
    logger.warning("Install with: pip install python-docx python-pptx")

class OpenWebUIClient:
    """Client for interacting with Open-WebUI API (v1 knowledge endpoints)"""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.auth_token = None

    def authenticate(self) -> bool:
        """Authenticate with Open-WebUI (POST /api/v1/auths/signin)"""
        try:
            auth_url = f"{self.base_url}/api/v1/auths/signin"
            logger.debug(f"Authenticating at {auth_url} with user '{self.username}'")
            response = self.session.post(auth_url, json={
                "email": self.username,
                "password": self.password
            })
            logger.debug(f"Auth response status: {response.status_code}")
            logger.debug(f"Auth response text: {response.text}")

            if response.status_code == 200:
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(f"Failed to parse JSON during auth: {e}")
                    return False

                self.auth_token = data.get("token")
                if self.auth_token:
                    self.session.headers.update({
                        "Authorization": f"Bearer {self.auth_token}"
                    })
                logger.info("Authentication successful")
                return True
            else:
                logger.error(f"Authentication failed with status {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Exception during authentication: {e}")
            return False

    def get_files(self) -> List[Dict]:
        """Get all files from Open-WebUI (GET /api/v1/files/)"""
        try:
            files_url = f"{self.base_url}/api/v1/files/"
            logger.debug(f"Requesting file list from {files_url}")
            response = self.session.get(files_url)
            logger.debug(f"Get files response status: {response.status_code}")
            logger.debug(f"Get files response text: {response.text}")

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as e:
                    logger.error(f"Error parsing JSON from get_files: {e}")
                    return []
            else:
                logger.error(f"Failed to list files: HTTP {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Exception while listing files: {e}")
            return []

    def download_file(self, file_id: str, file_name: str) -> Optional[bytes]:
        """Download a file by ID (GET /api/v1/files/{file_id}/content)"""
        try:
            download_url = f"{self.base_url}/api/v1/files/{file_id}/content"
            logger.debug(f"Downloading '{file_name}' from {download_url}")
            response = self.session.get(download_url)
            logger.debug(f"Download response status for '{file_name}': {response.status_code}")
            logger.debug(f"Download response headers for '{file_name}': {response.headers}")

            if response.status_code == 200:
                return response.content
            else:
                logger.error(f"Failed to download '{file_name}': HTTP {response.status_code}")
                logger.debug(f"Download response text for '{file_name}': {response.text}")
                return None

        except Exception as e:
            logger.error(f"Exception downloading '{file_name}': {e}")
            return None

    def upload_new_file(self, file_path: Path) -> Optional[str]:
        """
        Upload a new file to Open-WebUI (POST /api/v1/files/).
        Returns the new file_id on success, or None on failure.
        """
        try:
            upload_url = f"{self.base_url}/api/v1/files/"
            logger.debug(f"Uploading new file '{file_path.name}' to {upload_url}")

            with open(file_path, 'rb') as f:
                files = {'file': (file_path.name, f, 'application/octet-stream')}
                response = self.session.post(upload_url, files=files)

            logger.debug(f"Upload new file response status: {response.status_code}")
            logger.debug(f"Upload new file response text: {response.text}")

            if response.status_code == 200:
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(f"Failed to parse JSON after uploading '{file_path.name}': {e}")
                    return None

                new_file_id = data.get("id")
                if new_file_id:
                    logger.info(f"Uploaded '{file_path.name}' as new file ID '{new_file_id}'")
                return new_file_id
            else:
                logger.error(f"Failed to upload '{file_path.name}': HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Exception uploading '{file_path.name}': {e}")
            return None

    def list_knowledge_bases(self) -> List[Dict]:
        """
        List all existing knowledge bases (GET /api/v1/knowledge/).
        Returns a list of dicts, each containing 'id', 'name', 'description', etc.
        :contentReference[oaicite:5]{index=5}
        """
        try:
            kb_url = f"{self.base_url}/api/v1/knowledge/"
            logger.debug(f"Listing knowledge bases from {kb_url}")
            response = self.session.get(kb_url)
            logger.debug(f"List knowledge bases status: {response.status_code}")
            logger.debug(f"List knowledge bases response: {response.text}")

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as e:
                    logger.error(f"Failed to parse JSON from list_knowledge_bases: {e}")
                    return []
            else:
                logger.error(f"Failed to list knowledge bases: HTTP {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Exception while listing knowledge bases: {e}")
            return []

    def create_knowledge_base(self, name: str, description: str = "") -> Optional[str]:
        """
        Create a new knowledge base (POST /api/v1/knowledge/create).
        Returns the new knowledge_id on success, or None on failure.
        :contentReference[oaicite:6]{index=6}
        """
        try:
            create_url = f"{self.base_url}/api/v1/knowledge/create"
            payload = {
                "name": name,
                "description": description,
                "data": {},
                "access_control": {}
            }
            logger.debug(f"Creating knowledge base '{name}' at {create_url} with payload: {payload}")
            response = self.session.post(create_url, json=payload)
            logger.debug(f"Create knowledge base status: {response.status_code}")
            logger.debug(f"Create knowledge base response: {response.text}")

            if response.status_code == 200:
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(f"Failed to parse JSON from create_knowledge_base: {e}")
                    return None

                new_kb_id = data.get("id")
                if new_kb_id:
                    logger.info(f"Created knowledge base '{name}' with ID '{new_kb_id}'")
                return new_kb_id
            else:
                logger.error(f"Failed to create knowledge base '{name}': HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Exception while creating knowledge base '{name}': {e}")
            return None

    def get_or_create_knowledge_base(self, name: str, description: str = "") -> Optional[str]:
        """
        Return the ID of an existing knowledge base named `name`, or create it if it does not exist.
        """
        logger.debug(f"Looking for existing knowledge base named '{name}'")
        existing = self.list_knowledge_bases()
        for kb in existing:
            if kb.get("name") == name:
                kb_id = kb.get("id")
                logger.info(f"Knowledge base '{name}' already exists with ID '{kb_id}'")
                return kb_id

        logger.info(f"Knowledge base '{name}' not found; creating new one.")
        return self.create_knowledge_base(name=name, description=description)

    def add_file_to_knowledge(self, knowledge_id: str, file_id: str) -> bool:
        """
        Add an existing file (by file_id) to a knowledge base (POST /api/v1/knowledge/{knowledge_id}/file/add).
        :contentReference[oaicite:7]{index=7}
        """
        try:
            add_url = f"{self.base_url}/api/v1/knowledge/{knowledge_id}/file/add"
            payload = {"file_id": file_id}
            logger.debug(f"Adding file_id '{file_id}' to knowledge '{knowledge_id}' at {add_url}")
            response = self.session.post(add_url, json=payload)
            logger.debug(f"Add file to knowledge status: {response.status_code}")
            logger.debug(f"Add file to knowledge response: {response.text}")

            if response.status_code == 200:
                logger.info(f"Successfully added file '{file_id}' to knowledge '{knowledge_id}'")
                return True
            else:
                logger.error(f"Failed to add file '{file_id}' to knowledge '{knowledge_id}': HTTP {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Exception while adding file '{file_id}' to knowledge '{knowledge_id}': {e}")
            return False

def is_office_file(filename: str) -> bool:
    """Check if a filename corresponds to an Office document (Word/PowerPoint/RTF)"""
    office_extensions = {
        # PowerPoint
        '.ppt', '.pptx', '.pptm', '.ppsx', '.ppsm', '.potx', '.potm',
        # Word
        '.doc', '.docx', '.docm', '.dotx', '.dotm', '.rtf'
    }
    return any(filename.lower().endswith(ext) for ext in office_extensions)

def needs_conversion(filename: str) -> bool:
    """Check if a file is macro-enabled (requires conversion)"""
    macro_extensions = {'.pptm', '.ppsm', '.potm', '.docm', '.dotm'}
    return any(filename.lower().endswith(ext) for ext in macro_extensions)

def convert_macro_file(input_path: Path, output_path: Path) -> bool:
    """
    Convert a macro-enabled Office file (Word .docm/.dotm or PowerPoint .pptm/.ppsm/.potm)
    into its non-macro equivalent (.docx or .pptx) using python-docx/python-pptx.
    """
    if not OFFICE_LIBS_AVAILABLE:
        logger.warning(f"Cannot convert '{input_path.name}' – Office libs not installed.")
        return False

    try:
        file_ext = input_path.suffix.lower()

        # PowerPoint conversions
        if file_ext in ['.pptm', '.ppsm', '.potm']:
            logger.debug(f"Converting PowerPoint macro file '{input_path.name}'")
            prs = Presentation(input_path)
            new_path = output_path.with_suffix('.pptx')
            prs.save(new_path)
            logger.info(f"Converted '{input_path.name}' to '{new_path.name}'")
            return True

        # Word conversions
        elif file_ext in ['.docm', '.dotm']:
            logger.debug(f"Converting Word macro file '{input_path.name}'")
            doc = Document(input_path)
            new_path = output_path.with_suffix('.docx')
            doc.save(new_path)
            logger.info(f"Converted '{input_path.name}' to '{new_path.name}'")
            return True

    except Exception as e:
        logger.error(f"Error converting '{input_path.name}': {e}")
        return False

    return False

def load_settings(settings_file: str = "settings.ini") -> Dict[str, str]:
    """
    Load settings from INI file. If it does not exist, create an example and exit.
    """
    config = configparser.ConfigParser()
    settings_path = Path(settings_file)

    if not settings_path.exists():
        logger.error(f"Settings file '{settings_file}' not found.")
        logger.info("Creating example settings.ini with placeholders...")
        example_config = configparser.ConfigParser()
        example_config['OpenWebUI'] = {
            'base_url': 'http://localhost:8080',
            'username': 'your_username',
            'password': 'your_password'
        }
        with open(settings_file, 'w') as f:
            example_config.write(f)
        logger.info(f"Example '{settings_file}' created. Please update with your credentials and rerun.")
        sys.exit(1)

    config.read(settings_file)

    if 'OpenWebUI' not in config:
        logger.error("Missing [OpenWebUI] section in settings.ini")
        sys.exit(1)

    required_keys = ['base_url', 'username', 'password']
    settings = {}
    for key in required_keys:
        if key not in config['OpenWebUI']:
            logger.error(f"Missing '{key}' in [OpenWebUI] of settings.ini")
            sys.exit(1)
        settings[key] = config['OpenWebUI'][key]

    return settings

def main() -> int:
    """
    Main entry point:
      1. Authenticate
      2. Create/get "DOCS" knowledge base
      3. List all files in Open-WebUI
      4. For each Office file:
         - If macro-enabled, download → convert → upload → add to knowledge.
         - If not macro-enabled, simply add existing file_id to knowledge.
    """
    settings = load_settings()
    BASE_URL = settings['base_url']
    USERNAME = settings['username']
    PASSWORD = settings['password']
    KNOWLEDGE_NAME = "DOCS"

    logger.info(f"Connecting to Open-WebUI at {BASE_URL}")
    client = OpenWebUIClient(BASE_URL, USERNAME, PASSWORD)

    # 1. Authenticate
    if not client.authenticate():
        logger.error("Authentication failed. Exiting.")
        return 1

    # 2. Create or retrieve the "DOCS" knowledge base
    knowledge_id = client.get_or_create_knowledge_base(name=KNOWLEDGE_NAME,
                                                       description="Office documents collection")
    if not knowledge_id:
        logger.error("Failed to create or retrieve the knowledge base. Exiting.")
        return 1

    # 3. List all files in Open-WebUI
    files = client.get_files()
    if not files:
        logger.warning("No files found in Open-WebUI. Exiting.")
        return 0

    # 4. Filter for Office documents
    office_files = [f for f in files if is_office_file(f.get('name', ''))]
    logger.info(f"Found {len(office_files)} Office files to process.")

    processed = 0
    skipped = 0

    # Use a temporary directory for any conversions
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)

        for file_info in office_files:
            file_id = file_info.get('id')
            file_name = file_info.get('name')

            if not file_id or not file_name:
                continue

            logger.info(f"Processing file '{file_name}' (ID: {file_id})")

            # 4a. If the file is macro-enabled, download → convert → upload → add
            if needs_conversion(file_name):
                if OFFICE_LIBS_AVAILABLE:
                    # 4a.i. Download the macro-enabled file
                    content = client.download_file(file_id=file_id, file_name=file_name)
                    if not content:
                        skipped += 1
                        logger.warning(f"Skipping '{file_name}' (download failed).")
                        continue

                    # 4a.ii. Save to temp and convert
                    local_input = temp_dir / file_name
                    local_input.write_bytes(content)

                    converted_filename = f"converted_{file_name}"
                    local_converted = temp_dir / converted_filename
                    if not convert_macro_file(local_input, local_converted):
                        skipped += 1
                        logger.warning(f"Skipping '{file_name}' (conversion failed).")
                        continue

                    # 4a.iii. Upload the converted file and get new_file_id
                    new_file_id = client.upload_new_file(file_path=local_converted)
                    if not new_file_id:
                        skipped += 1
                        logger.warning(f"Skipping '{file_name}' (upload of converted file failed).")
                        continue

                    # 4a.iv. Add the new file_id to the knowledge base
                    if client.add_file_to_knowledge(knowledge_id, new_file_id):
                        processed += 1
                    else:
                        skipped += 1

                else:
                    skipped += 1
                    logger.warning(f"Skipping '{file_name}' (no conversion libraries).")
                    continue

            # 4b. If not macro-enabled, simply add the existing file_id
            else:
                if client.add_file_to_knowledge(knowledge_id, file_id):
                    processed += 1
                else:
                    skipped += 1

    logger.info(f"Completed: {processed} files added, {skipped} files skipped.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
