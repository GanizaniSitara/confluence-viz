"""
Open-WebUI Local Document Uploader
Scans a local directory for Office documents, converts macro-enabled files if needed,
uploads them to Open-WebUI, and adds them to the "DOCS" knowledge base.
"""

import sys
import os
import tempfile
import configparser
from pathlib import Path
from typing import List, Dict, Optional
import requests
from requests.auth import HTTPBasicAuth
import logging

# Set root logger to INFO (suppresses DEBUG by default)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import python-docx and python-pptx for macro conversion
try:
    from docx import Document
    from pptx import Presentation
    OFFICE_LIBS_AVAILABLE = True
except ImportError:
    OFFICE_LIBS_AVAILABLE = False
    logger.warning("python-docx/python-pptx not installed – macro-enabled files will be skipped.")

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
        auth_url = f"{self.base_url}/api/v1/auths/signin"
        logger.debug(f"AUTH: POST {auth_url} payload={{'email':..., 'password':...}}")
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

    def upload_new_file(self, file_path: Path) -> Optional[str]:
        """
        Upload a new file to Open-WebUI (POST /api/v1/files/).
        Returns the new file_id on success.
        """
        upload_url = f"{self.base_url}/api/v1/files/"
        logger.debug(f"Uploading new file '{file_path.name}' to {upload_url}")
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (file_path.name, f, 'application/octet-stream')}
                response = self.session.post(upload_url, files=files)
                logger.debug(f"UPLOAD [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception uploading '{file_path.name}': {e}")
            return None

        if response.status_code != 200:
            logger.error(f"Failed to upload '{file_path.name}': HTTP {response.status_code}")
            return None

        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Error parsing upload JSON for '{file_path.name}': {e}")
            return None

        new_file_id = data.get("id")
        if not new_file_id:
            logger.error(f"No file ID returned after uploading '{file_path.name}'")
            return None

        logger.info(f"Uploaded '{file_path.name}', file_id={new_file_id}")
        return new_file_id

    def list_knowledge_bases(self) -> List[Dict]:
        """
        List existing knowledge bases (GET /api/v1/knowledge/).
        """
        kb_url = f"{self.base_url}/api/v1/knowledge/"
        logger.debug(f"GET {kb_url}")
        try:
            response = self.session.get(kb_url)
            logger.debug(f"LIST KB [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception listing knowledge bases: {e}")
            return []

        if response.status_code != 200:
            logger.error(f"Failed to list knowledge bases: HTTP {response.status_code}")
            return []

        try:
            return response.json()
        except ValueError as e:
            logger.error(f"Error parsing knowledge bases JSON: {e}")
            return []

    def create_knowledge_base(self, name: str, description: str = "") -> Optional[str]:
        """
        Create a new knowledge base (POST /api/v1/knowledge/create).
        Returns the new knowledge_id on success.
        """
        create_url = f"{self.base_url}/api/v1/knowledge/create"
        payload = {"name": name, "description": description, "data": {}, "access_control": {}}
        logger.debug(f"POST {create_url} payload={payload}")
        try:
            response = self.session.post(create_url, json=payload)
            logger.debug(f"CREATE KB [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception creating knowledge base '{name}': {e}")
            return None

        if response.status_code != 200:
            logger.error(f"Failed to create knowledge base '{name}': HTTP {response.status_code}")
            return None

        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Error parsing create KB JSON: {e}")
            return None

        kb_id = data.get("id")
        if not kb_id:
            logger.error(f"No KB ID returned after creating '{name}'")
            return None

        logger.info(f"Created knowledge base '{name}', id={kb_id}")
        return kb_id

    def get_or_create_knowledge_base(self, name: str, description: str = "") -> Optional[str]:
        """
        Return the ID of an existing knowledge base named `name`, or create it if missing.
        """
        existing = self.list_knowledge_bases()
        for kb in existing:
            if kb.get("name") == name:
                kb_id = kb.get("id")
                logger.info(f"Knowledge base '{name}' already exists (id={kb_id})")
                return kb_id

        logger.info(f"Knowledge base '{name}' not found. Creating new one.")
        return self.create_knowledge_base(name, description)

    def add_file_to_knowledge(self, knowledge_id: str, file_id: str) -> bool:
        """
        Add an existing file to a knowledge base (POST /api/v1/knowledge/{knowledge_id}/file/add).
        """
        add_url = f"{self.base_url}/api/v1/knowledge/{knowledge_id}/file/add"
        payload = {"file_id": file_id}
        logger.debug(f"POST {add_url} payload={payload}")
        try:
            response = self.session.post(add_url, json=payload)
            logger.debug(f"ADD FILE TO KB [{response.status_code}]: {response.text}")
        except Exception as e:
            logger.error(f"Exception adding file '{file_id}' to knowledge '{knowledge_id}': {e}")
            return False

        if response.status_code != 200:
            logger.error(f"Failed to add file '{file_id}' to KB '{knowledge_id}': HTTP {response.status_code}")
            return False

        return True

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
        logger.warning(f"Cannot convert '{input_path.name}' – Office libs missing.")
        return False

    try:
        file_ext = input_path.suffix.lower()

        if file_ext in ['.pptm', '.ppsm', '.potm']:
            prs = Presentation(input_path)
            new_path = output_path.with_suffix('.pptx')
            prs.save(new_path)
            logger.debug(f"Converted PowerPoint '{input_path.name}' → '{new_path.name}'")
            return True

        elif file_ext in ['.docm', '.dotm']:
            doc = Document(input_path)
            new_path = output_path.with_suffix('.docx')
            doc.save(new_path)
            logger.debug(f"Converted Word '{input_path.name}' → '{new_path.name}'")
            return True

    except Exception as e:
        logger.error(f"Error converting '{input_path.name}': {e}")
        return False

    return False

def find_local_office_files(root_dir: str) -> List[Path]:
    """
    Recursively walk `root_dir` to find all Office documents.
    Returns a list of Path objects for matching files.
    """
    matches: List[Path] = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if is_office_file(filename):
                matches.append(Path(dirpath) / filename)
    return matches

def load_settings(settings_file: str = "settings.ini") -> Dict[str, str]:
    """
    Load settings from INI file. If missing, create an example and exit.
    Expects:
      [OpenWebUI]
      base_url = http://localhost:8080
      username = your_username
      password = your_password
      upload_dir = /path/to/local/documents
    """
    config = configparser.ConfigParser()
    settings_path = Path(settings_file)

    if not settings_path.exists():
        logger.error(f"Settings file '{settings_file}' not found. Creating an example.")
        example = configparser.ConfigParser()
        example['OpenWebUI'] = {
            'base_url': 'http://localhost:8080',
            'username': 'your_username',
            'password': 'your_password',
            'upload_dir': '/path/to/local/documents'
        }
        with open(settings_file, 'w') as f:
            example.write(f)
        logger.info(f"Example '{settings_file}' created. Please configure and rerun.")
        sys.exit(1)

    config.read(settings_file)
    if 'OpenWebUI' not in config:
        logger.error("Missing [OpenWebUI] section in settings.ini")
        sys.exit(1)

    required = ['base_url', 'username', 'password', 'upload_dir']
    settings: Dict[str, str] = {}
    for key in required:
        if key not in config['OpenWebUI']:
            logger.error(f"Missing '{key}' in [OpenWebUI] section")
            sys.exit(1)
        settings[key] = config['OpenWebUI'][key]

    return settings

def main() -> int:
    """
    Main process:
      1. Authenticate
      2. Create/get 'DOCS' knowledge base
      3. Scan local upload_dir for Office documents
      4. For each local Office file:
           - If macro-enabled → convert → upload converted → add to KB
           - If not macro-enabled → upload original → add to KB
    """
    settings = load_settings()
    BASE_URL = settings['base_url']
    USERNAME = settings['username']
    PASSWORD = settings['password']
    UPLOAD_DIR = settings['upload_dir']
    KB_NAME = "DOCS"

    if not Path(UPLOAD_DIR).exists():
        logger.error(f"Upload directory '{UPLOAD_DIR}' does not exist. Exiting.")
        return 1

    logger.info(f"Connecting to Open-WebUI at {BASE_URL}")
    client = OpenWebUIClient(BASE_URL, USERNAME, PASSWORD)

    # 1. Authenticate
    if not client.authenticate():
        logger.error("Authentication failed. Exiting.")
        return 1

    # 2. Create or retrieve the 'DOCS' knowledge base
    kb_id = client.get_or_create_knowledge_base(name=KB_NAME,
                                                description="Office documents collection")
    if not kb_id:
        logger.error("Failed to get/create knowledge base. Exiting.")
        return 1

    # 3. Scan local directory for Office files
    local_files = find_local_office_files(UPLOAD_DIR)
    logger.info(f"Found {len(local_files)} Office files in '{UPLOAD_DIR}' to upload.")

    if not local_files:
        logger.info("No Office files found locally. Exiting.")
        return 0

    processed = 0
    skipped = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)

        for local_path in local_files:
            file_name = local_path.name
            logger.debug(f"Processing local file '{file_name}'")

            # If macro-enabled: convert → upload converted → add to KB
            if needs_conversion(file_name):
                if OFFICE_LIBS_AVAILABLE:
                    converted_filename = f"converted_{file_name}"
                    local_converted = temp_dir / converted_filename

                    if not convert_macro_file(local_path, local_converted):
                        skipped += 1
                        logger.warning(f"Skipped '{file_name}' (conversion failed).")
                        continue

                    new_file_id = client.upload_new_file(local_converted)
                    if not new_file_id:
                        skipped += 1
                        logger.warning(f"Skipped '{file_name}' (upload failed).")
                        continue

                    if client.add_file_to_knowledge(kb_id, new_file_id):
                        processed += 1
                    else:
                        skipped += 1

                else:
                    skipped += 1
                    logger.warning(f"Skipped '{file_name}' (no conversion libs).")
                    continue

            # If not macro-enabled: upload original → add to KB
            else:
                new_file_id = client.upload_new_file(local_path)
                if not new_file_id:
                    skipped += 1
                    logger.warning(f"Skipped '{file_name}' (upload failed).")
                    continue

                if client.add_file_to_knowledge(kb_id, new_file_id):
                    processed += 1
                else:
                    skipped += 1

    logger.info(f"Done: {processed} files uploaded and added, {skipped} files skipped.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
