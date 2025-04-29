import os
import argparse
import sys
import logging
import time
import json
import requests
import base64
import random
from getpass import getpass # Use getpass if not using env vars for password

# Import the config loader
from config_loader import load_confluence_settings

# Suppress only the single InsecureRequestWarning from urllib3 needed
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging to see library messages if needed, but keep it clean by default
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.getLogger("urllib3").setLevel(logging.WARNING) # Keep urllib3 quiet except for warnings other than InsecureRequestWarning

# --- Configuration ---
# Load settings from settings.ini
try:
    confluence_settings = load_confluence_settings()
    CONFLUENCE_URL = confluence_settings['api_base_url']
    CONFLUENCE_USERNAME = confluence_settings['username']
    CONFLUENCE_PASSWORD = confluence_settings['password']
    VERIFY_SSL = confluence_settings['verify_ssl']
except FileNotFoundError:
    # Fallback to environment variables if settings.ini is not found
    print("Warning: settings.ini not found, falling back to environment variables.")
    CONFLUENCE_URL = os.environ.get('CONFLUENCE_URL')
    CONFLUENCE_USERNAME = os.environ.get('CONFLUENCE_USER')
    CONFLUENCE_PASSWORD = os.environ.get('CONFLUENCE_PASSWORD')
    VERIFY_SSL = False

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Find empty, deletable pages in a Confluence space.")
parser.add_argument("space_key", help="The key of the Confluence space to check (e.g., 'MYSPACE').")
# Optional: Add arguments for credentials if not using environment variables
# parser.add_argument("-u", "--user", help="Confluence username (overrides CONFLUENCE_USER env var)")
# parser.add_argument("-p", "--password", help="Confluence password (overrides CONFLUENCE_PASSWORD env var)")
# parser.add_argument("--url", help="Confluence URL (overrides CONFLUENCE_URL env var)")

args = parser.parse_args()
SPACE_KEY = args.space_key.upper() # Confluence space keys are typically uppercase

# --- Credential Handling ---
# Override env vars if command-line args are provided (if you add them to argparse)
# if args.url: CONFLUENCE_URL = args.url
# if args.user: CONFLUENCE_USERNAME = args.user
# if args.password: CONFLUENCE_PASSWORD = args.password

# Validate essential configuration
if not CONFLUENCE_URL:
    print("Error: Confluence URL not configured. Check your settings.ini file.")
    sys.exit(1)
if not CONFLUENCE_USERNAME:
    print("Error: Confluence username not configured. Check your settings.ini file.")
    sys.exit(1)
if not CONFLUENCE_PASSWORD:
    print("Error: Confluence password not configured. Check your settings.ini file.")
    sys.exit(1)

# --- REST API Helpers ---
def create_session():
    """Create a requests session with basic auth and common headers"""
    session = requests.Session()
    session.auth = (CONFLUENCE_USERNAME, CONFLUENCE_PASSWORD)
    session.headers.update({
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    })
    session.verify = VERIFY_SSL
    return session

def make_api_request(session, url, method='GET', params=None, data=None, max_retries=5):
    """
    Make API request with exponential backoff for 429 responses
    """
    retry_count = 0
    base_wait_time = 2  # Start with 2 seconds
    
    while retry_count <= max_retries:
        try:
            if method == 'GET':
                response = session.get(url, params=params)
            elif method == 'POST':
                response = session.post(url, json=data, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # If successful or non-429 error, return immediately
            if response.status_code != 429:
                response.raise_for_status()  # Raise exception for 4xx/5xx (except 429)
                return response.json()
                
            # Handle 429 Too Many Requests with exponential backoff
            retry_count += 1
            
            # Get retry-after header or use exponential backoff with jitter
            retry_after = response.headers.get('Retry-After')
            if retry_after and retry_after.isdigit():
                wait_time = int(retry_after)
            else:
                wait_time = base_wait_time * (2 ** (retry_count - 1)) + random.uniform(0, 1)
            
            print(f"Rate limited (429). Retrying in {wait_time:.2f} seconds... (Attempt {retry_count}/{max_retries})")
            time.sleep(wait_time)
            
        except requests.exceptions.RequestException as e:
            # For connection errors or other issues, retry with backoff
            retry_count += 1
            if retry_count > max_retries:
                raise Exception(f"Maximum retries reached. Last error: {str(e)}")
            
            wait_time = base_wait_time * (2 ** (retry_count - 1)) + random.uniform(0, 1)
            print(f"Request failed: {str(e)}. Retrying in {wait_time:.2f} seconds... (Attempt {retry_count}/{max_retries})")
            time.sleep(wait_time)
    
    raise Exception("Maximum retries reached without successful response")

def get_current_user(session):
    """Get current user information"""
    url = f"{CONFLUENCE_URL}/rest/api/user/current"
    return make_api_request(session, url)

def get_all_pages_from_space(session, space_key, limit=100, expand=None):
    """
    Get all pages from a Confluence space with pagination handling
    """
    url = f"{CONFLUENCE_URL}/rest/api/content"
    params = {
        'spaceKey': space_key,
        'type': 'page',
        'status': 'current',
        'limit': limit,
        'start': 0
    }
    
    if expand:
        params['expand'] = expand
    
    all_pages = []
    more_results = True
    
    while more_results:
        response_data = make_api_request(session, url, params=params)
        
        if 'results' in response_data:
            all_pages.extend(response_data['results'])
            
            # Check if there are more pages to fetch
            if response_data.get('_links', {}).get('next'):
                params['start'] += limit
            else:
                more_results = False
        else:
            more_results = False
    
    return all_pages

def get_attachments_from_content(session, content_id, limit=1):
    """
    Get attachments for a content ID
    """
    url = f"{CONFLUENCE_URL}/rest/api/content/{content_id}/child/attachment"
    params = {'limit': limit}
    
    return make_api_request(session, url, params=params)

# --- Main Logic ---
def check_pages_in_space(space_key):
    """
    Connects to Confluence, finds pages in the space, and checks emptiness
    and delete permissions.
    """
    print(f"Connecting to Confluence at: {CONFLUENCE_URL}")
    print(f"Using username: {CONFLUENCE_USERNAME}")
    print(f"Checking space: {space_key}")
    if not VERIFY_SSL:
        print("SSL verification is DISABLED.")
    print("-" * 30)

    try:
        # Create a session for all requests
        session = create_session()
        
        # Verify connection by getting current user info
        user_info = get_current_user(session)
        print(f"Successfully connected as: {user_info.get('displayName', CONFLUENCE_USERNAME)}")
        print("-" * 30)

    except Exception as e:
        print(f"\nError connecting to Confluence: {e}")
        print("Please check URL, credentials, network connectivity, and ensure the Confluence instance is running.")
        sys.exit(1)

    found_eligible_page = False
    page_count = 0
    eligible_count = 0

    try:
        # Fetch pages with expanded data: body (storage format) and operations (for permissions)
        # Note: 'body.storage' gets the raw Confluence format, which is best for checking emptiness.
        # Note: 'operations' includes actions the *current user* can perform on the page.
        print(f"Fetching pages from space '{space_key}' (this might take a while)...")
        pages = get_all_pages_from_space(
            session,
            space_key,
            limit=100,  # Pages per request, adjusted for API
            expand='body.storage,version,operations'  # Crucial expands
        )
        print(f"Found {len(pages)} pages in total. Checking each...")

        for page in pages:
            page_count += 1
            page_id = page['id']
            page_title = page['title']
            page_link = page.get('_links', {}).get('webui', '')
            if not page_link and 'base' in page.get('_links', {}):
                 page_link = page['_links']['base'] + page.get('_links',{}).get('webui','')

            # 1. Check for empty content
            # An empty page might have no 'body', no 'storage', or an empty 'value'
            # or a value containing only whitespace or basic empty tags like <p></p>.
            storage_value = page.get('body', {}).get('storage', {}).get('value', '').strip()
            # Basic check: is the stripped storage value empty?
            # More advanced: could parse HTML/XML to check for meaningful content beyond empty tags
            is_content_empty = not storage_value

            # 2. Check for attachments
            try:
                attachments = get_attachments_from_content(session, page_id, limit=1)  # Only need to know if > 0
                has_no_attachments = len(attachments.get('results', [])) == 0
            except Exception as e:
                print(f"Warning: Failed to check attachments for page '{page_title}': {e}")
                has_no_attachments = False  # Assume it has attachments to be safe

            # 3. Check for delete permission
            # Look through the 'operations' array provided by the expand parameter.
            can_delete = False
            for operation in page.get('operations', []):
                # Check both 'operation' and 'rel' for robustness, usually 'delete' for both
                if operation.get('operation') == 'delete' and operation.get('rel') == 'delete':
                    can_delete = True
                    break

            # Combine checks
            if is_content_empty and has_no_attachments and can_delete:
                print(f"YES: Page '{page_title}' (ID: {page_id}) is empty and deletable.")
                print(f"     Link: {CONFLUENCE_URL}{page_link}")
                found_eligible_page = True
                eligible_count += 1
            # else:
            #     # Optional: Add verbose logging for pages that don't meet criteria
            #     details = []
            #     if not is_content_empty: details.append("has content")
            #     if not has_no_attachments: details.append("has attachments")
            #     if not can_delete: details.append("delete permission missing")
            #     print(f"INFO: Page '{page_title}' (ID: {page_id}) is not eligible ({', '.join(details)})")

    except Exception as e:
        print(f"\nAn error occurred during page processing: {e}")
        # You might want more specific error handling here (e.g., for 404 Not Found if space doesn't exist)

    print("-" * 30)
    print(f"Checked {page_count} pages in space '{space_key}'.")
    if found_eligible_page:
        print(f"Found {eligible_count} page(s) that are empty and deletable by the current user.")
    else:
        print(f"No pages found in space '{space_key}' that are both empty and deletable by the current user.")


if __name__ == "__main__":
    check_pages_in_space(SPACE_KEY)