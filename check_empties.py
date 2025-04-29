import os
import argparse
import sys
import logging
import time
import json
import requests
import base64
import random
import datetime
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
parser = argparse.ArgumentParser(description="Check for empty pages in a Confluence space OR list spaces.")
group = parser.add_mutually_exclusive_group(required=True) # Ensure one action is chosen
group.add_argument("--space-key", help="The key of the Confluence space to check (e.g., 'MYSPACE' or '~username').")
group.add_argument(
    "--list-spaces",
    choices=['all', 'user', 'space'],
    help="List spaces instead of checking pages. 'user' lists personal spaces (~key), 'space' lists global spaces, 'all' lists both."
)
# Optional: Add arguments for credentials if not using environment variables
# parser.add_argument("-u", "--user", help="Confluence username (overrides CONFLUENCE_USER env var)")
# parser.add_argument("-p", "--password", help="Confluence password (overrides CONFLUENCE_PASSWORD env var)")
# parser.add_argument("--url", help="Confluence URL (overrides CONFLUENCE_URL env var)")

args = parser.parse_args()
# SPACE_KEY is now potentially None if --list-spaces is used
SPACE_KEY = args.space_key

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
                
            # Handle 429 Too Many Requests with Retry-After header
            retry_count += 1
            
            # ALWAYS respect the Retry-After header if present (RFC 7231 compliance)
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                # Retry-After can be a timestamp or number of seconds
                if retry_after.isdigit():
                    wait_time = int(retry_after)
            else:
                # Fallback to exponential backoff with jitter if no Retry-After header
                wait_time = base_wait_time * (2 ** (retry_count - 1)) + random.uniform(0, 1)
                
            if wait_time == 0:
                wait_time = 2
                
            print(f"Rate limited (429). Server Retry-After: {retry_after or 'Not provided'}. Waiting {wait_time:.2f} seconds... (Attempt {retry_count}/{max_retries})")
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
    # Construct the URL using the /rest/api/user/current endpoint
    url = f"{CONFLUENCE_URL.rstrip('/')}/rest/api/user/current"
    return make_api_request(session, url)

def get_all_spaces(session, space_type=None, limit=100):
    """
    Get all spaces from Confluence with pagination handling.
    Can filter by type ('global' or 'personal').
    """
    # Construct the URL using the standard /rest/api/space endpoint
    url = f"{CONFLUENCE_URL.rstrip('/')}/rest/api/space"
    params = {
        'limit': limit,
        'start': 0
    }
    if space_type:
        params['type'] = space_type    
    all_spaces = []
    more_results = True
    page_count = 0

    print(f"Fetching spaces (type: {space_type or 'all'})...")
    while more_results:
        try:
            page_count += 1
            current_start = params['start']
            print(f"  API call #{page_count}: GET {url} (start={current_start}, limit={limit}, type={space_type})")
            
            response_data = make_api_request(session, url, params=params)
            
            if 'results' in response_data:
                results_count = len(response_data['results'])
                all_spaces.extend(response_data['results'])
                
                print(f"  ✓ Received {results_count} spaces (total so far: {len(all_spaces)})")

                # Check if there are more pages to fetch
                if response_data.get('_links', {}).get('next'):
                    params['start'] += len(response_data['results']) # Increment start based on results received
                    print(f"  → More spaces available, will fetch next page...")
                else:
                    more_results = False
                    print(f"  → No more spaces to fetch.")
            else:
                print("Warning: Unexpected response structure for spaces, 'results' key missing.")
                more_results = False
        except Exception as e:
            print(f"Error fetching spaces: {e}")
            more_results = False # Stop fetching on error

    print(f"Completed fetching spaces. Found {len(all_spaces)} spaces in {page_count} API calls.")
    return all_spaces

def get_all_pages_from_space(session, space_key, limit=100, expand=None):
    """
    Get all pages from a Confluence space with pagination handling
    """
    # Construct the URL using the standard /rest/api/content endpoint
    url = f"{CONFLUENCE_URL.rstrip('/')}/rest/api/content"
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
    # Construct the URL using the standard /rest/api/content/{id}/child/attachment endpoint
    url = f"{CONFLUENCE_URL.rstrip('/')}/rest/api/content/{content_id}/child/attachment"
    params = {'limit': limit}
    
    return make_api_request(session, url, params=params)

# --- Main Logic ---

def list_spaces(space_filter):
    """Connects to Confluence and lists spaces based on the filter."""
    print(f"Connecting to Confluence at: {CONFLUENCE_URL}")
    print(f"Using username: {CONFLUENCE_USERNAME}")
    print(f"Listing spaces: {space_filter}")
    if not VERIFY_SSL:
        print("SSL verification is DISABLED.")
    print("-" * 30)

    try:
        session = create_session()
        user_info = get_current_user(session)
        print(f"Successfully connected as: {user_info.get('displayName', CONFLUENCE_USERNAME)}")
        print("-" * 30)

        api_space_type = None
        if space_filter == 'user':
            api_space_type = 'personal'
        elif space_filter == 'space':
            api_space_type = 'global'
        # 'all' uses api_space_type = None

        spaces = get_all_spaces(session, space_type=api_space_type)

        if not spaces:
            print(f"No spaces found matching filter '{space_filter}'.")
            return

        print(f"\n--- {space_filter.capitalize()} Spaces ---")
        count = 0
        for space in spaces:
            key = space.get('key')
            name = space.get('name')
            is_personal = key and key.startswith('~')

            should_list = False
            if space_filter == 'all':
                should_list = True
            elif space_filter == 'user' and is_personal:
                should_list = True
            elif space_filter == 'space' and not is_personal:
                should_list = True

            if should_list:
                print(f"  Key: {key:<15} Name: {name}")
                count += 1

        print(f"\nListed {count} {space_filter} space(s).")
        print("-" * 30)

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        sys.exit(1)

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
            
            # Output counter and page name for tracking progress
            print(f"Checking page {page_count}/{len(pages)}: '{page_title}' (ID: {page_id})")

            # 1. Check for empty content
            storage_value = page.get('body', {}).get('storage', {}).get('value', '').strip()
            is_content_empty = not storage_value

            # 2. Check for attachments
            try:
                attachments = get_attachments_from_content(session, page_id, limit=1)  # Only need to know if > 0
                has_no_attachments = len(attachments.get('results', [])) == 0
            except Exception as e:
                print(f"Warning: Failed to check attachments for page '{page_title}': {e}")
                has_no_attachments = False  # Assume it has attachments to be safe

            # 3. Check for delete permission
            can_delete = False
            for operation in page.get('operations', []):
                if operation.get('operation') == 'delete' and operation.get('rel') == 'delete':
                    can_delete = True
                    break

            # Combine checks
            if is_content_empty and has_no_attachments and can_delete:
                print(f"YES: Page '{page_title}' (ID: {page_id}) is empty and deletable.")
                print(f"     Link: {CONFLUENCE_URL}{page_link}")
                found_eligible_page = True
                eligible_count += 1

    except Exception as e:
        print(f"\nAn error occurred during page processing: {e}")

    print("-" * 30)
    print(f"Checked {page_count} pages in space '{space_key}'.")
    if found_eligible_page:
        print(f"Found {eligible_count} page(s) that are empty and deletable by the current user.")
    else:
        print(f"No pages found in space '{space_key}' that are both empty and deletable by the current user.")


if __name__ == "__main__":
    if args.list_spaces:
        list_spaces(args.list_spaces)
    elif args.space_key:
        check_pages_in_space(args.space_key)
    else:
        print("Error: No action specified. Use --space-key or --list-spaces.")
        parser.print_help()
        sys.exit(1)