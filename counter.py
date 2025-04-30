import requests
import time
import warnings
import urllib3 # Import urllib3 to reference its warning class
import argparse # Import argparse for command-line arguments
import random # Import random for jitter
import os
from config_loader import load_confluence_settings

# --- Suppress InsecureRequestWarning ---
# WARNING: Disabling SSL verification is INSECURE and should only be done
# in controlled environments where you understand the risks (e.g., testing
# against an instance with a self-signed cert you cannot trust system-wide).
warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made to',
                        category=urllib3.exceptions.InsecureRequestWarning)
# ---------------------------------------


# --- Configuration ---
# Load settings from settings.ini
try:
    settings = load_confluence_settings()
    CONFLUENCE_BASE_URL = settings['api_base_url'].rstrip('/rest/api')  # Remove API path if present
    USERNAME = settings['username']
    PASSWORD = settings['password']
    VERIFY_SSL = settings['verify_ssl']
except Exception as e:
    print(f"Error loading settings: {e}")
    print("Using default values")
    # Fallback to defaults if settings can't be loaded
    CONFLUENCE_BASE_URL = 'https://your-confluence-instance.com:8443'
    USERNAME = None
    PASSWORD = None
    VERIFY_SSL = False
# ---------------------

API_SPACE_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/space'
API_CONTENT_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/content'

def make_api_request(url, params=None, max_retries=5):
    """
    Makes an API request, handling 429 rate limiting and disabling SSL verification.
    """
    retries = 0
    while retries < max_retries:
        print(f"Attempt {retries + 1} to fetch from: {url}")
        try:
            # Use authentication if credentials are available
            auth = None
            if USERNAME and PASSWORD:
                auth = (USERNAME, PASSWORD)
                
            # Use the VERIFY_SSL setting from config
            response = requests.get(url, params=params, verify=VERIFY_SSL, auth=auth)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                # Add a small buffer + random jitter to wait_time in case multiple clients hit 429
                wait_time = int(retry_after) if retry_after else (2 ** retries) * 5 # Exponential backoff if no header
                jitter = random.uniform(0, 1) * 2 # Add up to 2 seconds of jitter
                wait_time += jitter
                print(f"Rate limited (429). Waiting for {wait_time:.2f} seconds.")
                time.sleep(wait_time)
                retries += 1
                continue # Retry the request

            else:
                print(f"Error: Received status code {response.status_code} for {url}")
                print(f"Response body: {response.text}")
                # Handle other errors like 401, 403, 404, 500 etc.
                # For unauthorized/forbidden with no auth, check anonymous access config
                return None # Or raise an exception

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            # Basic handling, could implement retry for network errors too
            return None

    print(f"Failed to fetch data from {url} after {max_retries} retries.")
    return None

# --- Command Line Argument Parsing ---
parser = argparse.ArgumentParser(
    description='Count spaces and pages on a Confluence instance via REST API.',
    formatter_class=argparse.RawTextHelpFormatter # Preserve newlines in description
)
parser.add_argument(
    '--url',
    default=CONFLUENCE_BASE_URL,
    help=f'Base URL of the Confluence instance (e.g., {CONFLUENCE_BASE_URL}).\n'
         'Use http:// or https:// as appropriate.\n'
         'WARNING: If using https://, SSL verification is disabled (--verify=False).'
)
parser.add_argument(
    '--space-filter',
    choices=['global', 'personal', 'both'],
    default='both',
    help='Filter spaces by type.\n'
         '  global: Count only non-personal spaces.\n'
         '  personal: Count only personal spaces.\n'
         '  both: Count both global and personal spaces (default).'
)

args = parser.parse_args()
CONFLUENCE_BASE_URL = args.url # Update URL from command line if provided

# Update API endpoints in case URL changed
API_SPACE_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/space'
API_CONTENT_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/content'


def get_all_items(base_url, params, item_type="items"):
    """
    Handles pagination to get all items from a Confluence API endpoint.
    
    Args:
        base_url: The API endpoint URL
        params: Request parameters
        item_type: Description of the items being fetched (for logging)
        
    Returns:
        The total count of items across all pages
    """
    all_items_count = 0
    start = 0
    limit = params.get('limit', 100)
    
    while True:
        # Update start parameter for pagination
        current_params = params.copy()
        current_params['start'] = start
        
        # Make the API request
        print(f"Fetching {item_type} (start={start}, limit={limit})...")
        response_data = make_api_request(base_url, params=current_params)
        
        # Check if we got valid data
        if not response_data or 'results' not in response_data:
            print(f"Failed to retrieve {item_type} or unexpected response structure")
            break
            
        # Count items in this page
        items_in_page = len(response_data['results'])
        all_items_count += items_in_page
        
        # Check if there are more pages
        if '_links' in response_data and 'next' in response_data['_links'] and response_data['_links']['next']:
            # Move to next page
            start += limit
            print(f"Found {items_in_page} {item_type} in current page, moving to next page...")
        else:
            # No more pages
            print(f"Retrieved all {item_type}: {all_items_count} total")
            break
            
    return all_items_count

# --- Get Total Spaces ---
space_params = {'limit': 100}  # Get spaces in batches of 100
space_filter_desc = "all" # Default description
if args.space_filter == 'global':
    space_params['type'] = 'global'
    space_filter_desc = "non-personal (global)"
elif args.space_filter == 'personal':
    space_params['type'] = 'personal'
    space_filter_desc = "personal"

print(f"\nFetching total number of {space_filter_desc} spaces...")
total_spaces = get_all_items(API_SPACE_ENDPOINT, space_params, f"{space_filter_desc} spaces")


# --- Get Total Pages ---
# NOTE: The /rest/api/content endpoint cannot filter by the *type* of the containing space
# in a single request. This count is for ALL visible pages, regardless of space type.
print("\nFetching total number of pages (across all visible spaces)...")
content_params = {
    'type': 'page',
    'limit': 100  # Get pages in batches of 100
}
total_pages = get_all_items(API_CONTENT_ENDPOINT, content_params, "pages")


# --- Print Results ---
print("\n--- Confluence Totals ---")
print(f"Base URL: {CONFLUENCE_BASE_URL}")

if total_spaces is not None:
    print(f"Total {space_filter_desc.capitalize()} Spaces: {total_spaces}")
else:
    print(f"Could not retrieve total {space_filter_desc} spaces.")

if total_pages is not None:
    print(f"Total Pages (across all visible spaces): {total_pages}")
else:
    print("Could not retrieve total pages.")
print("-------------------------")