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


# --- Get Total Spaces ---
space_params = {'limit': 1} # Always limit to 1 to get totalSize efficiently
space_filter_desc = "all" # Default description
if args.space_filter == 'global':
    space_params['type'] = 'global'
    space_filter_desc = "non-personal (global)"
elif args.space_filter == 'personal':
    space_params['type'] = 'personal'
    space_filter_desc = "personal"

print(f"\nFetching total number of {space_filter_desc} spaces...")
space_data = make_api_request(API_SPACE_ENDPOINT, params=space_params)

total_spaces = None
if space_data and 'totalSize' in space_data:
    total_spaces = space_data['totalSize']
    print(f"Successfully retrieved total {space_filter_desc} spaces.")
elif space_data is not None:
    print("Response structure unexpected: 'totalSize' not found in space data.")
    # Add debugging: Print the keys to see what is available
    print(f"DEBUG: Available keys in space data response: {list(space_data.keys())}")
    # Optionally print the whole structure if keys aren't enough
    # print(f"DEBUG: Full space data response: {space_data}")


# --- Get Total Pages ---
# NOTE: The /rest/api/content endpoint cannot filter by the *type* of the containing space
# in a single request. This count is for ALL visible pages, regardless of space type.
print("\nFetching total number of pages (across all visible spaces)...")
content_params = {
    'type': 'page',
    'limit': 1 # We only need the metadata, fetching 1 item is sufficient
}
page_data = make_api_request(API_CONTENT_ENDPOINT, params=content_params)

total_pages = None
if page_data and 'totalSize' in page_data:
    total_pages = page_data['totalSize']
    print(f"Successfully retrieved total pages.")
elif page_data is not None:
     print("Response structure unexpected: 'totalSize' not found in page data.")
     # Add debugging: Print the keys to see what is available
     print(f"DEBUG: Available keys in page data response: {list(page_data.keys())}")
     # Optionally print the whole structure if keys aren't enough
     # print(f"DEBUG: Full page data response: {page_data}")


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