# description: Provides counting utilities for Confluence visualization.

import sys as _sys, os as _os; _sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
import requests
import time
import warnings
import urllib3 # Import urllib3 to reference its warning class
import argparse # Import argparse for command-line arguments
import random # Import random for jitter
import os
import pickle
import datetime # Import datetime for date parsing and timestamp operations
from utils.config_loader import load_confluence_settings
import json

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

# Define API endpoints
API_SPACE_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/space'
API_CONTENT_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/content'
API_SEARCH_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/search'

def make_api_request(url, params=None, max_retries=5):
    """
    Makes an API request, handling 429 rate limiting and disabling SSL verification.
    """
    retries = 0
    while retries < max_retries:
        # Print details about the request we're making
        query_params = '&'.join([f"{k}={v}" for k, v in (params or {}).items()])
        request_url = f"{url}?{query_params}" if query_params else url
        print(f"REST Request: GET {request_url}")
        
        try:
            # Use authentication if credentials are available
            auth = None
            if USERNAME and PASSWORD:
                auth = (USERNAME, PASSWORD)
                
            # Use the VERIFY_SSL setting from config
            response = requests.get(url, params=params, verify=VERIFY_SSL, auth=auth)
            print(f"Response Status: {response.status_code}")

            if response.status_code == 200:
                json_response = response.json()
                # Print summary of response data
                if isinstance(json_response, dict):
                    keys = list(json_response.keys())
                    print(f"Response contains keys: {keys}")
                    if 'results' in json_response:
                        print(f"Results count: {len(json_response['results'])}")
                    if 'size' in json_response:
                        print(f"Size value: {json_response['size']}")
                    if 'totalSize' in json_response:
                        print(f"Total size value: {json_response['totalSize']}")
                return json_response

            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                # Add a small buffer + random jitter to wait_time in case multiple clients hit 429
                wait_time = int(retry_after) if retry_after else (2 ** retries) * 5 # Exponential backoff if no header
                jitter = random.uniform(0, 1) * 2 # Add up to 2 seconds of jitter
                wait_time += jitter
                print(f"Rate limited (429). Server requested Retry-After: {retry_after or 'Not specified'}")
                print(f"Waiting for {wait_time:.2f} seconds before retry {retries + 1}/{max_retries}")
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
    '--all',
    action='store_true',
    help='Count both personal and non-personal spaces.\n'
         'By default, only non-personal spaces are counted.'
)
parser.add_argument(
    '--personal',
    action='store_true',
    help='Count only personal spaces.\n'
         'By default, only non-personal spaces are counted.\n'
         'Takes precedence over --all if both are specified.'
)
parser.add_argument(
    '--space-key',
    help='Specific space key to count pages for (e.g., "TEST").\n'
         'When specified, only counts pages in this space.\n'
         'Takes precedence over --all and --personal.'
)


# Add date filter and menu arguments
parser.add_argument('--date-filter', help="Page last updated filter: >YYYY-MM-DD (after), <YYYY-MM-DD (before). If not set, no filter.")
parser.add_argument('--menu', action='store_true', help='Show interactive menu for date filter.')

args = parser.parse_args()
CONFLUENCE_BASE_URL = args.url # Update URL from command line if provided

# Update API endpoints in case URL changed
API_SPACE_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/space'
API_CONTENT_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/content'
API_SEARCH_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/search'

# --- Date Filter Menu ---
def get_date_filter_interactive(current_filter=None):
    print("\nSet date filter to include pages last updated before/after a specific date.")
    print("Format: >YYYY-MM-DD (after date) or <YYYY-MM-DD (before date)")
    print("Examples: >2017-01-01 (pages updated after Jan 1, 2017)")
    print("          <2020-03-15 (pages updated before March 15, 2020)")
    print(f"Current filter: {current_filter if current_filter else 'None'}")
    print("Enter an empty string to clear the filter.")
    
    while True:
        date_input = input("Enter date filter: ").strip()
        if not date_input:
            return None
        if date_input[0] in ['<', '>']:
            date_str = date_input[1:].strip()
            print(f"Parsing date: '{date_str}'")
            try:
                datetime.datetime.strptime(date_str, '%Y-%m-%d')
                return date_input[0] + date_str  # Ensure we return with the operator
            except Exception as e:
                print(f"Invalid date format: {e}")
                print("Use YYYY-MM-DD format (example: <2014-01-01)")
        else:
            print("Error: Date filter must start with < or >")
            print("Format should be >YYYY-MM-DD or <YYYY-MM-DD")

# --- Page Date Filter ---
def filter_pages_by_date(pages, date_filter):
    if not date_filter:
        return pages
    operator = date_filter[0]
    date_str = date_filter[1:].strip()
    try:
        print(f"Trying to parse filter date: '{date_str}'")
        target_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        target_ts = target_date.timestamp()
        print(f"Filter date parsed: {target_date.strftime('%Y-%m-%d')} (timestamp: {target_ts})")
    except Exception as e:
        print(f"Invalid date filter: {date_filter} - Error: {e}")
        return pages
    
    filtered = []
    for page in pages:
        # Try multiple fields that might contain date information
        date_fields = [
            page.get('lastModified'),
            page.get('version', {}).get('when'),
            page.get('history', {}).get('lastUpdated', {}).get('when'),
            page.get('history', {}).get('createdDate')
        ]
        
        # Use the first non-empty date field
        page_date_str = next((d for d in date_fields if d), None)
        
        if not page_date_str:
            continue
            
        try:
            # Debug the date format we're working with
            print(f"Page {page.get('id')} date: {page_date_str[:30]}...")
            
            # First try ISO format parsing (2020-01-01T12:00:00.000Z)
            if 'T' in page_date_str:
                # Remove any timezone indicator and milliseconds
                date_part = page_date_str.split('T')[0]
                time_part = page_date_str.split('T')[1].split('.')[0]
                page_date_str = f"{date_part}T{time_part}"
                page_date = datetime.datetime.fromisoformat(page_date_str.replace('Z', '+00:00'))
            else:
                # Try various formats
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y']:
                    try:
                        page_date = datetime.datetime.strptime(page_date_str, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # If no format worked, skip this page
                    continue
                    
            page_ts = page_date.timestamp()
            print(f"  -> Converted to timestamp: {page_ts}")
            
            if operator == '>' and page_ts > target_ts:
                filtered.append(page)
                print(f"  -> INCLUDED (newer than filter)")
            elif operator == '<' and page_ts < target_ts:
                filtered.append(page)
                print(f"  -> INCLUDED (older than filter)")
            else:
                print(f"  -> EXCLUDED (doesn't match filter)")
                
        except Exception as e:
            print(f"  -> Error parsing date '{page_date_str[:30]}': {e}")
            continue
    
    # Print summary of filtering
    print(f"Filtered {len(filtered)} of {len(pages)} pages")
    return filtered

# --- Pickle helpers ---
def save_pages_pickle(space_key, pages, folder='temp_counter'):
    os.makedirs(folder, exist_ok=True)
    try:
        with open(os.path.join(folder, f'{space_key}.pkl'), 'wb') as f:
            pickle.dump(pages, f)
        print(f"Successfully saved {len(pages)} pages for space {space_key} to cache.")
    except Exception as e:
        print(f"Error saving pickle for {space_key}: {str(e)}")

def load_pages_pickle(space_key, folder='temp_counter'):
    path = os.path.join(folder, f'{space_key}.pkl')
    if os.path.exists(path):
        try:
            with open(path, 'rb') as f:
                return pickle.load(f)
        except EOFError:
            print(f"Corrupted pickle file for {space_key}, will re-fetch data")
            # Remove the corrupted file so we don't try to load it again
            try:
                os.remove(path)
                print(f"Removed corrupted pickle file: {path}")
            except Exception as e:
                print(f"Could not remove corrupted file {path}: {str(e)}")
        except Exception as e:
            print(f"Error loading pickle for {space_key}: {str(e)}")
    return None


def count_using_cql(cql_query, description="items"):
    """
    Uses Confluence Query Language (CQL) to efficiently count items.
    
    Args:
        cql_query: The CQL query string to execute
        description: Description of what's being counted (for logging)
        
    Returns:
        The total count of items matching the query
    """
    print(f"\n=== Counting {description} using CQL ===")
    print(f"CQL Query: {cql_query}")
    
    # Set limit to 0 to get just the count without any actual results
    params = {
        'cql': cql_query,
        'limit': 0  # We only need the count, not the actual results
    }
    
    response_data = make_api_request(API_SEARCH_ENDPOINT, params=params)
    
    if response_data:
        print(f"CQL search completed successfully")
        
        if 'totalSize' in response_data:
            count = response_data['totalSize']
            print(f"Found {count} {description} matching the query")
            return count
        else:
            print(f"Warning: CQL response doesn't contain 'totalSize' field")
            print(f"Available response keys: {list(response_data.keys())}")
            
            # Try alternative response fields
            if 'size' in response_data:
                count = response_data['size']
                print(f"Using 'size' field instead: Found {count} {description}")
                return count
            
            # If results are available, count them
            if 'results' in response_data and isinstance(response_data['results'], list):
                count = len(response_data['results'])
                print(f"Counting results array: Found {count} {description} (may be limited by pagination)")
                return count
                
            print("No usable count information found in response")
            return None
    else:
        print(f"Error: CQL query failed or returned no data")
        return None


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
    print(f"\n=== Fetching all {item_type} with pagination ===")
    print(f"Base URL: {base_url}")
    print(f"Initial parameters: {params}")
    
    # Check if we're filtering spaces and need to apply additional personal space filtering
    # Default behavior is to exclude personal spaces unless --personal or --all is specified
    exclude_personal = (base_url == API_SPACE_ENDPOINT and 
                       'space' in item_type.lower() and 
                       not args.personal and 
                       not args.space_key)
    
    if exclude_personal:
        print("Extra filter: Excluding personal spaces (those with keys starting with '~')")
    
    all_items_count = 0
    start = 0
    limit = params.get('limit', 100)
    page_number = 1
    
    while True:
        # Update start parameter for pagination
        current_params = params.copy()
        current_params['start'] = start
        
        # Make the API request
        print(f"\nFetching page {page_number} of {item_type} (start={start}, limit={limit})...")
        response_data = make_api_request(base_url, params=current_params)
        
        # Check if we got valid data
        if not response_data:
            print(f"Error: No valid response data returned")
            break
            
        if 'results' not in response_data:
            print(f"Error: 'results' not found in response. Available keys: {list(response_data.keys())}")
            # Try to provide debugging information
            if 'message' in response_data:
                print(f"Error message: {response_data['message']}")
            break
        
        results = response_data['results']
        
        # Apply additional filtering for personal spaces if needed
        if exclude_personal:
            # Count before filtering
            original_count = len(results)
            
            # Filter out personal spaces (those with keys starting with '~')
            filtered_results = [space for space in results if 'key' not in space or not space['key'].startswith('~')]
            
            # Count after filtering
            filtered_count = len(filtered_results)
            filtered_out = original_count - filtered_count
            
            if filtered_out > 0:
                print(f"Filtered out {filtered_out} personal spaces (keys starting with '~')")
            
            # Update the count for this page
            items_in_page = filtered_count
            
            # Replace the original results with filtered results
            results = filtered_results
        else:
            # No additional filtering needed
            items_in_page = len(results)
        
        all_items_count += items_in_page
        
        # Check if there are more pages
        has_next_page = False
        if '_links' in response_data and 'next' in response_data['_links'] and response_data['_links']['next']:
            has_next_page = True
            
        print(f"Page {page_number}: Found {items_in_page} {item_type}")
        print(f"Running total: {all_items_count} {item_type}")
        
        if has_next_page:
            # Move to next page
            start += limit
            page_number += 1
            print(f"Moving to next page...")
        else:
            # No more pages
            print(f"\nPagination complete. Retrieved all {item_type}: {all_items_count} total")
            break
            
    return all_items_count


# We no longer need to load spaces at startup
# We'll only fetch spaces when the user chooses to run the pickling process
# This improves startup performance and reduces unnecessary API calls



# --- Interactive menu for date filter if requested ---
date_filter = args.date_filter
if args.menu:
    date_filter = get_date_filter_interactive()

print("\n" + "=" * 50)
print("COUNTING CONFLUENCE PAGES")
print("=" * 50)

def get_pages_for_space(space_key, date_filter=None):
    """
    Fetch pages for a specific space.
    Personal spaces are always excluded.
    """
    # Skip personal spaces
    if space_key.startswith('~'):
        print(f"Skipping personal space {space_key}")
        return []
        
    # Try to load from pickle first
    pages = load_pages_pickle(space_key)
    if pages is not None:
        print(f"Loaded {len(pages)} pages for space {space_key} from cache.")
    else:
        print(f"Fetching pages for space {space_key} from API...")
        params = {'type': 'page', 'spaceKey': space_key, 'limit': 100}
        all_pages = []
        start = 0
        try:
            while True:
                params['start'] = start
                resp = make_api_request(API_CONTENT_ENDPOINT, params=params)
                if not resp or 'results' not in resp:
                    break
                batch = resp['results']
                all_pages.extend(batch)
                if len(batch) < 100:
                    break
                start += 100
            pages = all_pages
            save_pages_pickle(space_key, pages)
        except Exception as e:
            print(f"Error fetching pages for space {space_key}: {str(e)}")
            # If we can't fetch, return an empty list to avoid breaking the process
            pages = []
    
    if date_filter:
        filtered = filter_pages_by_date(pages, date_filter)
        print(f"Filtered pages by date: {len(filtered)} of {len(pages)} remain.")
        return filtered
    return pages


def count_pages_from_pickle(date_filter=None):
    """
    Count pages from pickled data, applying the specified date filter.
    Personal spaces are always excluded.
    """
    print("\n" + "=" * 50)
    print("COUNTING PAGES FROM PICKLED DATA")
    print("=" * 50)
    
    if date_filter:
        print(f"Applying date filter: {date_filter}")
    
    print("Personal spaces (those with keys starting with '~') will be excluded.")
    
    # Get list of pickled space files
    pickle_dir = 'temp_counter'
    total_spaces = 0
    total_pages = 0
    filtered_pages = 0
    
    try:
        if not os.path.exists(pickle_dir):
            print(f"Pickle directory '{pickle_dir}' does not exist.")
            return
            
        pickle_files = [f for f in os.listdir(pickle_dir) if f.endswith('.pkl')]
        if not pickle_files:
            print(f"No pickle files found in directory '{pickle_dir}'.")
            return
            
        print(f"Found {len(pickle_files)} total pickled space files.")
        
        # Always filter out personal spaces
        original_count = len(pickle_files)
        non_personal_files = [f for f in pickle_files if not os.path.splitext(f)[0].startswith('~')]
        filtered_out = original_count - len(non_personal_files)
        if filtered_out > 0:
            print(f"Filtered out {filtered_out} personal spaces (keys starting with '~')")
            print(f"Proceeding with {len(non_personal_files)} non-personal spaces")
        
        total_spaces = len(non_personal_files)
        
        for pkl_file in non_personal_files:
            space_key = os.path.splitext(pkl_file)[0]
            try:
                pages = load_pages_pickle(space_key)
                if pages is None:
                    print(f"No data for space {space_key}, skipping.")
                    continue
                    
                total_pages += len(pages)
                
                if date_filter:
                    filtered = filter_pages_by_date(pages, date_filter)
                    filtered_pages += len(filtered)
                    print(f"Space {space_key}: {len(filtered)} of {len(pages)} pages match the date filter.")
                else:
                    print(f"Space {space_key}: {len(pages)} pages.")
                    
            except Exception as e:
                print(f"Error processing space {space_key}: {str(e)}")
        
        # Print summary
        print("\n" + "=" * 50)
        print("COUNTING RESULTS")
        print("=" * 50)
        print(f"Total Spaces (excluding personal): {total_spaces}")
        print(f"Total Pages (before date filtering): {total_pages}")
        
        if date_filter:
            print(f"Total Pages (after date filter '{date_filter}'): {filtered_pages}")
            print(f"Filtered out by date: {total_pages - filtered_pages} pages ({(total_pages - filtered_pages) / total_pages * 100:.1f}% of total)")
        
        print("=" * 50)
        print(f"Counting completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
    except Exception as e:
        print(f"Error during counting process: {str(e)}")


def show_main_menu():
    date_filter = None
    while True:
        print("\n==== Confluence Counter Main Menu ====")
        print(f"Current date filter: {date_filter if date_filter else 'None'}")
        print("1. Set/clear date filter")
        print("2. Run pickling process (excludes personal spaces)")
        print("3. Count pages from pickled data (excludes personal spaces)")
        print("Q. Quit")
        choice = input("Select option: ").strip().lower()

        if choice == '1':
            date_filter = get_date_filter_interactive(date_filter)
            if date_filter:
                print(f"Date filter set to: {date_filter}")
            else:
                print("Date filter cleared.")
        elif choice == '2':
            print("Fetching non-personal spaces only...")
            params = {'limit': 100, 'type': 'global'}  # Exclude personal spaces
            
            all_spaces = []
            start = 0
            while True:
                params['start'] = start
                resp = make_api_request(API_SPACE_ENDPOINT, params=params)
                if not resp or 'results' not in resp:
                    break
                
                batch = resp['results']
                # Double-check to filter out any personal spaces
                batch = [space for space in batch if 'key' in space and not space['key'].startswith('~')]
                all_spaces.extend(batch)
                if len(batch) < 100:
                    break
                
                start += 100
            
            print(f"Found {len(all_spaces)} non-personal spaces.")
            for space in all_spaces:
                key = space.get('key')
                if not key:
                    continue
                pages = get_pages_for_space(key, date_filter)
                print(f"Space {key}: {len(pages)} pages pickled (date filter applied if set).")
            print("Pickling process complete.")
        elif choice == '3':
            count_pages_from_pickle(date_filter)
        elif choice == 'q':
            print("Exiting.")
            break
        else:
            print("Invalid option.")

if __name__ == "__main__":
    show_main_menu()