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

args = parser.parse_args()
CONFLUENCE_BASE_URL = args.url # Update URL from command line if provided

# Update API endpoints in case URL changed
API_SPACE_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/space'
API_CONTENT_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/content'
API_SEARCH_ENDPOINT = f'{CONFLUENCE_BASE_URL}/rest/api/search'


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
                       not args.all and
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


# --- Get Total Spaces ---
print("\n" + "=" * 50)
print("COUNTING CONFLUENCE SPACES")
print("=" * 50)

# Determine the space filter based on command-line arguments
space_params = {'limit': 100}  # Get spaces in batches of 100

# Default is to count only non-personal spaces
space_filter_desc = "non-personal (global)"
space_params['type'] = 'global'

# Override default if --personal or --all is specified
if args.personal:
    space_filter_desc = "personal"
    space_params['type'] = 'personal'
elif args.all:
    space_filter_desc = "all"
    # Don't specify type parameter to get all spaces

print(f"Space filter: {space_filter_desc}")
total_spaces = get_all_items(API_SPACE_ENDPOINT, space_params, f"{space_filter_desc} spaces")


# --- Get Total Pages ---
print("\n" + "=" * 50)
print("COUNTING CONFLUENCE PAGES")
print("=" * 50)

# For a specific space or all pages
if args.space_key:
    # Count pages in a specific space using CQL
    print(f"Counting pages in specific space: {args.space_key}")
    
    # Check if the space key is a personal space
    if args.space_key.startswith('~') and not (args.all or args.personal):
        print(f"Warning: Space key '{args.space_key}' appears to be a personal space (starts with '~')")
        print(f"By default, only non-personal spaces are counted.")
        print(f"Continuing with the specific space as requested...")
    
    # Try CQL approach first (faster)
    cql_query = f"space = {args.space_key} AND type = page"
    total_pages = count_using_cql(cql_query, f"pages in space {args.space_key}")
    
    # Description for output
    page_count_desc = f"in space {args.space_key}"
else:
    # Count all pages using CQL based on space filter
    if args.personal:
        print("Counting pages in personal spaces only")
        cql_query = "type = page AND space.type = 'personal'"
        total_pages = count_using_cql(cql_query, "pages in personal spaces")
        page_count_desc = "across all personal spaces"
    elif args.all:
        print("Counting pages across all spaces (personal and non-personal)")
        cql_query = "type = page"
        total_pages = count_using_cql(cql_query, "pages across all spaces")
        page_count_desc = "across all spaces (personal and non-personal)"
    else:
        # Default: count non-personal spaces only
        print("Counting pages across non-personal spaces only")
        cql_query = "type = page AND space.type = 'global'"
        total_pages = count_using_cql(cql_query, "pages in non-personal spaces")
        page_count_desc = "across all non-personal spaces"

# If CQL counting fails, fall back to the original method
if total_pages is None:
    print("\nCQL search failed or returned unexpected response.")
    print("Falling back to API-based page counting (this may take longer)...")
    
    if args.space_key:
        # For a specific space
        content_params = {
            'type': 'page',
            'spaceKey': args.space_key,
            'limit': 100
        }
        total_pages = get_all_items(API_CONTENT_ENDPOINT, content_params, f"pages in space {args.space_key}")
    else:
        # For all spaces
        content_params = {
            'type': 'page',
            'limit': 100
        }
        
        # Apply space type filtering for the fallback method
        if args.personal:
            content_params['spaceType'] = 'personal'
        elif not args.all:
            content_params['spaceType'] = 'global'
            
        total_pages = get_all_items(API_CONTENT_ENDPOINT, content_params, "pages")


# --- Print Results ---
print("\n" + "=" * 50)
print("CONFLUENCE COUNT RESULTS")
print("=" * 50)
print(f"Base URL: {CONFLUENCE_BASE_URL}")
print("-" * 50)

if total_spaces is not None:
    print(f"Total {space_filter_desc.capitalize()} Spaces: {total_spaces}")
else:
    print(f"Could not retrieve total {space_filter_desc} spaces.")

if total_pages is not None:
    print(f"Total Pages {page_count_desc}: {total_pages}")
else:
    print(f"Could not retrieve total pages {page_count_desc}.")
print("=" * 50)
print(f"Counting completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 50)