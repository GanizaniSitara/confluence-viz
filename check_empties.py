# filepath: c:\Solutions\PythonProject\confluence_visualization\check_empties.py
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
import pickle  # Required for loading pickled spaces
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

# --- Settings ---
TEMP_DIR = 'temp'  # Directory where pickled space data is stored
DEFAULT_MIN_PAGES = 0
DEFAULT_MAX_PAGES = None

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Check for empty pages in a Confluence space, a specific page OR list spaces.")
group = parser.add_mutually_exclusive_group(required=False) # Make group optional
group.add_argument("--space-key", help="The key of the Confluence space to check (e.g., 'MYSPACE' or '~username').")
group.add_argument("--page-id", help="The ID of a specific Confluence page to check.")
group.add_argument(
    "--list-spaces",
    choices=['all', 'user', 'space'],
    help="List spaces instead of checking pages. 'user' lists personal spaces (~key), 'space' lists global spaces, 'all' lists both."
)
group.add_argument(
    "--check-all-spaces",
    action="store_true",
    help="Check all spaces for empty pages. This will loop through all non-user spaces in the Confluence instance."
)

args = parser.parse_args()
# SPACE_KEY is now potentially None if --list-spaces or --page-id is used
SPACE_KEY = args.space_key
PAGE_ID = args.page_id

# Initialize filter values
min_pages = DEFAULT_MIN_PAGES
max_pages = DEFAULT_MAX_PAGES
date_filter = None
cached_spaces = []  # Will store spaces loaded from pickle files

# --- Credential Handling ---
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
    # Check if CONFLUENCE_URL already contains /rest/api
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/user/current"
    else:
        url = f"{base_url}/rest/api/user/current"
    
    # Debug output to check the constructed URL
    print(f"Debug - Current user URL: {url}")
    
    return make_api_request(session, url)

def get_all_spaces(session, space_type=None, limit=100):
    """
    Get all spaces from Confluence with pagination handling.
    Can filter by type ('global' or 'personal').
    """
    # Check if CONFLUENCE_URL already contains /rest/api
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/space"
    else:
        url = f"{base_url}/rest/api/space"
    
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
    
    Args:
        session: The requests session to use
        space_key: The Confluence space key
        limit: Number of results per page (default 100)
        expand: Comma-separated list of properties to expand. If None, defaults to including attachments
    
    Returns:
        List of page objects with requested expansions
    """
    # Check if CONFLUENCE_URL already contains /rest/api
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/content"
    else:
        url = f"{base_url}/rest/api/content"
    
    params = {
        'spaceKey': space_key,
        'type': 'page',
        'status': 'current',
        'limit': limit,
        'start': 0
    }
    
    # Include attachments in the expansion by default if not specified
    if expand:
        if 'attachments' not in expand:
            params['expand'] = f"{expand},attachments"
        else:
            params['expand'] = expand
    else:
        params['expand'] = 'attachments'
    
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
    # Check if CONFLUENCE_URL already contains /rest/api
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/content/{content_id}/child/attachment"
    else:
        url = f"{base_url}/rest/api/content/{content_id}/child/attachment"
    
    params = {'limit': limit}
    
    return make_api_request(session, url, params=params)

def get_page_by_id(session, page_id, expand=None):
    """
    Get a specific page by its ID with optional expanded properties
    """
    # Check if CONFLUENCE_URL already contains /rest/api
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/content/{page_id}"
    else:
        url = f"{base_url}/rest/api/content/{page_id}"
    
    params = {}
    
    if expand:
        params['expand'] = expand
    
    return make_api_request(session, url, params=params)

# --- Space Filtering Functions ---
def load_spaces(temp_dir=TEMP_DIR, min_pages=0, max_pages=None):
    """
    Load spaces from pickled files in the temp directory.
    Filters based on page count.
    """
    spaces = []
    if not os.path.exists(temp_dir):
        print(f"Error: Temp directory '{temp_dir}' not found.")
        return spaces

    print(f"Loading spaces from {temp_dir}...")
    pkl_count = 0
    loaded_count = 0
    
    for fname in os.listdir(temp_dir):
        if fname.endswith('.pkl'):
            pkl_count += 1
            try:
                with open(os.path.join(temp_dir, fname), 'rb') as f:
                    data = pickle.load(f)
                    if 'space_key' in data and 'sampled_pages' in data:
                        # Use total_pages for filtering if available, otherwise fallback to sampled_pages length
                        page_count = data.get('total_pages', len(data['sampled_pages']))
                        # Apply both min and max filters
                        meets_min = page_count >= min_pages
                        meets_max = max_pages is None or page_count <= max_pages
                        if meets_min and meets_max:
                            spaces.append(data)
                            loaded_count += 1
            except Exception as e:
                print(f"Error loading {fname}: {e}")
    
    print(f"Loaded {loaded_count} spaces from {pkl_count} pickle files.")
    return spaces

def calculate_avg_timestamps(spaces):
    """Calculate average timestamp for each space from page timestamps if available"""
    print("Calculating average timestamps for spaces...")
    for space in spaces:
        timestamps = []
        for page in space.get('sampled_pages', []):
            # Check 'updated' field which is set by sample_and_pickle_spaces.py
            when = page.get('updated')
            if not when and 'version' in page:
                # Fallback to version.when format if updated isn't available
                when = page.get('version', {}).get('when')
                
            if when:
                try:
                    # Parse ISO format timestamp and convert to unix timestamp
                    ts = datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
                    timestamps.append(ts)
                except ValueError:
                    pass
        
        # Calculate average timestamp if we have any valid timestamps
        avg_ts = sum(timestamps) / len(timestamps) if timestamps else 0
        space['avg'] = avg_ts  # Store avg timestamp for color mapping
    
    return spaces

def filter_spaces_by_date(spaces, date_filter):
    """
    Filter spaces based on average date.
    date_filter should be a string in format '>YYYY-MM-DD' or '<YYYY-MM-DD'.
    Returns filtered spaces list.
    """
    # Check if spaces have avg timestamps, if not, calculate them
    if any('avg' not in s for s in spaces):
        spaces = calculate_avg_timestamps(spaces)
    
    filtered_spaces = []
    
    # Parse filter
    if not date_filter:
        return spaces  # No filter, return all spaces
    
    try:
        # Extract the operator and date string
        operator = date_filter[0]  # '>' or '<'
        date_str = date_filter[1:].strip()
        
        # Parse the target date string
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        target_timestamp = target_date.timestamp()
        
        # Apply filter
        for space in spaces:
            avg_ts = space.get('avg', 0)
            if avg_ts > 0:  # Only include spaces with valid timestamps
                if operator == '>' and avg_ts > target_timestamp:
                    filtered_spaces.append(space)
                elif operator == '<' and avg_ts < target_timestamp:
                    filtered_spaces.append(space)
        
        print(f"Applied date filter {date_filter}: {len(filtered_spaces)} spaces match (from {len(spaces)} total)")
        
    except (ValueError, IndexError) as e:
        print(f"Error parsing date filter: {e}")
        print("Format should be >YYYY-MM-DD or <YYYY-MM-DD")
        return spaces  # Return original spaces on error
    
    return filtered_spaces

def get_filter_info():
    """Return a formatted string with current filter information"""
    filters = []
    
    if min_pages > 0:
        filters.append(f"Min pages: {min_pages}")
    if max_pages is not None:
        filters.append(f"Max pages: {max_pages}")
    if date_filter:
        filters.append(f"Date filter: {date_filter}")
        
    if filters:
        return "Current filters: " + ", ".join(filters)
    else:
        return "No filters applied"

# --- Interactive Menu Functions ---
def ensure_data_loaded():
    """Load cached data only when needed, applying current filters"""
    global cached_spaces, min_pages, max_pages, date_filter
    
    print(f"\nLoading space data with filter: >= {min_pages} pages" + 
          (f" and <= {max_pages} pages" if max_pages else "") + 
          (f" and date {date_filter}" if date_filter else "") + "...")
    
    # Load spaces with page count filter already applied
    cached_spaces = load_spaces(min_pages=min_pages, max_pages=max_pages)
    
    # Apply date filter if specified
    if date_filter:
        initial_count = len(cached_spaces)
        cached_spaces = filter_spaces_by_date(cached_spaces, date_filter)
        print(f"After date filter: {len(cached_spaces)} out of {initial_count} spaces match.")
    
    return cached_spaces

def show_filter_menu():
    """Show the interactive filter menu and handle user input"""
    global min_pages, max_pages, date_filter, cached_spaces
    
    while True:
        print("\n--- Filter Settings ---")
        print(get_filter_info())
        print("\nOptions:")
        print("1. Set minimum pages filter (current: {})".format(min_pages))
        print("2. Set maximum pages filter (current: {})".format(max_pages if max_pages else "No limit"))
        print("3. Set date filter (current: {})".format(date_filter if date_filter else "None"))
        print("4. Apply filters and continue")
        print("Q. Quit")
        
        choice = input("\nSelect option: ").strip()
        
        if choice == '1':
            try:
                inp = input("Enter minimum pages per space (0 for all): ").strip()
                min_pages = int(inp) if inp else 0
                print(f"Minimum pages filter set to {min_pages}")
                cached_spaces = []  # Clear cache to force reload
            except ValueError:
                print("Invalid input. Please enter a number.")
        elif choice == '2':
            try:
                inp = input("Enter maximum pages per space (leave empty for no limit): ").strip()
                max_pages = int(inp) if inp else None
                print(f"Maximum pages filter set to {max_pages if max_pages else 'No limit'}")
                cached_spaces = []  # Clear cache to force reload
            except ValueError:
                print("Invalid input. Please enter a number.")
        elif choice == '3':
            print("\nSet date filter to include spaces with average dates before/after a specific date.")
            print("Format: >YYYY-MM-DD (after date) or <YYYY-MM-DD (before date)")
            print("Examples: >2017-01-01 (spaces updated after Jan 1, 2017)")
            print("          <2020-03-15 (spaces updated before March 15, 2020)")
            print("Enter an empty string to clear the filter.")
            
            date_input = input("Enter date filter: ").strip()
            if date_input:
                if date_input[0] not in ['<', '>']:
                    print("Error: Date filter must start with < or >")
                    print("Format should be >YYYY-MM-DD or <YYYY-MM-DD")                
                else:
                    date_filter = date_input
                    print(f"Date filter set to {date_filter}")
                    cached_spaces = []  # Clear cache to force reload
            else:
                date_filter = None
                print("Date filter cleared")
                cached_spaces = []  # Clear cache to force reload
        elif choice == '4':
            # Load data with new filters
            ensure_data_loaded()
            print(f"\nApplying filters: {get_filter_info()}")
            print(f"Found {len(cached_spaces)} spaces after applying filters.")
            return True
        elif choice.upper() == 'Q':
            print("Exiting...")
            sys.exit(0)
        else:
            print("Invalid option.")

def show_main_menu():
    """Show the main interactive menu and handle user input"""
    while True:
        print("\n=== Confluence Empty Pages Tool ===")
        print("Choose an action:")
        print("1. List spaces")
        print("2. Check a specific space for empty pages")
        print("3. Check a specific page")
        print("4. Check all spaces for empty pages")
        print("Q. Quit")
        
        choice = input("\nSelect option: ").strip()
        
        if choice == '1':
            print("\nWhich spaces to list?")
            print("1. All spaces")
            print("2. User spaces")
            print("3. Global spaces")
            
            space_choice = input("\nSelect option: ").strip()
            if space_choice == '1':
                list_spaces('all')
            elif space_choice == '2':
                list_spaces('user')
            elif space_choice == '3':
                list_spaces('space')
            else:
                print("Invalid choice.")
                
        elif choice == '2':
            space_key = input("\nEnter the space key (e.g., 'MYSPACE' or '~username'): ").strip()
            if space_key:
                check_pages_in_space(space_key)
            else:
                print("No space key provided.")
                
        elif choice == '3':
            page_id = input("\nEnter the page ID: ").strip()
            if page_id:
                check_single_page(page_id)
            else:
                print("No page ID provided.")
                
        elif choice == '4':
            check_all_spaces()
            
        elif choice.upper() == 'Q':
            print("Exiting...")
            sys.exit(0)
            
        else:
            print("Invalid option.")

# --- Main Logic ---
def list_spaces(space_filter):
    """Connects to Confluence and lists spaces based on the filter."""
    global min_pages, max_pages, date_filter, cached_spaces
    
    print(f"Connecting to Confluence at: {CONFLUENCE_URL}")
    print(f"Using username: {CONFLUENCE_USERNAME}")
    print(f"Listing spaces: {space_filter}")
    if not VERIFY_SSL:
        print("SSL verification is DISABLED.")
    print("-" * 30)
    
    # Show filter menu and load data
    show_filter_menu()
    spaces = cached_spaces  # Use already loaded and filtered spaces

    try:
        session = create_session()
        user_info = get_current_user(session)
        print(f"Successfully connected as: {user_info.get('displayName', CONFLUENCE_USERNAME)}")
        print("-" * 30)

        # If using cached data, filter by space type
        filtered_spaces = []
        for s in spaces:
            space_key = s.get('space_key', '')
            is_personal = space_key and space_key.startswith('~')
            
            if space_filter == 'all':
                filtered_spaces.append(s)
            elif space_filter == 'user' and is_personal:
                filtered_spaces.append(s)
            elif space_filter == 'space' and not is_personal:
                filtered_spaces.append(s)
        
        # Sort spaces alphabetically by key
        filtered_spaces.sort(key=lambda s: s.get('space_key', ''))

        if not filtered_spaces:
            print(f"No spaces found matching filter '{space_filter}'.")
            return

        print(f"\n--- {space_filter.capitalize()} Spaces ({len(filtered_spaces)}) ---")
        count = 0
        for space in filtered_spaces:
            key = space.get('space_key', '')
            name = space.get('space_name', key)  # Fallback to key if no name
            page_count = space.get('total_pages', len(space.get('sampled_pages', [])))
            
            # Format the date if available
            avg_timestamp = space.get('avg', 0)
            if avg_timestamp > 0:
                date_str = datetime.fromtimestamp(avg_timestamp).strftime('%Y-%m-%d')
            else:
                date_str = "No date"

            print(f"  Key: {key:<15} Pages: {page_count:<5} Avg Last Edit: {date_str} Name: {name}")
            count += 1

        print(f"\nListed {count} {space_filter} space(s) after applying filters.")
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

    # Open file for writing deletable pages
    deletable_file = open("deletable_pages.txt", "w")
    deletable_file.write("# Deletable pages in Confluence\n")
    deletable_file.write(f"# Generated: {datetime.datetime.now()}\n")
    deletable_file.write("# Format: SPACE,URL\n\n")

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
        deletable_file.close()
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
            expand='body.storage,version,operations,attachments'  # Crucial expands including attachments
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

            if not is_content_empty:
                continue  # Skip to next page if content is not empty

            # 2. Check for delete permission
            can_delete = False
            for operation in page.get('operations', []):
                if operation.get('operation') == 'delete':
                    can_delete = True
                    break
            
            if not can_delete:
                continue  # Skip to next page if we don't have delete permission

            # 3. Check for attachments - only if the page is empty and we have delete permission
            has_no_attachments = False
            try:
                # Use attachments from expanded data instead of making a separate API call
                attachments = page.get('attachments', {}).get('results', [])
                has_no_attachments = len(attachments) == 0
            except Exception as e:
                print(f"Warning: Failed to check attachments for page '{page_title}': {e}")
                has_no_attachments = False  # Assume it has attachments to be safe
            
            # Combine checks
            if is_content_empty and has_no_attachments and can_delete:
                full_link = f"{CONFLUENCE_URL}{page_link}"
                print(f"YES: Page '{page_title}' (ID: {page_id}) is empty and deletable.")
                print(f"     Link: {full_link}")
                # Write to file: SPACE,URL
                deletable_file.write(f"{space_key},{full_link}\n")
                found_eligible_page = True
                eligible_count += 1

    except Exception as e:
        print(f"\nAn error occurred during page processing: {e}")
    finally:
        deletable_file.close()

    print("-" * 30)
    print(f"Checked {page_count} pages in space '{space_key}'.")
    if found_eligible_page:
        print(f"Found {eligible_count} page(s) that are empty and deletable by the current user.")
        print(f"Results written to: deletable_pages.txt")
    else:
        print(f"No pages found in space '{space_key}' that are both empty and deletable by the current user.")

def check_single_page(page_id):
    """
    Connects to Confluence and checks a specific page by ID for emptiness
    and delete permissions.
    """
    print(f"Connecting to Confluence at: {CONFLUENCE_URL}")
    print(f"Using username: {CONFLUENCE_USERNAME}")
    print(f"Checking page ID: {page_id}")
    if not VERIFY_SSL:
        print("SSL verification is DISABLED.")
    print("-" * 30)

    # Open file for writing deletable pages
    deletable_file = open("deletable_pages.txt", "w")
    deletable_file.write("# Deletable pages in Confluence\n")
    deletable_file.write(f"# Generated: {datetime.datetime.now()}\n")
    deletable_file.write("# Format: SPACE,URL\n\n")

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
        deletable_file.close()
        sys.exit(1)

    try:
        # Fetch the specific page with expanded data
        print(f"Fetching page with ID '{page_id}'...")
        
        try:
            page = get_page_by_id(
                session,
                page_id,
                expand='body.storage,version,operations'  # Crucial expands
            )
            
            page_title = page['title']
            space_key = page.get('space', {}).get('key', 'UNKNOWN')
            page_link = page.get('_links', {}).get('webui', '')
            if not page_link and 'base' in page.get('_links', {}):
                page_link = page['_links']['base'] + page.get('_links',{}).get('webui','')
            
            print(f"Found page: '{page_title}' (ID: {page_id})")

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
                if operation.get('operation'):
                    can_delete = True
                    break

            # Display detailed results
            print(f"\nAnalysis for page '{page_title}' (ID: {page_id}):")
            print(f"- Content is empty: {'YES' if is_content_empty else 'NO'}")
            print(f"- Has no attachments: {'YES' if has_no_attachments else 'NO'}")
            print(f"- Current user can delete: {'YES' if can_delete else 'NO'}")
            full_link = f"{CONFLUENCE_URL}{page_link}"
            print(f"- Link: {full_link}")

            # Combine checks
            if is_content_empty and has_no_attachments and can_delete:
                print(f"\nCONCLUSION: This page is EMPTY and DELETABLE.")
                # Write to file: SPACE,URL
                deletable_file.write(f"{space_key},{full_link}\n")
                print(f"Result written to: deletable_pages.txt")
            else:
                print(f"\nCONCLUSION: This page is {'NOT EMPTY' if not is_content_empty else 'empty'}, {'HAS ATTACHMENTS' if not has_no_attachments else 'has no attachments'}, and {'NOT DELETABLE' if not can_delete else 'deletable'}.")

        except Exception as e:
            print(f"Error: Failed to fetch or process page with ID '{page_id}': {e}")
            sys.exit(1)

    except Exception as e:
        print(f"\nAn error occurred during page processing: {e}")
        sys.exit(1)
    finally:
        deletable_file.close()

def check_all_spaces():
    """Connects to Confluence, uses filtered spaces from pickle files, and checks each for empty pages."""
    global min_pages, max_pages, date_filter, cached_spaces
    
    print(f"Connecting to Confluence at: {CONFLUENCE_URL}")
    print(f"Using username: {CONFLUENCE_USERNAME}")
    print(f"Checking spaces for empty pages")
    if not VERIFY_SSL:
        print("SSL verification is DISABLED.")
    print("-" * 60)
    
    # Show filter menu and ensure data is loaded
    show_filter_menu()
    spaces = cached_spaces  # Use already loaded and filtered spaces
    
    # Filter to only non-personal spaces
    filtered_spaces = [s for s in spaces if not s.get('space_key', '').startswith('~')]
    print(f"Focusing on {len(filtered_spaces)} non-personal spaces from {len(spaces)} total spaces.")

    # Open file for writing deletable pages
    deletable_file = open("deletable_pages.txt", "w")
    deletable_file.write("# Deletable pages in Confluence\n")
    deletable_file.write(f"# Generated: {datetime.now()}\n")
    deletable_file.write(f"# Filters: {get_filter_info()}\n")
    deletable_file.write("# Format: SPACE,URL\n\n")

    try:
        # Create a session for all requests
        session = create_session()
        
        # Verify connection by getting current user info
        user_info = get_current_user(session)
        print(f"Successfully connected as: {user_info.get('displayName', CONFLUENCE_USERNAME)}")
        print("-" * 60)

        # Process each space from the pickled data
        total_eligible_pages = 0
        for idx, space in enumerate(filtered_spaces, 1):
            space_key = space.get('space_key')
            space_name = space.get('space_name', space_key)
            
            print(f"\n[{idx}/{len(filtered_spaces)}] Processing space: {space_name} (key: {space_key})")
            print("-" * 60)
            
            try:
                # Fetch pages with expanded data
                print(f"Fetching pages from space '{space_key}' (this might take a while)...")
                pages = get_all_pages_from_space(
                    session,
                    space_key,
                    limit=100,
                    expand='body.storage,version,operations,attachments'
                )
                
                print(f"Found {len(pages)} pages in total. Checking each...")
                
                space_eligible_count = 0
                for page_idx, page in enumerate(pages, 1):
                    page_id = page['id']
                    page_title = page['title']
                    page_link = page.get('_links', {}).get('webui', '')
                    if not page_link and 'base' in page.get('_links', {}):
                        page_link = page['_links']['base'] + page.get('_links',{}).get('webui','')
                    
                    # Output progress for larger spaces
                    if len(pages) > 10 and page_idx % 10 == 0:
                        print(f"  Progress: Checked {page_idx}/{len(pages)} pages in space '{space_key}'")
                    
                    # 1. Check for empty content
                    storage_value = page.get('body', {}).get('storage', {}).get('value', '').strip()
                    is_content_empty = not storage_value
                    
                    if not is_content_empty:
                        continue  # Skip to next page if content is not empty
                    
                    # 2. Check for delete permission
                    can_delete = False
                    for operation in page.get('operations', []):
                        if operation.get('operation') == 'delete':
                            can_delete = True
                            break
                    
                    if not can_delete:
                        continue  # Skip to next page if we don't have delete permission
                    
                    # 3. Check for attachments
                    has_no_attachments = False
                    try:
                        attachments = page.get('attachments', {}).get('results', [])
                        has_no_attachments = len(attachments) == 0
                    except Exception as e:
                        print(f"  Warning: Failed to check attachments for page '{page_title}': {e}")
                        has_no_attachments = False  # Assume it has attachments to be safe
                    
                    # Combine checks
                    if is_content_empty and has_no_attachments and can_delete:
                        full_link = f"{CONFLUENCE_URL}{page_link}"
                        print(f"  YES: Page '{page_title}' (ID: {page_id}) is empty and deletable.")
                        print(f"       Link: {full_link}")
                        # Write to file: SPACE,URL
                        deletable_file.write(f"{space_key},{full_link}\n")
                        space_eligible_count += 1
                        total_eligible_pages += 1
                
                print(f"\nFinished checking space '{space_key}'. Found {space_eligible_count} eligible pages for deletion.")
            
            except Exception as e:
                print(f"\nError processing space '{space_key}': {e}")
        
        # Final summary
        print("\n" + "=" * 60)
        print(f"SUMMARY: Checked {len(filtered_spaces)} spaces after applying filters.")
        print(f"Found {total_eligible_pages} pages that are empty and deletable across all spaces.")
        print(f"Results written to: deletable_pages.txt")
        print("=" * 60)
    
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        sys.exit(1)
    finally:
        deletable_file.close()

if __name__ == "__main__":
    # Check if any arguments were provided
    has_args = any([args.space_key, args.page_id, args.list_spaces, args.check_all_spaces])
    
    if not has_args:
        # No arguments provided, show interactive menu
        print("\n" + "="*80)
        print("""
    ┌─────────────────────────────────────────────────────────┐
    │                                                         │
    │   CONFLUENCE EMPTY PAGES TOOL                           │
    │   =========================                             │
    │                                                         │
    │   Find and manage empty Confluence pages                │
    │                                                         │
    └─────────────────────────────────────────────────────────┘
    """)    
        print("="*80 + "\n")
        
        show_main_menu()
    else:
        # Process command line arguments
        if args.list_spaces:
            list_spaces(args.list_spaces)
        elif args.space_key:
            check_pages_in_space(args.space_key)
        elif args.page_id:
            check_single_page(args.page_id)
        elif args.check_all_spaces:
            check_all_spaces()
        else:
            print("Error: No action specified. Use --space-key, --page-id, --list-spaces, or --check-all-spaces.")
            parser.print_help()
            sys.exit(1)