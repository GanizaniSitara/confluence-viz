\
import requests
import time
import warnings
import urllib3 # Import urllib3 to reference its warning class
import argparse # Import argparse for command-line arguments (optional for menu)
import random # Import random for jitter
import os
import pickle
import datetime # Import datetime for date parsing and timestamp operations
import re # Import re for regular expressions
from config_loader import load_confluence_settings
from urllib.parse import urlparse, parse_qs
import pprint # Import pprint for pretty printing
import urllib.parse # Make sure urllib.parse is imported

# --- Suppress InsecureRequestWarning ---
# WARNING: Disabling SSL verification is INSECURE and should only be done
# in controlled environments where you understand the risks.
warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made to',
                        category=urllib3.exceptions.InsecureRequestWarning)
# ---------------------------------------

# --- Configuration ---
try:
    settings = load_confluence_settings()
    CONFLUENCE_BASE_URL = settings['api_base_url'].rstrip('/rest/api') # Ensure no trailing /rest/api
    USERNAME = settings['username']
    PASSWORD = settings['password']
    VERIFY_SSL = settings['verify_ssl']
    print("Settings loaded successfully.")
    print(f"Base URL: {CONFLUENCE_BASE_URL}")
    print(f"Username: {USERNAME}")
    print(f"Verify SSL: {VERIFY_SSL}")
except Exception as e:
    print(f"Error loading settings: {e}")
    print("Please ensure settings.ini exists and is correctly formatted.")
    exit(1) # Exit if settings can't be loaded
# ---------------------

# Define API endpoints dynamically
API_BASE = f'{CONFLUENCE_BASE_URL}/rest/api'
API_CONTENT_ENDPOINT = f'{API_BASE}/content'
API_USER_ENDPOINT = f'{API_BASE}/user' # May need this later

def make_api_request(url, params=None, max_retries=5):
    """
    Makes an API request, handling authentication, 429 rate limiting, and SSL verification.
    """
    retries = 0
    while retries < max_retries:
        query_params = '&'.join([f"{k}={v}" for k, v in (params or {}).items()])
        request_url = f"{url}?{query_params}" if query_params else url
        print(f"REST Request: GET {request_url}")

        try:
            auth = None
            if USERNAME and PASSWORD:
                auth = (USERNAME, PASSWORD)

            response = requests.get(url, params=params, verify=VERIFY_SSL, auth=auth)
            print(f"Response Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    print("Error: Response was not valid JSON.")
                    print(f"Response text: {response.text[:500]}...") # Show beginning of text
                    return None # Treat as failure

            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                wait_time = int(retry_after) if retry_after else (2 ** retries) * 5
                jitter = random.uniform(0, 1) * 2
                wait_time += jitter
                print(f"Rate limited (429). Server requested Retry-After: {retry_after or 'Not specified'}")
                print(f"Waiting for {wait_time:.2f} seconds before retry {retries + 1}/{max_retries}")
                time.sleep(wait_time)
                retries += 1
                continue

            elif response.status_code in [401, 403]:
                 print(f"Error: Authentication failed ({response.status_code}). Check username/password in settings.ini.")
                 print(f"Response body: {response.text}")
                 return None # Authentication errors are unlikely to succeed on retry

            elif response.status_code == 404:
                 print(f"Error: Resource not found ({response.status_code}) for URL: {request_url}")
                 print(f"Response body: {response.text}")
                 return None # Not found errors won't succeed on retry

            else:
                print(f"Error: Received status code {response.status_code} for {url}")
                print(f"Response body: {response.text}")
                # Consider retrying for 5xx server errors? For now, fail.
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            # Basic handling, could implement retry for network errors too
            # Wait briefly before retrying network errors
            wait_time = (2 ** retries) * 2 # Shorter backoff for network issues
            print(f"Network error. Waiting {wait_time} seconds before retry {retries + 1}/{max_retries}")
            time.sleep(wait_time)
            retries += 1
            # return None # Fail immediately for now

    print(f"Failed to fetch data from {url} after {max_retries} retries.")
    return None

def get_page_id_from_url(url):
    """Extracts the page ID from various Confluence URL formats."""
    original_url = url # Keep for logging
    try:
        # Handle potential fragments (#) or query strings (?src=) by taking the base URL path
        # Although urlparse handles fragments, let's clean common query params if needed
        if "?src=" in url:
            url = url.split("?src=")[0]
        if "#" in url:
            url = url.split("#")[0]

        parsed_url = urlparse(url)
        path_parts = [part for part in parsed_url.path.split('/') if part] # Split path and remove empty parts

        # --- Direct ID patterns --- 

        # Pattern 1: /pages/<pageId>/...
        if 'pages' in path_parts:
            try:
                pages_index = path_parts.index('pages')
                if pages_index + 1 < len(path_parts) and path_parts[pages_index + 1].isdigit():
                    print(f"Extracted Page ID {path_parts[pages_index + 1]} using /pages/ pattern.")
                    return path_parts[pages_index + 1]
            except (ValueError, IndexError):
                pass # Continue to next pattern

        # Pattern 2: /wiki/spaces/<spaceKey>/pages/<pageId>/...
        if 'wiki' in path_parts and 'spaces' in path_parts and 'pages' in path_parts:
             try:
                pages_index = path_parts.index('pages')
                if pages_index + 1 < len(path_parts) and path_parts[pages_index + 1].isdigit():
                    print(f"Extracted Page ID {path_parts[pages_index + 1]} using /wiki/spaces/.../pages/ pattern.")
                    return path_parts[pages_index + 1]
             except (ValueError, IndexError):
                 pass

        # Pattern 3: Query parameter ?pageId=<pageId>
        query_params = parse_qs(parsed_url.query)
        if 'pageId' in query_params and query_params['pageId'][0].isdigit():
            print(f"Extracted Page ID {query_params['pageId'][0]} using ?pageId= query parameter.")
            return query_params['pageId'][0]

        # --- Indirect ID patterns (require API lookup) --- 

        # Pattern 4: /display/<spaceKey>/<pageTitle>
        # Also handles /wiki/display/<spaceKey>/<pageTitle>
        display_indices = [i for i, part in enumerate(path_parts) if part.lower() == 'display']
        if display_indices:
            display_index = display_indices[0] # Take the first 'display'
            if display_index + 2 < len(path_parts):
                space_key = path_parts[display_index + 1]
                # Page title might contain slashes if it's nested, join remaining parts
                raw_title = '/'.join(path_parts[display_index + 2:])
                # Titles in URLs are URL-encoded (e.g., spaces are '+')
                page_title = urllib.parse.unquote(raw_title).replace('+', ' ')
                print(f"Detected /display/ pattern. SpaceKey: '{space_key}', Title: '{page_title}'. Attempting API lookup...")
                # Use API to find page ID by space and title
                params = {
                    'spaceKey': space_key,
                    'title': page_title,
                    'limit': 1 # We only need one result to get the ID
                }
                content_data = make_api_request(API_CONTENT_ENDPOINT, params=params)
                if content_data and 'results' in content_data and len(content_data['results']) > 0:
                    page_id = content_data['results'][0].get('id')
                    if page_id:
                        print(f"Found Page ID {page_id} via API lookup for title '{page_title}' in space '{space_key}'.")
                        return page_id
                    else:
                        print(f"API lookup succeeded but no page ID found for title '{page_title}' in space '{space_key}'.")
                else:
                    print(f"API lookup failed or returned no results for title '{page_title}' in space '{space_key}'.")
                # If API lookup fails, fall through to other patterns or return None

        # Pattern 5: Tiny URLs like /x/<base64>
        if len(path_parts) == 1 and len(path_parts[0]) > 1 and path_parts[0].lower() == 'x':
            print("Warning: Tiny URLs (/x/...) cannot be directly resolved to a Page ID by this script.")
            print("Please use the full page URL after the redirect.")
            return None

    except Exception as e:
        print(f"Error parsing URL '{original_url}': {e}")
        return None

    print(f"Could not extract a numeric Page ID from the URL: {original_url}")
    return None

def check_page_user_status():
    """
    Prompts for a page URL and checks the status of its creator and contributors.
    """
    page_url_input = input("Enter the Confluence Page URL: ").strip()
    page_id = get_page_id_from_url(page_url_input)

    if not page_id:
        # Error message is printed within get_page_id_from_url
        return

    print(f"\nExtracted Page ID: {page_id}. Checking user status...")
    # Expand history to get creator and contributors (publishers)
    # Also expand version to potentially get the last modifier if history isn't enough
    params = {'expand': 'history,history.lastUpdated,history.contributors.publishers.users,version'}
    page_api_url = f"{API_CONTENT_ENDPOINT}/{page_id}"

    page_data = make_api_request(page_api_url, params=params)

    if not page_data:
        print(f"Could not retrieve data for Page ID {page_id} (from URL: {page_url_input}).")
        return

    users_found = {} # Store user info: { 'username': {'displayName': '...', 'type': '...', 'active': True/False/Unknown} }

    # Regex to match the pattern 'user-' followed by hex digits (case-insensitive)
    inactive_user_pattern = re.compile(r'^user-[0-9a-f]+$', re.IGNORECASE)

    def is_user_active(user_data):
        """Checks if a user appears active based on displayName and username patterns."""
        displayName = user_data.get('displayName', '')
        username = user_data.get('username', '')

        # Check 1: Display name contains '(unknown)'
        if '(unknown)' in displayName.lower():
            return False, "displayName contains '(unknown)'"

        # Check 2: Username matches 'user-<hex>' pattern
        if inactive_user_pattern.match(username):
            return False, f"username matches '{inactive_user_pattern.pattern}'"

        # If neither pattern matches, assume active
        return True, "Assumed active (no inactive patterns matched)"

    # --- Check Creator ---
    creator = None
    if 'history' in page_data and 'createdBy' in page_data['history']:
        creator = page_data['history']['createdBy']
        username = creator.get('username', 'UNKNOWN_USERNAME')
        displayName = creator.get('displayName', 'UNKNOWN_DISPLAYNAME')
        user_type = creator.get('type', 'UNKNOWN_TYPE')

        is_active, reason = is_user_active(creator)

        users_found[username] = {
            'displayName': displayName,
            'type': user_type,
            'active': is_active,
            'reason': reason,
            'role': 'Creator'
        }
        print(f"Creator Found: {displayName} (Username: {username}, Type: {user_type}, Active: {is_active}, Reason: {reason})")
    else:
        print("Creator information not found in history.")

    # --- Check Last Modifier (from version) ---
    if 'version' in page_data and 'by' in page_data['version']:
         modifier = page_data['version']['by']
         username = modifier.get('username', 'UNKNOWN_USERNAME')
         if username not in users_found: # Only add if not already the creator
             displayName = modifier.get('displayName', 'UNKNOWN_DISPLAYNAME')
             user_type = modifier.get('type', 'UNKNOWN_TYPE')
             is_active, reason = is_user_active(modifier)
             users_found[username] = {
                 'displayName': displayName,
                 'type': user_type,
                 'active': is_active,
                 'reason': reason,
                 'role': 'Last Modifier'
             }
             print(f"Last Modifier Found: {displayName} (Username: {username}, Type: {user_type}, Active: {is_active}, Reason: {reason})")

    # --- Check Contributors (Publishers) ---
    contributors = []
    if ('history' in page_data and
        'contributors' in page_data['history'] and
        'publishers' in page_data['history']['contributors'] and
        'users' in page_data['history']['contributors']['publishers']):

        contributors = page_data['history']['contributors']['publishers']['users']
        print(f"Found {len(contributors)} contributors (publishers).")

        for user in contributors:
            username = user.get('username', 'UNKNOWN_USERNAME')
            if username not in users_found: # Only add if not creator or last modifier
                displayName = user.get('displayName', 'UNKNOWN_DISPLAYNAME')
                user_type = user.get('type', 'UNKNOWN_TYPE')
                is_active, reason = is_user_active(user)
                users_found[username] = {
                    'displayName': displayName,
                    'type': user_type,
                    'active': is_active,
                    'reason': reason,
                    'role': 'Contributor'
                }
                print(f"Contributor Found: {displayName} (Username: {username}, Type: {user_type}, Active: {is_active}, Reason: {reason})")
            elif users_found[username]['role'] != 'Creator': # Update role if they were just 'Last Modifier'
                 # Avoid duplicating 'Contributor' if already added
                 if 'Contributor' not in users_found[username]['role'].split('/'):
                     users_found[username]['role'] += '/Contributor'


    # --- Summary ---
    print("\n--- User Status Summary ---")
    if not users_found:
        print("No user information could be extracted for this page.")
        return

    active_count = 0
    inactive_count = 0
    for username, info in users_found.items():
        status = "ACTIVE" if info['active'] else f"INACTIVE (Reason: {info['reason']})"
        print(f"- {info['displayName']} (Username: {username}, Role: {info['role']}): {status}")
        if info['active']:
            active_count += 1
        else:
            inactive_count += 1

    print(f"\nTotal Unique Users Found: {len(users_found)}")
    print(f"Active Users: {active_count}")
    print(f"Inactive Users (heuristic): {inactive_count}")
    print("--------------------------")


def view_space_pickle_from_folder(folder_name):
    """
    Prompts for a space short code, loads the corresponding pickle file
    from the specified folder, and prints its contents.

    Args:
        folder_name (str): The name of the folder to look for pickles in (e.g., 'temp', 'temp_counter').
    """
    space_code = input(f"Enter the space short code (e.g., ITCOGLOB, REINASSE) to load from '{folder_name}/': ").strip().upper()
    if not space_code:
        print("No space code entered.")
        return

    # Construct the expected pickle filename
    pickle_filename = f"{space_code}.pkl"
    pickle_filepath = os.path.join(folder_name, pickle_filename)

    print(f"Attempting to load: {pickle_filepath}")

    if not os.path.exists(pickle_filepath):
        print(f"Error: Pickle file not found at {pickle_filepath}")
        # Try listing directory contents for debugging help
        try:
            print(f"Files in {folder_name}/ directory:")
            folder_files = os.listdir(folder_name)
            if folder_files:
                print("\n".join(folder_files))
            else:
                print("(Directory is empty)")
        except FileNotFoundError:
            print(f"Error: {folder_name}/ directory not found.")
        return

    try:
        with open(pickle_filepath, 'rb') as f:
            data = pickle.load(f)

        print(f"\n--- Content of {pickle_filename} from {folder_name}/ ---")
        # Use pprint for potentially large/complex data structures
        pprint.pprint(data)
        print("-------------------------------------------------")

    except pickle.UnpicklingError as e:
        print(f"Error: Failed to unpickle {pickle_filename}. File might be corrupted or not a pickle file.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while reading {pickle_filename}: {e}")


def show_main_menu():
    """
    Displays the main menu and handles user input.
    """
    while True:
        print("\n==== Confluence Space Explorer Menu ====")
        print("1. Check User Status for a Page URL")
        print("2. View Space Pickle Content (from temp/)") # Updated text
        print("3. View Space Pickle Content (from temp_counter/)") # New option
        # Add more options here later
        print("Q. Quit")
        choice = input("Select option: ").strip().lower()

        if choice == '1':
            check_page_user_status()
        elif choice == '2': # Handle updated option 2
            view_space_pickle_from_folder("temp") # Call generalized function
        elif choice == '3': # Handle new option 3
            view_space_pickle_from_folder("temp_counter") # Call generalized function
        elif choice == 'q':
            print("Exiting.")
            break
        else:
            print("Invalid option. Please try again.")

if __name__ == "__main__":
    print("Starting Confluence Space Explorer...")
    # Basic check if settings seem okay before showing menu
    if not CONFLUENCE_BASE_URL or not USERNAME:
         print("\nWarning: Confluence URL or Username might be missing in settings.")
         print("API calls may fail. Please check settings.ini.")

    show_main_menu()
