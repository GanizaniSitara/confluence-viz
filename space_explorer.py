# description: Explores Confluence spaces.

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
import glob # Import glob for file pattern matching

# --- Suppress InsecureRequestWarning ---
# WARNING: Disabling SSL verification is INSECURE and should only be done
# in controlled environments where you understand the risks.
warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made to',
                        category=urllib3.exceptions.InsecureRequestWarning)
# ---------------------------------------

# --- Configuration ---
try:
    settings = load_confluence_settings()
    CONFLUENCE_API_BASE_URL = settings['base_url'] # Changed from api_base_url
    API_ENDPOINT = "/rest/api" # Define the API endpoint suffix
    USERNAME = settings['username']
    PASSWORD = settings['password']
    VERIFY_SSL = settings['verify_ssl']
    print("Settings loaded successfully.")
    print(f"Base URL: {CONFLUENCE_API_BASE_URL}")
    print(f"Username: {USERNAME}")
    print(f"Verify SSL: {VERIFY_SSL}")
except Exception as e:
    print(f"Error loading settings: {e}")
    print("Please ensure settings.ini exists and is correctly formatted.")
    exit(1) # Exit if settings can't be loaded
# ---------------------

# Define API endpoints dynamically
API_BASE = f'{CONFLUENCE_API_BASE_URL}{API_ENDPOINT}'
API_CONTENT_ENDPOINT = f'{API_BASE}/content'
API_USER_ENDPOINT = f'{API_BASE}/user' # May need this later

def make_api_request(url, params=None, max_retries=10):
    """
    Makes an API request, handling authentication, 429 rate limiting, and SSL verification.
    Increased retries to 10 and properly respects Retry-After header.
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
                if retry_after:
                    # Retry-After can be in seconds or HTTP-date format
                    try:
                        # Try parsing as integer (seconds)
                        wait_time = float(retry_after)
                    except ValueError:
                        # If not a number, might be HTTP-date, default to exponential backoff
                        wait_time = min((2 ** retries) * 5, 300)  # Cap at 5 minutes
                else:
                    # No Retry-After header, use exponential backoff with cap
                    wait_time = min((2 ** retries) * 5, 300)  # Cap at 5 minutes
                
                # Add small jitter to prevent thundering herd
                jitter = random.uniform(0, min(wait_time * 0.1, 5))  # Max 10% jitter or 5 seconds
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

            elif response.status_code >= 500:
                # Server errors might be temporary, retry them
                wait_time = min((2 ** retries) * 3, 60)  # Shorter wait for server errors, cap at 1 minute
                print(f"Server error ({response.status_code}). Waiting {wait_time} seconds before retry {retries + 1}/{max_retries}")
                time.sleep(wait_time)
                retries += 1
                continue

            else:
                print(f"Error: Received status code {response.status_code} for {url}")
                print(f"Response body: {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            # Wait briefly before retrying network errors
            wait_time = min((2 ** retries) * 2, 30)  # Cap at 30 seconds for network errors
            print(f"Network error. Waiting {wait_time} seconds before retry {retries + 1}/{max_retries}")
            time.sleep(wait_time)
            retries += 1

    print(f"Failed to fetch data from {url} after {max_retries} retries.")
    return None

def get_page_id_from_url(url):
    """Extracts the page ID from various Confluence URL formats."""
    original_url = url # Keep for logging
    try:
        # Handle potential fragments (#) or query strings (?src=) by taking the base URL path
        if "?src=" in url:
            url = url.split("?src=")[0]
        if "#" in url:
            url = url.split("#")[0]

        parsed_url = urlparse(url)
        path_parts = [part for part in parsed_url.path.split('/') if part] # Split path and remove empty parts

        # --- Direct ID patterns ---
        if 'pages' in path_parts:
            try:
                pages_index = path_parts.index('pages')
                if pages_index + 1 < len(path_parts) and path_parts[pages_index + 1].isdigit():
                    print(f"Extracted Page ID {path_parts[pages_index + 1]} using /pages/ pattern.")
                    return path_parts[pages_index + 1]
            except (ValueError, IndexError):
                pass

        if 'wiki' in path_parts and 'spaces' in path_parts and 'pages' in path_parts:
             try:
                pages_index = path_parts.index('pages')
                if pages_index + 1 < len(path_parts) and path_parts[pages_index + 1].isdigit():
                    print(f"Extracted Page ID {path_parts[pages_index + 1]} using /wiki/spaces/.../pages/ pattern.")
                    return path_parts[pages_index + 1]
             except (ValueError, IndexError):
                 pass

        query_params = parse_qs(parsed_url.query)
        if 'pageId' in query_params and query_params['pageId'][0].isdigit():
            print(f"Extracted Page ID {query_params['pageId'][0]} using ?pageId= query parameter.")
            return query_params['pageId'][0]

        # --- Indirect ID patterns (require API lookup) ---
        display_indices = [i for i, part in enumerate(path_parts) if part.lower() == 'display']
        if display_indices:
            display_index = display_indices[0]
            if display_index + 2 < len(path_parts):
                space_key = path_parts[display_index + 1]
                raw_title = '/'.join(path_parts[display_index + 2:])
                page_title = urllib.parse.unquote(raw_title).replace('+', ' ')
                print(f"Detected /display/ pattern. SpaceKey: '{space_key}', Title: '{page_title}'. Attempting API lookup...")
                params = {
                    'spaceKey': space_key,
                    'title': page_title,
                    'limit': 1
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
        return

    print(f"\nExtracted Page ID: {page_id}. Checking user status...")
    params = {'expand': 'history,history.lastUpdated,history.contributors.publishers.users,version'}
    page_api_url = f"{API_CONTENT_ENDPOINT}/{page_id}"

    page_data = make_api_request(page_api_url, params=params)

    if not page_data:
        print(f"Could not retrieve data for Page ID {page_id} (from URL: {page_url_input}).")
        return

    users_found = {}
    inactive_user_pattern = re.compile(r'^user-[0-9a-f]+$', re.IGNORECASE)

    def is_user_active(user_data):
        displayName = user_data.get('displayName', '')
        username = user_data.get('username', '')
        if '(unknown)' in displayName.lower():
            return False, "displayName contains '(unknown)'"
        if inactive_user_pattern.match(username):
            return False, f"username matches '{inactive_user_pattern.pattern}'"
        return True, "Assumed active (no inactive patterns matched)"

    if 'history' in page_data and 'createdBy' in page_data['history']:
        creator = page_data['history']['createdBy']
        username = creator.get('username', 'UNKNOWN_USERNAME')
        displayName = creator.get('displayName', 'UNKNOWN_DISPLAYNAME')
        user_type = creator.get('type', 'UNKNOWN_TYPE')
        is_active, reason = is_user_active(creator)
        users_found[username] = {'displayName': displayName, 'type': user_type, 'active': is_active, 'reason': reason, 'role': 'Creator'}
        print(f"Creator Found: {displayName} (Username: {username}, Type: {user_type}, Active: {is_active}, Reason: {reason})")
    else:
        print("Creator information not found in history.")

    if 'version' in page_data and 'by' in page_data['version']:
         modifier = page_data['version']['by']
         username = modifier.get('username', 'UNKNOWN_USERNAME')
         if username not in users_found:
             displayName = modifier.get('displayName', 'UNKNOWN_DISPLAYNAME')
             user_type = modifier.get('type', 'UNKNOWN_TYPE')
             is_active, reason = is_user_active(modifier)
             users_found[username] = {'displayName': displayName, 'type': user_type, 'active': is_active, 'reason': reason, 'role': 'Last Modifier'}
             print(f"Last Modifier Found: {displayName} (Username: {username}, Type: {user_type}, Active: {is_active}, Reason: {reason})")

    contributors = []
    if ('history' in page_data and 'contributors' in page_data['history'] and
        'publishers' in page_data['history']['contributors'] and
        'users' in page_data['history']['contributors']['publishers']):
        contributors = page_data['history']['contributors']['publishers']['users']
        print(f"Found {len(contributors)} contributors (publishers).")
        for user in contributors:
            username = user.get('username', 'UNKNOWN_USERNAME')
            if username not in users_found:
                displayName = user.get('displayName', 'UNKNOWN_DISPLAYNAME')
                user_type = user.get('type', 'UNKNOWN_TYPE')
                is_active, reason = is_user_active(user)
                users_found[username] = {'displayName': displayName, 'type': user_type, 'active': is_active, 'reason': reason, 'role': 'Contributor'}
                print(f"Contributor Found: {displayName} (Username: {username}, Type: {user_type}, Active: {is_active}, Reason: {reason})")
            elif users_found[username]['role'] != 'Creator':
                 if 'Contributor' not in users_found[username]['role'].split('/'):
                     users_found[username]['role'] += '/Contributor'

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
    """
    space_code = input(f"Enter the space short code (e.g., ITCOGLOB, REINASSE) to load from '{folder_name}/': ").strip().upper()
    if not space_code:
        print("No space code entered.")
        return

    pickle_filename = f"{space_code}.pkl"
    pickle_filepath = os.path.join(folder_name, pickle_filename)

    print(f"Attempting to load: {pickle_filepath}")

    if not os.path.exists(pickle_filepath):
        print(f"Error: Pickle file not found at {pickle_filepath}")
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
        pprint.pprint(data)
        print("-------------------------------------------------")
    except pickle.UnpicklingError as e:
        print(f"Error: Failed to unpickle {pickle_filename}. File might be corrupted or not a pickle file.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while reading {pickle_filename}: {e}")

def view_space_pickle_summary(folder_name="temp_space_explorer_body"):
    """
    Prompts for a space short code, loads the corresponding pickle file
    from the specified folder (defaults to body included), removes the body content,
    and prints a summary.
    """
    space_code = input(f"Enter the space short code (e.g., ITCOGLOB) to load summary from '{folder_name}/': ").strip().upper()
    if not space_code:
        print("No space code entered.")
        return

    pickle_filename = f"{space_code}.pkl"
    pickle_filepath = os.path.join(folder_name, pickle_filename)

    print(f"Attempting to load: {pickle_filepath}")

    if not os.path.exists(pickle_filepath):
        print(f"Error: Pickle file not found at {pickle_filepath}")
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
        if 'pages' in data:
            for page_id, page_data in data['pages'].items():
                if 'body_storage' in page_data:
                    del page_data['body_storage']
        print(f"\n--- Summary Content of {pickle_filename} from {folder_name}/ (Body Excluded) ---")
        pprint.pprint(data)
        print("---------------------------------------------------------------------")
    except pickle.UnpicklingError as e:
        print(f"Error: Failed to unpickle {pickle_filename}. File might be corrupted or not a pickle file.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while reading {pickle_filename}: {e}")

def get_all_non_personal_spaces():
    """Fetches a list of all non-personal space keys from Confluence."""
    all_spaces = []
    limit = 100 # Max limit might vary, 100 is often safe
    start = 0
    print("Fetching list of all spaces...")
    while True:
        space_list_url = f"{API_BASE}/space"
        params = {
            'limit': limit,
            'start': start,
            'type': 'global' # Filter for global (non-personal) spaces
        }
        response_data = make_api_request(space_list_url, params=params)

        if not response_data or 'results' not in response_data:
            print(f"Error: Failed to fetch space list at start={start}. Aborting.")
            return None # Indicate failure

        results = response_data.get('results', [])
        all_spaces.extend(results)
        print(f"Fetched {len(results)} spaces. Total so far: {len(all_spaces)}")

        # Check if this is the last page of results
        size = response_data.get('size', 0)
        if size < limit:
            print("Reached the end of the space list.")
            break
        else:
            start += size # Prepare for the next page

    # Extract just the keys
    space_keys = [space['key'] for space in all_spaces if space.get('key')]
    print(f"Found {len(space_keys)} non-personal spaces.")
    return space_keys

def pickle_space_details(space_key, include_body=True):
    """
    Fetches detailed information for all pages in a space using efficient batch requests
    and pickles it into the appropriate directory based on include_body.
    Overwrites existing file. Returns True on success, False on failure.

    Args:
        space_key (str): The Confluence space key.
        include_body (bool): Whether to include the 'body.storage' content in the fetch and pickle.
    """
    pickle_dir = "temp_space_explorer_body" if include_body else "temp_space_explorer_no_body"

    print(f"Starting to fetch details efficiently for space: {space_key} (Include Body: {include_body}) -> Saving to '{pickle_dir}'")
    space_data = {
        'space_key': space_key,
        'retrieved_at': datetime.datetime.now().isoformat(),
        'pages': {}
    }
    processed_count = 0
    failed_pages = []
    total_pages_fetched = 0

    limit = 25 if include_body else 50
    start = 0
    base_expand_fields = [
        'version', 'history', 'history.lastUpdated', 'history.createdBy',
        'history.contributors.publishers.users', 'metadata.labels', 'ancestors',
        'restrictions.read', 'restrictions.update', 'space',
        'metadata.attachments', 'children.page'
    ]
    if include_body:
        base_expand_fields.append('body.storage')

    expand_fields = ",".join(base_expand_fields)

    while True:
        space_content_url = f"{API_BASE}/space/{space_key}/content/page"
        params = {
            'limit': limit,
            'start': start,
            'expand': expand_fields
        }
        print(f"Fetching pages with details from space {space_key} (start={start}, limit={limit})...")
        response_data = make_api_request(space_content_url, params=params)

        if not response_data or 'results' not in response_data:
            print(f"Error: Failed to fetch page list with details for space {space_key} at start={start}.")
            if total_pages_fetched == 0:
                print("Could not fetch initial page list. Aborting fetch for this space.")
                return False
            else:
                print("Continuing with pages processed so far for this space.")
                break

        results = response_data.get('results', [])
        current_batch_size = len(results)
        total_pages_fetched += current_batch_size
        print(f"Fetched {current_batch_size} pages with details. Total so far: {total_pages_fetched}")

        for page_detail_data in results:
            page_id = page_detail_data.get('id')
            page_title = page_detail_data.get('title', 'Untitled')
            if not page_id:
                print(f"Warning: Skipping page data with no ID: {page_detail_data.get('_links', {}).get('self', 'Unknown URL')}")
                continue

            try:
                version_info = page_detail_data.get('version', {})
                history_info = page_detail_data.get('history', {})
                last_updated_info = history_info.get('lastUpdated', {})
                last_modifier_info = version_info.get('by', {})
                last_updated_by_user_obj = last_updated_info.get('by', last_modifier_info)
                last_updated_timestamp = last_updated_info.get('when') or version_info.get('when')
                creator_info = history_info.get('createdBy', {})
                contributors_list = history_info.get('contributors', {}).get('publishers', {}).get('users', [])
                metadata = page_detail_data.get('metadata', {})
                labels = [label['name'] for label in metadata.get('labels', {}).get('results', [])]
                ancestors = [{'id': anc.get('id'), 'title': anc.get('title')} for anc in page_detail_data.get('ancestors', [])]
                restrictions = page_detail_data.get('restrictions', {})
                read_restrictions = restrictions.get('read', {}).get('restrictions', {})
                update_restrictions = restrictions.get('update', {}).get('restrictions', {})
                space_info = page_detail_data.get('space', {})

                attachments_metadata = []
                for att in metadata.get('attachments', {}).get('results', []):
                    attachments_metadata.append({
                        'id': att.get('id'),
                        'title': att.get('title'),
                        'fileSize': att.get('extensions', {}).get('fileSize'),
                        'mediaType': att.get('extensions', {}).get('mediaType'),
                        'createdDate': att.get('version', {}).get('when'),
                        'createdBy': att.get('version', {}).get('by', {}).get('username'),
                        'downloadLink': att.get('_links', {}).get('download')
                    })

                child_page_ids = [child.get('id') for child in page_detail_data.get('children', {}).get('page', {}).get('results', []) if child.get('id')]

                page_entry = {
                    'title': page_title,
                    'last_updated': last_updated_timestamp,
                    'last_updated_by_username': last_updated_by_user_obj.get('username'),
                    'last_updated_by_displayname': last_updated_by_user_obj.get('displayName'),
                    'version_number': version_info.get('number'),
                    'creator_username': creator_info.get('username'),
                    'creator_displayname': creator_info.get('displayName'),
                    'contributors_usernames': [c.get('username') for c in contributors_list if c.get('username')],
                    'labels': labels,
                    'ancestors': ancestors,
                    'read_restricted_users': [u.get('username') for u in read_restrictions.get('user', {}).get('results', [])],
                    'read_restricted_groups': [g.get('name') for g in read_restrictions.get('group', {}).get('results', [])],
                    'update_restricted_users': [u.get('username') for u in update_restrictions.get('user', {}).get('results', [])],
                    'update_restricted_groups': [g.get('name') for g in update_restrictions.get('group', {}).get('results', [])],
                    'space_id': space_info.get('id'),
                    'space_name': space_info.get('name'),
                    'attachments': attachments_metadata,
                    'child_page_ids': child_page_ids
                }

                if include_body:
                    body_content = page_detail_data.get('body', {}).get('storage', {}).get('value', '')
                    page_entry['body_storage'] = body_content

                space_data['pages'][page_id] = page_entry
                processed_count += 1
            except Exception as e:
                print(f"Error processing details for Page ID {page_id} ('{page_title}'): {e}")
                failed_pages.append({'id': page_id, 'title': page_title, 'reason': f'Processing error: {e}'})

        if current_batch_size < limit:
            print("Reached the end of pages for this space.")
            break
        else:
            start += current_batch_size

    print(f"\nFinished fetching details for {space_key}. Processed {processed_count}/{total_pages_fetched} pages successfully.")
    if failed_pages:
        print(f"Failed to process {len(failed_pages)} pages for {space_key}:")
        for i, failed in enumerate(failed_pages):
            if i < 10:
                print(f"  - ID: {failed['id']}, Title: '{failed['title']}', Reason: {failed['reason']}")
            elif i == 10:
                print(f"  ... (plus {len(failed_pages) - 10} more)")
                break

    try:
        os.makedirs(pickle_dir, exist_ok=True)
        pickle_filename = f"{space_key}.pkl"
        pickle_filepath = os.path.join(pickle_dir, pickle_filename)

        print(f"\nAttempting to save data to: {pickle_filepath}")
        with open(pickle_filepath, 'wb') as f:
            pickle.dump(space_data, f)
        print(f"Successfully saved data for space {space_key} to {pickle_filepath}")
        return True

    except Exception as e:
        print(f"Error: Failed to save pickle file {pickle_filepath}: {e}")
        return False

def pickle_all_spaces(include_body):
    """
    Iterates through all non-personal spaces and pickles their details,
    skipping spaces that already have a pickle file in the target directory.

    Args:
        include_body (bool): Whether to include body content.
    """
    space_keys = get_all_non_personal_spaces()
    if space_keys is None:
        print("Could not retrieve space list. Aborting bulk pickle.")
        return

    target_dir = "temp_space_explorer_body" if include_body else "temp_space_explorer_no_body"
    print(f"\nStarting bulk pickle process (Include Body: {include_body}) -> Target Dir: '{target_dir}'")
    print(f"Will process {len(space_keys)} spaces found.")

    processed_count = 0
    skipped_count = 0
    failed_count = 0

    for i, space_key in enumerate(space_keys):
        print(f"\n[{i+1}/{len(space_keys)}] Processing Space: {space_key}")
        pickle_filename = f"{space_key}.pkl"
        pickle_filepath = os.path.join(target_dir, pickle_filename)

        if os.path.exists(pickle_filepath):
            print(f"Checkpoint: Pickle file already exists at '{pickle_filepath}'. Skipping.")
            skipped_count += 1
            continue
        else:
            print(f"Pickle file not found. Proceeding to fetch details for {space_key}...")
            try:
                success = pickle_space_details(space_key, include_body=include_body)
                if success:
                    processed_count += 1
                else:
                    print(f"Failed to process space {space_key}.")
                    failed_count += 1
            except Exception as e:
                print(f"Critical Error during pickle_space_details call for {space_key}: {e}")
                failed_count += 1

    print("\n--- Bulk Pickle Summary ---")
    print(f"Total Spaces Checked: {len(space_keys)}")
    print(f"Successfully Processed/Pickled: {processed_count}")
    print(f"Skipped (Already Existed): {skipped_count}")
    print(f"Failed: {failed_count}")
    print("---------------------------")

def count_non_user_spaces():
    """
    Counts the total number of non-user (global) spaces in Confluence.
    Also writes space keys to a file for comparison with pickle files.
    """
    print("Counting non-user spaces...")
    space_keys = get_all_non_personal_spaces()
    if space_keys is not None:
        print(f"\nTotal non-user spaces: {len(space_keys)}")
        
        # Write space keys to file
        output_file = "confluence_space_keys.txt"
        try:
            with open(output_file, 'w') as f:
                for key in sorted(space_keys):
                    f.write(f"{key}\n")
            print(f"Space keys written to: {output_file}")
        except Exception as e:
            print(f"Error writing to file: {e}")
    else:
        print("Failed to count spaces.")

def three_way_comparison():
    """
    Three-way comparison between Production, UAT, and pickled spaces.
    UAT URL is constructed by adding '-uat' suffix to the production URL.
    """
    print("Performing three-way comparison: Production vs UAT vs Pickled spaces...\n")
    
    # Get production spaces
    print("Fetching Production spaces...")
    prod_keys = get_all_non_personal_spaces()
    if prod_keys is None:
        print("Failed to get Production spaces. Aborting comparison.")
        return
    prod_keys_set = set(prod_keys)
    print(f"Found {len(prod_keys_set)} spaces in Production")
    
    # Temporarily switch to UAT
    global CONFLUENCE_API_BASE_URL, API_BASE, API_CONTENT_ENDPOINT
    original_base_url = CONFLUENCE_API_BASE_URL
    original_api_base = API_BASE
    original_content_endpoint = API_CONTENT_ENDPOINT
    
    # Create UAT URL by adding -uat suffix
    if "https://confluence." in CONFLUENCE_API_BASE_URL:
        uat_base_url = CONFLUENCE_API_BASE_URL.replace("https://confluence.", "https://confluence-uat.")
    else:
        print("Warning: Production URL doesn't match expected pattern. Adding -uat before first dot.")
        parts = CONFLUENCE_API_BASE_URL.split('.')
        if len(parts) > 1:
            parts[0] = parts[0] + "-uat"
            uat_base_url = '.'.join(parts)
        else:
            print("Error: Cannot construct UAT URL. Aborting.")
            return
    
    print(f"\nSwitching to UAT: {uat_base_url}")
    CONFLUENCE_API_BASE_URL = uat_base_url
    API_BASE = f'{CONFLUENCE_API_BASE_URL}{API_ENDPOINT}'
    API_CONTENT_ENDPOINT = f'{API_BASE}/content'
    
    # Get UAT spaces
    print("Fetching UAT spaces...")
    uat_keys = get_all_non_personal_spaces()
    if uat_keys is None:
        print("Failed to get UAT spaces. Continuing with Production and Pickled only.")
        uat_keys_set = set()
    else:
        uat_keys_set = set(uat_keys)
        print(f"Found {len(uat_keys_set)} spaces in UAT")
    
    # Restore original URLs
    CONFLUENCE_API_BASE_URL = original_base_url
    API_BASE = original_api_base
    API_CONTENT_ENDPOINT = original_content_endpoint
    
    # Get pickle files from temp directory
    print("\nChecking pickle files...")
    pickle_files = glob.glob(os.path.join("temp", "*.pkl"))
    pickle_space_keys = set()
    
    for filepath in pickle_files:
        filename = os.path.basename(filepath)
        if filename.endswith('.pkl'):
            space_key = filename[:-4]
            pickle_space_keys.add(space_key)
    
    print(f"Found {len(pickle_space_keys)} pickle files in temp/")
    
    # Calculate all combinations
    in_all_three = prod_keys_set & uat_keys_set & pickle_space_keys
    in_prod_and_uat = (prod_keys_set & uat_keys_set) - pickle_space_keys
    in_prod_and_pickle = (prod_keys_set & pickle_space_keys) - uat_keys_set
    in_uat_and_pickle = (uat_keys_set & pickle_space_keys) - prod_keys_set
    only_in_prod = prod_keys_set - uat_keys_set - pickle_space_keys
    only_in_uat = uat_keys_set - prod_keys_set - pickle_space_keys
    only_in_pickle = pickle_space_keys - prod_keys_set - uat_keys_set
    
    # Display results
    print("\n" + "="*60)
    print("THREE-WAY COMPARISON RESULTS")
    print("="*60)
    
    print(f"\nIn all three (Prod + UAT + Pickled): {len(in_all_three)}")
    if in_all_three and len(in_all_three) <= 10:
        for key in sorted(in_all_three)[:10]:
            print(f"  {key}")
    
    print(f"\nIn Prod + UAT only (not pickled): {len(in_prod_and_uat)}")
    if in_prod_and_uat:
        for key in sorted(in_prod_and_uat)[:10]:
            print(f"  {key}")
        if len(in_prod_and_uat) > 10:
            print(f"  ... and {len(in_prod_and_uat) - 10} more")
    
    print(f"\nIn Prod + Pickled only (not in UAT): {len(in_prod_and_pickle)}")
    if in_prod_and_pickle:
        for key in sorted(in_prod_and_pickle)[:10]:
            print(f"  {key}")
        if len(in_prod_and_pickle) > 10:
            print(f"  ... and {len(in_prod_and_pickle) - 10} more")
    
    print(f"\nIn UAT + Pickled only (not in Prod): {len(in_uat_and_pickle)}")
    if in_uat_and_pickle:
        for key in sorted(in_uat_and_pickle)[:10]:
            print(f"  {key}")
        if len(in_uat_and_pickle) > 10:
            print(f"  ... and {len(in_uat_and_pickle) - 10} more")
    
    print(f"\nOnly in Production: {len(only_in_prod)}")
    if only_in_prod:
        for key in sorted(only_in_prod)[:10]:
            print(f"  {key}")
        if len(only_in_prod) > 10:
            print(f"  ... and {len(only_in_prod) - 10} more")
    
    print(f"\nOnly in UAT: {len(only_in_uat)}")
    if only_in_uat:
        for key in sorted(only_in_uat)[:10]:
            print(f"  {key}")
        if len(only_in_uat) > 10:
            print(f"  ... and {len(only_in_uat) - 10} more")
    
    print(f"\nOnly in Pickle files: {len(only_in_pickle)}")
    if only_in_pickle:
        for key in sorted(only_in_pickle)[:10]:
            print(f"  {key}")
        if len(only_in_pickle) > 10:
            print(f"  ... and {len(only_in_pickle) - 10} more")
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY:")
    print(f"  Production spaces: {len(prod_keys_set)}")
    print(f"  UAT spaces: {len(uat_keys_set)}")
    print(f"  Pickle files: {len(pickle_space_keys)}")
    print(f"  In all three: {len(in_all_three)}")
    print("="*60)
    
    # Save detailed results to file
    output_file = "three_way_comparison_results.txt"
    try:
        with open(output_file, 'w') as f:
            f.write("Three-Way Space Comparison Results\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Production spaces: {len(prod_keys_set)}\n")
            f.write(f"UAT spaces: {len(uat_keys_set)}\n")
            f.write(f"Pickle files: {len(pickle_space_keys)}\n\n")
            
            f.write(f"In all three (Prod + UAT + Pickled): {len(in_all_three)}\n")
            for key in sorted(in_all_three):
                f.write(f"  {key}\n")
            
            f.write(f"\nIn Prod + UAT only (not pickled): {len(in_prod_and_uat)}\n")
            for key in sorted(in_prod_and_uat):
                f.write(f"  {key}\n")
            
            f.write(f"\nIn Prod + Pickled only (not in UAT): {len(in_prod_and_pickle)}\n")
            for key in sorted(in_prod_and_pickle):
                f.write(f"  {key}\n")
            
            f.write(f"\nIn UAT + Pickled only (not in Prod): {len(in_uat_and_pickle)}\n")
            for key in sorted(in_uat_and_pickle):
                f.write(f"  {key}\n")
            
            f.write(f"\nOnly in Production: {len(only_in_prod)}\n")
            for key in sorted(only_in_prod):
                f.write(f"  {key}\n")
            
            f.write(f"\nOnly in UAT: {len(only_in_uat)}\n")
            for key in sorted(only_in_uat):
                f.write(f"  {key}\n")
            
            f.write(f"\nOnly in Pickle files: {len(only_in_pickle)}\n")
            for key in sorted(only_in_pickle):
                f.write(f"  {key}\n")
        
        print(f"\nDetailed results saved to: {output_file}")
    except Exception as e:
        print(f"Error saving results to file: {e}")

def compare_spaces_to_pickles():
    """
    Compares Confluence space keys from the API with pickle files in temp/ directory.
    Identifies discrepancies between what's in Confluence and what's been pickled.
    """
    print("Comparing Confluence spaces with pickle files...\n")
    
    # Get space keys from Confluence
    confluence_keys = get_all_non_personal_spaces()
    if confluence_keys is None:
        print("Failed to get Confluence spaces. Aborting comparison.")
        return
    
    confluence_keys_set = set(confluence_keys)
    
    # Get pickle files from temp directory
    pickle_files = glob.glob(os.path.join("temp", "*.pkl"))
    pickle_space_keys = set()
    
    for filepath in pickle_files:
        filename = os.path.basename(filepath)
        # Remove .pkl extension
        if filename.endswith('.pkl'):
            space_key = filename[:-4]
            pickle_space_keys.add(space_key)
    
    # Calculate deltas
    in_confluence_not_pickled = confluence_keys_set - pickle_space_keys
    in_pickle_not_confluence = pickle_space_keys - confluence_keys_set
    in_both = confluence_keys_set & pickle_space_keys
    
    # Display results
    print(f"Total spaces in Confluence: {len(confluence_keys_set)}")
    print(f"Total pickle files in temp/: {len(pickle_space_keys)}")
    print(f"Spaces in both: {len(in_both)}")
    print()
    
    if in_confluence_not_pickled:
        print(f"\nSpaces in Confluence but NOT pickled ({len(in_confluence_not_pickled)}):")
        for key in sorted(in_confluence_not_pickled):
            print(f"  {key}")
    else:
        print("\nAll Confluence spaces have been pickled.")
    
    if in_pickle_not_confluence:
        print(f"\nPickle files with NO corresponding Confluence space ({len(in_pickle_not_confluence)}):")
        print("(These might be deleted/archived spaces or personal spaces)")
        for key in sorted(in_pickle_not_confluence):
            print(f"  {key}")
    else:
        print("\nAll pickle files correspond to existing Confluence spaces.")
    
    # Summary
    print("\n" + "="*50)
    print("SUMMARY:")
    print(f"  Missing pickles: {len(in_confluence_not_pickled)}")
    print(f"  Extra pickles: {len(in_pickle_not_confluence)}")
    print(f"  Matched: {len(in_both)}")
    
    # Save results to file
    output_file = "space_comparison_results.txt"
    try:
        with open(output_file, 'w') as f:
            f.write("Space Comparison Results\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Total spaces in Confluence: {len(confluence_keys_set)}\n")
            f.write(f"Total pickle files in temp/: {len(pickle_space_keys)}\n")
            f.write(f"Spaces in both: {len(in_both)}\n\n")
            
            if in_confluence_not_pickled:
                f.write(f"Spaces in Confluence but NOT pickled ({len(in_confluence_not_pickled)}):\n")
                for key in sorted(in_confluence_not_pickled):
                    f.write(f"  {key}\n")
            
            if in_pickle_not_confluence:
                f.write(f"\nPickle files with NO corresponding Confluence space ({len(in_pickle_not_confluence)}):\n")
                for key in sorted(in_pickle_not_confluence):
                    f.write(f"  {key}\n")
        
        print(f"\nDetailed results saved to: {output_file}")
    except Exception as e:
        print(f"Error saving results to file: {e}")

def show_main_menu():
    """
    Displays the main menu and handles user input.
    """
    while True:
        print("\n==== Confluence Space Explorer Menu ====")
        print("1. Check User Status for a Page URL")
        print("2. View Space Pickle Content (from temp/)")
        print("3. View Full Pickle (from temp_space_explorer_body/)")
        print("4. Fetch & Pickle (incl. body) (to temp_space_explorer_body/)")
        print("5. View Pickle Summary (from temp_space_explorer_body/, no body shown)")
        print("6. Fetch & Pickle (NO body) (to temp_space_explorer_no_body/)")
        print("7. View Full Pickle (from temp_space_explorer_no_body/)")
        print("8. Fetch ALL Spaces (NO body) (to temp_space_explorer_no_body/) [CHECKPOINT]")
        print("9. Fetch ALL Spaces (incl. body) (to temp_space_explorer_body/) [CHECKPOINT]")
        print("10. Count Non-User Spaces")
        print("11. Compare Confluence Spaces to Pickle Files")
        print("12. Three-Way Comparison (Prod vs UAT vs Pickled)")
        print("Q. Quit")
        choice = input("Select option: ").strip().lower()

        if choice == '1':
            check_page_user_status()
        elif choice == '2':
            view_space_pickle_from_folder("temp")
        elif choice == '3':
            view_space_pickle_from_folder("temp_space_explorer_body")
        elif choice == '4':
            space_key_input = input("Enter Space Key to fetch (incl. body): ").strip()
            if space_key_input:
                pickle_space_details(space_key_input, include_body=True)
            else:
                print("No space key entered.")
        elif choice == '5':
            view_space_pickle_summary()
        elif choice == '6':
            space_key_input = input("Enter Space Key to fetch (NO body): ").strip()
            if space_key_input:
                pickle_space_details(space_key_input, include_body=False)
            else:
                print("No space key entered.")
        elif choice == '7':
            view_space_pickle_from_folder("temp_space_explorer_no_body")
        elif choice == '8':
            print("Starting bulk fetch for ALL non-personal spaces (NO body)...")
            pickle_all_spaces(include_body=False)
        elif choice == '9':
            print("Starting bulk fetch for ALL non-personal spaces (incl. body)...")
            pickle_all_spaces(include_body=True)
        elif choice == '10':
            count_non_user_spaces()
        elif choice == '11':
            compare_spaces_to_pickles()
        elif choice == '12':
            three_way_comparison()
        elif choice == 'q':
            print("Exiting.")
            break
        else:
            print("Invalid option. Please try again.")

if __name__ == "__main__":
    print("Starting Confluence Space Explorer...")
    if not CONFLUENCE_API_BASE_URL or not USERNAME:
         print("\nWarning: Confluence URL or Username might be missing in settings.")
         print("API calls may fail. Please check settings.ini.")

    show_main_menu()
