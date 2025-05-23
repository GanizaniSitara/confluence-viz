# description: Samples and pickles Confluence spaces.

import os
import pickle
import json
from collections import defaultdict
from datetime import datetime
import requests
import time
import urllib3
import argparse
import sys # Added import
from config_loader import load_confluence_settings, load_visualization_settings # MODIFIED IMPORT

# Load settings
confluence_settings = load_confluence_settings()
USERNAME = confluence_settings['username']
PASSWORD = confluence_settings['password']
BASE_URL = confluence_settings['base_url'] # Ensure this is base_url
API_ENDPOINT = "/rest/api" # Define the API endpoint suffix
VERIFY_SSL = confluence_settings['verify_ssl']

# Load visualization settings to get remote_full_pickle_dir
visualization_settings = load_visualization_settings()
REMOTE_FULL_PICKLE_DIR = visualization_settings.get('remote_full_pickle_dir')

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_with_retry(url, params=None, auth=None, headers=None, verify=False, stream=False, timeout=30):
    while True:
        resp = requests.get(url, params=params, auth=auth, headers=headers, verify=verify, stream=stream, timeout=timeout)
        if resp.status_code == 429:
            # Extract the Retry-After header value
            retry_after = resp.headers.get('Retry-After')
            
            if retry_after:
                # Retry-After can be either a number of seconds or a date
                if retry_after.isdigit():
                    wait_time = int(retry_after)
                else:
                    # Parse HTTP date format
                    try:
                        retry_date = datetime.strptime(retry_after, '%a, %d %b %Y %H:%M:%S %Z')
                        wait_time = max(1, (retry_date - datetime.now()).total_seconds())
                    except (ValueError, TypeError):
                        # Fallback to 2 seconds if parsing fails
                        wait_time = 2
            else:
                # Default to 2 seconds if no Retry-After header
                wait_time = 2
                
            print(f"Warning: Rate limited (429). Retrying {url} in {wait_time}s as specified by server...")
            time.sleep(wait_time)
            continue
        if resp.status_code >= 400:
            print(f"Error {resp.status_code} fetching {url}. Response: {resp.text}")
        return resp

TOP_N_ROOT = 10
TOP_N_RECENT = 30
TOP_N_FREQUENT = 30
OUTPUT_DIR = 'temp'

# Determine the effective directory for full pickles
DEFAULT_FULL_PICKLE_SUBDIR = 'full_pickles' # Subdirectory within the local OUTPUT_DIR
EFFECTIVE_FULL_PICKLE_OUTPUT_DIR = OUTPUT_DIR # Default to local ./temp

if REMOTE_FULL_PICKLE_DIR and os.path.exists(REMOTE_FULL_PICKLE_DIR) and os.path.isdir(REMOTE_FULL_PICKLE_DIR):
    EFFECTIVE_FULL_PICKLE_OUTPUT_DIR = REMOTE_FULL_PICKLE_DIR
    print(f"Using remote directory for full pickles: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")
else:
    EFFECTIVE_FULL_PICKLE_OUTPUT_DIR = os.path.join(OUTPUT_DIR, DEFAULT_FULL_PICKLE_SUBDIR)
    if REMOTE_FULL_PICKLE_DIR:
        print(f"Warning: remote_full_pickle_dir '{REMOTE_FULL_PICKLE_DIR}' not found or not a directory. Using local: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")
    else:
        print(f"Using local directory for full pickles: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")

# Checkpoint file for full pickle mode will be relative to the EFFECTIVE_FULL_PICKLE_OUTPUT_DIR
# However, for simplicity and to avoid writing into a potentially read-only remote_full_pickle_dir for checkpoints,
# let's keep the full_pickle_checkpoint in the local OUTPUT_DIR for now, unless specified otherwise.
# For this iteration, we will place it next to the pickles if remote, or in local temp if local.
# This means if remote_full_pickle_dir is used, the checkpoint goes there too.
CHECKPOINT_FILE = 'confluence_checkpoint.json' # For standard sampling, always local
FULL_PICKLE_CHECKPOINT_FILENAME = 'confluence_full_pickle_checkpoint.json' # Just the filename

# Ensure local OUTPUT_DIR exists (for standard pickles and potentially the full pickle checkpoint if local)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Ensure the EFFECTIVE_FULL_PICKLE_OUTPUT_DIR exists
if not os.path.exists(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR):
    try:
        os.makedirs(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR)
        print(f"Created directory: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")
    except OSError as e:
        print(f"Error creating directory {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}: {e}. Please check permissions or path.")
        # Depending on the mode, we might need to exit if this dir is crucial and cannot be created.
        # For now, we'll let it proceed, and failures will occur during file writing.

# Define the full path for the full pickle checkpoint file
# It will reside in the same directory as the full pickles themselves.
FULL_PICKLE_CHECKPOINT_FILE_PATH = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, FULL_PICKLE_CHECKPOINT_FILENAME)


def fetch_page_metadata(space_key):
    print(f"  Fetching page metadata for space: {space_key}")
    pages = []
    start = 0
    page_limit = 100 # Define page_limit
    while True:
        # Construct URL using BASE_URL and API_ENDPOINT
        url = f"{BASE_URL}{API_ENDPOINT}/content"
        params = {"type": "page", "spaceKey": space_key, "start": start, "limit": page_limit, "expand": "version,ancestors"}
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"  Failed to fetch pages for space {space_key}. Status code: {r.status_code}")
            break
        results = r.json().get("results", [])
        if not results:
            break
        for page in results:
            page_info = {
                'id': page.get('id'),
                'title': page.get('title'),
                'updated': page.get('version', {}).get('when', ''),
                'update_count': page.get('version', {}).get('number', 0),
                'parent_id': page['ancestors'][0]['id'] if page.get('ancestors') else None,
                'level': len(page.get('ancestors', [])),
                'space_key': space_key
            }
            pages.append(page_info)
        if len(results) < page_limit:
            break
        start += page_limit
    print(f"  Total metadata pages fetched for space {space_key}: {len(pages)}")
    return pages

def fetch_page_body(page_id):
    # Construct URL using BASE_URL and API_ENDPOINT
    url = f"{BASE_URL}{API_ENDPOINT}/content/{page_id}"
    params = {"expand": "body.storage"}
    r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    if r.status_code == 200:
        body = r.json().get('body', {}).get('storage', {}).get('value', '')
        return body
    else:
        print(f"    Failed to fetch body for page {page_id}. Status code: {r.status_code}")
        return ''

def sample_and_fetch_bodies(space_key, pages, fetch_all=False):
    if fetch_all:
        print(f"  Fetching bodies for all {len(pages)} pages in space {space_key} (full pickle mode)...")
        deduped = pages # In full mode, all fetched metadata pages are processed
    else:
        # Root + 1 level (up to TOP_N_ROOT)
        root_and_first = [p for p in pages if p.get('level', 0) <= 1][:TOP_N_ROOT]
        # Top TOP_N_RECENT most recently updated
        most_recent = sorted(pages, key=lambda p: p.get('updated', ''), reverse=True)[:TOP_N_RECENT]
        # Top TOP_N_FREQUENT most frequently updated
        most_frequent = sorted(pages, key=lambda p: p.get('update_count', 0), reverse=True)[:TOP_N_FREQUENT]
        # Combine and deduplicate by page id
        all_pages_sampled = root_and_first + most_recent + most_frequent
        seen = set()
        deduped = []
        for p in all_pages_sampled:
            pid = p.get('id')
            if pid and pid not in seen:
                deduped.append(p)
                seen.add(pid)
        print(f"  Sampling resulted in {len(deduped)} unique pages to fetch bodies for space {space_key}.")

    # Fetch bodies for each page (either all or sampled)
    for i, p in enumerate(deduped):
        if (i + 1) % 10 == 0 or i == 0 or (i + 1) == len(deduped):
            print(f"    Fetching body for page {i+1}/{len(deduped)} (ID: {p.get('id')})...")
        p['body'] = fetch_page_body(p.get('id'))
    return deduped, len(pages)

def load_checkpoint(filename=CHECKPOINT_FILE): # Added filename parameter with default
    """Load the checkpoint file if it exists."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                checkpoint = json.load(f)
                processed_count = 0
                if filename == FULL_PICKLE_CHECKPOINT_FILE_PATH:
                    processed_count = len(checkpoint.get('processed_space_keys', []))
                else:
                    processed_count = len(checkpoint.get('processed_spaces', []))
                print(f"Loaded checkpoint from {filename} with {processed_count} processed items.")
                return checkpoint
        except Exception as e:
            print(f"Error loading checkpoint file {filename}: {e}")
    
    # Return a new checkpoint structure if file doesn't exist or error occurred
    if filename == FULL_PICKLE_CHECKPOINT_FILE_PATH:
        return {
            "all_fetched_space_keys": [], # For full pickle mode, to compare if the list of spaces changed
            "processed_space_keys": [],
            "last_updated": datetime.now().isoformat()
        }
    else: # Default structure for sampling checkpoint
        return {
            "total_spaces": 0,
            "processed_spaces": [],
            "last_position": 0,
            "last_updated": datetime.now().isoformat()
        }

def save_checkpoint(checkpoint, filename=CHECKPOINT_FILE): # Added filename parameter with default
    """Save the current checkpoint to disk."""
    checkpoint["last_updated"] = datetime.now().isoformat()
    try:
        with open(filename, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        print(f"Error saving checkpoint file {filename}: {e}")

def fetch_space_details(target_space_key, auth_details, verify_ssl_cert):
    """Fetches details for a specific space, including description and icon."""
    # Construct URL using BASE_URL and API_ENDPOINT
    space_details_url = f"{BASE_URL}{API_ENDPOINT}/space/{target_space_key}"
    params = {"expand": "description.plain,icon"}
    sd_r = get_with_retry(space_details_url, params=params, auth=auth_details, verify=verify_ssl_cert)
    if sd_r.status_code == 200:
        try:
            return sd_r.json()
        except requests.exceptions.JSONDecodeError:
            print(f"  Warning: Could not parse JSON response for space details of {target_space_key}. Response: {sd_r.text}")
    else:
        print(f"  Warning: Could not fetch space details for {target_space_key}. Status: {sd_r.status_code}")
    return {}

def fetch_all_spaces_with_details(auth_details, verify_ssl_cert):
    """Fetches all spaces with their details, excluding personal spaces."""
    print("Fetching all non-personal spaces with details...")
    spaces_data = []
    start = 0
    limit = 100 # Changed from 50 to 100

    while True:
        # Construct URL using BASE_URL and API_ENDPOINT
        url = f"{BASE_URL}{API_ENDPOINT}/space"
        params = {
            "start": start,
            "limit": limit,
            "type": "global", 
            "expand": "description.plain,icon"
        }
        print(f"  Fetching next batch of spaces from Confluence: {url}?start={start}&limit={limit}")
        r = get_with_retry(url, params=params, auth=auth_details, verify=verify_ssl_cert)
        if r.status_code != 200:
            print(f"Failed to fetch spaces. Status code: {r.status_code}. Response: {r.text}")
            break
        results = r.json().get("results", [])
        if not results:
            break
        for sp in results:
            if sp.get("key", "").startswith("~"):  # Exclude user spaces
                # print(f"Skipping user space: key={sp.get('key')}, name={sp.get('name')}")
                continue
            # print(f"[{fetch_idx}] Fetched space metadata: key={sp.get('key')}, name={sp.get('name')}")
            spaces_data.append(sp)
        if len(results) < limit: # API's limit for spaces is often 50 or per its documentation, changed to 100
            break
        start += len(results) # Correctly increment for next API page
    print(f"Total non-user spaces fetched: {len(spaces_data)}")
    return spaces_data

def fetch_pages_for_space_concurrently(space_key, auth_details, verify_ssl_cert, max_workers=10):
    """
    Fetches all pages for a given space key concurrently, including their content and version history.
    """
    # Initialize start and page_limit for this function
    start = 0
    page_limit = 100 # Or another appropriate limit
    while True:
        # Construct URL using BASE_URL and API_ENDPOINT
        url = f"{BASE_URL}{API_ENDPOINT}/content"
        params = {
            "spaceKey": space_key,
            "start": start, # Now defined
            "limit": page_limit, # Now defined
            "expand": "body.storage,version,history.previousVersion"
        }
        # ...existing code...

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Sample and pickle Confluence spaces. Handles checkpointing for resumable execution.",
        epilog="If no run mode argument (--reset, --batch-continue, --pickle-space-full SPACE_KEY) is provided, an interactive menu is shown." # Updated epilog
    )
    # Group for mutually exclusive run modes
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--reset', action='store_true', help='Reset checkpoint and start from beginning (non-interactive).')
    mode_group.add_argument('--batch-continue', action='store_true', help='Run in continue mode using checkpoint without interactive menu (non-interactive).')
    mode_group.add_argument('--pickle-space-full', type=str, metavar='SPACE_KEY',
                               help=f'Pickle all pages for a single space. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}. Bypasses sampling, checkpointing, and interactive menu.') # Updated help
    mode_group.add_argument('--pickle-all-spaces-full', action='store_true',
                               help=f'Pickle all pages for ALL non-user spaces. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}. Uses checkpoint file \'{FULL_PICKLE_CHECKPOINT_FILENAME}\' in that directory. Bypasses sampling and interactive menu.') # New argument, updated help
    args = parser.parse_args()

    # Handle --pickle-all-spaces-full mode
    if args.pickle_all_spaces_full:
        print(f"Mode: Pickling all pages for ALL non-user spaces (Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}, Checkpoint: {FULL_PICKLE_CHECKPOINT_FILE_PATH}).")
        
        checkpoint = load_checkpoint(FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
        processed_space_keys_set = set(checkpoint.get("processed_space_keys", []))

        all_non_user_spaces = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)

        if not all_non_user_spaces:
            print("No non-user spaces found to process. Exiting.")
            sys.exit(0)

        current_api_space_keys = sorted([s['key'] for s in all_non_user_spaces if s.get('key')])
        
        # If the list of spaces from API differs from what's in checkpoint, reset processed list for safety.
        if sorted(checkpoint.get("all_fetched_space_keys", [])) != current_api_space_keys:
            print("Warning: The list of spaces from Confluence API has changed since the last run or checkpoint is new.")
            print("Resetting processed spaces list for this mode to ensure all current spaces are considered.")
            checkpoint["all_fetched_space_keys"] = current_api_space_keys
            checkpoint["processed_space_keys"] = []
            processed_space_keys_set = set()
            save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
        
        print(f"Found {len(all_non_user_spaces)} non-user spaces. {len(processed_space_keys_set)} already processed according to checkpoint.")
        
        spaces_to_actually_process = [s for s in all_non_user_spaces if s.get('key') not in processed_space_keys_set]

        if not spaces_to_actually_process:
            print("All non-user spaces have already been processed according to the checkpoint. Exiting.")
            sys.exit(0)
            
        print(f"Attempting to process {len(spaces_to_actually_process)} remaining non-user spaces.")

        overall_processed_count = len(processed_space_keys_set) # Count from checkpoint
        newly_processed_this_run = 0
        failed_this_run = 0

        for space_info in spaces_to_actually_process: # Iterate only through spaces not yet processed
            target_space_key = space_info.get('key')
            space_name_for_pickle = space_info.get('name', "N/A (Full Pickle)")
            
            if not target_space_key: # Should have been filtered by current_api_space_keys logic, but good check
                print(f"  Warning: Found a space without a key during iteration. Skipping: {space_info}")
                failed_this_run += 1
                continue

            # Check if pickle file already exists BEFORE attempting to process
            out_filename_check = f'{target_space_key}_full.pkl'
            out_path_check = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename_check)
            if os.path.exists(out_path_check):
                print(f"  Pickle file {out_path_check} already exists. Skipping space {target_space_key}.")
                # Ensure it's marked as processed in checkpoint if not already, then continue
                if target_space_key not in processed_space_keys_set:
                    checkpoint["processed_space_keys"].append(target_space_key)
                    processed_space_keys_set.add(target_space_key) # Keep the set in sync
                    save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                continue

            print(f"\nProcessing space: {target_space_key} (Name: {space_name_for_pickle})")
            
            try:
                pages_metadata = fetch_page_metadata(target_space_key)

                if not pages_metadata:
                    print(f"  No pages found for space {target_space_key} or error fetching metadata. Skipping.")
                    failed_this_run +=1
                    continue

                pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(target_space_key, pages_metadata, fetch_all=True)

                out_filename = f'{target_space_key}_full.pkl'
                out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename) # Use effective path
                
                with open(out_path, 'wb') as f:
                    pickle.dump({
                        'space_key': target_space_key,
                        'name': space_name_for_pickle,
                        'sampled_pages': pages_with_bodies, # These are all pages with their bodies
                        'total_pages_in_space': total_pages_metadata
                    }, f)
                print(f'  Successfully wrote {len(pages_with_bodies)} pages for space {target_space_key} to {out_path} (total pages in space: {total_pages_metadata})')
                
                # Update checkpoint after successful processing
                checkpoint["processed_space_keys"].append(target_space_key)
                # No need to re-add to set, just save checkpoint
                save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                newly_processed_this_run += 1
            except Exception as e:
                print(f"  An unexpected error occurred during pickling for space {target_space_key}: {e}")
                failed_this_run += 1
        
        overall_processed_count += newly_processed_this_run
        print(f"\nFinished pickling all non-user spaces for this run.")
        print(f"Total successfully processed (including previous runs): {overall_processed_count} spaces based on checkpoint.")
        print(f"Newly processed in this run: {newly_processed_this_run} spaces.")
        print(f"Failed to process in this run: {failed_this_run} spaces.")
        if failed_this_run > 0:
            print("Rerun the script to attempt processing failed spaces.")
        sys.exit(0)

    # Handle --pickle-space-full first as it\'s a distinct mode
    if args.pickle_space_full:
        target_space_key = args.pickle_space_full
        print(f"Mode: Pickling all pages for space: {target_space_key}. Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")

        # Check if pickle file already exists
        out_filename_check = f'{target_space_key}_full.pkl'
        out_path_check = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename_check)
        if os.path.exists(out_path_check):
            print(f"Pickle file {out_path_check} already exists. Exiting.")
            sys.exit(0)

        # Fetch space details to get the name
        # Construct URL using BASE_URL and API_ENDPOINT
        space_details_url = f"{BASE_URL}{API_ENDPOINT}/space/{target_space_key}"
        
        # The check for double slashes is good, but API_ENDPOINT starts with /, so BASE_URL should not end with /
        # Assuming BASE_URL does not end with / and API_ENDPOINT starts with /
        # No special handling needed here if BASE_URL is truly a base like http://host:port

        print(f"  Fetching details for space {target_space_key} from {space_details_url}...")
        sd_r = get_with_retry(space_details_url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        space_name_for_pickle = "N/A (Full Pickle)" # Default space name
        if sd_r.status_code == 200:
            try:
                space_name_for_pickle = sd_r.json().get('name', space_name_for_pickle)
            except requests.exceptions.JSONDecodeError:
                print(f"  Warning: Could not parse JSON response for space details of {target_space_key}. Response: {sd_r.text}")
        else:
            print(f"  Warning: Could not fetch space name for {target_space_key}. Status: {sd_r.status_code}")

        print(f"  Processing space: {target_space_key} (Name: {space_name_for_pickle})")
        pages_metadata = fetch_page_metadata(target_space_key)

        if not pages_metadata:
            print(f"  No pages found for space {target_space_key} or error fetching metadata. Exiting.")
            sys.exit(1)

        # sample_and_fetch_bodies will be modified to accept fetch_all=True
        pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(target_space_key, pages_metadata, fetch_all=True)

        out_filename = f'{target_space_key}_full.pkl'
        out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename) # MODIFIED path
        try:
            with open(out_path, 'wb') as f:
                pickle.dump({
                    'space_key': target_space_key,
                    'name': space_name_for_pickle,
                    'sampled_pages': pages_with_bodies, # These are all pages with their bodies
                    'total_pages_in_space': total_pages_metadata
                }, f)
            print(f'  Successfully wrote {len(pages_with_bodies)} pages for space {target_space_key} to {out_path} (total pages in space: {total_pages_metadata})')
        except IOError as e:
            print(f"  Error writing pickle file {out_path}: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"  An unexpected error occurred during pickling for space {target_space_key}: {e}")
            sys.exit(1)
        sys.exit(0)

    perform_reset = False
    # run_script = True # This variable was not used

    if args.reset:
        print("Mode: Running with --reset")
        perform_reset = True
    elif args.batch_continue:
        print("Mode: Running with --batch-continue (using checkpoint)")
        perform_reset = False # Default for continue is not to reset
    else: # Interactive mode
        print("\nConfluence Space Sampler and Pickler") # Corrected to \n
        print("------------------------------------")
        print("This script samples pages from Confluence spaces and saves them locally (standard mode),")
        print("or pickles all pages from a single specified space or all spaces (full pickle modes).") # Updated line
        print("Standard mode uses a checkpoint file (confluence_checkpoint.json) to resume progress.")
        print(f"Full pickle modes for all spaces use a separate checkpoint ({FULL_PICKLE_CHECKPOINT_FILENAME}) in the target pickle directory.") # New line
        print("\nAvailable command-line options for non-interactive use:") # Corrected to \n
        print("  --reset                       : Clears all previous progress and starts fresh (standard sampling mode).") # Updated help
        print("  --batch-continue              : Skips this menu and continues from the last checkpoint (standard sampling mode).") # Updated help
        print(f"  --pickle-space-full SPACE_KEY : Pickles all pages for a single space. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}.") # Updated help
        print(f"  --pickle-all-spaces-full      : Pickles all pages for ALL non-user spaces. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}.") # New help line
        print(f"                                  Uses its own checkpoint ({FULL_PICKLE_CHECKPOINT_FILENAME}) and bypasses sampling and this interactive menu.") # Updated help
        print("------------------------------------\n") # Corrected to \n
        while True:
            choice = input("Choose an action:\n" # Updated prompt
                           "  1: Continue with existing progress (standard sampling mode, uses checkpoint)\n"
                           "  2: Reset and start from beginning (standard sampling mode, deletes checkpoint)\n"
                           "  3: Pickle all pages for a single space (e.g., DOC). Saves to configured full pickle directory.\n"
                           "  4: Pickle all pages for ALL non-user spaces. Saves to configured full pickle directory (uses its own checkpoint).\n"
                           "  q: Quit\n"
                           "Enter choice (1, 2, 3, 4, or q): ").strip().lower()
            if choice == '1':
                perform_reset = False
                print("Mode: Continuing with existing progress (standard sampling).")
                break
            elif choice == '2':
                perform_reset = True
                print("Mode: Resetting and starting from beginning (standard sampling).")
                break
            elif choice == '3':
                target_space_key_interactive = input("Enter the SPACE_KEY to pickle in full: ").strip().upper()
                if not target_space_key_interactive:
                    print("No space key provided. Please try again.")
                    continue
                args.pickle_space_full = target_space_key_interactive
                # Call the relevant part of main or refactor to a function
                print(f"Mode: Pickling all pages for space: {args.pickle_space_full}. Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")

                # Check if pickle file already exists
                out_filename_check_interactive = f'{args.pickle_space_full}_full.pkl'
                out_path_check_interactive = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename_check_interactive)
                if os.path.exists(out_path_check_interactive):
                    print(f"Pickle file {out_path_check_interactive} already exists. Returning to menu.")
                    continue # Go back to the interactive menu

                # Fetch space details to get the name
                space_details_url = f"{BASE_URL}{API_ENDPOINT}/space/{args.pickle_space_full}"
                print(f"  Fetching details for space {args.pickle_space_full} from {space_details_url}...")
                sd_r = get_with_retry(space_details_url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
                space_name_for_pickle = "N/A (Full Pickle)"
                if sd_r.status_code == 200:
                    try:
                        space_name_for_pickle = sd_r.json().get('name', space_name_for_pickle)
                    except requests.exceptions.JSONDecodeError:
                        print(f"  Warning: Could not parse JSON response for space details of {args.pickle_space_full}. Response: {sd_r.text}")
                else:
                    print(f"  Warning: Could not fetch space name for {args.pickle_space_full}. Status: {sd_r.status_code}")

                print(f"  Processing space: {args.pickle_space_full} (Name: {space_name_for_pickle})")
                pages_metadata = fetch_page_metadata(args.pickle_space_full)

                if not pages_metadata:
                    print(f"  No pages found for space {args.pickle_space_full} or error fetching metadata. Exiting.")
                    sys.exit(1)

                pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(args.pickle_space_full, pages_metadata, fetch_all=True)

                out_filename = f'{args.pickle_space_full}_full.pkl'
                out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename) # MODIFIED path
                try:
                    with open(out_path, 'wb') as f:
                        pickle.dump({
                            'space_key': args.pickle_space_full,
                            'name': space_name_for_pickle,
                            'sampled_pages': pages_with_bodies,
                            'total_pages_in_space': total_pages_metadata
                        }, f)
                    print(f'  Successfully wrote {len(pages_with_bodies)} pages for space {args.pickle_space_full} to {out_path} (total pages in space: {total_pages_metadata})')
                except IOError as e:
                    print(f"  Error writing pickle file {out_path}: {e}")
                    sys.exit(1)
                except Exception as e:
                    print(f"  An unexpected error occurred during pickling for space {args.pickle_space_full}: {e}")
                    sys.exit(1)
                sys.exit(0) # Exit after this action
            elif choice == '4':
                # Simulate args for --pickle-all-spaces-full
                args.pickle_all_spaces_full = True
                # Call the relevant part of main or refactor to a function
                print(f"Mode: Pickling all pages for ALL non-user spaces (Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}, Checkpoint: {FULL_PICKLE_CHECKPOINT_FILE_PATH}).")
        
                checkpoint = load_checkpoint(FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                processed_space_keys_set = set(checkpoint.get("processed_space_keys", []))

                all_non_user_spaces = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)

                if not all_non_user_spaces:
                    print("No non-user spaces found to process. Exiting.")
                    sys.exit(0)

                current_api_space_keys = sorted([s['key'] for s in all_non_user_spaces if s.get('key')])
                
                if sorted(checkpoint.get("all_fetched_space_keys", [])) != current_api_space_keys:
                    print("Warning: The list of spaces from Confluence API has changed since the last run or checkpoint is new.")
                    print("Resetting processed spaces list for this mode to ensure all current spaces are considered.")
                    checkpoint["all_fetched_space_keys"] = current_api_space_keys
                    checkpoint["processed_space_keys"] = []
                    processed_space_keys_set = set()
                    save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                
                print(f"Found {len(all_non_user_spaces)} non-user spaces. {len(processed_space_keys_set)} already processed according to checkpoint.")
                
                spaces_to_actually_process = [s for s in all_non_user_spaces if s.get('key') not in processed_space_keys_set]

                if not spaces_to_actually_process:
                    print("All non-user spaces have already been processed according to the checkpoint. Exiting.")
                    sys.exit(0)
                    
                print(f"Attempting to process {len(spaces_to_actually_process)} remaining non-user spaces.")

                overall_processed_count = len(processed_space_keys_set)
                newly_processed_this_run = 0
                failed_this_run = 0

                for space_info in spaces_to_actually_process:
                    target_space_key = space_info.get('key')
                    space_name_for_pickle = space_info.get('name', "N/A (Full Pickle)")
                    
                    if not target_space_key:
                        print(f"  Warning: Found a space without a key during iteration. Skipping: {space_info}")
                        failed_this_run += 1
                        continue

                    # Check if pickle file already exists BEFORE attempting to process
                    out_filename_check_interactive_all = f'{target_space_key}_full.pkl'
                    out_path_check_interactive_all = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename_check_interactive_all)
                    if os.path.exists(out_path_check_interactive_all):
                        print(f"  Pickle file {out_path_check_interactive_all} already exists. Skipping space {target_space_key}.")
                        if target_space_key not in processed_space_keys_set:
                            checkpoint["processed_space_keys"].append(target_space_key)
                            processed_space_keys_set.add(target_space_key) # Keep set in sync
                            save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                        continue

                    print(f"  Processing space: {target_space_key} (Name: {space_name_for_pickle})")
                    
                    try:
                        pages_metadata = fetch_page_metadata(target_space_key)

                        if not pages_metadata:
                            print(f"  No pages found for space {target_space_key} or error fetching metadata. Skipping.")
                            failed_this_run +=1
                            continue

                        pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(target_space_key, pages_metadata, fetch_all=True)

                        out_filename = f'{target_space_key}_full.pkl'
                        out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename) # MODIFIED path
                        
                        with open(out_path, 'wb') as f:
                            pickle.dump({
                                'space_key': target_space_key,
                                'name': space_name_for_pickle,
                                'sampled_pages': pages_with_bodies,
                                'total_pages_in_space': total_pages_metadata
                            }, f)
                        print(f'  Successfully wrote {len(pages_with_bodies)} pages for space {target_space_key} to {out_path} (total pages in space: {total_pages_metadata})')
                        
                        checkpoint["processed_space_keys"].append(target_space_key)
                        save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                        newly_processed_this_run += 1
                    except Exception as e:
                        print(f"  An unexpected error occurred during pickling for space {target_space_key}: {e}")
                        failed_this_run += 1
                
                overall_processed_count += newly_processed_this_run
                print(f"\nFinished pickling all non-user spaces for this run.")
                print(f"Total successfully processed (including previous runs): {overall_processed_count} spaces based on checkpoint.")
                print(f"Newly processed in this run: {newly_processed_this_run} spaces.")
                print(f"Failed to process in this run: {failed_this_run} spaces.")
                if failed_this_run > 0:
                    print("Rerun the script to attempt processing failed spaces.")
                sys.exit(0) # Exit after this action
            elif choice == 'q':
                print("Exiting script.")
                sys.exit(0) # Exit gracefully
            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, or q.")
    
    # --- Checkpoint handling ---
    if perform_reset and os.path.exists(CHECKPOINT_FILE):
        print("Resetting checkpoint file as requested...")
        try:
            os.remove(CHECKPOINT_FILE)
            print(f"Successfully deleted {CHECKPOINT_FILE}.")
        except OSError as e:
            print(f"Error deleting checkpoint file {CHECKPOINT_FILE}: {e}. Continuing without reset if file persists.")
            # If deletion fails, we might not be able to enforce a reset if load_checkpoint loads old data.
            # The logic below will try to ensure the checkpoint object is fresh for the run.

    # Load checkpoint. If perform_reset led to file deletion, load_checkpoint will return a fresh structure.
    checkpoint = load_checkpoint() # Defaults to CHECKPOINT_FILE for sampling mode

    # Ensure checkpoint reflects a reset state for this run if perform_reset is true.
    # This handles cases where os.remove might have failed or if we want to be absolutely sure.
    if perform_reset:
        checkpoint["processed_spaces"] = []
        checkpoint["last_position"] = 0
        # total_spaces will be updated later once all_spaces are fetched.
        # last_updated will be set by load_checkpoint or save_checkpoint.
        print("Checkpoint data has been reset for this run.")

    processed_space_keys = set(checkpoint.get("processed_spaces", []))
    # effective_start_idx_for_slicing is the 0-based index from which to start processing in all_spaces list.
    # It's the number of spaces already processed.
    effective_start_idx_for_slicing = checkpoint.get("last_position", 0) 
                                                                      
    print(f"\nStarting Confluence sampling and pickling process...") # Corrected to \n
    if perform_reset:
        print("- Run type: Fresh run (checkpoint was reset or is new)")
    elif effective_start_idx_for_slicing > 0:
        print(f"- Run type: Continuing. {effective_start_idx_for_slicing} spaces recorded as processed in checkpoint.")
    else:
        print("- Run type: Starting new run (no prior progress in checkpoint).")
    
    # Fetch all spaces
    all_spaces = [] # Renamed from 'spaces' for clarity
    start_fetch_api = 0 # API pagination start
    fetch_idx = 0
    print("Fetching list of all spaces from Confluence...")
    while True:
        # Construct URL using BASE_URL and API_ENDPOINT
        url = f"{BASE_URL}{API_ENDPOINT}/space"
        params = {"start": start_fetch_api, "limit": 100}
        print(f"  Fetching next batch of spaces from Confluence: {url}?start={start_fetch_api}&limit=100")
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch spaces. Status code: {r.status_code}. Response: {r.text}")
            break
        results = r.json().get("results", [])
        if not results:
            break
        for sp in results:
            if sp.get("key", "").startswith("~"):  # Exclude user spaces
                # print(f"Skipping user space: key={sp.get('key')}, name={sp.get('name')}")
                continue
            fetch_idx += 1
            # print(f"[{fetch_idx}] Fetched space metadata: key={sp.get('key')}, name={sp.get('name')}")
            all_spaces.append(sp)
        if len(results) < 100: # API's limit for spaces is often 50 or per its documentation, changed to 100
            break
        start_fetch_api += len(results) # Correctly increment for next API page
    print(f"Total non-user spaces fetched: {len(all_spaces)}")
    
    # Determine spaces to process based on checkpoint
    if effective_start_idx_for_slicing >= len(all_spaces) and not perform_reset and len(all_spaces) > 0:
        print(f"All {len(all_spaces)} spaces already processed according to checkpoint. Nothing to do.")
        print("Run with --reset to process all spaces again.")
        sys.exit(0)
        
    spaces_to_process = all_spaces[effective_start_idx_for_slicing:]
    
    if not spaces_to_process and len(all_spaces) > 0 :
        if not perform_reset: # Already covered by above check, but as a safeguard
             print(f"No new spaces to process. All {len(all_spaces)} spaces seem to be processed per checkpoint.")
             sys.exit(0)
    elif not spaces_to_process and len(all_spaces) == 0:
        print("No spaces found to process.")
        sys.exit(0)

    print(f"Spaces to process in this run: {len(spaces_to_process)}")

    # Ensure checkpoint has the total spaces count (based on all fetched spaces)
    checkpoint["total_spaces"] = len(all_spaces)
    save_checkpoint(checkpoint) # Save potentially updated total_spaces or reset checkpoint
    
    # Process each space
    # idx will be the 1-based global index of the space in all_spaces
    for loop_counter, space_data in enumerate(spaces_to_process):
        # Calculate global 1-based index for checkpointing and logging
        current_global_idx = effective_start_idx_for_slicing + loop_counter + 1
        
        space_key = space_data.get('key') # Ensure we use the correct variable name 'space_data'
        if not space_key:
            print(f"Warning: Space data at global index {current_global_idx-1} missing key. Skipping: {space_data}")
            continue
            
        # This check is mostly redundant if effective_start_idx_for_slicing is correct,
        # but good for safety if processed_space_keys was loaded from an out-of-sync checkpoint.
        if space_key in processed_space_keys and not perform_reset:
            print(f"Skipping already processed space (as per processed_keys set) {current_global_idx}/{len(all_spaces)}: {space_key}")
            # If we skip here, we should ensure the checkpoint's last_position is updated if it was somehow behind.
            # However, the main loop structure should prevent this if effective_start_idx_for_slicing is honored.
            if checkpoint.get("last_position", 0) < current_global_idx:
                 checkpoint["last_position"] = current_global_idx
                 # save_checkpoint(checkpoint) # Potentially save if we want to mark skipped ones this way
            continue
            
        try:
            print(f"Processing space {current_global_idx}/{len(all_spaces)}: {space_key} (Name: {space_data.get('name', 'N/A')})")
            pages_metadata = fetch_page_metadata(space_key) # Changed 'pages' to 'pages_metadata'
            sampled_pages, total_pages_in_space = sample_and_fetch_bodies(space_key, pages_metadata) # Changed 'pages' to 'pages_metadata'
            
            out_path = os.path.join(OUTPUT_DIR, f'{space_key}.pkl')
            with open(out_path, 'wb') as f:
                pickle.dump({'space_key': space_key, 'name': space_data.get('name'), 'sampled_pages': sampled_pages, 'total_pages_in_space': total_pages_in_space}, f)
            print(f'  Successfully wrote {len(sampled_pages)} sampled pages for space {space_key} to {out_path} (total pages in space: {total_pages_in_space})')
            
            # Update checkpoint after each successful space processing
            if space_key not in checkpoint["processed_spaces"]: # Add only if not already there (e.g. due to a partial run)
                 checkpoint["processed_spaces"].append(space_key)
            checkpoint["last_position"] = current_global_idx # Record 1-based index of last successfully processed space
            save_checkpoint(checkpoint)
            
        except Exception as e:
            print(f"Error processing space {space_key} (Global Index {current_global_idx}): {e}")
            # Decide if we want to update last_position even on error.
            # If we do, a subsequent run will skip this problematic space.
            # If we don't, it will be retried. For now, let's assume we want to retry.
            # So, we don't update checkpoint["last_position"] here on error, it remains at the previously successful one.
            # However, we should save any other checkpoint changes (like total_spaces if it was the first update)
            save_checkpoint(checkpoint) # Save at least to persist last_updated and total_spaces
            print(f"  Skipping to next space due to error with {space_key}.")
            # Continue to the next space rather than breaking the whole script
    
    print("\nAll done!") # Corrected to \n
    final_processed_count = len(checkpoint.get("processed_spaces",[]))
    total_spaces_in_checkpoint = checkpoint.get("total_spaces", len(all_spaces))
    print(f"Successfully processed and sampled {final_processed_count} spaces out of {total_spaces_in_checkpoint} total non-user spaces.")
    if final_processed_count < total_spaces_in_checkpoint:
        print(f"{total_spaces_in_checkpoint - final_processed_count} spaces may have been skipped due to errors or if the script was interrupted.")
        print("You can re-run the script to attempt processing remaining/failed spaces.")

if __name__ == '__main__':
    main()
