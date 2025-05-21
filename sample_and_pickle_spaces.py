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
from config_loader import load_confluence_settings

# Load settings
confluence_settings = load_confluence_settings()
API_BASE_URL = confluence_settings['api_base_url']
USERNAME = confluence_settings['username']
PASSWORD = confluence_settings['password']
VERIFY_SSL = confluence_settings['verify_ssl']

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_with_retry(url, params=None, auth=None, verify=False):
    while True:
        resp = requests.get(url, params=params, auth=auth, verify=verify)
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
CHECKPOINT_FILE = 'confluence_checkpoint.json'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_page_metadata(space_key):
    print(f"  Fetching page metadata for space: {space_key}")
    pages = []
    start = 0
    while True:
        url = f"{API_BASE_URL}/content" # Corrected to use API_BASE_URL for API endpoint
        params = {"type": "page", "spaceKey": space_key, "start": start, "limit": 100, "expand": "version,ancestors"}
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
        if len(results) < 100:
            break
        start += 100
    print(f"  Total metadata pages fetched for space {space_key}: {len(pages)}")
    return pages

def fetch_page_body(page_id):
    url = f"{API_BASE_URL}/content/{page_id}" # Corrected: Removed redundant /rest/api
    params = {"expand": "body.storage"}
    r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    if r.status_code == 200:
        body = r.json().get('body', {}).get('storage', {}).get('value', '')
        return body
    else:
        print(f"    Failed to fetch body for page {page_id}. Status code: {r.status_code}")
        return ''

def sample_and_fetch_bodies(space_key, pages):
    # Root + 1 level (up to 20)
    root_and_first = [p for p in pages if p.get('level', 0) <= 1][:TOP_N_ROOT]
    # Top 15 most recently updated
    most_recent = sorted(pages, key=lambda p: p.get('updated', ''), reverse=True)[:TOP_N_RECENT]
    # Top 15 most frequently updated
    most_frequent = sorted(pages, key=lambda p: p.get('update_count', 0), reverse=True)[:TOP_N_FREQUENT]
    # Combine and deduplicate by page id
    all_pages = root_and_first + most_recent + most_frequent
    seen = set()
    deduped = []
    for p in all_pages:
        pid = p.get('id')
        if pid and pid not in seen:
            deduped.append(p)
            seen.add(pid)
    # Fetch bodies for each unique page
    for p in deduped:
        p['body'] = fetch_page_body(p['id'])
    return deduped, len(pages)

def load_checkpoint():
    """Load the checkpoint file if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                print(f"Loaded checkpoint with {len(checkpoint.get('processed_spaces', []))} processed spaces")
                return checkpoint
        except Exception as e:
            print(f"Error loading checkpoint file: {e}")
    
    # Return a new checkpoint structure if file doesn't exist or error occurred
    return {
        "total_spaces": 0,
        "processed_spaces": [],
        "last_position": 0,
        "last_updated": datetime.now().isoformat()
    }

def save_checkpoint(checkpoint):
    """Save the current checkpoint to disk."""
    checkpoint["last_updated"] = datetime.now().isoformat()
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        print(f"Error saving checkpoint file: {e}")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Sample and pickle Confluence spaces. Handles checkpointing for resumable execution.",
        epilog="If no specific run mode argument (--reset or --batch-continue) is provided, an interactive menu is shown."
    )
    parser.add_argument('--reset', action='store_true', help='Reset checkpoint and start from beginning (non-interactive).')
    parser.add_argument('--batch-continue', action='store_true', help='Run in continue mode using checkpoint without interactive menu (non-interactive).')
    args = parser.parse_args()

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
        print("This script samples pages from Confluence spaces and saves them locally.")
        print("It uses a checkpoint file (confluence_checkpoint.json) to resume progress.")
        print("\nAvailable command-line options for non-interactive use:") # Corrected to \n
        print("  --reset           : Clears all previous progress and starts fresh.")
        print("  --batch-continue  : Skips this menu and continues from the last checkpoint.")
        print("------------------------------------\n") # Corrected to \n
        while True:
            choice = input("Choose an action:\n"
                           "  1: Continue with existing progress (uses checkpoint)\n"
                           "  2: Reset and start from beginning (deletes checkpoint)\n"
                           "  q: Quit\n"
                           "Enter choice (1, 2, or q): ").strip().lower()
            if choice == '1':
                perform_reset = False
                print("Mode: Continuing with existing progress.")
                break
            elif choice == '2':
                perform_reset = True
                print("Mode: Resetting and starting from beginning.")
                break
            elif choice == 'q':
                print("Exiting script.")
                sys.exit(0) # Exit gracefully
            else:
                print("Invalid choice. Please enter 1, 2, or q.")
    
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
    checkpoint = load_checkpoint() 

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
        # Use API_BASE_URL for /rest/api/space endpoint
        url = f"{API_BASE_URL}/space" # Corrected: Removed redundant /rest/api
        params = {"start": start_fetch_api, "limit": 100} # Standard limit for space fetching, changed to 100
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
