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
import logging
from config_loader import load_confluence_settings, load_data_settings # MODIFIED IMPORT

# Load settings
confluence_settings = load_confluence_settings()
USERNAME = confluence_settings['username']
PASSWORD = confluence_settings['password']
BASE_URL = confluence_settings['base_url'] # Ensure this is base_url
API_ENDPOINT = "/rest/api" # Define the API endpoint suffix
VERIFY_SSL = confluence_settings['verify_ssl']

# Load data settings to get pickle directories
data_settings = load_data_settings()
REMOTE_FULL_PICKLE_DIR = data_settings.get('remote_full_pickle_dir')

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
def setup_simple_logging(log_dir):
    """Setup simple file logging without the logging module."""
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(log_dir, f"confluence_processing_{timestamp}.log")
    
    try:
        # Test we can write to the file
        with open(log_filename, 'w') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - Logging initialized\n")
            f.flush()
        
        print(f"✓ Log file created: {log_filename}")
        return log_filename
        
    except Exception as e:
        print(f"✗ Failed to create log file: {e}")
        return None

def write_log(log_filename, level, message):
    """Simple log writing function."""
    if log_filename:
        try:
            with open(log_filename, 'a') as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"{timestamp} - {level} - {message}\n")
                f.flush()
        except Exception as e:
            print(f"Error writing to log: {e}")

def print_spaces_nicely(spaces_data):
    """Prints a list of space data in a readable format."""
    if not spaces_data:
        print("No spaces to display.")
        return
    print("\n--- Confluence Spaces (Non-User) ---")
    for i, space in enumerate(spaces_data):
        key = space.get('key', 'N/A')
        name = space.get('name', 'N/A')
        desc_plain = space.get('description', {}).get('plain', {}).get('value', 'No description')
        # Truncate long descriptions for display
        max_desc_len = 100
        display_desc = (desc_plain[:max_desc_len-3] + '...') if len(desc_plain) > max_desc_len else desc_plain
        print(f"{i+1}. Key: {key:<15} Name: {name:<40} Description: {display_desc}")
    print("--- End of Spaces List ---")


def print_space_keys_only(spaces_data):
    """Prints only the keys of the space data, one per line."""
    if not spaces_data:
        print("No space keys to display.")
        return
    print("\n--- Confluence Space Keys (Non-User) ---")
    for space in spaces_data:
        key = space.get('key')
        if key:
            print(key)
    print("--- End of Space Keys List ---")


def get_with_retry(url, params=None, auth=None, headers=None, verify=False, stream=False, timeout=30):
    logging.info(f"Making HTTP request to: {url}")
    logging.info(f"Request params: {params}")
    
    while True:
        try:
            logging.info("About to make requests.get call")
            resp = requests.get(url, params=params, auth=auth, headers=headers, verify=verify, stream=stream, timeout=timeout)
            logging.info(f"requests.get completed with status: {resp.status_code}")
            
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
                logging.warning(f"Rate limited (429). Retrying in {wait_time}s")
                time.sleep(wait_time)
                continue
                
            if resp.status_code >= 400:
                error_msg = f"Error {resp.status_code} fetching {url}. Response: {resp.text}"
                print(error_msg)
                logging.error(error_msg)
                
            logging.info(f"Request successful, returning response")
            return resp
            
        except requests.exceptions.Timeout as e:
            error_msg = f"Request timeout after {timeout}s for {url}: {e}"
            logging.error(error_msg)
            print(error_msg)
            return None
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request exception for {url}: {e}"
            logging.error(error_msg, exc_info=True)
            print(error_msg)
            return None

TOP_N_ROOT = 10
TOP_N_RECENT = 30
TOP_N_FREQUENT = 30

# Load configurable pickle directory from settings
OUTPUT_DIR = data_settings.get('pickle_dir', 'temp')

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
        params = {"type": "page", "spaceKey": space_key, "start": start, "limit": page_limit, "expand": "version,ancestors,children.page,children.attachment"} # MODIFIED expand
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
                'space_key': space_key,
                # NEW METADATA
                'attachments': page.get('children', {}).get('attachment', {}).get('results', []),
                'child_pages': [{'id': child.get('id'), 'title': child.get('title')} for child in page.get('children', {}).get('page', {}).get('results', [])]
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

def download_attachments_for_space(space_key, pages_metadata_list, base_output_dir, auth_tuple, verify_ssl_flag, base_confluence_url):
    if not pages_metadata_list:
        print(f"  No page metadata provided for space {space_key}, cannot download attachments.")
        return

    # Attachments will be stored in a folder named after the space_key, inside the base_output_dir
    space_attachment_dir = os.path.join(base_output_dir, space_key)
    
    if not os.path.exists(base_output_dir):
        print(f"  Error: Base output directory {base_output_dir} does not exist. Cannot create attachment folder for {space_key}.")
        return

    try:
        if not os.path.exists(space_attachment_dir):
            os.makedirs(space_attachment_dir)
            print(f"  Created attachment directory: {space_attachment_dir}")
    except OSError as e:
        print(f"  Error creating attachment directory {space_attachment_dir}: {e}. Skipping attachment downloads for this space.")
        return

    print(f"  Downloading attachments for space {space_key} to {space_attachment_dir}...")
    download_count = 0
    total_attachments_processed = 0

    for page_meta in pages_metadata_list:
        page_id = page_meta.get('id')
        attachments_on_page = page_meta.get('attachments', [])
        if not attachments_on_page:
            continue

        for att in attachments_on_page:
            total_attachments_processed += 1
            att_title = att.get('title')
            att_download_link_suffix = att.get('_links', {}).get('download')

            if not att_title or not att_download_link_suffix:
                print(f"    Skipping attachment with missing title or download link on page {page_id}.")
                continue
            
            # Sanitize filename (basic sanitization)
            # Replace characters that are problematic in filenames on some OSes
            # This is a basic sanitization, more robust might be needed depending on titles
            safe_att_title = "".join(c if c.isalnum() or c in ('.', '_', '-') else '_' for c in att_title)
            if not safe_att_title:
                safe_att_title = f"attachment_{att.get('id', 'unknown')}" # Fallback if title becomes empty

            file_path = os.path.join(space_attachment_dir, safe_att_title)
            
            if os.path.exists(file_path):
                # print(f"    Attachment {safe_att_title} already exists. Skipping download.")
                continue

            # Construct full download URL
            if att_download_link_suffix.startswith('/'):
                att_download_url = f"{base_confluence_url.rstrip('/')}{att_download_link_suffix}"
            else:
                att_download_url = f"{base_confluence_url.rstrip('/')}/{att_download_link_suffix}"

            try:
                # print(f"    Downloading: {safe_att_title} (Page ID: {page_id})")
                response = get_with_retry(att_download_url, auth=auth_tuple, verify=verify_ssl_flag, stream=True, timeout=60) # Increased timeout for downloads
                response.raise_for_status()

                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                download_count += 1
                if download_count % 20 == 0: # Log progress less frequently for downloads
                     print(f"    Downloaded {download_count} new attachments so far for space {space_key}...")
            except requests.exceptions.RequestException as e:
                print(f"    Error downloading attachment {safe_att_title} from {att_download_url}: {e}")
            except IOError as e:
                print(f"    Error writing attachment {safe_att_title} to {file_path}: {e}")
            except Exception as e:
                print(f"    Unexpected error with attachment {safe_att_title} on page {page_id}: {e}")
    
    if download_count > 0:
        print(f"  Finished: Downloaded {download_count} new attachments for space {space_key} (out of {total_attachments_processed} total attachment entries found).")
    elif total_attachments_processed > 0:
        print(f"  Finished: No new attachments downloaded for space {space_key} (all {total_attachments_processed} found attachments may already exist or had errors).")
    else:
        print(f"  Finished: No attachments found or processed for space {space_key}.")

def scan_existing_pickles(target_dir):
    """Scan a directory for existing pickle files and return the space keys they represent, excluding personal spaces."""
    if not os.path.exists(target_dir):
        print(f"Target directory {target_dir} does not exist.")
        return []
    
    pickle_files = [f for f in os.listdir(target_dir) if f.endswith('.pkl')]
    space_keys = []
    
    for pkl_file in pickle_files:
        # Extract space key from filename (remove .pkl extension)
        space_key = os.path.splitext(pkl_file)[0]
        # Remove _full suffix if present (for full pickle files)
        if space_key.endswith('_full'):
            space_key = space_key[:-5]
        
        # Exclude personal spaces (those starting with ~)
        if not space_key.startswith('~'):
            space_keys.append(space_key)
    
    return sorted(list(set(space_keys)))  # Remove duplicates and sort

def fetch_page_metadata_bulk(page_ids, batch_size=100):
    """Fetch page metadata for multiple page IDs using CQL search with batching."""
    logging.info(f"Starting bulk metadata fetch for {len(page_ids)} page IDs")
    all_metadata = []
    
    # Process page IDs in batches to avoid URL length limits
    for i in range(0, len(page_ids), batch_size):
        batch = page_ids[i:i + batch_size]
        
        # Build CQL query for this batch
        id_list = ','.join(batch)
        cql_query = f"id in ({id_list})"
        
        # Construct URL and parameters
        url = f"{BASE_URL}{API_ENDPOINT}/content/search"
        params = {
            "cql": cql_query,
            "expand": "version",
            "limit": batch_size
        }
        
        print(f"  Fetching metadata for {len(batch)} pages (batch {i//batch_size + 1})")
        logging.info(f"Making API call to: {url}")
        logging.info(f"CQL query: {cql_query}")
        logging.info(f"Batch {i//batch_size + 1}: Processing page IDs: {batch}")
        
        try:
            logging.info(f"About to call get_with_retry for batch {i//batch_size + 1}")
            r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
            logging.info(f"get_with_retry returned with status code: {r.status_code}")
            
            if r.status_code == 200:
                results = r.json().get("results", [])
                logging.info(f"Successfully retrieved {len(results)} results from API")
                all_metadata.extend(results)
            else:
                error_msg = f"Error fetching metadata batch: {r.status_code} - {r.text}"
                print(f"  {error_msg}")
                logging.error(error_msg)
                
        except Exception as e:
            error_msg = f"Exception during API call for batch {i//batch_size + 1}: {e}"
            logging.error(error_msg, exc_info=True)
            print(f"  {error_msg}")
    
    logging.info(f"Completed bulk metadata fetch. Total results: {len(all_metadata)}")
    return all_metadata

def update_existing_pickles(target_dir, log_file=None, reverse_order=False):
    """Update existing pickle files with latest page versions based on timestamp comparison."""
    abs_target_dir = os.path.abspath(target_dir)
    print(f"Pickle directory: {abs_target_dir}")
    write_log(log_file, "INFO", f"Starting pickle update process in directory: {abs_target_dir}")
    
    if not os.path.exists(target_dir):
        error_msg = f"Target directory {target_dir} does not exist."
        print(error_msg)
        logging.error(error_msg)
        return
    
    pickle_files = [f for f in os.listdir(target_dir) if f.endswith('.pkl')]
    if not pickle_files:
        warning_msg = f"No pickle files found in {target_dir}"
        print(warning_msg)
        logging.warning(warning_msg)
        return
    
    # Sort pickle files in the requested order
    if reverse_order:
        pickle_files.sort(reverse=True)
        print(f"Found {len(pickle_files)} pickle files to potentially update (processing in Z-A order)")
        logging.info(f"Found {len(pickle_files)} pickle files to potentially update (processing in Z-A order)")
    else:
        pickle_files.sort()
        print(f"Found {len(pickle_files)} pickle files to potentially update")
        logging.info(f"Found {len(pickle_files)} pickle files to potentially update")
    
    total_updated_pages = 0
    total_checked_files = 0
    failed_files = []
    skipped_files = []
    
    for file_index, pickle_file in enumerate(pickle_files, 1):
        # Extract space key from filename
        space_key = os.path.splitext(pickle_file)[0]
        if space_key.endswith('_full'):
            space_key = space_key[:-5]
        
        # Skip personal spaces
        if space_key.startswith('~'):
            skipped_files.append((pickle_file, "Personal space"))
            logging.debug(f"Skipping personal space: {space_key}")
            continue
        
        pickle_path = os.path.join(target_dir, pickle_file)
        print(f"\n[{file_index}/{len(pickle_files)}] Checking {pickle_file} (space: {space_key})")
        write_log(log_file, "INFO", f"Processing pickle file {file_index}/{len(pickle_files)}: {pickle_file} (space: {space_key})")
        
        try:
            # Load existing pickle data
            with open(pickle_path, 'rb') as f:
                existing_data = pickle.load(f)
            
            existing_pages = existing_data.get('sampled_pages', [])
            if not existing_pages:
                print(f"  No pages in pickle file, skipping")
                skipped_files.append((pickle_file, "No pages in pickle"))
                logging.warning(f"No pages found in pickle file: {pickle_file}")
                continue
            
            # Build lookup of existing pages by ID -> timestamp
            existing_page_lookup = {}
            page_ids = []
            for page in existing_pages:
                page_id = page.get('id')
                if page_id:
                    existing_page_lookup[page_id] = page.get('updated', '')
                    page_ids.append(page_id)
            
            if not page_ids:
                print(f"  No valid page IDs found in pickle, skipping")
                skipped_files.append((pickle_file, "No valid page IDs"))
                logging.warning(f"No valid page IDs found in pickle file: {pickle_file}")
                continue
            
            print(f"  Checking {len(page_ids)} existing pages for updates...")
            write_log(log_file, "INFO", f"Checking {len(page_ids)} existing pages for updates in space: {space_key}")
            
            # First, get ALL current pages in the space to find new pages
            print(f"  Fetching ALL current pages in space {space_key}...")
            write_log(log_file, "INFO", f"Fetching ALL current pages in space: {space_key}")
            all_current_pages = fetch_page_metadata(space_key)
            if not all_current_pages:
                logging.error(f"Failed to fetch current page list for space: {space_key}")
                failed_files.append((pickle_file, "Failed to fetch current page list from API"))
                continue
            
            print(f"  Found {len(all_current_pages)} total pages currently in space {space_key}")
            write_log(log_file, "INFO", f"Found {len(all_current_pages)} total pages currently in space: {space_key}")
            
            # Identify new pages (pages in space but not in pickle)
            all_current_page_ids = {page.get('id') for page in all_current_pages}
            existing_page_ids = set(page_ids)
            new_page_ids = all_current_page_ids - existing_page_ids
            
            if new_page_ids:
                print(f"  Found {len(new_page_ids)} NEW pages in space {space_key}")
                write_log(log_file, "INFO", f"Found {len(new_page_ids)} NEW pages in space: {space_key}")
            
            # Get metadata for existing pages to check for updates
            write_log(log_file, "INFO", f"About to call fetch_page_metadata_bulk for {len(page_ids)} existing pages in space: {space_key}")
            existing_pages_metadata = fetch_page_metadata_bulk(page_ids) if page_ids else []
            write_log(log_file, "INFO", f"fetch_page_metadata_bulk returned {len(existing_pages_metadata) if existing_pages_metadata else 0} results for existing pages")
            
            # Compare timestamps and identify changed existing pages
            pages_to_update = []
            for current_page in existing_pages_metadata:
                page_id = current_page.get('id')
                current_timestamp = current_page.get('version', {}).get('when', '')
                existing_timestamp = existing_page_lookup.get(page_id, '')
                
                if current_timestamp and current_timestamp > existing_timestamp:
                    pages_to_update.append(page_id)
                    logging.debug(f"Existing page {page_id} needs update: {existing_timestamp} -> {current_timestamp}")
            
            # Add all new pages to the update list
            pages_to_update.extend(new_page_ids)
            
            # Create combined metadata lookup for both existing and new pages
            current_page_metadata = existing_pages_metadata + [p for p in all_current_pages if p.get('id') in new_page_ids]
            
            if pages_to_update:
                updated_count = len([p for p in pages_to_update if p not in new_page_ids])
                new_count = len(new_page_ids)
                print(f"  Found {len(pages_to_update)} pages that need processing ({updated_count} updates, {new_count} new)")
                logging.info(f"Found {len(pages_to_update)} pages that need processing in space: {space_key} ({updated_count} updates, {new_count} new)")
                
                # Fetch full content for changed pages
                updated_pages_data = []
                for i, page_id in enumerate(pages_to_update, 1):
                    # Progress indicator every 20 pages or at the end
                    if i % 20 == 0 or i == len(pages_to_update):
                        print(f"    Progress: {i}/{len(pages_to_update)} pages fetched in space {space_key}")
                    
                    is_new_page = page_id in new_page_ids
                    action_type = "NEW" if is_new_page else "UPDATE"
                    logging.info(f"Fetching {action_type} content for page {page_id} ({i}/{len(pages_to_update)}) in space: {space_key}")
                    
                    try:
                        page_body = fetch_page_body(page_id)
                    except Exception as e:
                        logging.error(f"Failed to fetch body for page {page_id} in space {space_key}: {e}")
                        continue
                    
                    # Find the corresponding metadata
                    page_metadata = next((p for p in current_page_metadata if p.get('id') == page_id), None)
                    if page_metadata:
                        updated_page = {
                            'id': page_id,
                            'title': page_metadata.get('title', ''),
                            'updated': page_metadata.get('version', {}).get('when', ''),
                            'update_count': page_metadata.get('version', {}).get('number', 0),
                            'space_key': space_key,
                            'body': page_body
                        }
                        updated_pages_data.append(updated_page)
                
                # Update the pickle data in-place
                if updated_pages_data:
                    # Create a lookup for updated pages
                    updated_lookup = {page['id']: page for page in updated_pages_data}
                    
                    # Replace existing pages that were updated
                    for i, page in enumerate(existing_pages):
                        page_id = page.get('id')
                        if page_id in updated_lookup and page_id not in new_page_ids:
                            existing_pages[i] = updated_lookup[page_id]
                    
                    # Add new pages to the list
                    new_pages_to_add = [updated_lookup[page_id] for page_id in new_page_ids if page_id in updated_lookup]
                    existing_pages.extend(new_pages_to_add)
                    
                    # Update the total count in the pickle data
                    existing_data['total_pages_in_space'] = len(existing_pages)
                    
                    # Save updated pickle
                    with open(pickle_path, 'wb') as f:
                        pickle.dump(existing_data, f)
                    
                    print(f"  Successfully updated {updated_count} pages and added {len(new_pages_to_add)} new pages in {pickle_file}")
                    logging.info(f"Successfully updated {updated_count} pages and added {len(new_pages_to_add)} new pages in {pickle_file}")
                    total_updated_pages += len(updated_pages_data)
            else:
                print(f"  No updates needed for {pickle_file}")
                logging.info(f"No updates needed for {pickle_file}")
            
            total_checked_files += 1
            
            # Progress summary for this space
            print(f"  Completed space {space_key} - Total progress: {total_checked_files}/{len(pickle_files)} files processed, {total_updated_pages} pages updated so far")
            
        except Exception as e:
            error_msg = f"Error processing {pickle_file}: {e}"
            print(f"  {error_msg}")
            logging.error(error_msg, exc_info=True)
            failed_files.append((pickle_file, str(e)))
            continue
    
    # Summary logging
    print(f"\nUpdate summary:")
    print(f"  Files checked: {total_checked_files}")
    print(f"  Total pages updated: {total_updated_pages}")
    print(f"  Failed files: {len(failed_files)}")
    print(f"  Skipped files: {len(skipped_files)}")
    print(f"  Update process completed")
    
    # Detailed logging
    logging.info(f"Update process completed")
    logging.info(f"Files checked: {total_checked_files}")
    logging.info(f"Total pages updated: {total_updated_pages}")
    logging.info(f"Failed files: {len(failed_files)}")
    logging.info(f"Skipped files: {len(skipped_files)}")
    
    # Log details of failed files
    if failed_files:
        logging.warning(f"Failed to process {len(failed_files)} files:")
        for filename, reason in failed_files:
            logging.warning(f"  {filename}: {reason}")
    
    # Log details of skipped files  
    if skipped_files:
        logging.info(f"Skipped {len(skipped_files)} files:")
        for filename, reason in skipped_files:
            logging.info(f"  {filename}: {reason}")
    
    logging.info("Update process log completed")

def main():
    # Initialize log_file variable (will be set if logging is enabled)
    log_file = None
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Sample and pickle Confluence spaces. Handles checkpointing for resumable execution.",
        epilog="If no run mode argument (--reset, --batch-continue, --resume-from-pickles, --update-pickles, --update-pickles-reverse, --pickle-space-full SPACE_KEY) is provided, an interactive menu is shown." # Updated epilog
    )
    # Group for mutually exclusive run modes
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--reset', action='store_true', help='Reset checkpoint and start from beginning (non-interactive).')
    mode_group.add_argument('--batch-continue', action='store_true', help='Run in continue mode using checkpoint without interactive menu (non-interactive).')
    mode_group.add_argument('--resume-from-pickles', action='store_true', help='Resume work based on scanning existing pickle files in output directory (non-interactive).')
    mode_group.add_argument('--update-pickles', action='store_true', help='Update existing pickle files with latest page versions based on timestamp comparison (non-interactive).')
    mode_group.add_argument('--update-pickles-reverse', action='store_true', help='Update existing pickle files with latest page versions in Z-A order (non-interactive).')
    mode_group.add_argument('--pickle-space-full', type=str, metavar='SPACE_KEY',
                               help=f'Pickle all pages for a single space. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}. Bypasses sampling, checkpointing, and interactive menu.') # Updated help
    mode_group.add_argument('--pickle-all-spaces-full', action='store_true',
                               help=f'Pickle all pages for ALL non-user spaces. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}. Uses checkpoint file \'{FULL_PICKLE_CHECKPOINT_FILENAME}\' in that directory. Bypasses sampling and interactive menu.') # New argument, updated help
    parser.add_argument('--list-spaces', action='store_true', help='List all non-user spaces (key, name, description) to the console and exit.') # MODIFIED HELP
    parser.add_argument('--list-space-keys', action='store_true', help='List only the keys of all non-user spaces to the console and exit.') # NEW ARGUMENT
    parser.add_argument('--download-attachments', action='store_true',
                           help='Download attachments for processed spaces during full pickle modes. Attachments are saved into a subfolder named after the space key, within the full pickle output directory.')
    args = parser.parse_args()

    # Handle --list-spaces mode first as it's a simple informational command
    if args.list_spaces:
        print("Mode: Listing all non-user spaces from Confluence...")
        all_spaces = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
        print_spaces_nicely(all_spaces)
        sys.exit(0)

    # Handle --list-space-keys mode
    if args.list_space_keys:
        print("Mode: Listing all non-user space keys from Confluence...")
        all_spaces = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
        print_space_keys_only(all_spaces)
        sys.exit(0)

    # Handle --update-pickles mode
    if args.update_pickles:
        print(f"Mode: Updating existing pickle files with latest page versions")
        print(f"Target directory: {OUTPUT_DIR}")
        
        # Setup logging for update process
        log_file = setup_simple_logging('.')
        if log_file:
            print(f"Logging to: {log_file}")
        
        update_existing_pickles(OUTPUT_DIR, log_file)
        sys.exit(0)

    # Handle --update-pickles-reverse mode
    if args.update_pickles_reverse:
        print(f"Mode: Updating existing pickle files with latest page versions (Z-A order)")
        print(f"Target directory: {OUTPUT_DIR}")
        
        # Setup logging for update process
        log_file = setup_simple_logging('.')
        if log_file:
            print(f"Logging to: {log_file}")
        
        update_existing_pickles(OUTPUT_DIR, log_file, reverse_order=True)
        sys.exit(0)

    # Handle --pickle-all-spaces-full mode
    if args.pickle_all_spaces_full:
        print(f"Mode: Pickling all pages for ALL non-user spaces (Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}, Checkpoint: {FULL_PICKLE_CHECKPOINT_FILE_PATH}).")
        if args.download_attachments:
            print("Attachment download enabled via --download-attachments flag.")
        
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
            out_filename_check = f'{target_space_key}.pkl'
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
            
            # Create placeholder pickle immediately to claim this space and prevent race conditions
            out_filename = f'{target_space_key}.pkl'
            out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename)
            try:
                with open(out_path, 'wb') as f:
                    pickle.dump({
                        'space_key': target_space_key,
                        'name': space_name_for_pickle,
                        'status': 'processing',
                        'started_at': datetime.now().isoformat()
                    }, f)
                print(f"  Created placeholder pickle for {target_space_key}")
            except Exception as e:
                print(f"  Could not create placeholder pickle: {e}. Skipping to avoid race condition.")
                failed_this_run += 1
                continue
            
            try:
                pages_metadata = fetch_page_metadata(target_space_key)

                if not pages_metadata:
                    print(f"  No pages found for space {target_space_key} or error fetching metadata. Skipping.")
                    failed_this_run +=1
                    continue

                pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(target_space_key, pages_metadata, fetch_all=True)

                out_filename = f'{target_space_key}.pkl'
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

                # Download attachments if flag is set
                if args.download_attachments:
                    print(f"  Initiating attachment download for space {target_space_key}...")
                    # pages_metadata was fetched for this space_info earlier in the loop
                    download_attachments_for_space(target_space_key, pages_metadata, EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, (USERNAME, PASSWORD), VERIFY_SSL, BASE_URL)
            except Exception as e:
                print(f"  An unexpected error occurred during pickling or attachment download for space {target_space_key}: {e}")
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
        if args.download_attachments:
            print("Attachment download enabled via --download-attachments flag.")

        # Check if pickle file already exists
        out_filename_check = f'{target_space_key}.pkl'
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
        
        # Create placeholder pickle immediately to claim this space and prevent race conditions
        out_filename = f'{target_space_key}.pkl'
        out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename)
        try:
            with open(out_path, 'wb') as f:
                pickle.dump({
                    'space_key': target_space_key,
                    'name': space_name_for_pickle,
                    'status': 'processing',
                    'started_at': datetime.now().isoformat()
                }, f)
            print(f"  Created placeholder pickle for {target_space_key}")
        except Exception as e:
            print(f"  Could not create placeholder pickle: {e}. Exiting to avoid race condition.")
            sys.exit(1)
        
        pages_metadata = fetch_page_metadata(target_space_key)

        if not pages_metadata:
            print(f"  No pages found for space {target_space_key} or error fetching metadata. Exiting.")
            sys.exit(1)

        # sample_and_fetch_bodies will be modified to accept fetch_all=True
        pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(target_space_key, pages_metadata, fetch_all=True)

        out_filename = f'{target_space_key}.pkl'
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

        # Download attachments if flag is set
        if args.download_attachments:
            print(f"  Initiating attachment download for space {target_space_key}...")
            # pages_metadata was fetched for this space earlier
            download_attachments_for_space(target_space_key, pages_metadata, EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, (USERNAME, PASSWORD), VERIFY_SSL, BASE_URL)

        sys.exit(0)

    perform_reset = False
    full_mode = False  # Default to sample mode, will be overridden by menu choices
    # run_script = True # This variable was not used

    if args.reset:
        print("Mode: Running with --reset")
        perform_reset = True
    elif args.batch_continue:
        print("Mode: Running with --batch-continue (using checkpoint)")
        perform_reset = False # Default for continue is not to reset
    elif args.resume_from_pickles:
        print("Mode: Running with --resume-from-pickles (scanning existing pickle files)")
        perform_reset = False
        
        # Setup logging for resume process
        log_file = setup_simple_logging('.')
        if log_file:
            print(f"Logging to: {log_file}")
        
        # Scan existing pickles and create a synthetic checkpoint
        write_log(log_file, "INFO", "Starting resume from pickles process")
        existing_space_keys = scan_existing_pickles(OUTPUT_DIR)
        write_log(log_file, "INFO", f"Found {len(existing_space_keys)} existing pickle files in {OUTPUT_DIR}")
        
        print(f"Found {len(existing_space_keys)} existing pickle files in {OUTPUT_DIR}")
        if existing_space_keys:
            print(f"Existing space keys: {', '.join(existing_space_keys[:10])}{'...' if len(existing_space_keys) > 10 else ''}")
            write_log(log_file, "INFO", f"Sample existing space keys: {', '.join(existing_space_keys[:20])}")
        
        # Create a synthetic checkpoint based on existing pickles
        checkpoint = {
            "total_spaces": 0,  # Will be updated when we fetch all spaces
            "processed_spaces": existing_space_keys,
            "last_position": len(existing_space_keys),
            "last_updated": datetime.now().isoformat(),
            "created_from": "resume_from_pickles_scan"
        }
        # Save this synthetic checkpoint
        save_checkpoint(checkpoint)
        write_log(log_file, "INFO", f"Created synthetic checkpoint with {len(existing_space_keys)} pre-processed spaces")
        print(f"Created synthetic checkpoint with {len(existing_space_keys)} pre-processed spaces")
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
        print("  --resume-from-pickles         : Resume work based on scanning existing pickle files in output directory.") # NEW help line
        print("  --update-pickles              : Update existing pickle files with latest page versions based on timestamp comparison.") # NEW help line
        print("  --update-pickles-reverse      : Update existing pickle files with latest page versions in Z-A order.") # NEW help line
        print(f"  --pickle-space-full SPACE_KEY : Pickles all pages for a single space. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}.") # Updated help
        print(f"  --pickle-all-spaces-full      : Pickles all pages for ALL non-user spaces. Saves to {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}.") # New help line
        print(f"                                  Uses its own checkpoint ({FULL_PICKLE_CHECKPOINT_FILENAME}) and bypasses sampling and this interactive menu.") # Updated help
        print("  --list-spaces                 : Lists all non-user spaces (key, name, description) to the console and exits.") # MODIFIED HELP
        print("  --list-space-keys             : Lists only the keys of all non-user spaces to the console and exits.") # NEW HELP LINE
        print("  --download-attachments        : When used with --pickle-space-full or --pickle-all-spaces-full, downloads attachments.") # NEW HELP LINE FOR INTERACTIVE MODE
        print("------------------------------------\n") # Corrected to \\n
        while True:
            choice = input("Choose an action:\n" # Updated prompt
                           "  1: Fetch ALL pages - Continue from checkpoint (FULL mode)\n"
                           "  2: Fetch ALL pages - Reset and start fresh (FULL mode)\n"
                           "  3: Fetch ALL pages - Single space (FULL mode)\n"
                           "  4: Fetch ALL pages - All spaces with checkpoint (FULL mode)\n"
                           "  5: SAMPLE pages only - Continue from checkpoint (samples ~30-70 pages per space)\n"
                           "  6: SAMPLE pages only - Reset and start fresh (samples ~30-70 pages per space)\n"
                           "  7: Update existing pickles - Add newer page versions\n"
                           "  8: Update existing pickles - Add newer page versions (Z-A order)\n"
                           "  9: Resume from existing pickle files (scans output directory)\n"
                           "  10: List all non-user spaces (Key, Name, Description)\n"
                           "  11: List all non-user space KEYS only\n"
                           "  q: Quit\n"
                           "Enter choice (1-11 or q): ").strip().lower() # UPDATED PROMPT
            if choice == '1':
                # Full mode - continue from checkpoint for all spaces
                # This will use the full pickle checkpoint and directory
                print("Mode: Fetching ALL pages for all spaces - continuing from checkpoint (FULL mode).")
                # Jump to the full pickle all spaces logic (old choice 7)
                choice = 'full_all_continue'
                break
            elif choice == '2':
                # Full mode - reset and start fresh for all spaces  
                print("Mode: Fetching ALL pages for all spaces - resetting and starting fresh (FULL mode).")
                # Jump to the full pickle all spaces logic but with reset
                choice = 'full_all_reset'
                break
            elif choice == '3':
                # Full mode - single space
                print("Mode: Fetch ALL pages for a single space (FULL mode)")
                choice = 'full_single'
                break
            elif choice == '4':
                # Full mode - all spaces with checkpoint (same as choice 1)
                print("Mode: Fetching ALL pages for all spaces with checkpoint (FULL mode).")
                choice = 'full_all_continue'
                break
            elif choice == '5':
                # Sample mode - continue from checkpoint
                perform_reset = False
                full_mode = False
                print("Mode: SAMPLE pages only - continuing from checkpoint.")
                break
            elif choice == '6':
                # Sample mode - reset and start fresh
                perform_reset = True  
                full_mode = False
                print("Mode: SAMPLE pages only - resetting and starting fresh.")
                break
            elif choice == '7':
                # Update existing pickles
                print("Mode: Update existing pickle files with latest page versions")
                choice = 'update_existing'
                continue
            elif choice == '8':
                # Update existing pickles Z-A
                print("Mode: Update existing pickle files with latest page versions (Z-A order)")
                choice = 'update_existing_za'
                continue
            elif choice == '9':
                # Resume from existing pickle files
                print("Mode: Resume from existing pickle files (scanning output directory)")
                
                # Setup logging for resume process
                log_file = setup_simple_logging('.')
                if log_file:
                    print(f"Logging to: {log_file}")
                
                write_log(log_file, "INFO", "Starting resume from pickles process")
                existing_space_keys = scan_existing_pickles(OUTPUT_DIR)
                write_log(log_file, "INFO", f"Found {len(existing_space_keys)} existing pickle files in {OUTPUT_DIR}")
                
                print(f"Found {len(existing_space_keys)} existing pickle files in {OUTPUT_DIR}")
                if existing_space_keys:
                    print(f"Existing space keys: {', '.join(existing_space_keys[:10])}{'...' if len(existing_space_keys) > 10 else ''}")
                    write_log(log_file, "INFO", f"Sample existing space keys: {', '.join(existing_space_keys[:20])}")
                
                # Create a synthetic checkpoint based on existing pickles
                checkpoint = {
                    "total_spaces": 0,  # Will be updated when we fetch all spaces
                    "processed_spaces": existing_space_keys,
                    "last_position": len(existing_space_keys),
                    "last_updated": datetime.now().isoformat(),
                    "created_from": "resume_from_pickles_scan"
                }
                # Save this synthetic checkpoint
                save_checkpoint(checkpoint)
                write_log(log_file, "INFO", f"Created synthetic checkpoint with {len(existing_space_keys)} pre-processed spaces")
                print(f"Created synthetic checkpoint with {len(existing_space_keys)} pre-processed spaces")
                perform_reset = False
                full_mode = False  # Resume uses sampling by default
                break
            elif choice == '10':
                # List all non-user spaces
                print("Action: Listing all non-user spaces from Confluence...")
                all_spaces_interactive = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
                print_spaces_nicely(all_spaces_interactive)
                print("\nReturning to menu...")
                continue
            elif choice == '11':
                # List all non-user space keys only
                print("Action: Listing all non-user space KEYS only from Confluence...")
                all_spaces_interactive_keys = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
                print_space_keys_only(all_spaces_interactive_keys)
                print("\nReturning to menu...")
                continue
            elif choice == 'q':
                print("Exiting script.")
                sys.exit(0)
            elif choice == '4':
                print(f"Mode: Update existing pickle files with latest page versions")
                print(f"Target directory: {OUTPUT_DIR}")
                
                # Setup logging for update process
                log_file = setup_simple_logging('.')
                if log_file:
                    print(f"Logging to: {log_file}")
                
                update_existing_pickles(OUTPUT_DIR, log_file)
                print("\nReturning to menu...")
                continue # Go back to the interactive menu
            elif choice == '5':
                print(f"Mode: Update existing pickle files with latest page versions (Z-A order)")
                print(f"Target directory: {OUTPUT_DIR}")
                
                # Setup logging for update process
                log_file = setup_simple_logging('.')
                if log_file:
                    print(f"Logging to: {log_file}")
                
                update_existing_pickles(OUTPUT_DIR, log_file, reverse_order=True)
                print("\nReturning to menu...")
                continue # Go back to the interactive menu
            elif choice == '6':
                target_space_key_interactive = input("Enter the SPACE_KEY to pickle in full: ").strip().upper()
                if not target_space_key_interactive:
                    print("No space key provided. Please try again.")
                    continue
                args.pickle_space_full = target_space_key_interactive # Set this for logic reuse
                
                # Determine if attachments should be downloaded for this interactive session
                should_download_attachments_interactive_single = args.download_attachments # Respect CLI flag if present
                if not should_download_attachments_interactive_single:
                    dl_choice_single = input(f"Download attachments for space {args.pickle_space_full}? (y/n, default n): ").strip().lower()
                    if dl_choice_single == 'y':
                        should_download_attachments_interactive_single = True
                
                if should_download_attachments_interactive_single:
                    print(f"Attachment download will be attempted for {args.pickle_space_full}.")

                # Call the relevant part of main or refactor to a function
                print(f"Mode: Pickling all pages for space: {args.pickle_space_full}. Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}")

                # Check if pickle file already exists
                out_filename_check_interactive = f'{args.pickle_space_full}.pkl'
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
                
                # Create placeholder pickle immediately to claim this space and prevent race conditions
                out_filename = f'{args.pickle_space_full}.pkl'
                out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename)
                try:
                    with open(out_path, 'wb') as f:
                        pickle.dump({
                            'space_key': args.pickle_space_full,
                            'name': space_name_for_pickle,
                            'status': 'processing',
                            'started_at': datetime.now().isoformat()
                        }, f)
                    print(f"  Created placeholder pickle for {args.pickle_space_full}")
                except Exception as e:
                    print(f"  Could not create placeholder pickle: {e}. Returning to menu to avoid race condition.")
                    continue
                
                pages_metadata = fetch_page_metadata(args.pickle_space_full)

                if not pages_metadata:
                    print(f"  No pages found for space {args.pickle_space_full} or error fetching metadata. Exiting.")
                    sys.exit(1)

                pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(args.pickle_space_full, pages_metadata, fetch_all=True)

                out_filename = f'{args.pickle_space_full}.pkl'
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
                    # Potentially continue to attachment download if desired, or exit. For now, exiting on pickle error.
                    sys.exit(1)
                except Exception as e:
                    print(f"  An unexpected error occurred during pickling for space {args.pickle_space_full}: {e}")
                    sys.exit(1)

                # Download attachments if determined interactively or by flag
                if should_download_attachments_interactive_single:
                    print(f"  Initiating attachment download for space {args.pickle_space_full}...")
                    # pages_metadata was fetched for this space earlier in this block
                    download_attachments_for_space(args.pickle_space_full, pages_metadata, EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, (USERNAME, PASSWORD), VERIFY_SSL, BASE_URL)

                sys.exit(0) # Exit after this action
            elif choice == '7':
                # Simulate args for --pickle-all-spaces-full
                # args.pickle_all_spaces_full = True # This is implicitly handled by falling into the shared logic
                
                # Determine if attachments should be downloaded for this interactive session
                should_download_attachments_interactive_all = args.download_attachments # Respect CLI flag
                if not should_download_attachments_interactive_all:
                    dl_choice_all = input("Download attachments for ALL spaces in this operation? (y/n, default n): ").strip().lower()
                    if dl_choice_all == 'y':
                        should_download_attachments_interactive_all = True
                
                if should_download_attachments_interactive_all:
                    print("Attachment download will be attempted for all processed spaces.")

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
                    out_filename_check_interactive_all = f'{target_space_key}.pkl'
                    out_path_check_interactive_all = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename_check_interactive_all)
                    if os.path.exists(out_path_check_interactive_all):
                        print(f"  Pickle file {out_path_check_interactive_all} already exists. Skipping space {target_space_key}.")
                        if target_space_key not in processed_space_keys_set:
                            checkpoint["processed_space_keys"].append(target_space_key)
                            processed_space_keys_set.add(target_space_key) # Keep set in sync
                            save_checkpoint(checkpoint, FULL_PICKLE_CHECKPOINT_FILE_PATH) # MODIFIED
                        continue

                    print(f"  Processing space: {target_space_key} (Name: {space_name_for_pickle})")
                    
                    # Create placeholder pickle immediately to claim this space and prevent race conditions
                    out_filename = f'{target_space_key}.pkl'
                    out_path = os.path.join(EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, out_filename)
                    try:
                        with open(out_path, 'wb') as f:
                            pickle.dump({
                                'space_key': target_space_key,
                                'name': space_name_for_pickle,
                                'status': 'processing',
                                'started_at': datetime.now().isoformat()
                            }, f)
                        print(f"  Created placeholder pickle for {target_space_key}")
                    except Exception as e:
                        print(f"  Could not create placeholder pickle: {e}. Skipping to avoid race condition.")
                        failed_this_run += 1
                        continue
                    
                    try:
                        pages_metadata = fetch_page_metadata(target_space_key)

                        if not pages_metadata:
                            print(f"  No pages found for space {target_space_key} or error fetching metadata. Skipping.")
                            failed_this_run +=1
                            continue

                        pages_with_bodies, total_pages_metadata = sample_and_fetch_bodies(target_space_key, pages_metadata, fetch_all=True)

                        out_filename = f'{target_space_key}.pkl'
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

                        # Download attachments if determined interactively or by flag
                        if should_download_attachments_interactive_all:
                            print(f"  Initiating attachment download for space {target_space_key}...")
                            # pages_metadata was fetched for this space_info earlier in this loop
                            download_attachments_for_space(target_space_key, pages_metadata, EFFECTIVE_FULL_PICKLE_OUTPUT_DIR, (USERNAME, PASSWORD), VERIFY_SSL, BASE_URL)
                    except Exception as e:
                        print(f"  An unexpected error occurred during pickling or attachment download for space {target_space_key}: {e}")
                        failed_this_run += 1
                
                overall_processed_count += newly_processed_this_run
                print(f"\nFinished pickling all non-user spaces for this run.")
                print(f"Total successfully processed (including previous runs): {overall_processed_count} spaces based on checkpoint.")
                print(f"Newly processed in this run: {newly_processed_this_run} spaces.")
                print(f"Failed to process in this run: {failed_this_run} spaces.")
                if failed_this_run > 0:
                    print("Rerun the script to attempt processing failed spaces.")
                sys.exit(0) # Exit after this action
            elif choice == '8': # MODIFIED MENU HANDLING
                print("Action: Listing all non-user spaces from Confluence...")
                all_spaces_interactive = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
                print_spaces_nicely(all_spaces_interactive)
                print("\nReturning to menu...")
                continue # Go back to the interactive menu
            elif choice == '9': # NEW MENU HANDLING
                print("Action: Listing all non-user space KKEYS only from Confluence...")
                all_spaces_interactive_keys = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
                print_space_keys_only(all_spaces_interactive_keys)
                print("\nReturning to menu...")
                continue # Go back to the interactive menu
            elif choice == 'q':
                print("Exiting script.")
                sys.exit(0) # Exit gracefully
            else:
                print("Invalid choice. Please enter 1-11 or q.")
    
    # --- Handle special choice values for full mode ---
    if choice == 'full_all_continue' or choice == 'full_all_reset':
        # Full pickle all spaces mode
        if choice == 'full_all_reset' and os.path.exists(FULL_PICKLE_CHECKPOINT_FILE_PATH):
            print("Resetting full pickle checkpoint...")
            try:
                os.remove(FULL_PICKLE_CHECKPOINT_FILE_PATH)
                print(f"Successfully deleted {FULL_PICKLE_CHECKPOINT_FILE_PATH}.")
            except OSError as e:
                print(f"Error deleting checkpoint file: {e}")
        
        # Jump to full pickle all spaces logic
        print(f"Mode: Pickling all pages for ALL non-user spaces (Target dir: {EFFECTIVE_FULL_PICKLE_OUTPUT_DIR}).")
        
        checkpoint = load_checkpoint(FULL_PICKLE_CHECKPOINT_FILE_PATH)
        processed_space_keys_set = set(checkpoint.get("processed_space_keys", []))
        
        all_non_user_spaces = fetch_all_spaces_with_details(auth_details=(USERNAME, PASSWORD), verify_ssl_cert=VERIFY_SSL)
        
        if not all_non_user_spaces:
            print("No non-user spaces found to process. Exiting.")
            sys.exit(0)
            
        # Continue with full pickle logic...
        # [The rest of the full pickle all spaces logic would go here]
        sys.exit(0)  # Exit after processing
    
    elif choice == 'full_single':
        # Full pickle single space mode
        target_space_key = input("Enter the SPACE_KEY to pickle in full: ").strip().upper()
        if not target_space_key:
            print("No space key provided. Exiting.")
            sys.exit(1)
            
        # [The rest of the single space full pickle logic would go here]
        sys.exit(0)  # Exit after processing
    
    elif choice == 'update_existing':
        print(f"Mode: Update existing pickle files with latest page versions")
        print(f"Target directory: {OUTPUT_DIR}")
        log_file = setup_simple_logging('.')
        if log_file:
            print(f"Logging to: {log_file}")
        update_existing_pickles(OUTPUT_DIR, log_file)
        sys.exit(0)
        
    elif choice == 'update_existing_za':
        print(f"Mode: Update existing pickle files with latest page versions (Z-A order)")
        print(f"Target directory: {OUTPUT_DIR}")
        log_file = setup_simple_logging('.')
        if log_file:
            print(f"Logging to: {log_file}")
        update_existing_pickles(OUTPUT_DIR, log_file, reverse_order=True)
        sys.exit(0)
    
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
    write_log(log_file, "INFO", "Starting to fetch all spaces from Confluence")
    while True:
        # Construct URL using BASE_URL and API_ENDPOINT
        url = f"{BASE_URL}{API_ENDPOINT}/space"
        params = {"start": start_fetch_api, "limit": 100, "type": "global"}
        print(f"  Fetching next batch of spaces from Confluence: {url}?start={start_fetch_api}&limit=100&type=global")
        write_log(log_file, "INFO", f"Fetching spaces batch: start={start_fetch_api}, limit=100, type=global")
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch spaces. Status code: {r.status_code}. Response: {r.text}")
            write_log(log_file, "ERROR", f"Failed to fetch spaces. Status code: {r.status_code}. Response: {r.text}")
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
    write_log(log_file, "INFO", f"Total non-user spaces fetched: {len(all_spaces)}")
    
    # Determine spaces to process based on checkpoint
    # For resume from pickles, we need to check individual space keys rather than using positional indexing
    if checkpoint.get("created_from") == "resume_from_pickles_scan":
        # Filter out spaces that already have pickles (based on space key, not position)
        spaces_to_process = [space for space in all_spaces if space.get('key') not in processed_space_keys]
        write_log(log_file, "INFO", f"Resume from pickles mode: filtering {len(all_spaces)} total spaces against {len(processed_space_keys)} existing pickle keys")
        write_log(log_file, "INFO", f"Spaces to process in this run: {len(spaces_to_process)} (filtered by space key)")
        print(f"Resume from pickles: {len(spaces_to_process)} spaces need processing out of {len(all_spaces)} total")
    else:
        # Standard checkpoint mode - use positional indexing
        if effective_start_idx_for_slicing >= len(all_spaces) and not perform_reset and len(all_spaces) > 0:
            print(f"All {len(all_spaces)} spaces already processed according to checkpoint. Nothing to do.")
            print("Run with --reset to process all spaces again.")
            write_log(log_file, "INFO", f"All {len(all_spaces)} spaces already processed according to checkpoint. Nothing to do.")
            sys.exit(0)
            
        spaces_to_process = all_spaces[effective_start_idx_for_slicing:]
        write_log(log_file, "INFO", f"Standard checkpoint mode: {len(spaces_to_process)} spaces to process (starting from index {effective_start_idx_for_slicing})")
    
    write_log(log_file, "INFO", f"Processed spaces from checkpoint: {len(processed_space_keys)} unique keys")
    
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
    for loop_counter, space_data in enumerate(spaces_to_process):
        # Calculate global index for logging - different logic for resume vs standard mode
        if checkpoint.get("created_from") == "resume_from_pickles_scan":
            # For resume mode, find the actual position in all_spaces
            space_key = space_data.get('key')
            current_global_idx = next((i + 1 for i, sp in enumerate(all_spaces) if sp.get('key') == space_key), loop_counter + 1)
        else:
            # Standard mode - use positional indexing
            current_global_idx = effective_start_idx_for_slicing + loop_counter + 1
        
        space_key = space_data.get('key') # Ensure we use the correct variable name 'space_data'
        if not space_key:
            print(f"Warning: Space data at position {loop_counter} missing key. Skipping: {space_data}")
            write_log(log_file, "WARNING", f"Space data at position {loop_counter} missing key. Skipping: {space_data}")
            continue
            
        # This check is mostly redundant when resuming from pickles since we already filtered,
        # but good for safety in standard checkpoint mode.
        if space_key in processed_space_keys and not perform_reset:
            print(f"Skipping already processed space {current_global_idx}/{len(all_spaces)}: {space_key}")
            write_log(log_file, "INFO", f"Skipping already processed space {current_global_idx}/{len(all_spaces)}: {space_key}")
            # Update last_position only in standard checkpoint mode
            if checkpoint.get("created_from") != "resume_from_pickles_scan":
                if checkpoint.get("last_position", 0) < current_global_idx:
                     checkpoint["last_position"] = current_global_idx
            continue
            
        try:
            print(f"Processing space {current_global_idx}/{len(all_spaces)}: {space_key} (Name: {space_data.get('name', 'N/A')})")
            write_log(log_file, "INFO", f"Processing space {current_global_idx}/{len(all_spaces)}: {space_key} (Name: {space_data.get('name', 'N/A')})")
            pages_metadata = fetch_page_metadata(space_key) # Changed 'pages' to 'pages_metadata'
            
            # Check if we should fetch all pages or sample
            if full_mode or checkpoint.get("created_from") == "resume_from_pickles_scan":
                pages_with_bodies, total_pages_in_space = sample_and_fetch_bodies(space_key, pages_metadata, fetch_all=True)
                mode_desc = "FULL mode" if full_mode else "resume from pickles mode"
                print(f'  Fetching ALL pages for space {space_key} ({mode_desc})')
                write_log(log_file, "INFO", f"Fetching ALL pages for space {space_key} ({mode_desc})")
            else:
                pages_with_bodies, total_pages_in_space = sample_and_fetch_bodies(space_key, pages_metadata)
                print(f'  Sampling pages for space {space_key} (SAMPLE mode)')
                write_log(log_file, "INFO", f"Sampling pages for space {space_key} (SAMPLE mode)")
            
            out_path = os.path.join(OUTPUT_DIR, f'{space_key}.pkl')
            with open(out_path, 'wb') as f:
                pickle.dump({'space_key': space_key, 'name': space_data.get('name'), 'sampled_pages': pages_with_bodies, 'total_pages_in_space': total_pages_in_space}, f)
            print(f'  Successfully wrote {len(pages_with_bodies)} pages for space {space_key} to {out_path} (total pages in space: {total_pages_in_space})')
            write_log(log_file, "INFO", f"Successfully wrote {len(pages_with_bodies)} pages for space {space_key} to {out_path} (total pages in space: {total_pages_in_space})")
            
            # Update checkpoint after each successful space processing
            if space_key not in checkpoint["processed_spaces"]: # Add only if not already there (e.g. due to a partial run)
                 checkpoint["processed_spaces"].append(space_key)
            # Only update last_position in standard checkpoint mode
            if checkpoint.get("created_from") != "resume_from_pickles_scan":
                checkpoint["last_position"] = current_global_idx # Record 1-based index of last successfully processed space
            save_checkpoint(checkpoint)
            
        except Exception as e:
            print(f"Error processing space {space_key} (Global Index {current_global_idx}): {e}")
            write_log(log_file, "ERROR", f"Error processing space {space_key} (Global Index {current_global_idx}): {e}")
            # Decide if we want to update last_position even on error.
            # If we do, a subsequent run will skip this problematic space.
            # If we don't, it will be retried. For now, let's assume we want to retry.
            # So, we don't update checkpoint["last_position"] here on error, it remains at the previously successful one.
            # However, we should save any other checkpoint changes (like total_spaces if it was the first update)
            save_checkpoint(checkpoint) # Save at least to persist last_updated and total_spaces
            print(f"  Skipping to next space due to error with {space_key}.")
            write_log(log_file, "INFO", f"Skipping to next space due to error with {space_key}.")
            # Continue to the next space rather than breaking the whole script
    
    print("\nAll done!") # Corrected to \n
    final_processed_count = len(checkpoint.get("processed_spaces",[]))
    total_spaces_in_checkpoint = checkpoint.get("total_spaces", len(all_spaces))
    print(f"Successfully processed and sampled {final_processed_count} spaces out of {total_spaces_in_checkpoint} total non-user spaces.")
    write_log(log_file, "INFO", f"Processing completed. Successfully processed {final_processed_count} spaces out of {total_spaces_in_checkpoint} total non-user spaces.")
    if final_processed_count < total_spaces_in_checkpoint:
        skipped_count = total_spaces_in_checkpoint - final_processed_count
        print(f"{skipped_count} spaces may have been skipped due to errors or if the script was interrupted.")
        print("You can re-run the script to attempt processing remaining/failed spaces.")
        write_log(log_file, "WARNING", f"{skipped_count} spaces may have been skipped due to errors or interruption.")
    write_log(log_file, "INFO", "Script execution completed.")

if __name__ == '__main__':
    main()
