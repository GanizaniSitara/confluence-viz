import os
import pickle
import json
from collections import defaultdict
from datetime import datetime
import requests
import time
import urllib3
import argparse
from config_loader import load_confluence_settings

# Load settings
settings = load_confluence_settings()
API_BASE_URL = settings['api_base_url']
USERNAME = settings['username']
PASSWORD = settings['password']
VERIFY_SSL = settings['verify_ssl']

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

TOP_N_ROOT = 20
TOP_N_RECENT = 15
TOP_N_FREQUENT = 15
OUTPUT_DIR = 'temp'
CHECKPOINT_FILE = 'confluence_checkpoint.json'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_page_metadata(space_key):
    print(f"  Fetching page metadata for space: {space_key}")
    pages = []
    start = 0
    while True:
        url = f"{BASE_URL}/rest/api/content"
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
    url = f"{API_BASE_URL}/rest/api/content/{page_id}"
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
    parser = argparse.ArgumentParser(description="Sample and pickle Confluence spaces")
    parser.add_argument('--reset', action='store_true', help='Reset checkpoint and start from beginning')
    args = parser.parse_args()
    
    # Initialize or load checkpoint
    if args.reset and os.path.exists(CHECKPOINT_FILE):
        print("Resetting checkpoint as requested")
        os.remove(CHECKPOINT_FILE)
        
    checkpoint = load_checkpoint()
    processed_space_keys = set(checkpoint.get("processed_spaces", []))
    start_position = checkpoint.get("last_position", 0) if not args.reset else 0
    
    print(f"Starting Confluence sampling and pickling process...")
    print(f"- Start position: {start_position}")
    print(f"- Spaces already processed: {len(processed_space_keys)}")
    
    # Fetch all spaces (reuse your fetch_all_spaces logic)
    spaces = []
    start = 0
    idx = 0
    while True:
        url = f"{API_BASE_URL}/rest/api/space"
        params = {"start": start, "limit": 50}
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch spaces. Status code: {r.status_code}")
            break
        results = r.json().get("results", [])
        if not results:
            break
        for sp in results:
            if sp.get("key", "").startswith("~"):  # Exclude user spaces
                print(f"Skipping user space: key={sp.get('key')}, name={sp.get('name')}")
                continue
            idx += 1
            print(f"[{idx}] Fetched space: key={sp.get('key')}, name={sp.get('name')}")
            spaces.append(sp)
        if len(results) < 50:
            break
        start += 50    
        print(f"Total spaces fetched: {len(spaces)}")
    
    # Skip spaces that were already processed according to our checkpoint
    spaces_to_process = spaces
    if start_position > 0:
        print(f"Resuming from position {start_position} based on checkpoint")
        spaces_to_process = spaces[start_position:]
    
    # Ensure checkpoint has the total spaces count
    checkpoint["total_spaces"] = len(spaces)
    save_checkpoint(checkpoint)
    
    # Process each space
    for idx, space in enumerate(spaces_to_process, start=start_position + 1):
        space_key = space.get('key') or space.get('space_key')
        if not space_key:
            continue
            
        # Skip if already processed (additional check)
        if space_key in processed_space_keys:
            print(f"Skipping already processed space {idx}/{len(spaces)}: {space_key}")
            continue
            
        try:
            print(f"Processing space {idx}/{len(spaces)}: {space_key}")
            pages = fetch_page_metadata(space_key)
            sampled, total_pages = sample_and_fetch_bodies(space_key, pages)
            out_path = os.path.join(OUTPUT_DIR, f'{space_key}.pkl')
            with open(out_path, 'wb') as f:
                pickle.dump({'space_key': space_key, 'sampled_pages': sampled, 'total_pages': total_pages}, f)
            print(f'  Wrote {len(sampled)} sampled pages (with bodies) for space {space_key} to {out_path} (total pages: {total_pages})')
            
            # Update checkpoint after each successful space processing
            checkpoint["processed_spaces"].append(space_key)
            checkpoint["last_position"] = idx
            save_checkpoint(checkpoint)
            
        except Exception as e:
            print(f"Error processing space {space_key}: {e}")
            # Still update the position even if an error occurred
            checkpoint["last_position"] = idx 
            save_checkpoint(checkpoint)
            # Continue to the next space rather than breaking
    
    print("All done!")
    print(f"Processed {len(checkpoint['processed_spaces'])} spaces out of {len(spaces)} total spaces.")

if __name__ == '__main__':
    main()
