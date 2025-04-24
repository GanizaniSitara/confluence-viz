import os
import pickle
from collections import defaultdict
from datetime import datetime
import requests
import time
import urllib3
from config_loader import load_confluence_settings

# Load settings
settings = load_confluence_settings()
API_BASE_URL = settings['api_base_url']
USERNAME = settings['username']
PASSWORD = settings['password']
VERIFY_SSL = settings['verify_ssl']

# Suppress InsecureRequestWarning if SSL verification is off
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_with_retry(url, params=None, auth=None, verify=False):
    """HTTP GET with retry on 429 (rate limit) and exponential backoff."""
    backoff = 1
    while True:
        resp = requests.get(url, params=params, auth=auth, verify=verify)
        if resp.status_code == 429:
            print(f"Warning: Rate limited (429). Retrying {url} in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        if resp.status_code >= 400:
            print(f"Error {resp.status_code} fetching {url}. Response: {resp.text}")
        return resp

# Adjust these as needed
INPUT_PICKLE = 'confluence_data.pkl'  # or your main data pickle
OUTPUT_DIR = 'temp'
TOP_N = 100
SPACES_PAGE_LIMIT = 50
CONTENT_PAGE_LIMIT = 100

def fetch_all_spaces():
    print("Fetching all spaces from Confluence...")
    spaces = []
    start = 0
    idx = 0
    while True:
        url = f"{API_BASE_URL}/rest/api/space"
        params = {"start": start, "limit": SPACES_PAGE_LIMIT}
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
        if len(results) < SPACES_PAGE_LIMIT:
            break
        start += SPACES_PAGE_LIMIT
    print(f"Total spaces fetched: {len(spaces)}")
    return spaces

def fetch_pages_for_space(space_key):
    print(f"  Fetching pages for space: {space_key}")
    pages = []
    start = 0
    while True:
        url = f"{API_BASE_URL}/rest/api/content"
        params = {"type": "page", "spaceKey": space_key, "start": start, "limit": CONTENT_PAGE_LIMIT, "expand": "version,ancestors"}
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"  Failed to fetch pages for space {space_key}. Status code: {r.status_code}")
            break
        results = r.json().get("results", [])
        if not results:
            break
        for page in results:
            # Extract relevant info for sampling
            page_info = {
                'id': page.get('id'),
                'title': page.get('title'),
                'updated': page.get('version', {}).get('when', ''),
                'update_count': page.get('version', {}).get('number', 0),
                'parent_id': page['ancestors'][0]['id'] if page.get('ancestors') else None,
                'level': len(page.get('ancestors', [])),
                'space_key': space_key,
                'raw': page
            }
            pages.append(page_info)
        if len(results) < CONTENT_PAGE_LIMIT:
            break
        start += CONTENT_PAGE_LIMIT
    print(f"  Total pages fetched for space {space_key}: {len(pages)}")
    return pages

def sample_pages(space_pages):
    # Root + 1 level
    root_and_first = [p for p in space_pages if p.get('level', 0) <= 1]
    # Top 100 most recently updated
    most_recent = sorted(space_pages, key=lambda p: p.get('updated', ''), reverse=True)[:TOP_N]
    # Top 100 most frequently updated
    most_frequent = sorted(space_pages, key=lambda p: p.get('update_count', 0), reverse=True)[:TOP_N]
    # Combine and deduplicate by page id
    all_pages = root_and_first + most_recent + most_frequent
    seen = set()
    deduped = []
    for p in all_pages:
        pid = p.get('id')
        if pid and pid not in seen:
            deduped.append(p)
            seen.add(pid)
    return deduped

def main():
    print("Starting Confluence sampling and pickling process...")
    spaces = fetch_all_spaces()
    if not spaces:
        print("No spaces found. Exiting.")
        return
    for idx, space in enumerate(spaces, start=1):
        space_key = space.get('key') or space.get('space_key')
        if not space_key:
            continue
        print(f"Processing space {idx}/{len(spaces)}: {space_key}")
        pages = fetch_pages_for_space(space_key)
        sampled = sample_pages(pages)
        out_path = os.path.join(OUTPUT_DIR, f'{space_key}.pkl')
        with open(out_path, 'wb') as f:
            pickle.dump({'space_key': space_key, 'sampled_pages': sampled}, f)
        print(f'  Wrote {len(sampled)} sampled pages for space {space_key} to {out_path}')
    print("All done!")

if __name__ == '__main__':
    main()
