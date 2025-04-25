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

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_with_retry(url, params=None, auth=None, verify=False):
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

TOP_N_ROOT = 20
TOP_N_RECENT = 15
TOP_N_FREQUENT = 15
OUTPUT_DIR = 'temp'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_page_metadata(space_key):
    print(f"  Fetching page metadata for space: {space_key}")
    pages = []
    start = 0
    while True:
        url = f"{API_BASE_URL}/rest/api/content"
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

def main():
    print("Starting Confluence sampling and pickling process...")
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
    for idx, space in enumerate(spaces, start=1):
        space_key = space.get('key') or space.get('space_key')
        if not space_key:
            continue
        print(f"Processing space {idx}/{len(spaces)}: {space_key}")
        pages = fetch_page_metadata(space_key)
        sampled, total_pages = sample_and_fetch_bodies(space_key, pages)
        out_path = os.path.join(OUTPUT_DIR, f'{space_key}.pkl')
        with open(out_path, 'wb') as f:
            pickle.dump({'space_key': space_key, 'sampled_pages': sampled, 'total_pages': total_pages}, f)
        print(f'  Wrote {len(sampled)} sampled pages (with bodies) for space {space_key} to {out_path} (total pages: {total_pages})')
    print("All done!")

if __name__ == '__main__':
    main()
