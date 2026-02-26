# description: Deletes a Confluence page given its URL or page ID.

import sys as _sys, os as _os; _sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
import argparse
import sys
import re
import time
import random
import requests
import urllib3

from utils.config_loader import load_confluence_settings

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
try:
    confluence_settings = load_confluence_settings()
    CONFLUENCE_URL = confluence_settings['base_url']
    CONFLUENCE_USERNAME = confluence_settings['username']
    CONFLUENCE_PASSWORD = confluence_settings['password']
    VERIFY_SSL = confluence_settings['verify_ssl']
except FileNotFoundError:
    print("Error: settings.ini not found.")
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
    """Make API request with exponential backoff for 429 responses"""
    retry_count = 0
    base_wait_time = 2

    while retry_count <= max_retries:
        try:
            if method == 'GET':
                response = session.get(url, params=params)
            elif method == 'DELETE':
                response = session.delete(url, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # If successful or non-429 error, return
            if response.status_code == 204:
                # DELETE success returns no content
                return None
            if response.status_code != 429:
                response.raise_for_status()
                return response.json() if response.text else None

            # Handle 429 Too Many Requests
            retry_count += 1
            retry_after = response.headers.get('Retry-After')
            if retry_after and retry_after.isdigit():
                wait_time = int(retry_after)
            else:
                wait_time = base_wait_time * (2 ** (retry_count - 1)) + random.uniform(0, 1)

            if wait_time == 0:
                wait_time = 2

            print(f"Rate limited (429). Waiting {wait_time:.2f}s... (Attempt {retry_count}/{max_retries})")
            time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count > max_retries:
                raise Exception(f"Maximum retries reached. Last error: {str(e)}")

            wait_time = base_wait_time * (2 ** (retry_count - 1)) + random.uniform(0, 1)
            print(f"Request failed: {str(e)}. Retrying in {wait_time:.2f}s... (Attempt {retry_count}/{max_retries})")
            time.sleep(wait_time)

    raise Exception("Maximum retries reached without successful response")


def extract_page_id(url):
    """
    Extract page ID from various Confluence URL formats:
    - .../pages/12345/...           (Cloud)
    - .../pages/viewpage.action?pageId=12345  (Server/DC)
    - .../display/SPACE/...         (need to resolve via API)
    """
    # Try pageId query param first
    match = re.search(r'pageId=(\d+)', url)
    if match:
        return match.group(1)

    # Try /pages/12345/ pattern
    match = re.search(r'/pages/(\d+)', url)
    if match:
        return match.group(1)

    return None


def get_page_info(session, page_id):
    """Get page title and space for confirmation"""
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/content/{page_id}"
    else:
        url = f"{base_url}/rest/api/content/{page_id}"

    return make_api_request(session, url, params={'expand': 'space'})


def delete_page(session, page_id):
    """Delete a page by ID"""
    base_url = CONFLUENCE_URL.rstrip('/')
    if base_url.endswith('/rest/api'):
        url = f"{base_url}/content/{page_id}"
    else:
        url = f"{base_url}/rest/api/content/{page_id}"

    make_api_request(session, url, method='DELETE')


def main():
    parser = argparse.ArgumentParser(
        description="Delete a Confluence page given its URL or page ID.",
        epilog="""Examples:
  python delete_confluence_page.py https://confluence.example.com/pages/12345/My-Page
  python delete_confluence_page.py --id 12345
  python delete_confluence_page.py --id 12345 -y"""
    )
    parser.add_argument("url", nargs='?', help="The Confluence page URL to delete")
    parser.add_argument("--id", dest="page_id", help="The Confluence page ID to delete")
    parser.add_argument("-y", "--yes", action="store_true",
                        help="Skip confirmation prompt")

    args = parser.parse_args()

    # Get page ID from --id or extract from URL
    if args.page_id:
        page_id = args.page_id
        if not page_id.isdigit():
            print(f"Error: Page ID must be numeric: {page_id}")
            sys.exit(1)
    elif args.url:
        page_id = extract_page_id(args.url)
        if not page_id:
            print(f"Error: Could not extract page ID from URL: {args.url}")
            print("Supported URL formats:")
            print("  - https://domain/wiki/spaces/SPACE/pages/12345/Page+Title")
            print("  - https://domain/pages/viewpage.action?pageId=12345")
            sys.exit(1)
    else:
        print("Error: Either a URL or --id must be provided.")
        parser.print_help()
        sys.exit(1)

    print(f"Confluence URL: {CONFLUENCE_URL}")
    print(f"Page ID: {page_id}")
    if not VERIFY_SSL:
        print("SSL verification: DISABLED")
    print("-" * 40)

    try:
        session = create_session()

        # Get page info for confirmation
        page_info = get_page_info(session, page_id)
        title = page_info.get('title', 'Unknown')
        space_key = page_info.get('space', {}).get('key', 'Unknown')

        print(f"Page found: '{title}'")
        print(f"Space: {space_key}")
        print("-" * 40)

        # Confirm deletion
        if not args.yes:
            confirm = input(f"Delete this page? [y/N]: ").strip().lower()
            if confirm != 'y':
                print("Cancelled.")
                sys.exit(0)

        # Delete the page
        print("Deleting page...")
        delete_page(session, page_id)
        print(f"Page '{title}' (ID: {page_id}) deleted successfully.")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Error: Page with ID {page_id} not found.")
        elif e.response.status_code == 403:
            print(f"Error: Permission denied. You don't have delete access to this page.")
        else:
            print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
