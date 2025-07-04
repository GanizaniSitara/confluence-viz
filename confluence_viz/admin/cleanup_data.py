# description: Cleans up data for Confluence visualization.

import os
import argparse
import re
import sys
import time
import requests
import urllib3

# Load Confluence settings
from config_loader import load_confluence_settings

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def sleep_with_backoff(attempt):    
    wait_time = min(2 ** attempt, 60) # Cap wait time at 60 seconds
    print(f"    Retrying in {wait_time}s...", file=sys.stderr)
    time.sleep(wait_time)

def request_with_retry(method, url, auth, verify, params=None, json_payload=None, headers=None):    
    attempt = 0
    while True:
        try:
            resp = requests.request(method, url, params=params, json=json_payload, auth=auth, verify=verify, headers=headers)

            if resp.status_code == 429:
                print(f"Warning: Rate limited (429) on {method} {url}. ", file=sys.stderr, end='')
                attempt += 1
                sleep_with_backoff(attempt)
                continue # Retry the request

            # Log other client/server errors but don't necessarily retry
            if 400 <= resp.status_code < 600:
                 # Log more details for debugging
                error_details = ""
                try:
                    error_details = resp.json() # Try to get JSON error details
                except requests.exceptions.JSONDecodeError:
                    error_details = resp.text # Fallback to text
                print(f"Error: {method} {url} failed with status {resp.status_code}. Details: {error_details}", file=sys.stderr)

            return resp # Return response regardless of status code (caller handles success/failure)

        except requests.exceptions.RequestException as e:
            print(f"Error: Network or request exception during {method} {url}: {e}", file=sys.stderr)
            attempt += 1
            # Retry on general request exceptions as well, could be transient network issues
            sleep_with_backoff(attempt)
            if attempt > 5: # Limit retries for general exceptions
                 print(f"Error: Max retries exceeded for {method} {url}. Giving up.", file=sys.stderr)
                 # Return a dummy response or raise an exception? Let's return None
                 return None


# --------------------------------------------------------------------------
# Core Confluence API Functions
# --------------------------------------------------------------------------

def fetch_spaces(base_url, auth, verify, space_limit=50):
    """Fetches all spaces from Confluence, handling pagination."""
    all_spaces = []
    start = 0
    print("Fetching all spaces from Confluence...")
    while True:
        url = f"{base_url}/rest/api/space"
        params = {"start": start, "limit": space_limit, "type": "global"} # Only fetch global spaces
        print(f"  Fetching spaces batch starting at index {start}...")
        resp = request_with_retry("GET", url, auth=auth, verify=verify, params=params)

        if resp is None or resp.status_code != 200:
            print(f"Error: Failed to fetch spaces batch starting at {start}. Status: {resp.status_code if resp else 'None'}", file=sys.stderr)
            break # Stop fetching if a batch fails

        try:
            data = resp.json()
            results = data.get("results", [])
            all_spaces.extend(results)
            print(f"  Fetched {len(results)} spaces in this batch. Total fetched: {len(all_spaces)}")

            # Check if this is the last page
            if len(results) < space_limit: # Assumes 'limit' parameter is respected
                 print("  Reached the last page of spaces.")
                 break
            else:
                 start += len(results) # Use actual results length for next start, safer than adding limit

        except requests.exceptions.JSONDecodeError:
            print(f"Error: Failed to decode JSON response when fetching spaces at start={start}.", file=sys.stderr)
            break
        except Exception as e:
             print(f"Error: Unexpected error processing spaces batch: {e}", file=sys.stderr)
             break

    print(f"Finished fetching spaces. Total found: {len(all_spaces)}")
    return all_spaces

def fetch_pages_for_space(base_url, auth, verify, space_key, content_limit=100):
    """Fetches all page IDs within a given space."""
    page_ids = []
    start = 0
    print(f"  Fetching pages for space '{space_key}'...")
    while True:
        url = f"{base_url}/rest/api/content"
        params = {"spaceKey": space_key, "type": "page", "start": start, "limit": content_limit, "status": "current"}
        # print(f"    Fetching pages batch starting at index {start}...") # Verbose
        resp = request_with_retry("GET", url, auth=auth, verify=verify, params=params)

        if resp is None or resp.status_code != 200:
            print(f"Error: Failed to fetch pages for space '{space_key}' (start={start}). Status: {resp.status_code if resp else 'None'}", file=sys.stderr)
            break

        try:
            data = resp.json()
            results = data.get("results", [])
            current_page_ids = [page['id'] for page in results]
            page_ids.extend(current_page_ids)
            # print(f"    Fetched {len(results)} pages in this batch. Total for space: {len(page_ids)}") # Verbose

            if len(results) < content_limit:
                # print(f"    Reached the last page of content for space '{space_key}'.") # Verbose
                break
            else:
                start += len(results)

        except requests.exceptions.JSONDecodeError:
            print(f"Error: Failed to decode JSON response when fetching pages for space '{space_key}' at start={start}.", file=sys.stderr)
            break
        except Exception as e:
             print(f"Error: Unexpected error processing pages batch for space '{space_key}': {e}", file=sys.stderr)
             break

    print(f"  Found {len(page_ids)} pages in space '{space_key}'.")
    return page_ids

def delete_page(base_url, auth, verify, page_id):
    """Deletes a single page by its ID."""
    url = f"{base_url}/rest/api/content/{page_id}"
    print(f"    Deleting page ID: {page_id}...")
    resp = request_with_retry("DELETE", url, auth=auth, verify=verify)

    if resp is not None and resp.status_code == 204:
        print(f"      Successfully deleted page ID: {page_id}")
        return True
    else:
        print(f"      Failed to delete page ID: {page_id}. Status: {resp.status_code if resp else 'None'}", file=sys.stderr)
        return False

def delete_space(base_url, auth, verify, space_key):
    """Deletes a single space by its key. Requires pages to be deleted first."""
    url = f"{base_url}/rest/api/space/{space_key}"
    print(f"  Attempting to delete space: {space_key}...")
    resp = request_with_retry("DELETE", url, auth=auth, verify=verify)

    # Note: Deleting a space is often asynchronous. A 202 Accepted means it's queued.
    if resp is not None and (resp.status_code == 204 or resp.status_code == 202 or resp.status_code == 200):
         status_meaning = "Deleted" if resp.status_code == 204 else "Deletion Queued/Accepted"
         print(f"    Successfully initiated deletion for space: {space_key} (Status: {resp.status_code} - {status_meaning})")
         # It might take time for the space to actually disappear.
         # We might need to poll the task status if the response provides a task ID.
         # For now, just report success/accepted.
         # Check for task ID in response headers or body if needed:
         # task_id = resp.headers.get('Location') or resp.json().get('taskId')
         return True
    else:
        print(f"    Failed to delete space: {space_key}. Status: {resp.status_code if resp else 'None'}. (Ensure all content was deleted first)", file=sys.stderr)
        return False

# --------------------------------------------------------------------------
# Main script logic
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Clean up Confluence spaces potentially created by seed.py.")
    parser.add_argument("--pattern", default=r" Space \d+$", help="Regex pattern to match space names for deletion (default targets ' Space 123' suffix). CAUTION: Matches names, not keys.")
    parser.add_argument("--dry-run", action="store_true", help="List spaces that would be deleted without actually deleting them.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt before deleting.")
    # Arguments for Confluence connection will be loaded from config file

    # Check if no arguments were provided in command line (sys.argv only has the script name)
    if len(sys.argv) == 1:
        print("No arguments provided. Showing help information:")
        print("-" * 60)
        parser.print_help()
        print("-" * 60)
        print("\nExample usage:")
        print("  python cleanup_data.py --dry-run             # List spaces that would be deleted")
        print("  python cleanup_data.py --pattern 'Test.*'    # Delete spaces with names matching 'Test*'")
        print("  python cleanup_data.py --yes                 # Delete spaces without confirmation")
        sys.exit(0)

    args = parser.parse_args()

    try:
        settings = load_confluence_settings()
        base_url = settings['api_base_url']
        user = settings['username']
        password = settings['password']
        verify_ssl = settings['verify_ssl']
    except Exception as e:
        print(f"Error loading Confluence settings: {e}", file=sys.stderr)
        print("Please ensure settings.ini is configured correctly.", file=sys.stderr)
        sys.exit(1)

    print("-" * 60)
    print("Confluence Cleanup Script")
    print("-" * 60)
    print(f"Targeting Confluence: {base_url}")
    print(f"Using username: {user}")
    print(f"Matching space names with regex: '{args.pattern}'")
    print(f"SSL Verification: {verify_ssl}")
    print(f"Dry Run: {args.dry_run}")
    print("-" * 60)

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        print("Warning: SSL verification is disabled.", file=sys.stderr)

    auth = (user, password)

    # 1. Fetch all spaces
    all_spaces = fetch_spaces(base_url, auth, verify_ssl)
    if not all_spaces:
        print("No spaces found or failed to fetch spaces. Exiting.")
        sys.exit(0)

    # 2. Filter spaces based on the pattern
    try:
        target_pattern = re.compile(args.pattern)
    except re.error as e:
        print(f"Error: Invalid regex pattern provided: {e}", file=sys.stderr)
        sys.exit(1)

    spaces_to_delete = []
    print(f"\nFiltering {len(all_spaces)} spaces using pattern: '{args.pattern}'...")
    for space in all_spaces:
        space_name = space.get("name", "")
        space_key = space.get("key", "")
        if target_pattern.search(space_name):
            # Exclude personal spaces (keys starting with '~') just in case
            if not space_key.startswith("~"):
                spaces_to_delete.append({"key": space_key, "name": space_name})
            else:
                 print(f"  Skipping personal space matching pattern: {space_name} ({space_key})")
    if not spaces_to_delete:
        print("No spaces matched the specified pattern. Nothing to delete.")
        sys.exit(0)
        
    print(f"\nFound {len(spaces_to_delete)} spaces matching the pattern:")
    for i, space in enumerate(spaces_to_delete):
        print(f"  {i+1}. {space['name']} (Key: {space['key']})")
    
    # 3. Handle Dry Run or Confirmation
    if args.dry_run:
        print("\nDry run complete. No changes were made.")
        sys.exit(0)
        
    if not args.yes:
        print("\n" + "!" * 80)
        print("WARNING! WARNING! WARNING!")
        print("You are about to PERMANENTLY DELETE the following:")
        print(f"- {len(spaces_to_delete)} Confluence spaces")
        total_pages = sum(len(fetch_pages_for_space(base_url, auth, verify_ssl, space['key'])) for space in spaces_to_delete)
        print(f"- Approximately {total_pages} pages of content")
        print("\nThis action CANNOT be undone and may result in PERMANENT DATA LOSS!")
        print("!" * 80)
        
        confirm_phrase = "DELETE THESE SPACES"
        confirm = input(f"\nTo proceed, please type '{confirm_phrase}' (exactly as shown): ")
        
        if confirm != confirm_phrase:
            print("Confirmation phrase did not match. Operation aborted for safety.")
            sys.exit(0)
            
        print("\nFinal confirmation...")
        final_confirm = input(f"Are you ABSOLUTELY SURE you want to delete these {len(spaces_to_delete)} spaces and ALL their content? (yes/no): ")
        final_confirm = final_confirm.lower().strip()
        if final_confirm != 'yes':
            print("Aborted by user.")
            sys.exit(0)

    print("\nStarting deletion process...")
    # 4. Delete pages and then spaces
    overall_success = True
    for i, space_info in enumerate(spaces_to_delete):
        space_key = space_info['key']
        space_name = space_info['name']
        print(f"\nProcessing space {i+1}/{len(spaces_to_delete)}: {space_name} ({space_key})")

        # Delete pages first
        page_ids = fetch_pages_for_space(base_url, auth, verify_ssl, space_key)
        if page_ids:
            print(f"  Deleting {len(page_ids)} pages in space '{space_key}'...")
            pages_deleted_count = 0
            for page_id in page_ids:
                if delete_page(base_url, auth, verify_ssl, page_id):
                    pages_deleted_count += 1
                else:
                    overall_success = False # Mark failure if any page deletion fails
                    print(f"    Warning: Failed to delete page {page_id} in space {space_key}. Space deletion might fail.", file=sys.stderr)
            print(f"  Finished deleting pages for space '{space_key}'. {pages_deleted_count}/{len(page_ids)} successful.")
        else:
            print(f"  No pages found to delete in space '{space_key}'.")

        # Now attempt to delete the space
        if not delete_space(base_url, auth, verify_ssl, space_key):
            overall_success = False # Mark failure if space deletion fails
            print(f"  Failed to delete space '{space_key}'. Check logs and Confluence permissions.", file=sys.stderr)

    print("\nCleanup process finished.")
    if not overall_success:
        print("Warning: Some errors occurred during deletion. Please check the logs above.", file=sys.stderr)
        sys.exit(1)
    else:
        print("All targeted spaces and their content have been processed for deletion.")

if __name__ == "__main__":
    main()
