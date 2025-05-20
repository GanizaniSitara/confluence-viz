#!/usr/bin/env python3
# filepath: c:\Solutions\Python\confluence_visualization\audit_admins.py
# Script to audit space administrators in Confluence

import csv
import json
import random
import requests
import urllib3  # For disabling SSL warnings
import sys
import time
import warnings
from config_loader import load_confluence_settings

# --- Suppress InsecureRequestWarning ---
warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made to',
                      category=urllib3.exceptions.InsecureRequestWarning)
# ---------------------------------------

# --- Configuration ---
try:
    SETTINGS = load_confluence_settings()
    # Fix: Properly strip /rest/api from the end if it exists
    CONFLUENCE_BASE_URL = SETTINGS['api_base_url'].rstrip('/rest/api')  # Ensure no trailing /rest/api
    USERNAME = SETTINGS['username']
    PASSWORD = SETTINGS['password']
    VERIFY_SSL = SETTINGS['verify_ssl']
    
    # Define API endpoints dynamically
    API_BASE = f'{CONFLUENCE_BASE_URL}/rest/api'
    
    print("Settings loaded successfully.", file=sys.stderr)
    print(f"Base URL: {CONFLUENCE_BASE_URL}", file=sys.stderr)
    print(f"API Base: {API_BASE}", file=sys.stderr)
    print(f"Username: {USERNAME}", file=sys.stderr)
    print(f"Verify SSL: {VERIFY_SSL}", file=sys.stderr)
except Exception as e:
    print(f"Error loading settings: {e}", file=sys.stderr)
    print("Please ensure settings.ini exists and is correctly formatted.", file=sys.stderr)
    sys.exit(1)  # Exit if settings can't be loaded

def make_api_request(url, params=None, max_retries=5):
    """
    Makes an API request, handling authentication, 429 rate limiting, and SSL verification.
    Copied from space_explorer.py which works successfully.
    """
    retries = 0
    while retries < max_retries:
        query_params = '&'.join([f"{k}={v}" for k, v in (params or {}).items()])
        request_url = f"{url}?{query_params}" if query_params else url
        print(f"REST Request: GET {request_url}", file=sys.stderr)

        try:
            auth = None
            if USERNAME and PASSWORD:
                auth = (USERNAME, PASSWORD)

            response = requests.get(url, params=params, verify=VERIFY_SSL, auth=auth)
            print(f"Response Status: {response.status_code}", file=sys.stderr)

            if response.status_code == 200:
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    print("Error: Response was not valid JSON.", file=sys.stderr)
                    print(f"Response text: {response.text[:500]}...", file=sys.stderr)
                    return None

            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                wait_time = int(retry_after) if retry_after else (2 ** retries) * 5
                jitter = random.uniform(0, 1) * 2
                wait_time += jitter
                print(f"Rate limited (429). Server requested Retry-After: {retry_after or 'Not specified'}", file=sys.stderr)
                print(f"Waiting for {wait_time:.2f} seconds before retry {retries + 1}/{max_retries}", file=sys.stderr)
                time.sleep(wait_time)
                retries += 1
                continue

            else:
                print(f"Error: Received status code {response.status_code} for {url}", file=sys.stderr)
                print(f"Response body: {response.text}", file=sys.stderr)
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}", file=sys.stderr)
            wait_time = (2 ** retries) * 2
            print(f"Network error. Waiting {wait_time} seconds before retry {retries + 1}/{max_retries}", file=sys.stderr)
            time.sleep(wait_time)
            retries += 1

    print(f"Failed to fetch data from {url} after {max_retries} retries.", file=sys.stderr)
    return None

def get_all_space_keys():
    """
    Fetches a list of all non-personal space keys from Confluence.
    Uses the working approach from space_explorer.py.
    """
    all_spaces = []
    limit = 100  # Max limit might vary, 100 is often safe
    start = 0
    print("\nFetching list of all spaces...", file=sys.stderr)
    while True:
        space_list_url = f"{API_BASE}/space"
        params = {
            'limit': limit,
            'start': start,
            'type': 'global'  # Filter for global (non-personal) spaces
        }
        response_data = make_api_request(space_list_url, params=params)

        if not response_data or 'results' not in response_data:
            print(f"Error: Failed to fetch space list at start={start}. Aborting.", file=sys.stderr)
            return None  # Indicate failure

        results = response_data.get('results', [])
        all_spaces.extend(results)
        print(f"Fetched {len(results)} spaces. Total so far: {len(all_spaces)}", file=sys.stderr)

        # Check if this is the last page of results
        size = response_data.get('size', 0)
        if size < limit:
            print("Reached the end of the space list.", file=sys.stderr)
            break
        else:
            start += size  # Prepare for the next page

    # Extract just the keys, filtering out personal spaces (those starting with ~)
    space_keys = [space['key'] for space in all_spaces if space.get('key') and not space['key'].startswith('~')]
    print(f"Found {len(space_keys)} non-personal spaces.", file=sys.stderr)
    return space_keys

def get_space_admins(space_key):
    """Fetches a list of usernames who have 'SETSPACEPERMISSIONS' for a given space key using JSON-RPC."""
    rpc_url = f"{CONFLUENCE_BASE_URL}/rpc/json-rpc/confluenceservice-v2?os_authType=basic"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "getSpacePermissionSet",
        "params": [space_key, "SETSPACEPERMISSIONS"],
        "id": random.randint(1, 10000)
    }
    admin_usernames = []

    try:
        response = requests.post(rpc_url, auth=(USERNAME, PASSWORD), headers=headers, json=payload, verify=VERIFY_SSL)
        response.raise_for_status()
        response_data = response.json()
        
        if 'error' in response_data and response_data['error'] is not None:
            return None  # Return None to indicate an error
        elif 'result' in response_data:
            result_content = response_data['result']
            if isinstance(result_content, dict) and 'spacePermissions' in result_content:
                permissions_list = result_content['spacePermissions']
                if isinstance(permissions_list, list):
                    for perm_entry in permissions_list:
                        if isinstance(perm_entry, dict) and perm_entry.get('userName'):
                            admin_usernames.append(perm_entry['userName'])
                else:
                    pass # Silently ignore or decide if this should be an error
            else:
                pass # Silently ignore or decide if this should be an error
        else:
            return None # Treat as an error if structure is not as expected
            
    except requests.exceptions.HTTPError as e:
        return None  # Return None to indicate an error
    except requests.exceptions.RequestException as e:
        return None  # Return None to indicate an error
    except json.JSONDecodeError:
        return None  # Return None to indicate an error
    
    return list(set(admin_usernames))

def read_logins_from_csv(file_path):
    """Reads user logins from a CSV file into a set."""
    logins = set()
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if 'Login' not in reader.fieldnames:
                print(f"  Error: 'Login' column not found in {file_path}. Found columns: {reader.fieldnames}", file=sys.stderr)
                return logins
            for row in reader:
                if row['Login']:  # Ensure Login is not empty
                    logins.add(row['Login'])
    except FileNotFoundError:
        print(f"  Error: {file_path} not found.", file=sys.stderr)
    except Exception as e:
        print(f"  An error occurred while reading {file_path}: {e}", file=sys.stderr)
    return logins

def audit_all_spaces():
    """Audits all spaces for admin permissions."""
    print("\n--- Auditing All Spaces for Admin Permissions ---")
    
    # Get all space keys (excluding personal spaces)
    all_space_keys = get_all_space_keys()
    if not all_space_keys:
        print("No spaces found in Confluence. Cannot audit admins.")
        return

    # Read current active logins
    current_active_logins = read_logins_from_csv('contributors_current.csv')
    if not current_active_logins:
        print("Warning: contributors_current.csv is empty or not found. All admins may be reported as 'Departed'.")

    # Prepare output for CSV
    # csv_data = [] # No longer needed to store all data
    csv_headers = ["Space Key", "Admin Status", "Admin Count", "Admins", "Current Admins", "Departed Admins", "Error"]

    # Counters for summary
    total_spaces = len(all_space_keys)
    spaces_with_admins = 0
    spaces_with_errors = 0
    spaces_with_no_admins = 0
    
    print(f"\nBeginning audit of {total_spaces} spaces...")
    
    # Print CSV header to console
    writer = csv.DictWriter(sys.stdout, fieldnames=csv_headers)
    writer.writeheader()
    
    # Audit each space
    for space_key in all_space_keys:
        # print(f"\\nAuditing space: {space_key}") # Removed for cleaner CSV output
        
        # Get admins for this space
        confluence_admin_usernames = get_space_admins(space_key)
        
        row = {
            "Space Key": space_key,
            "Admin Status": "",
            "Admin Count": 0,
            "Admins": "",
            "Current Admins": "",
            "Departed Admins": "",
            "Error": ""
        }
        
        if confluence_admin_usernames is None:
            # Error occurred during admin retrieval
            row["Admin Status"] = "ERROR"
            row["Error"] = "Failed to retrieve admin information"
            spaces_with_errors += 1
        elif not confluence_admin_usernames:
            # No admins found
            row["Admin Status"] = "NO_ADMINS"
            spaces_with_no_admins += 1
            # print(f"  No administrators with 'SETSPACEPERMISSIONS' found for space {space_key}") # Removed
        else:
            # Admins found
            spaces_with_admins += 1
            row["Admin Status"] = "HAS_ADMINS"
            row["Admin Count"] = len(confluence_admin_usernames)
            row["Admins"] = ", ".join(confluence_admin_usernames)
            
            # Categorize admins as current or departed
            current_admins = []
            departed_admins = []
            
            for admin_username in confluence_admin_usernames:
                if admin_username in current_active_logins:
                    current_admins.append(admin_username)
                else:
                    departed_admins.append(admin_username)
            
            row["Current Admins"] = ", ".join(current_admins)
            row["Departed Admins"] = ", ".join(departed_admins)
            
            # print(f"  Found {len(confluence_admin_usernames)} admin username(s) with 'SETSPACEPERMISSIONS'") # Removed
            # if current_admins: # Removed
            #     print(f"  Current Active Admins ({len(current_admins)}): {', '.join(current_admins)}") # Removed
            # if departed_admins: # Removed
            #     print(f"  Departed Admins ({len(departed_admins)}): {', '.join(departed_admins)}") # Removed
        
        # csv_data.append(row) # No longer needed
        writer.writerow(row) # Stream row to stdout
    
    # Print results to console in CSV format - This block is now removed as we stream
    # print("\\n--- CSV Output for Space Admin Audit ---")
    # if csv_data:
    #     # Ensure sys.stdout is used for the writer
    #     writer = csv.DictWriter(sys.stdout, fieldnames=csv_headers)
    #     writer.writeheader()
    #     writer.writerows(csv_data)
    # else:
    #     print("No data to output for the audit.")
    # print("--- End of CSV Output ---")
    
    # Print summary
    print("\\n\\n--- Audit Summary ---") # Added newline for separation from CSV data
    print(f"Total spaces audited: {total_spaces}")
    print(f"Spaces with admins: {spaces_with_admins}")
    print(f"Spaces with no admins: {spaces_with_no_admins}")
    print(f"Spaces with errors: {spaces_with_errors}")
    print("--- Audit Complete ---")

def check_current_user_admin():
    """Checks if the current user (from settings.ini) has admin rights on spaces."""
    current_user = USERNAME
    print(f"\n--- Checking Admin Rights for Current User: {current_user} ---")
    
    # Get all space keys (excluding personal spaces)
    all_space_keys = get_all_space_keys()
    if not all_space_keys:
        print("No spaces found in Confluence. Cannot check admin rights.")
        return

    # Prepare output for CSV
    # csv_data = []
    csv_headers = ["Space Key", "Has Admin Rights", "Error"]

    # Counters for summary
    total_spaces = len(all_space_keys)
    spaces_with_rights = 0
    spaces_without_rights = 0
    spaces_with_errors = 0
    
    print(f"\nChecking admin rights in {total_spaces} spaces...")
    
    # Print CSV header to console
    writer = csv.DictWriter(sys.stdout, fieldnames=csv_headers)
    writer.writeheader()

    # Check each space
    for space_key in all_space_keys:
        # print(f"\\nChecking space: {space_key}") # Removed for cleaner CSV output
        
        # Get admins for this space
        confluence_admin_usernames = get_space_admins(space_key)
        
        row = {
            "Space Key": space_key,
            "Has Admin Rights": "",
            "Error": ""
        }
        
        if confluence_admin_usernames is None:
            # Error occurred during admin retrieval
            row["Error"] = "Failed to retrieve admin information"
            spaces_with_errors += 1
        else:
            # Check if current user is admin
            if current_user in confluence_admin_usernames:
                row["Has Admin Rights"] = "YES"
                spaces_with_rights += 1
                # print(f"  User {current_user} has admin rights on space {space_key}") # Removed
            else:
                row["Has Admin Rights"] = "NO"
                spaces_without_rights += 1
                # print(f"  User {current_user} does NOT have admin rights on space {space_key}") # Removed
        
        # csv_data.append(row) # No longer needed
        writer.writerow(row) # Stream row to stdout

    # Print results to console in CSV format - This block is now removed as we stream
    # print("\\n--- CSV Output for Current User Admin Rights ---")
    # if csv_data:
    #     # Ensure sys.stdout is used for the writer
    #     writer = csv.DictWriter(sys.stdout, fieldnames=csv_headers)
    #     writer.writeheader()
    #     writer.writerows(csv_data)
    # else:
    #     print("No data to output for the current user admin rights check.")
    # print("--- End of CSV Output ---")

    # Print summary
    print("\\n\\n--- Summary for Current User Admin Rights ---") # Added newline for separation
    print(f"Current user: {current_user}")
    print(f"Total spaces checked: {total_spaces}")
    print(f"Spaces where user has admin rights: {spaces_with_rights}")
    print(f"Spaces where user does NOT have admin rights: {spaces_without_rights}")
    print(f"Spaces with errors: {spaces_with_errors}")
    print("--- Admin Rights Check Complete ---")

if __name__ == "__main__":
    while True:
        print("\n--- Confluence Space Admin Audit Tool ---")
        print("1. Audit All Spaces for Admin Permissions")
        print("2. Check Admin Rights for Current User")
        print("Q. Exit")
        choice = input("Enter your choice (1, 2, or Q): ").upper()

        if choice == '1':
            audit_all_spaces()
        elif choice == '2': 
            check_current_user_admin()
        elif choice == 'Q':
            print("Exiting script.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or Q.")
