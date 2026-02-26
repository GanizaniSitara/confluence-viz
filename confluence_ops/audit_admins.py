#!/usr/bin/env python3
# description: Audits and reports Confluence space administrator permissions.

import sys as _sys, os as _os; _sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
import csv
import json
import random
import requests
import urllib3  # For disabling SSL warnings
import sys
import time
import warnings
from utils.config_loader import load_confluence_settings

# --- Suppress InsecureRequestWarning ---
warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made to',
                      category=urllib3.exceptions.InsecureRequestWarning)
# ---------------------------------------

# --- Configuration ---
try:
    SETTINGS = load_confluence_settings()
    # Assuming SETTINGS['api_base_url'] is the true base URL without /rest/api,
    # as per user feedback that '/rest/api' has been removed from this setting's value.
    CONFLUENCE_BASE_URL = SETTINGS['base_url']
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
    """
    Fetches a list of usernames who have 'administer' permissions for a given space key using REST API.
    This function replaces the previous JSON-RPC implementation.
    """
    admin_usernames = []
    admin_group_names = [] # To acknowledge groups that grant admin rights

    limit = 50  # Number of permissions to fetch per page
    start = 0
    print(f"Fetching admin permissions for space {space_key} using REST API...", file=sys.stderr)

    while True:
        permissions_url = f"{API_BASE}/space/{space_key}/permission"
        params = {'limit': limit, 'start': start}
        
        response_data = make_api_request(permissions_url, params=params)

        if response_data is None:
            print(f"Error: Failed to fetch permissions for space {space_key} (url: {permissions_url}).", file=sys.stderr)
            return None # Indicate error

        results = response_data.get('results', [])
        if not isinstance(results, list):
            print(f"Error: Unexpected format for permissions results for space {space_key}.", file=sys.stderr)
            print(f"Response: {str(response_data)[:200]}", file=sys.stderr)
            return None

        for perm in results:
            operation_details = perm.get('operation', {}) # operation_details is a dict
            if not isinstance(operation_details, dict):
                continue # Should not happen if API is consistent, but good for safety

            is_admin_perm = False # Initialize for current permission entry

            # Extract operation name
            op_name = operation_details.get('operation') # Primary field for operation name
            if not isinstance(op_name, str):
                op_name = operation_details.get('key') # Fallback field

            # Extract target type
            target_name = operation_details.get('targetType') # Primary field for target
            if not isinstance(target_name, str):
                target_name = operation_details.get('target') # Fallback field

            if isinstance(op_name, str): # Ensure op_name is a string (already assumed to be UPPERCASE from API)
                # target_name is used directly (assumed to be UPPERCASE if string, or handled if not)
                current_target_name_for_comparison = target_name if isinstance(target_name, str) else ""

                # Condition 1: Operation is 'ADMINISTER' AND target is 'SPACE'
                is_admin_by_administer_space = (op_name == 'ADMINISTER' and current_target_name_for_comparison == 'SPACE')
                
                # Condition 2: Operation is 'SETSPACEPERMISSIONS'. 
                # The target for 'SETSPACEPERMISSIONS' should be 'SPACE'.
                # If current_target_name_for_comparison is empty, we assume the space context from the API endpoint.
                # If current_target_name_for_comparison is present, it must be 'SPACE'.
                is_admin_by_setspacepermissions_on_space = (
                    op_name == 'SETSPACEPERMISSIONS' and
                    (current_target_name_for_comparison == 'SPACE' or not current_target_name_for_comparison) # Target is 'SPACE' or not specified
                )

                if is_admin_by_administer_space or is_admin_by_setspacepermissions_on_space:
                    is_admin_perm = True
            
            if is_admin_perm:
                username_to_add = None
                group_name_to_add = None

                # Try to get username from 'user' object or 'subject'
                user_details = perm.get('user') # Older structure
                subject = perm.get('subject')   # Newer structure

                if subject and subject.get('type') == 'user':
                    # Prefer explicit username fields if available within subject or its nested user object
                    if 'username' in subject: username_to_add = subject['username']
                    elif 'name' in subject: username_to_add = subject['name'] # Sometimes 'name' holds username
                    elif isinstance(subject.get('user'), dict) and subject['user'].get('username'):
                        username_to_add = subject['user']['username']
                    # Note: subject.get('identifier') is often accountId, not directly used here to avoid listing accountIds.
                elif user_details and isinstance(user_details, dict):
                    if 'username' in user_details: username_to_add = user_details['username']
                    elif 'name' in user_details: username_to_add = user_details['name']
                
                if username_to_add:
                    admin_usernames.append(username_to_add)
                else:
                    # Check for group permissions
                    if subject and subject.get('type') == 'group' and subject.get('identifier'):
                        group_name_to_add = subject['identifier']
                    elif perm.get('group') and isinstance(perm['group'], dict) and perm['group'].get('name'):
                        group_name_to_add = perm['group']['name']
                    
                    if group_name_to_add:
                        admin_group_names.append(group_name_to_add)

        current_size = len(results)
        is_last_page = True 
        if '_links' in response_data and 'next' in response_data['_links']:
            is_last_page = False
            start += current_size 
        
        if is_last_page or current_size == 0:
            break
            
    if admin_group_names:
        unique_group_names = sorted(list(set(admin_group_names)))
        print(f"Note: The following groups also have admin rights on space {space_key}: {', '.join(unique_group_names)}", file=sys.stderr)
        print("This script primarily lists direct user admins. Group memberships are not expanded.", file=sys.stderr)

    return sorted(list(set(admin_usernames)))

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

def list_admins_for_specific_space():
    """Prompts for a space key and lists its administrators."""
    space_key_input = input("Enter the space key to list admins for: ").strip() # Removed .upper()
    if not space_key_input:
        print("No space key entered. Returning to menu.")
        return

    print(f"\\n--- Admins for Space: {space_key_input} ---")
    admin_usernames = get_space_admins(space_key_input) # Uses the updated REST API based function

    if admin_usernames is None:
        print(f"Error: Could not retrieve admin information for space '{space_key_input}'.")
        print("This could be due to an invalid space key, network issues, insufficient permissions, or the space not existing.")
    elif not admin_usernames:
        # This message might appear if only groups have admin rights and no direct users,
        # or if user admins are identified by accountId only and username is not available.
        print(f"No direct user administrators found for space '{space_key_input}'.")
        print("Check notes above if any groups were listed with admin rights.")
    else:
        print(f"Found {len(admin_usernames)} direct user admin(s) for space '{space_key_input}':")
        for username in admin_usernames:
            print(f"  - {username}")
    print("--- End of Admin List ---")

if __name__ == "__main__":
    while True:
        print("\n--- Confluence Space Admin Audit Tool ---")
        print("1. Audit All Spaces for Admin Permissions")
        print("2. Check Admin Rights for Current User")
        print("3. List Admins for a Specific Space") # New option
        print("Q. Exit")
        choice = input("Enter your choice (1, 2, 3, or Q): ").upper() # Updated prompt

        if choice == '1':
            audit_all_spaces()
        elif choice == '2': 
            check_current_user_admin()
        elif choice == '3': # New handler
            list_admins_for_specific_space()
        elif choice == 'Q':
            print("Exiting script.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or Q.")
