#!/usr/bin/env python3
# filepath: c:\Solutions\Python\confluence_visualization\audit_admins.py
# Script to audit space administrators in Confluence

import csv
import json
import random
import requests
import urllib3  # For disabling SSL warnings
import sys
from config_loader import load_confluence_settings

# Load configuration settings
try:
    SETTINGS = load_confluence_settings()
    CONFLUENCE_API_BASE_URL = SETTINGS['api_base_url']
    # Fix: Properly strip /rest/api from the end if it exists
    CONFLUENCE_BASE_URL = SETTINGS['api_base_url'].rstrip('/rest/api')  # Ensure no trailing /rest/api
    USERNAME = SETTINGS['username']
    PASSWORD = SETTINGS['password']
    VERIFY_SSL = SETTINGS['verify_ssl']
    
    print("Settings loaded successfully.")
    print(f"API Base URL: {CONFLUENCE_API_BASE_URL}")
    print(f"Base URL: {CONFLUENCE_BASE_URL}")
    print(f"Username: {USERNAME}")
    print(f"Verify SSL: {VERIFY_SSL}")
except Exception as e:
    print(f"Error loading settings: {e}")
    print("Please ensure settings.ini exists and is correctly formatted.")
    sys.exit(1)  # Exit if settings can't be loaded

# Suppress InsecureRequestWarning if VERIFY_SSL is False
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_all_space_keys():
    """Fetches all global space keys from Confluence, excluding personal spaces (those starting with ~)."""
    api_url = f"{CONFLUENCE_API_BASE_URL.rstrip('/')}/space"
    headers = {"Accept": "application/json"}
    space_keys = []
    params = {"type": "global", "limit": 50, "start": 0}  # Increased limit, adjust as needed
    print("\nFetching all space keys from Confluence...")
    try:
        while True:
            response = requests.get(api_url, auth=(USERNAME, PASSWORD), headers=headers, params=params, verify=VERIFY_SSL)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            for space in results:
                if 'key' in space and not space['key'].startswith('~'):  # Exclude personal spaces
                    space_keys.append(space['key'])
            
            print(f"  Fetched {len(results)} spaces in this page. Total fetched so far: {len(space_keys)}")

            if data.get('isLast', True) or not results:
                break
            
            # Correctly get nextPageStart if it exists, otherwise increment manually
            if '_links' in data and 'next' in data['_links']:
                next_link = data['_links']['next']
                if 'start=' in next_link:
                    try:
                        params['start'] = int(next_link.split('start=')[1].split('&')[0])
                    except ValueError:
                        print("  Warning: Could not parse start from next link. Falling back to manual increment.")
                        params['start'] += len(results)  # Fallback
                else:
                    params['start'] += len(results)  # Fallback
            else:  # If no 'next' link but not 'isLast', try manual increment (less reliable)
                params['start'] += len(results)

    except requests.exceptions.HTTPError as e:
        print(f"  Failed to get space keys. Status: {e.response.status_code}, Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"  An error occurred while getting space keys: {e}")
    except json.JSONDecodeError:
        print(f"  Failed to decode JSON response for space keys.")
    
    unique_space_keys = list(set(space_keys))  # Return unique list
    print(f"Finished fetching space keys. Total found: {len(unique_space_keys)}")
    return unique_space_keys

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

    print(f"  Fetching admins for space {space_key} with 'SETSPACEPERMISSIONS' via JSON-RPC...")
    
    try:
        response = requests.post(rpc_url, auth=(USERNAME, PASSWORD), headers=headers, json=payload, verify=VERIFY_SSL)
        response.raise_for_status()
        response_data = response.json()
        
        if 'error' in response_data and response_data['error'] is not None:
            error_details = response_data['error']
            print(f"    Failed to get space admins. JSON-RPC Error: Code {error_details.get('code')}, Message: {error_details.get('message')}")
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
                    print(f"    Warning: 'spacePermissions' field within 'result' is not a list. Found: {type(permissions_list)}")
            else:
                print(f"    Warning: Expected 'result' to be a dictionary with a 'spacePermissions' key, or 'spacePermissions' was not a list.")
        else:
            print(f"    Failed to get space admins. Unexpected JSON-RPC response structure.")
            
    except requests.exceptions.HTTPError as e:
        print(f"    HTTP error occurred while getting space admins: {e}")
        return None  # Return None to indicate an error
    except requests.exceptions.RequestException as e:
        print(f"    Network error occurred while getting space admins: {e}")
        return None  # Return None to indicate an error
    except json.JSONDecodeError:
        print(f"    Failed to decode JSON response from server.")
        return None  # Return None to indicate an error
    
    return list(set(admin_usernames))

def read_logins_from_csv(file_path):
    """Reads user logins from a CSV file into a set."""
    logins = set()
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if 'Login' not in reader.fieldnames:
                print(f"  Error: 'Login' column not found in {file_path}. Found columns: {reader.fieldnames}")
                return logins
            for row in reader:
                if row['Login']:  # Ensure Login is not empty
                    logins.add(row['Login'])
    except FileNotFoundError:
        print(f"  Error: {file_path} not found.")
    except Exception as e:
        print(f"  An error occurred while reading {file_path}: {e}")
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
    csv_data = []
    csv_headers = ["Space Key", "Admin Status", "Admin Count", "Admins", "Current Admins", "Departed Admins", "Error"]

    # Counters for summary
    total_spaces = len(all_space_keys)
    spaces_with_admins = 0
    spaces_with_errors = 0
    spaces_with_no_admins = 0
    
    print(f"\nBeginning audit of {total_spaces} spaces...")
    
    # Audit each space
    for space_key in all_space_keys:
        print(f"\nAuditing space: {space_key}")
        
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
            print(f"  No administrators with 'SETSPACEPERMISSIONS' found for space {space_key}")
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
            
            print(f"  Found {len(confluence_admin_usernames)} admin username(s) with 'SETSPACEPERMISSIONS'")
            if current_admins:
                print(f"  Current Active Admins ({len(current_admins)}): {', '.join(current_admins)}")
            if departed_admins:
                print(f"  Departed Admins ({len(departed_admins)}): {', '.join(departed_admins)}")
        
        csv_data.append(row)
    
    # Write results to CSV
    csv_filename = "space_admin_audit.csv"
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
            writer.writeheader()
            writer.writerows(csv_data)
        print(f"\nAudit results written to {csv_filename}")
    except Exception as e:
        print(f"\nError writing to CSV file: {e}")
    
    # Print summary
    print("\n--- Audit Summary ---")
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
    csv_data = []
    csv_headers = ["Space Key", "Has Admin Rights", "Error"]

    # Counters for summary
    total_spaces = len(all_space_keys)
    spaces_with_rights = 0
    spaces_without_rights = 0
    spaces_with_errors = 0
    
    print(f"\nChecking admin rights in {total_spaces} spaces...")
    
    # Check each space
    for space_key in all_space_keys:
        print(f"\nChecking space: {space_key}")
        
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
                print(f"  User {current_user} has admin rights on space {space_key}")
            else:
                row["Has Admin Rights"] = "NO"
                spaces_without_rights += 1
                print(f"  User {current_user} does NOT have admin rights on space {space_key}")
        
        csv_data.append(row)
    
    # Write results to CSV
    csv_filename = "current_user_admin_rights.csv"
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
            writer.writeheader()
            writer.writerows(csv_data)
        print(f"\nAdmin rights check results written to {csv_filename}")
    except Exception as e:
        print(f"\nError writing to CSV file: {e}")
    
    # Print summary
    print("\n--- Admin Rights Check Summary ---")
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
