# description: Manages users in Confluence.

import csv
import json
import random
import requests
import urllib3 # For disabling SSL warnings

# Added for shuffling and sampling
from config_loader import load_confluence_settings

SETTINGS = load_confluence_settings()
CONFLUENCE_API_BASE_URL = SETTINGS['api_base_url'] # This is likely .../rest/api
CONFLUENCE_BASE_URL = CONFLUENCE_API_BASE_URL.replace('/rest/api', '') # Get http://host:port
USERNAME = SETTINGS['username']
PASSWORD = SETTINGS['password']
VERIFY_SSL = SETTINGS['verify_ssl']

# Suppress InsecureRequestWarning if VERIFY_SSL is False
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_PASSWORD = "password" # Default password for new users

def get_user_key(username_to_find):
    """Retrieves the userKey for a given username from Confluence."""
    api_url = f"{CONFLUENCE_API_BASE_URL.rstrip('/')}/user?username={username_to_find}"
    headers = {"Accept": "application/json"}
    print(f"  Attempting to get userKey for {username_to_find} from {api_url}")
    try:
        response = requests.get(api_url, auth=(USERNAME, PASSWORD), headers=headers, verify=VERIFY_SSL)
        response.raise_for_status()
        user_details = response.json()
        if 'userKey' in user_details:
            print(f"    Successfully retrieved userKey for {username_to_find}: {user_details['userKey']}")
            return user_details['userKey']
        else:
            print(f"    Warning: 'userKey' not found in response for user {username_to_find}. Response: {json.dumps(user_details)}")
            return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"    User {username_to_find} not found when trying to get userKey.")
        else:
            print(f"    Failed to get userKey for {username_to_find}. Status: {e.response.status_code}, Response: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    An error occurred while getting userKey for {username_to_find}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"    Failed to decode JSON response when getting userKey for {username_to_find}.")
        return None

def get_username_from_userkey(user_key):
    """Retrieves the username for a given userKey from Confluence."""
    api_url = f"{CONFLUENCE_API_BASE_URL.rstrip('/')}/user?key={user_key}"
    headers = {"Accept": "application/json"}
    # print(f"  DEBUG: Attempting to get username for userKey {user_key} from {api_url}")
    try:
        response = requests.get(api_url, auth=(USERNAME, PASSWORD), headers=headers, verify=VERIFY_SSL)
        response.raise_for_status()
        user_details = response.json()
        if 'username' in user_details:
            # print(f"    DEBUG: Successfully retrieved username for userKey {user_key}: {user_details['username']}")
            return user_details['username']
        else:
            # print(f"    DEBUG: Warning: 'username' not found in response for userKey {user_key}. Response: {json.dumps(user_details)}")
            return None
    except requests.exceptions.HTTPError as e:
        # print(f"    DEBUG: Failed to get username for userKey {user_key}. Status: {e.response.status_code}, Response: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        # print(f"    DEBUG: An error occurred while getting username for userKey {user_key}: {e}")
        return None
    except json.JSONDecodeError:
        # print(f"    DEBUG: Failed to decode JSON response when getting username for userKey {user_key}.")
        return None

def create_user(user_data):
    """Creates a user in Confluence, matching the successful cURL command structure."""
    confluence_api_base = CONFLUENCE_API_BASE_URL.rstrip('/') # CONFLUENCE_URL is http://.../rest/api
    # Matching cURL: use /admin/user endpoint
    api_url = f"{confluence_api_base}/admin/user"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"  # Added Accept header from cURL
    }

    # Matching cURL payload structure and user requirements
    payload = {
        "userName": user_data['Login'],       # Key from cURL
        "fullName": user_data['Name'],        # Key from cURL
        "email": user_data['Email'],          # Key from cURL
        "password": user_data['Login'],       # User requirement: password is username
        "notifyViaEmail": False             # Key from cURL, value from user requirement
    }

    print(f"Attempting to create user: {user_data['Login']} with password {user_data['Login']} (notify: False) at {api_url}")
    try:
        response = requests.post(api_url, auth=(USERNAME, PASSWORD), headers=headers, json=payload, verify=VERIFY_SSL)
        response.raise_for_status() # Raises an exception for 4XX/5XX errors
        print(f"Successfully created user: {user_data['Login']}")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"Failed to create user {user_data['Login']}. Status code: {e.response.status_code}, Response: {e.response.text}")
        # More detailed error logging
        if e.response.status_code == 403:
            print("  Error 403: Ensure the user '{USERNAME}' has permissions to create users (e.g., Confluence Administrator).")
        elif e.response.status_code == 400:
            print(f"  Error 400: Bad Request. Check the payload: {json.dumps(payload)}")
            if 'already exists' in e.response.text.lower():
                 print(f"  User {user_data['Login']} might already exist.")
        elif e.response.status_code == 500:
             print("  Error 500: Internal Server Error. Check Confluence logs.")
        return False
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while trying to create user {user_data['Login']}: {e}")
        return False

def generate_delete_user_curl(username_to_delete):
    """Generates the cURL command to delete a user."""
    confluence_api_base = CONFLUENCE_API_BASE_URL.rstrip('/')
    api_url = f"{confluence_api_base}/admin/user/{username_to_delete}" # Corrected URL format
    # Ensure USERNAME and PASSWORD are treated as strings in the command
    # and special characters in PASSWORD are handled if necessary (though curl usually handles this)
    curl_command = f'curl -u "{USERNAME}:{PASSWORD}" --request DELETE --url "{api_url}" --header "Accept: application/json"'
    return curl_command

def delete_user(username_to_delete):
    """Deletes or deactivates a user in Confluence, matching successful cURL command."""
    confluence_api_base = CONFLUENCE_API_BASE_URL.rstrip('/') # CONFLUENCE_URL is http://.../rest/api
    # Corrected URL format
    api_url = f"{confluence_api_base}/admin/user/{username_to_delete}"

    headers = {
        "Accept": "application/json" # Matching the cURL command
    }

    print(f"Attempting to delete user: {username_to_delete} at {api_url}")
    try:
        response = requests.delete(api_url, auth=(USERNAME, PASSWORD), headers=headers, verify=VERIFY_SSL)
        response.raise_for_status() # Raises an exception for 4XX/5XX errors
        print(f"Successfully deleted/deactivated user: {username_to_delete}")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"Failed to delete user {username_to_delete}. Status code: {e.response.status_code}, Response: {e.response.text}")
        if e.response.status_code == 403:
            print(f"  Error 403: Ensure the user '{USERNAME}' has permissions to delete/deactivate users.")
        elif e.response.status_code == 404:
            print(f"  Error 404: User '{username_to_delete}' not found or endpoint incorrect.")
        elif e.response.status_code == 405: # Specific handling for 405
            print(f"  Error 405: Method Not Allowed. This can be due to incorrect endpoint, permissions, or CSRF protection. Check Confluence logs.")
        return False
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while trying to delete user {username_to_delete}: {e}")
        return False

def assign_space_admin(space_key, username):
    """Assigns 'SETSPACEPERMISSIONS' to a user for a given space using JSON-RPC."""
    
    # Construct the JSON-RPC URL from the base Confluence URL
    rpc_url = f"{CONFLUENCE_BASE_URL}/rpc/json-rpc/confluenceservice-v2?os_authType=basic"
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "jsonrpc": "2.0",
        "method": "addPermissionToSpace",
        "params": ["SETSPACEPERMISSIONS", username, space_key],
        "id": random.randint(1, 10000) # Unique ID for the RPC call
    }
    
    print(f"Attempting to assign 'SETSPACEPERMISSIONS' to user '{username}' for space '{space_key}' via JSON-RPC...")
    print(f"  DEBUG: Making POST request to: {rpc_url}")
    print(f"  DEBUG: Headers: {json.dumps(headers)}")
    print(f"  DEBUG: Payload: {json.dumps(payload)}")
    
    try:
        response = requests.post(rpc_url, auth=(USERNAME, PASSWORD), headers=headers, json=payload, verify=VERIFY_SSL)
        response.raise_for_status() # Check for HTTP errors (4xx, 5xx)
        
        response_data = response.json()
        print(f"  DEBUG: Response Data: {json.dumps(response_data)}")
        
        if 'error' in response_data and response_data['error'] is not None:
            error_details = response_data['error']
            print(f"  Failed to assign permission. JSON-RPC Error: Code {error_details.get('code')}, Message: {error_details.get('message')}")
            return False
        elif 'result' in response_data and response_data['result'] is True:
            print(f"  Successfully assigned 'SETSPACEPERMISSIONS' to user '{username}' for space '{space_key}'.")
            return True
        else:
            print(f"  Failed to assign permission. Unexpected JSON-RPC response: {json.dumps(response_data)}")
            return False
            
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP error occurred while trying to assign permission: {e}. Response: {e.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  Network error occurred while trying to assign permission: {e}")
        return False
    except json.JSONDecodeError:
        print(f"  Failed to decode JSON response from server. Response text: {response.text}")
        return False

def remove_space_admin(space_key, username):
    """Removes 'SETSPACEPERMISSIONS' from a user for a given space using JSON-RPC."""
    rpc_url = f"{CONFLUENCE_BASE_URL}/rpc/json-rpc/confluenceservice-v2?os_authType=basic"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "removePermissionFromSpace", # Key change: method to remove permission
        "params": ["SETSPACEPERMISSIONS", username, space_key],
        "id": random.randint(1, 10000) 
    }
    
    print(f"Attempting to remove 'SETSPACEPERMISSIONS' from user '{username}' for space '{space_key}' via JSON-RPC...")
    print(f"  DEBUG: Making POST request to: {rpc_url}")
    print(f"  DEBUG: Headers: {json.dumps(headers)}")
    print(f"  DEBUG: Payload: {json.dumps(payload)}")
    
    try:
        response = requests.post(rpc_url, auth=(USERNAME, PASSWORD), headers=headers, json=payload, verify=VERIFY_SSL)
        response.raise_for_status()
        
        response_data = response.json()
        print(f"  DEBUG: Response Data: {json.dumps(response_data)}")
        
        if 'error' in response_data and response_data['error'] is not None:
            error_details = response_data['error']
            print(f"  Failed to remove permission. JSON-RPC Error: Code {error_details.get('code')}, Message: {error_details.get('message')}")
            return False
        # For removePermissionFromSpace, a successful result is often true or might not have a specific 'result' value if it's just a confirmation
        # We'll consider it a success if there's no error and the HTTP status was OK.
        # Some versions might return True in 'result'.
        elif 'result' in response_data and response_data['result'] is True:
             print(f"  Successfully removed 'SETSPACEPERMISSIONS' from user '{username}' for space '{space_key}'.")
             return True
        elif 'result' not in response_data and 'error' not in response_data : # Or no error and no specific result but HTTP 200
            print(f"  Successfully submitted request to remove 'SETSPACEPERMISSIONS' from user '{username}' for space '{space_key}'. Assuming success based on HTTP 200 and no error in response.")
            return True
        else:
            print(f"  Failed to remove permission. Unexpected JSON-RPC response: {json.dumps(response_data)}")
            return False
            
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP error occurred while trying to remove permission: {e}. Response: {e.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  Network error occurred while trying to remove permission: {e}")
        return False
    except json.JSONDecodeError:
        print(f"  Failed to decode JSON response from server. Response text: {response.text if 'response' in locals() else 'Unknown response'}")
        return False

def get_all_space_keys():
    """Fetches all global space keys from Confluence."""
    api_url = f"{CONFLUENCE_API_BASE_URL.rstrip('/')}/space"
    headers = {"Accept": "application/json"}
    space_keys = []
    params = {"type": "global", "limit": 50, "start": 0} # Increased limit, adjust as needed
    print("\\nFetching all space keys from Confluence...")
    try:
        while True:
            response = requests.get(api_url, auth=(USERNAME, PASSWORD), headers=headers, params=params, verify=VERIFY_SSL)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            for space in results:
                if 'key' in space:
                    space_keys.append(space['key'])
            
            print(f"  Fetched {len(results)} spaces in this page. Total fetched so far: {len(space_keys)}")

            if data.get('isLast', True) or not results:
                break
            
            # Correctly get nextPageStart if it exists, otherwise increment manually
            if '_links' in data and 'next' in data['_links']:
                # Extract start from the 'next' link if possible, or parse it
                next_link = data['_links']['next']
                if 'start=' in next_link:
                    try:
                        params['start'] = int(next_link.split('start=')[1].split('&')[0])
                    except ValueError:
                        print("  Warning: Could not parse start from next link. Falling back to manual increment.")
                        params['start'] += len(results) # Fallback
                else:
                    params['start'] += len(results) # Fallback
            else: # If no 'next' link but not 'isLast', try manual increment (less reliable)
                 params['start'] += len(results)


    except requests.exceptions.HTTPError as e:
        print(f"  Failed to get space keys. Status: {e.response.status_code}, Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"  An error occurred while getting space keys: {e}")
    except json.JSONDecodeError:
        print(f"  Failed to decode JSON response for space keys.")
    print(f"Finished fetching space keys. Total found: {len(set(space_keys))}")
    return list(set(space_keys)) # Return unique list

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
    print(f"    DEBUG: Making POST request to: {rpc_url}")
    print(f"    DEBUG: Payload: {json.dumps(payload)}")

    try:
        response = requests.post(rpc_url, auth=(USERNAME, PASSWORD), headers=headers, json=payload, verify=VERIFY_SSL)
        response.raise_for_status()
        response_data = response.json()
        print(f"    DEBUG: Response Data: {json.dumps(response_data)}")

        if 'error' in response_data and response_data['error'] is not None:
            error_details = response_data['error']
            print(f"    Failed to get space admins. JSON-RPC Error: Code {error_details.get('code')}, Message: {error_details.get('message')}")
        elif 'result' in response_data:
            result_content = response_data['result']
            if isinstance(result_content, dict) and 'spacePermissions' in result_content:
                permissions_list = result_content['spacePermissions']
                if isinstance(permissions_list, list):
                    for perm_entry in permissions_list:
                        if isinstance(perm_entry, dict) and perm_entry.get('userName'):
                            admin_usernames.append(perm_entry['userName'])
                else:
                    print(f"    Warning: 'spacePermissions' field within 'result' is not a list. Found: {type(permissions_list)}. Data: {json.dumps(permissions_list)}")
            else:
                print(f"    Warning: Expected 'result' to be a dictionary with a 'spacePermissions' key, or 'spacePermissions' was not a list. Result data: {json.dumps(result_content)}")
        else:
            print(f"    Failed to get space admins. Unexpected JSON-RPC response structure: {json.dumps(response_data)}")
            
    except requests.exceptions.HTTPError as e:
        print(f"    HTTP error occurred while getting space admins: {e}. Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"    Network error occurred while getting space admins: {e}")
    except json.JSONDecodeError:
        print(f"    Failed to decode JSON response from server. Response text: {response.text if 'response' in locals() else 'Unknown'}")
    
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
                if row['Login']: # Ensure Login is not empty
                    logins.add(row['Login'])
    except FileNotFoundError:
        print(f"  Error: {file_path} not found.")
    except Exception as e:
        print(f"  An error occurred while reading {file_path}: {e}")
    return logins

if __name__ == "__main__":
    while True:
        print("\\n--- Confluence User Management & Audit ---")
        print("1. Create users from contributors.csv")
        print("2. Delete users from contributors.csv")
        print("3. Assign Space Admin (Simplified - First Space, First Contributor - JSON-RPC)")
        print("4. Audit Space Admins (Simplified - First Space - JSON-RPC)")
        print("5. Remove Space Admin (Simplified - First Space, First Contributor - JSON-RPC)")
        print("Q. Exit")
        choice = input("Enter your choice (1, 2, 3, 4, 5, or Q): ").upper()

        if choice == '1':
            print("\\n--- Creating Users ---")
            users_created_count = 0
            try:
                with open('contributors.csv', mode='r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if create_user(row):
                            users_created_count += 1
                print(f"\\nFinished creating users. {users_created_count} users processed for creation.")
            except FileNotFoundError:
                print("Error: contributors.csv not found. Please create it first.")
            except Exception as e:
                print(f"An unexpected error occurred during user creation: {e}")

        elif choice == '2':
            print("\\n--- Deleting Users ---")
            # Flexible yes/no input
            confirm_input = input("Are you sure you want to delete all users listed in contributors.csv from Confluence? (Y/N): ").lower()
            if confirm_input in ['y', 'yes']:
                users_deleted_count = 0
                # print("\\n--- cURL Commands for Deletion ---") # Commented out as we revert to direct deletion
                try:
                    with open('contributors.csv', mode='r', encoding='utf-8') as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            if 'Login' in row and row['Login']: # Ensure Login column exists and is not empty
                                # Revert to calling delete_user directly
                                if delete_user(row['Login']):
                                    users_deleted_count += 1
                            else:
                                print("Skipping row due to missing or empty 'Login' field.")
                    print(f"\\\\nFinished processing deletions. {users_deleted_count} users processed for deletion.")
                except FileNotFoundError:
                    print("Error: contributors.csv not found.")
                except Exception as e:
                    print(f"An unexpected error occurred during user deletion: {e}")
            else:
                print("User deletion cancelled.")

        elif choice == '3': 
            print("\\n--- Assigning Space Admin (Simplified - First Space, First Contributor - JSON-RPC) ---")
            all_space_keys = get_all_space_keys()
            if not all_space_keys:
                print("No spaces found in Confluence. Cannot assign admin.")
            else:
                first_space_key = all_space_keys[0]
                print(f"Targeting the first space: {first_space_key}")
                
                all_contributor_logins_set = read_logins_from_csv('contributors.csv')
                if not all_contributor_logins_set:
                    print("No users found in contributors.csv. Cannot assign admin.")
                else:
                    # Ensure the set is not empty before trying to get an element
                    if not all_contributor_logins_set:
                        print("Contributors set is empty. Cannot pick a user.")
                    else:
                        first_contributor_login = list(all_contributor_logins_set)[0]
                        print(f"Attempting to assign 'SETSPACEPERMISSIONS' for contributor '{first_contributor_login}' to space '{first_space_key}'.")
                        if assign_space_admin(first_space_key, first_contributor_login):
                            print(f"Successfully processed assignment for {first_contributor_login} to {first_space_key}.")
                        else:
                            print(f"Failed to assign {first_contributor_login} to {first_space_key}.")
            print("\\nFinished simplified admin assignment attempt.")

        elif choice == '4': 
            print("\\n--- Auditing Space Admins (Simplified - First Space - JSON-RPC) ---")
            all_space_keys = get_all_space_keys()
            if not all_space_keys:
                print("No spaces found in Confluence. Cannot audit admins.")
            else:
                first_space_key = all_space_keys[0]
                print(f"Auditing admins for the first space: {first_space_key}")
                
                current_active_logins = read_logins_from_csv('contributors_current.csv')
                if not current_active_logins:
                    print("Warning: contributors_current.csv is empty or not found. All admins may be reported as 'Departed'.")

                # get_space_admins now returns a list of usernames directly
                confluence_admin_usernames = get_space_admins(first_space_key) 

                if not confluence_admin_usernames:
                    print(f"  No administrators with 'SETSPACEPERMISSIONS' found for space {first_space_key} in Confluence.")
                else:
                    print(f"  Found {len(confluence_admin_usernames)} admin username(s) with 'SETSPACEPERMISSIONS' for space {first_space_key}: {confluence_admin_usernames}")
                    current_admins_in_space = []
                    departed_admins_in_space = []

                    for admin_username in confluence_admin_usernames:
                        if admin_username in current_active_logins:
                            current_admins_in_space.append(admin_username)
                        else:
                            departed_admins_in_space.append(admin_username)
                    
                    print(f"\\\\n  --- Audit Results for Space: {first_space_key} (SETSPACEPERMISSIONS) ---")
                    if current_admins_in_space:
                        print(f"  Current Active Admins ({len(current_admins_in_space)}): {', '.join(current_admins_in_space)}")
                    else:
                        print("  No Current Active Admins found for this space.")
                    
                    if departed_admins_in_space:
                        print(f"  Departed Admins ({len(departed_admins_in_space)}): {', '.join(departed_admins_in_space)}")
                    else:
                        print("  No Departed Admins found for this space.")
            
            print(f"\\\\n--- Simplified Audit Complete ---")

        elif choice == '5':
            print("\\\\n--- Removing Space Admin (Simplified - First Space, First Contributor - JSON-RPC) ---")
            all_space_keys = get_all_space_keys()
            if not all_space_keys:
                print("No spaces found in Confluence. Cannot remove admin.")
            else:
                first_space_key = all_space_keys[0]
                print(f"Targeting the first space: {first_space_key}")
                
                all_contributor_logins_set = read_logins_from_csv('contributors.csv')
                if not all_contributor_logins_set:
                    print("No users found in contributors.csv. Cannot select user to remove admin rights from.")
                else:
                    if not all_contributor_logins_set: # Should be caught by above, but defensive
                        print("Contributors set is empty. Cannot pick a user.")
                    else:
                        first_contributor_login = list(all_contributor_logins_set)[0]
                        print(f"Attempting to remove 'SETSPACEPERMISSIONS' for contributor '{first_contributor_login}' from space '{first_space_key}'.")
                        if remove_space_admin(first_space_key, first_contributor_login):
                            print(f"Successfully processed admin removal for {first_contributor_login} from {first_space_key}.")
                        else:
                            print(f"Failed to remove admin {first_contributor_login} from {first_space_key}.")
            print("\\\\nFinished simplified admin removal attempt.")

        elif choice == 'Q':
            print("Exiting script.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, 3, 4, 5, or Q.")
