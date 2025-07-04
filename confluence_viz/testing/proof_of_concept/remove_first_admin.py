# description: Removes the first administrator from a Confluence space.

import csv
import json
import random
import requests
import urllib3 # For disabling SSL warnings
from config_loader import load_confluence_settings

# Load Confluence settings
SETTINGS = load_confluence_settings()
CONFLUENCE_API_BASE_URL = SETTINGS['api_base_url']
CONFLUENCE_BASE_URL = CONFLUENCE_API_BASE_URL.replace('/rest/api', '')
USERNAME = SETTINGS['username']
PASSWORD = SETTINGS['password']
VERIFY_SSL = SETTINGS['verify_ssl']

# Suppress InsecureRequestWarning if VERIFY_SSL is False
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def remove_space_admin(space_key, username):
    """Removes 'SETSPACEPERMISSIONS' from a user for a given space using JSON-RPC."""
    rpc_url = f"{CONFLUENCE_BASE_URL}/rpc/json-rpc/confluenceservice-v2?os_authType=basic"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "removePermissionFromSpace",
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
        elif 'result' in response_data and response_data['result'] is True:
             print(f"  Successfully removed 'SETSPACEPERMISSIONS' from user '{username}' for space '{space_key}'.")
             return True
        elif 'result' not in response_data and 'error' not in response_data :
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
    params = {"type": "global", "limit": 50, "start": 0}
    print("\nFetching all space keys from Confluence...")
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
            
            if '_links' in data and 'next' in data['_links']:
                next_link = data['_links']['next']
                if 'start=' in next_link:
                    try:
                        params['start'] = int(next_link.split('start=')[1].split('&')[0])
                    except ValueError:
                        print("  Warning: Could not parse start from next link. Falling back to manual increment.")
                        params['start'] += len(results)
                else:
                    params['start'] += len(results)
            else:
                 params['start'] += len(results)
    except requests.exceptions.HTTPError as e:
        print(f"  Failed to get space keys. Status: {e.response.status_code}, Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"  An error occurred while getting space keys: {e}")
    except json.JSONDecodeError:
        print(f"  Failed to decode JSON response for space keys.")
    print(f"Finished fetching space keys. Total found: {len(set(space_keys))}")
    return list(set(space_keys))

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
                if row['Login']:
                    logins.add(row['Login'])
    except FileNotFoundError:
        print(f"  Error: {file_path} not found.")
    except Exception as e:
        print(f"  An error occurred while reading {file_path}: {e}")
    return logins

if __name__ == "__main__":
    print("\n--- Removing Space Admin (Simplified - First Space, First Contributor - JSON-RPC) ---")
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
            if not all_contributor_logins_set:
                print("Contributors set is empty. Cannot pick a user.")
            else:
                first_contributor_login = list(all_contributor_logins_set)[0]
                print(f"Attempting to remove 'SETSPACEPERMISSIONS' for contributor '{first_contributor_login}' from space '{first_space_key}'.")
                if remove_space_admin(first_space_key, first_contributor_login):
                    print(f"Successfully processed admin removal for {first_contributor_login} from {first_space_key}.")
                else:
                    print(f"Failed to remove admin {first_contributor_login} from {first_space_key}.")
    print("\nFinished simplified admin removal attempt.")
