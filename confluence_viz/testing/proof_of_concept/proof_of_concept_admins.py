import requests
from requests.auth import HTTPBasicAuth
import json # For printing debug info if needed
from config_loader import load_confluence_settings # To load settings

# Load Confluence settings
SETTINGS = load_confluence_settings()
# CONFLUENCE_API_BASE_URL is likely .../rest/api, we need the base
CONFLUENCE_BASE_URL = SETTINGS['base_url'] # Use 'base_url' directly
USERNAME = SETTINGS['username']
PASSWORD = SETTINGS['password'] # This will be your API token or password
VERIFY_SSL = SETTINGS['verify_ssl']

# --- User's provided logic starts here, adapted for loaded settings ---
base_url   = CONFLUENCE_BASE_URL        # Confluence base URL from settings
space_key  = "ITLEHUND"                      # space key to query (can be changed)
username   = USERNAME                   # your username from settings
api_token  = PASSWORD                   # password or API token from settings

print(f"Attempting to fetch admins for space '{space_key}' using URL: {base_url} and user: {username}")

url = f"{base_url}/rest/api/space/{space_key}?expand=permissions"

try:
    resp = requests.get(url, auth=HTTPBasicAuth(username, api_token), verify=VERIFY_SSL)
    resp.raise_for_status() # Raises an exception for 4XX/5XX errors
    data = resp.json()

    # Filter for space-admin permission entries
    admins = []
    if "permissions" in data:
        for perm in data.get("permissions", []):
            op = perm.get("operation", {})
            # Check for the specific 'administer' permission on 'space'
            if op.get("operation") == "administer" and op.get("targetType") == "space":
                # collect user subjects
                if "subjects" in perm and "user" in perm["subjects"] and "results" in perm["subjects"]["user"]:
                    users = perm["subjects"]["user"]["results"]
                    admins.extend([u.get("displayName", u.get("username", "Unknown User")) for u in users if u]) # Added fallback for username
                # collect group subjects
                if "subjects" in perm and "group" in perm["subjects"] and "results" in perm["subjects"]["group"]:
                    groups = perm["subjects"]["group"]["results"]
                    admins.extend([f"group:{g.get('name', 'Unknown Group')}" for g in groups if g]) # Added fallback for group name
    else:
        print(f"Warning: 'permissions' key not found in response for space {space_key}. Full response data: {json.dumps(data, indent=2)}")


    if admins:
        print(f"Space admins (users with 'administer space' permission) for '{space_key}': {admins}")
    else:
        print(f"No direct 'administer space' permissions found for users or groups in space '{space_key}'.")
        print(f"Note: This script specifically checks for the 'ADMINISTER' permission on the 'SPACE' target.")
        print(f"It does not check for 'SETSPACEPERMISSIONS' which might also grant administrative capabilities.")

except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
except requests.exceptions.RequestException as e:
    print(f"Request Exception: {e}")
except json.JSONDecodeError:
    print(f"Failed to decode JSON response. Response text: {resp.text}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
