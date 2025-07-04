# filepath: c:\Solutions\Python\confluence_visualization\create_user_test.py
# description: Test script for creating users in Confluence.

# This code sample uses the 'requests' library:
# http://docs.python-requests.org
import requests
import json

url = "http://192.168.65.128:8090/confluence/rest/api/admin/user"

headers = {
  "Accept": "application/json",
  "Content-Type": "application/json"
}

payload = json.dumps( {
  "userName": "user1",
  "fullName": "Some User",
  "email": "someuser@someemail.com",
  "password": "password",
  "notifyViaEmail": True
} )

response = requests.request(
   "POST",
   url,
   data=payload,
   headers=headers
)

print(json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")))