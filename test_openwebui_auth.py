#!/usr/bin/env python3
"""
Test Open-WebUI Authentication
Tests credentials from settings.ini to verify authentication is working
"""

import sys
import configparser
import requests
import json
from pathlib import Path

def load_settings():
    """Load Open-WebUI settings from settings.ini"""
    config = configparser.ConfigParser()
    
    if not Path('settings.ini').exists():
        print("❌ ERROR: settings.ini not found!")
        print("   Please copy settings.example.ini to settings.ini and configure it.")
        return None
    
    config.read('settings.ini')
    
    # Handle both 'openwebui' and 'OpenWebUI' section names
    settings = {}
    if 'openwebui' in config:
        section = config['openwebui']
    elif 'OpenWebUI' in config:
        section = config['OpenWebUI']
    else:
        print("❌ ERROR: No [openwebui] or [OpenWebUI] section found in settings.ini")
        return None
    
    settings['base_url'] = section.get('base_url', 'http://localhost:8080')
    settings['username'] = section.get('username', '')
    settings['password'] = section.get('password', '')
    
    # Filter out placeholder values
    if settings['username'] in ['your_username', 'your_email@example.com', '']:
        settings['username'] = None
    if settings['password'] in ['your_password', '']:
        settings['password'] = None
    
    return settings

def test_authentication(base_url, username, password):
    """Test authentication with Open-WebUI"""
    print(f"\n🔐 Testing authentication with {base_url}...")
    print(f"   Username: {username if username else 'Not provided'}")
    print(f"   Password: {'*' * len(password) if password else 'Not provided'}")
    
    # First, check if the base URL is reachable
    print(f"\n🌐 Checking base URL: {base_url}")
    try:
        response = requests.get(base_url, timeout=10)
        print(f"   Status: {response.status_code}")
        print(f"   Headers: {dict(response.headers)}")
        
        # Try to detect what kind of server this is
        if 'text/html' in response.headers.get('Content-Type', ''):
            print("   Response appears to be HTML (web interface)")
        elif 'application/json' in response.headers.get('Content-Type', ''):
            print("   Response appears to be JSON (API)")
            try:
                data = response.json()
                print(f"   JSON Response: {json.dumps(data, indent=2)[:500]}...")
            except:
                pass
                
    except Exception as e:
        print(f"   Error accessing base URL: {str(e)}")
    
    if not username or not password:
        print("\n⚠️  No credentials provided - some Open-WebUI instances don't require auth")
        print("   Attempting to access API without authentication...")
        
        # Try accessing API without auth
        try:
            test_url = f"{base_url}/api/v1/auths/"
            response = requests.get(test_url, timeout=30)
            if response.status_code == 200:
                print("✅ API accessible without authentication")
                return True
            else:
                print(f"❌ API returned {response.status_code} - authentication may be required")
                return False
        except Exception as e:
            print(f"❌ Failed to connect: {str(e)}")
            return False
    
    # Try to find the correct authentication endpoint
    print("\n🔍 Searching for authentication endpoint...")
    
    possible_auth_endpoints = [
        "/api/v1/auths/signin",
        "/api/v1/auth/signin",
        "/api/auth/signin",
        "/api/signin",
        "/auth/signin",
        "/api/v1/login",
        "/api/login",
        "/login",
    ]
    
    auth_url = None
    for endpoint in possible_auth_endpoints:
        test_url = f"{base_url}{endpoint}"
        try:
            # Try POST with empty data to see if endpoint exists
            test_response = requests.post(test_url, json={}, timeout=5)
            if test_response.status_code in [400, 401, 422]:  # Bad request, unauthorized, or validation error means endpoint exists
                print(f"   ✓ Found possible auth endpoint: {endpoint}")
                auth_url = test_url
                break
            elif test_response.status_code == 200:
                print(f"   ✓ Found auth endpoint (accepts empty data?): {endpoint}")
                auth_url = test_url
                break
        except:
            pass
    
    if not auth_url:
        print("   ⚠️  Could not find auth endpoint, using default: /api/v1/auths/signin")
        auth_url = f"{base_url}/api/v1/auths/signin"
    
    print(f"\n🌐 POST {auth_url}")
    
    try:
        response = requests.post(auth_url, 
            json={
                "email": username,
                "password": password
            }, 
            timeout=30,
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if 'token' in data:
                print("✅ Authentication successful!")
                print(f"   Token received: {data['token'][:20]}...")
                
                # Test the token
                print("\n🔍 Testing token validity...")
                headers = {'Authorization': f"Bearer {data['token']}"}
                test_response = requests.get(f"{base_url}/api/v1/auths/", headers=headers, timeout=30)
                
                if test_response.status_code == 200:
                    print("✅ Token is valid and working!")
                else:
                    print(f"⚠️  Token test returned {test_response.status_code}")
                
                return True
            else:
                print("❌ No token in response")
                print(f"   Response: {response.text}")
                return False
        else:
            print(f"❌ Authentication failed: HTTP {response.status_code}")
            
            if response.status_code == 401:
                print("   Unauthorized - check your username/password")
            elif response.status_code == 404:
                print("   Auth endpoint not found - check the server URL")
                print(f"   Tried: {auth_url}")
            
            try:
                error_data = response.json()
                if 'detail' in error_data:
                    print(f"   Server message: {error_data['detail']}")
            except:
                print(f"   Response body: {response.text}")
            
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Authentication timeout - server did not respond within 30 seconds")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection error: {str(e)}")
        print("   Check that Open-WebUI is running and the URL is correct")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def test_api_endpoints(base_url, token=None):
    """Test various API endpoints"""
    print("\n📋 Testing API endpoints...")
    
    headers = {}
    if token:
        headers['Authorization'] = f"Bearer {token}"
    
    # Try different API path patterns
    endpoint_patterns = [
        # Pattern: (paths to try, description)
        (["/api/v1/auths/", "/api/v1/auth/", "/api/auth/", "/auth/"], "Auth check"),
        (["/api/v1/knowledge/", "/api/knowledge/", "/knowledge/"], "Knowledge collections"),
        (["/api/v1/models/", "/api/models/", "/models/"], "Models"),
    ]
    
    for path_options, description in endpoint_patterns:
        found = False
        for path in path_options:
            url = f"{base_url}{path}"
            try:
                response = requests.get(url, headers=headers, timeout=5)
                if response.status_code == 200:
                    print(f"✅ {description}: OK (at {path})")
                    found = True
                    break
                elif response.status_code == 401:
                    print(f"⚠️  {description}: Unauthorized (at {path})")
                    found = True
                    break
            except:
                pass
        
        if not found:
            print(f"❌ {description}: Not found at any standard path")

def main():
    """Main test function"""
    print("Open-WebUI Authentication Test")
    print("=" * 40)
    
    # Load settings
    settings = load_settings()
    if not settings:
        return 1
    
    print("\n📄 Loaded settings from settings.ini:")
    print(f"   base_url: {settings['base_url']}")
    print(f"   username: {settings['username'] if settings['username'] else 'Not set'}")
    print(f"   password: {'***' if settings['password'] else 'Not set'}")
    
    # Test authentication
    success = test_authentication(
        settings['base_url'],
        settings['username'],
        settings['password']
    )
    
    if success:
        print("\n✅ Authentication test passed!")
        
        # Test API endpoints if we have a way to get the token
        # (Would need to modify test_authentication to return the token)
        
        return 0
    else:
        print("\n❌ Authentication test failed!")
        print("\nTroubleshooting tips:")
        print("1. Check your settings.ini file")
        print("2. Verify Open-WebUI is running at the specified URL")
        print("3. Try logging into the web UI to confirm credentials")
        print("4. Check if you're using email (not username) for login")
        print("5. Some instances may have API authentication disabled")
        
        return 1

if __name__ == "__main__":
    sys.exit(main())