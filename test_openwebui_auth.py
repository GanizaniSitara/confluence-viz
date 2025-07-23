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
    
    # Try authentication
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
    
    endpoints = [
        ("/api/v1/auths/", "Auth check"),
        ("/api/v1/knowledge/", "Knowledge collections"),
        ("/api/v1/models/", "Models"),
    ]
    
    for endpoint, description in endpoints:
        url = f"{base_url}{endpoint}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                print(f"✅ {description}: OK")
            else:
                print(f"❌ {description}: {response.status_code}")
        except Exception as e:
            print(f"❌ {description}: {str(e)}")

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