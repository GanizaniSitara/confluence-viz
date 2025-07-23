#!/usr/bin/env python3
"""
Probe Open-WebUI endpoints to discover the correct API paths
"""

import requests
import json
import sys

def probe_url(base_url):
    """Probe various possible API endpoints"""
    print(f"Probing Open-WebUI at {base_url}")
    print("=" * 60)
    
    # Common endpoint patterns to try
    endpoints = [
        "",  # Base URL
        "/api",
        "/api/v1",
        "/api/v1/auths",
        "/api/v1/auths/signin",
        "/api/v1/auth/signin",
        "/api/auth/signin",
        "/api/signin",
        "/auth/signin",
        "/api/v1/knowledge",
        "/api/knowledge",
        "/api/v1/models",
        "/api/models",
        "/docs",
        "/api/docs",
        "/openapi.json",
        "/api/openapi.json",
    ]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'OpenWebUI-Probe/1.0',
        'Accept': 'application/json, text/html, */*'
    })
    
    for endpoint in endpoints:
        url = f"{base_url}{endpoint}"
        print(f"\n🔍 Checking: {url}")
        
        try:
            # Try GET first
            response = session.get(url, timeout=5, allow_redirects=False)
            print(f"   GET Status: {response.status_code}")
            
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                print(f"   Content-Type: {content_type}")
                
                if 'application/json' in content_type:
                    try:
                        data = response.json()
                        print(f"   JSON keys: {list(data.keys()) if isinstance(data, dict) else 'Array/Other'}")
                        # Print first 200 chars of response
                        print(f"   Response: {json.dumps(data, indent=2)[:200]}...")
                    except:
                        print("   Could not parse JSON")
                elif 'text/html' in content_type:
                    print("   HTML page (likely web interface)")
                    # Check for common indicators
                    text = response.text[:1000]
                    if 'Open WebUI' in text or 'open-webui' in text:
                        print("   ✓ Appears to be Open-WebUI")
                    if '<title>' in text:
                        import re
                        title = re.search(r'<title>(.*?)</title>', text)
                        if title:
                            print(f"   Title: {title.group(1)}")
            
            elif response.status_code == 401:
                print("   ⚠️  Unauthorized - may require authentication")
            elif response.status_code == 405:
                print("   Method not allowed - trying POST")
                
                # Try POST for signin endpoints
                if 'signin' in endpoint:
                    post_response = session.post(url, json={}, timeout=5)
                    print(f"   POST Status: {post_response.status_code}")
                    if post_response.status_code == 422:
                        print("   ✓ Endpoint exists but needs proper data")
                    elif post_response.status_code == 400:
                        print("   ✓ Endpoint exists but bad request")
                        
        except requests.exceptions.Timeout:
            print("   ⏱️  Timeout")
        except requests.exceptions.ConnectionError:
            print("   ❌ Connection error")
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
    
    print("\n" + "=" * 60)
    print("Probe complete. Check the results above to find the correct endpoints.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    else:
        base_url = "http://localhost:8080"
    
    probe_url(base_url)