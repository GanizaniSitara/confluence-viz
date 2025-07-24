#!/usr/bin/env python3
"""
Ollama connectivity diagnostic script
Tests various aspects of Ollama connection
"""

import subprocess
import requests
import json
import socket
import os
from urllib.parse import urlparse

def check_ollama_process():
    """Check if Ollama process is running"""
    print("1. Checking Ollama process...")
    try:
        result = subprocess.run(['pgrep', '-f', 'ollama'], capture_output=True, text=True)
        if result.stdout:
            print("✓ Ollama process found (PIDs):", result.stdout.strip())
            # Get more details
            subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            ps_result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            for line in ps_result.stdout.split('\n'):
                if 'ollama' in line.lower():
                    print("  ", line)
        else:
            print("✗ Ollama process not found")
    except Exception as e:
        print(f"✗ Error checking process: {e}")

def check_ollama_service():
    """Check systemd service status"""
    print("\n2. Checking Ollama service status...")
    try:
        result = subprocess.run(['systemctl', 'status', 'ollama'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ Ollama service is active")
            # Print first few lines of status
            for line in result.stdout.split('\n')[:10]:
                print("  ", line)
        else:
            print("✗ Ollama service not active or not found")
    except Exception as e:
        print(f"✗ Error checking service: {e}")

def check_port_binding():
    """Check if Ollama is listening on expected ports"""
    print("\n3. Checking port bindings...")
    ports_to_check = [11434, 11435]  # Common Ollama ports
    
    for port in ports_to_check:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            
            if result == 0:
                print(f"✓ Port {port} is open on localhost")
                # Check what's listening
                try:
                    lsof_result = subprocess.run(['lsof', '-i', f':{port}'], capture_output=True, text=True)
                    if lsof_result.stdout:
                        print(f"   Listening on port {port}:")
                        for line in lsof_result.stdout.split('\n')[1:3]:  # First few lines
                            if line:
                                print("  ", line)
                except:
                    pass
            else:
                print(f"✗ Port {port} is not open on localhost")
        except Exception as e:
            print(f"✗ Error checking port {port}: {e}")

def check_ollama_api():
    """Test Ollama API endpoints"""
    print("\n4. Testing Ollama API endpoints...")
    
    base_urls = [
        "http://localhost:11434",
        "http://127.0.0.1:11434",
        "http://0.0.0.0:11434"
    ]
    
    # Check OLLAMA_HOST environment variable
    ollama_host = os.environ.get('OLLAMA_HOST')
    if ollama_host:
        print(f"   OLLAMA_HOST is set to: {ollama_host}")
        if not ollama_host.startswith('http'):
            ollama_host = f"http://{ollama_host}"
        base_urls.insert(0, ollama_host)
    
    for base_url in base_urls:
        print(f"\n   Testing {base_url}...")
        
        # Test root endpoint
        try:
            response = requests.get(f"{base_url}/", timeout=5)
            print(f"   ✓ Root endpoint: {response.status_code} - {response.text[:50]}...")
        except Exception as e:
            print(f"   ✗ Root endpoint failed: {e}")
        
        # Test version endpoint
        try:
            response = requests.get(f"{base_url}/api/version", timeout=5)
            print(f"   ✓ Version endpoint: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"   ✗ Version endpoint failed: {e}")
        
        # Test tags endpoint (list models)
        try:
            response = requests.get(f"{base_url}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json()
                print(f"   ✓ Models endpoint: Found {len(models.get('models', []))} models")
                for model in models.get('models', [])[:5]:  # First 5 models
                    print(f"      - {model['name']}")
            else:
                print(f"   ✗ Models endpoint: {response.status_code}")
        except Exception as e:
            print(f"   ✗ Models endpoint failed: {e}")

def check_firewall():
    """Check firewall rules"""
    print("\n5. Checking firewall rules...")
    
    # Check iptables
    try:
        result = subprocess.run(['sudo', 'iptables', '-L', '-n'], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            ollama_rules = [line for line in lines if '11434' in line or '11435' in line]
            if ollama_rules:
                print("✓ Found Ollama-related firewall rules:")
                for rule in ollama_rules:
                    print("  ", rule)
            else:
                print("⚠ No specific Ollama firewall rules found")
        else:
            print("✗ Could not check iptables (need sudo)")
    except:
        print("⚠ iptables not available")
    
    # Check ufw if available
    try:
        result = subprocess.run(['sudo', 'ufw', 'status'], capture_output=True, text=True)
        if result.returncode == 0:
            if 'inactive' in result.stdout.lower():
                print("✓ UFW is inactive")
            else:
                print("⚠ UFW is active:")
                for line in result.stdout.split('\n')[:10]:
                    if line:
                        print("  ", line)
    except:
        print("⚠ UFW not available")

def check_ollama_config():
    """Check Ollama configuration"""
    print("\n6. Checking Ollama configuration...")
    
    # Check environment variables
    env_vars = ['OLLAMA_HOST', 'OLLAMA_ORIGINS', 'OLLAMA_MODELS', 'OLLAMA_NUM_PARALLEL']
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            print(f"   {var}={value}")
    
    # Check systemd override
    try:
        override_path = "/etc/systemd/system/ollama.service.d/override.conf"
        if os.path.exists(override_path):
            print(f"\n   Found systemd override at {override_path}:")
            with open(override_path, 'r') as f:
                for line in f:
                    print("  ", line.strip())
    except:
        pass

def test_model_generation():
    """Test actual model generation"""
    print("\n7. Testing model generation...")
    
    test_prompt = "Say 'Hello, Ollama is working!'"
    model = "llama3.2:latest"  # Common model, adjust as needed
    
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": test_prompt,
                "stream": False
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Model generation successful!")
            print(f"   Response: {result.get('response', '')[:100]}...")
        else:
            print(f"✗ Model generation failed: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"✗ Model generation error: {e}")

def main():
    print("=== Ollama Connectivity Diagnostic ===\n")
    
    check_ollama_process()
    check_ollama_service()
    check_port_binding()
    check_ollama_api()
    check_firewall()
    check_ollama_config()
    test_model_generation()
    
    print("\n=== Diagnostic Complete ===")
    print("\nCommon fixes:")
    print("1. If Ollama is not listening on all interfaces:")
    print("   export OLLAMA_HOST=0.0.0.0:11434")
    print("   sudo systemctl restart ollama")
    print("\n2. If firewall is blocking:")
    print("   sudo ufw allow 11434/tcp")
    print("\n3. If service not running:")
    print("   sudo systemctl start ollama")
    print("   sudo systemctl enable ollama")
    print("\n4. Check Ollama logs:")
    print("   sudo journalctl -u ollama -f")

if __name__ == "__main__":
    main()