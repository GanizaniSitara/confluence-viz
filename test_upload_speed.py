#!/usr/bin/env python3
"""
Simple test script to verify upload functionality and speed
"""
import subprocess
import sys
import time
import os

def test_upload(workers=None):
    """Test upload with timing"""
    print(f"\n{'='*60}")
    if workers:
        print(f"Testing PARALLEL upload with {workers} workers...")
        cmd = [sys.executable, 'open-webui-parallel.py', '--format', 'txt', '--workers', str(workers)]
    else:
        print("Testing SEQUENTIAL upload...")
        cmd = [sys.executable, 'open-webui.py', '--format', 'txt']
    
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    start = time.time()
    try:
        # Run with real-time output
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        
        # Print output in real-time
        for line in iter(process.stdout.readline, ''):
            if line:
                print(line.rstrip())
        
        process.wait()
        elapsed = time.time() - start
        
        print(f"\n{'='*60}")
        print(f"Completed in {elapsed:.2f} seconds")
        print(f"Exit code: {process.returncode}")
        
        return elapsed, process.returncode == 0
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        process.terminate()
        return None, False
    except Exception as e:
        print(f"Error: {e}")
        return None, False

def main():
    """Run speed tests"""
    print("Upload Speed Test")
    print("=" * 60)
    print("\nThis will upload text format to Open-WebUI")
    print("Make sure your settings.ini is configured correctly")
    
    # Test sequential first
    response = input("\nTest sequential upload? (y/n): ").strip().lower()
    if response == 'y':
        seq_time, seq_success = test_upload()
    else:
        seq_time = None
    
    # Test parallel
    response = input("\nTest parallel upload with 4 workers? (y/n): ").strip().lower()
    if response == 'y':
        par_time, par_success = test_upload(workers=4)
        
        if seq_time and par_time:
            speedup = seq_time / par_time
            print(f"\nSpeedup: {speedup:.2f}x faster with parallel processing")

if __name__ == "__main__":
    main()