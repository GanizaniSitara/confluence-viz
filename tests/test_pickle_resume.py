#!/usr/bin/env python3
"""
Test script to verify the new --resume-from-pickles functionality
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from collectors.sample_and_pickle_spaces import scan_existing_pickles

def test_scan_pickles():
    """Test the scan_existing_pickles function"""
    print("Testing scan_existing_pickles function...")
    
    # Test scanning the temp directory
    temp_dir = 'temp'
    if os.path.exists(temp_dir):
        print(f"\nScanning {temp_dir} for standard pickle files:")
        standard_pickles = scan_existing_pickles(temp_dir, mode='standard')
        print(f"Standard pickles found: {sorted(list(standard_pickles))}")
        
        print(f"\nScanning {temp_dir} for full pickle files:")  
        full_pickles = scan_existing_pickles(temp_dir, mode='full')
        print(f"Full pickles found: {sorted(list(full_pickles))}")
    else:
        print(f"Directory {temp_dir} does not exist")
    
    # Test scanning the full_pickles subdirectory
    full_pickle_dir = 'temp/full_pickles'
    if os.path.exists(full_pickle_dir):
        print(f"\nScanning {full_pickle_dir} for full pickle files:")
        full_pickles = scan_existing_pickles(full_pickle_dir, mode='full')
        print(f"Full pickles found: {sorted(list(full_pickles))}")
    else:
        print(f"Directory {full_pickle_dir} does not exist")

if __name__ == '__main__':
    test_scan_pickles()