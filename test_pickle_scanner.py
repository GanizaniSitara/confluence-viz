#!/usr/bin/env python3
"""
Simple test to verify pickle file scanning works
"""

import os

def scan_existing_pickles(directory, mode='all'):
    """
    Scan a directory for existing pickle files and return list of processed space keys.
    """
    existing_space_keys = set()
    
    if not os.path.exists(directory):
        print(f"Warning: Directory {directory} does not exist for pickle scanning.")
        return existing_space_keys
    
    try:
        files = os.listdir(directory)
        for file in files:
            if file.endswith('.pkl'):
                # Extract space key from filename like SPACENAME.pkl
                space_key = file[:-4]  # Remove '.pkl'
                existing_space_keys.add(space_key)
        
        print(f"Found {len(existing_space_keys)} existing pickle files in {directory}")
        if existing_space_keys:
            sorted_keys = sorted(list(existing_space_keys))
            print(f"Existing space keys: {', '.join(sorted_keys[:10])}{'...' if len(sorted_keys) > 10 else ''}")
    
    except Exception as e:
        print(f"Error scanning directory {directory}: {e}")
    
    return existing_space_keys

def test_functionality():
    """Test the functionality"""
    print("Testing pickle file scanning...")
    
    # Test temp directory
    temp_dir = 'temp'
    if os.path.exists(temp_dir):
        print(f"\n=== Scanning {temp_dir} ===")
        temp_pickles = scan_existing_pickles(temp_dir)
        print(f"Pickle files: {len(temp_pickles)} files")
    
    # Test full_pickles subdirectory
    full_dir = 'temp/full_pickles'
    if os.path.exists(full_dir):
        print(f"\n=== Scanning {full_dir} ===")
        full_pickles = scan_existing_pickles(full_dir)
        print(f"Pickle files: {len(full_pickles)} files")

if __name__ == '__main__':
    test_functionality()