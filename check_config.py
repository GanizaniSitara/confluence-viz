#!/usr/bin/env python3
"""
Quick utility to check current configuration settings
"""
import os
import sys
from config_loader import load_visualization_settings

def main():
    print("=== Configuration Check ===\n")
    
    # Check if settings.ini exists
    if os.path.exists('settings.ini'):
        print("✓ settings.ini found")
    else:
        print("✗ settings.ini NOT found - using defaults")
        print("  Please copy settings.example.ini to settings.ini and configure it")
    
    print("\n=== Visualization Settings ===")
    try:
        viz_settings = load_visualization_settings()
        for key, value in viz_settings.items():
            print(f"  {key}: {value}")
        
        # Check spaces_dir
        spaces_dir = viz_settings.get('spaces_dir', 'temp/full_pickles')
        print(f"\n=== Checking spaces_dir: {spaces_dir} ===")
        
        if os.path.exists(spaces_dir):
            print(f"✓ Directory exists: {spaces_dir}")
            pkl_files = [f for f in os.listdir(spaces_dir) if f.endswith('.pkl')]
            print(f"  Found {len(pkl_files)} .pkl files")
        else:
            print(f"✗ Directory NOT found: {spaces_dir}")
            print(f"  Current working directory: {os.getcwd()}")
            print(f"  Absolute path would be: {os.path.abspath(spaces_dir)}")
            
    except Exception as e:
        print(f"ERROR loading settings: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())