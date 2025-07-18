#!/usr/bin/env python3
"""
Script to compare Confluence space keys from the API with pickle files in temp/ directory.
Identifies discrepancies between what's in Confluence and what's been pickled.
"""

import os
import glob
from config_loader import load_config

def get_pickle_space_keys(pickle_dir="temp"):
    """Extract space keys from pickle filenames in the specified directory"""
    pickle_files = glob.glob(os.path.join(pickle_dir, "*.pkl"))
    pickle_space_keys = set()
    
    for filepath in pickle_files:
        filename = os.path.basename(filepath)
        # Remove .pkl extension
        if filename.endswith('.pkl'):
            space_key = filename[:-4]
            pickle_space_keys.add(space_key)
    
    return pickle_space_keys

def read_confluence_space_keys(filepath="confluence_space_keys.txt"):
    """Read space keys from the file created by space_explorer.py"""
    confluence_keys = set()
    
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            for line in f:
                key = line.strip()
                if key:
                    confluence_keys.add(key)
    else:
        print(f"Warning: {filepath} not found. Run space_explorer.py option 10 first.")
    
    return confluence_keys

def compare_spaces():
    """Compare Confluence spaces with pickle files and show the delta"""
    print("Comparing Confluence spaces with pickle files...\n")
    
    # Get space keys from both sources
    confluence_keys = read_confluence_space_keys()
    pickle_keys = get_pickle_space_keys()
    
    if not confluence_keys:
        print("No Confluence space keys found. Please run space_explorer.py option 10 first.")
        return
    
    # Calculate deltas
    in_confluence_not_pickled = confluence_keys - pickle_keys
    in_pickle_not_confluence = pickle_keys - confluence_keys
    in_both = confluence_keys & pickle_keys
    
    # Display results
    print(f"Total spaces in Confluence: {len(confluence_keys)}")
    print(f"Total pickle files in temp/: {len(pickle_keys)}")
    print(f"Spaces in both: {len(in_both)}")
    print()
    
    if in_confluence_not_pickled:
        print(f"\nSpaces in Confluence but NOT pickled ({len(in_confluence_not_pickled)}):")
        for key in sorted(in_confluence_not_pickled):
            print(f"  {key}")
    else:
        print("\nAll Confluence spaces have been pickled.")
    
    if in_pickle_not_confluence:
        print(f"\nPickle files with NO corresponding Confluence space ({len(in_pickle_not_confluence)}):")
        print("(These might be deleted/archived spaces or personal spaces)")
        for key in sorted(in_pickle_not_confluence):
            print(f"  {key}")
    else:
        print("\nAll pickle files correspond to existing Confluence spaces.")
    
    # Summary
    print("\n" + "="*50)
    print("SUMMARY:")
    print(f"  Missing pickles: {len(in_confluence_not_pickled)}")
    print(f"  Extra pickles: {len(in_pickle_not_confluence)}")
    print(f"  Matched: {len(in_both)}")
    
    # Save results to file
    output_file = "space_comparison_results.txt"
    with open(output_file, 'w') as f:
        f.write("Space Comparison Results\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total spaces in Confluence: {len(confluence_keys)}\n")
        f.write(f"Total pickle files in temp/: {len(pickle_keys)}\n")
        f.write(f"Spaces in both: {len(in_both)}\n\n")
        
        if in_confluence_not_pickled:
            f.write(f"Spaces in Confluence but NOT pickled ({len(in_confluence_not_pickled)}):\n")
            for key in sorted(in_confluence_not_pickled):
                f.write(f"  {key}\n")
        
        if in_pickle_not_confluence:
            f.write(f"\nPickle files with NO corresponding Confluence space ({len(in_pickle_not_confluence)}):\n")
            for key in sorted(in_pickle_not_confluence):
                f.write(f"  {key}\n")
    
    print(f"\nDetailed results saved to: {output_file}")

def main():
    """Main function"""
    compare_spaces()

if __name__ == "__main__":
    main()