#!/usr/bin/env python3
"""
Script to reconcile pickle files between old naming convention (SPACENAME_full.pkl)
and new naming convention (SPACENAME.pkl) by keeping the larger file.
"""

import os
import shutil
import pickle
from pathlib import Path

def get_pickle_info(filepath):
    """Get information about a pickle file"""
    try:
        file_size = os.path.getsize(filepath)
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            num_pages = len(data.get('sampled_pages', []))
            space_key = data.get('space_key', 'Unknown')
            space_name = data.get('name', 'Unknown')
        return {
            'size': file_size,
            'pages': num_pages,
            'space_key': space_key,
            'space_name': space_name
        }
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def reconcile_pickles(directory, dry_run=True):
    """
    Reconcile pickle files in the given directory.
    For each space, if both SPACENAME.pkl and SPACENAME_full.pkl exist,
    keep the larger one and rename it to SPACENAME.pkl
    """
    print(f"Scanning directory: {directory}")
    print(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL RUN'}")
    print("=" * 80)
    
    # Find all pickle files
    all_files = list(Path(directory).glob("*.pkl"))
    
    # Group files by space key
    space_files = {}
    for file in all_files:
        filename = file.name
        if filename.endswith('_full.pkl'):
            space_key = filename[:-9]  # Remove '_full.pkl'
            file_type = 'full'
        elif filename.endswith('.pkl'):
            space_key = filename[:-4]  # Remove '.pkl'
            file_type = 'standard'
        else:
            continue
            
        if space_key not in space_files:
            space_files[space_key] = {}
        space_files[space_key][file_type] = file
    
    # Process each space
    reconciled = 0
    errors = 0
    already_good = 0
    
    for space_key, files in sorted(space_files.items()):
        if 'full' in files and 'standard' in files:
            # Both files exist - need to reconcile
            full_path = files['full']
            standard_path = files['standard']
            
            full_info = get_pickle_info(full_path)
            standard_info = get_pickle_info(standard_path)
            
            if not full_info or not standard_info:
                print(f"\n[ERROR] {space_key}: Could not read one or both files")
                errors += 1
                continue
            
            print(f"\n[RECONCILE] {space_key}:")
            print(f"  - {standard_path.name}: {full_info['size']:,} bytes, {standard_info['pages']} pages")
            print(f"  - {full_path.name}: {full_info['size']:,} bytes, {full_info['pages']} pages")
            
            # Determine which file to keep
            if full_info['pages'] > standard_info['pages']:
                keep_file = full_path
                remove_file = standard_path
                source = "full"
            elif standard_info['pages'] > full_info['pages']:
                keep_file = standard_path
                remove_file = full_path
                source = "standard"
            else:
                # Same number of pages, use file size
                if full_info['size'] >= standard_info['size']:
                    keep_file = full_path
                    remove_file = standard_path
                    source = "full"
                else:
                    keep_file = standard_path
                    remove_file = full_path
                    source = "standard"
            
            print(f"  → Keeping {source} file with {get_pickle_info(keep_file)['pages']} pages")
            
            if not dry_run:
                try:
                    # If keeping full file, rename it to standard name
                    if source == "full":
                        target_path = directory / f"{space_key}.pkl"
                        shutil.move(str(full_path), str(target_path))
                        os.remove(str(standard_path))
                        print(f"  ✓ Renamed {full_path.name} to {target_path.name}")
                        print(f"  ✓ Removed {standard_path.name}")
                    else:
                        # Keeping standard file, just remove full
                        os.remove(str(full_path))
                        print(f"  ✓ Removed {full_path.name}")
                    reconciled += 1
                except Exception as e:
                    print(f"  ✗ Error: {e}")
                    errors += 1
            else:
                print(f"  [DRY RUN] Would rename/remove files as shown above")
                reconciled += 1
                
        elif 'full' in files and 'standard' not in files:
            # Only full file exists - rename it
            full_path = files['full']
            full_info = get_pickle_info(full_path)
            
            if not full_info:
                print(f"\n[ERROR] {space_key}: Could not read {full_path.name}")
                errors += 1
                continue
                
            print(f"\n[RENAME] {space_key}:")
            print(f"  - {full_path.name}: {full_info['size']:,} bytes, {full_info['pages']} pages")
            print(f"  → Renaming to {space_key}.pkl")
            
            if not dry_run:
                try:
                    target_path = directory / f"{space_key}.pkl"
                    shutil.move(str(full_path), str(target_path))
                    print(f"  ✓ Renamed {full_path.name} to {target_path.name}")
                    reconciled += 1
                except Exception as e:
                    print(f"  ✗ Error: {e}")
                    errors += 1
            else:
                print(f"  [DRY RUN] Would rename to {space_key}.pkl")
                reconciled += 1
                
        else:
            # Only standard file exists - nothing to do
            already_good += 1
    
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print(f"  - Spaces already using new naming: {already_good}")
    print(f"  - Spaces reconciled/renamed: {reconciled}")
    print(f"  - Errors: {errors}")
    print(f"  - Total spaces: {len(space_files)}")
    
    if dry_run and reconciled > 0:
        print("\nThis was a DRY RUN. To actually reconcile files, run with --execute flag")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Reconcile old and new pickle file naming conventions')
    parser.add_argument('directory', nargs='?', default='temp', 
                       help='Directory containing pickle files (default: temp)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually perform the reconciliation (default is dry run)')
    parser.add_argument('--full-dir', 
                       help='Also check this directory for _full.pkl files (e.g., temp/full_pickles)')
    
    args = parser.parse_args()
    
    directory = Path(args.directory)
    if not directory.exists():
        print(f"Error: Directory {directory} does not exist")
        return 1
    
    # Process main directory
    reconcile_pickles(directory, dry_run=not args.execute)
    
    # Process additional directory if specified
    if args.full_dir:
        print(f"\n\nProcessing additional directory: {args.full_dir}")
        full_dir = Path(args.full_dir)
        if full_dir.exists():
            reconcile_pickles(full_dir, dry_run=not args.execute)
        else:
            print(f"Warning: Directory {full_dir} does not exist")
    
    return 0

if __name__ == "__main__":
    exit(main())