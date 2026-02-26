#!/usr/bin/env python3
"""
Diagnostic script to inspect pickle file body structures.
Run this to diagnose why pages_with_body is 0.
"""
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
import pickle
import os
import sys
from utils.config_loader import load_data_settings

def inspect_pickles(temp_dir=None, max_files=3, max_pages_per_file=5):
    """Inspect pickle files to understand body structure."""

    if temp_dir is None:
        data_settings = load_data_settings()
        temp_dir = data_settings.get('pickle_dir', 'temp')

    print(f"Inspecting pickles in: {temp_dir}")
    print("=" * 80)

    pkl_files = [f for f in os.listdir(temp_dir) if f.endswith('.pkl')]
    if not pkl_files:
        print(f"ERROR: No pickle files found in {temp_dir}")
        return

    print(f"Found {len(pkl_files)} pickle files")

    stats = {
        'total_pages': 0,
        'pages_with_body_string': 0,
        'pages_with_body_dict': 0,
        'pages_with_body_none': 0,
        'pages_with_empty_body': 0,
        'pages_with_content': 0,
    }

    for pkl_file in pkl_files[:max_files]:
        pkl_path = os.path.join(temp_dir, pkl_file)
        print(f"\n{'='*80}")
        print(f"FILE: {pkl_file}")
        print(f"{'='*80}")

        with open(pkl_path, 'rb') as f:
            data = pickle.load(f)

        print(f"Top-level keys: {list(data.keys())}")
        print(f"Space key: {data.get('space_key')}")

        pages = data.get('sampled_pages', [])
        print(f"Number of pages: {len(pages)}")

        if not pages:
            print("  NO PAGES!")
            continue

        for i, page in enumerate(pages[:max_pages_per_file]):
            stats['total_pages'] += 1
            print(f"\n  --- Page {i+1}: {page.get('title', 'NO TITLE')[:50]} ---")
            print(f"  Page keys: {list(page.keys())}")

            body = page.get('body')

            if body is None:
                stats['pages_with_body_none'] += 1
                print(f"  Body: None")
            elif isinstance(body, dict):
                stats['pages_with_body_dict'] += 1
                print(f"  Body type: dict")
                print(f"  Body keys: {list(body.keys())}")

                # Try various extraction paths
                storage = body.get('storage', {})
                if isinstance(storage, dict):
                    value = storage.get('value', '')
                    print(f"  body['storage']['value'] length: {len(value) if value else 0}")
                    if value:
                        stats['pages_with_content'] += 1
                        print(f"  Preview: {repr(value[:200])}")
                    else:
                        stats['pages_with_empty_body'] += 1
                else:
                    print(f"  body['storage'] type: {type(storage)}")

                # Check other common paths
                for key in ['view', 'export_view', 'styled_view']:
                    if key in body:
                        sub = body[key]
                        if isinstance(sub, dict) and 'value' in sub:
                            print(f"  body['{key}']['value'] length: {len(sub['value']) if sub.get('value') else 0}")

            elif isinstance(body, str):
                stats['pages_with_body_string'] += 1
                print(f"  Body type: string")
                print(f"  Body length: {len(body)}")
                if body:
                    stats['pages_with_content'] += 1
                    print(f"  Preview: {repr(body[:200])}")
                else:
                    stats['pages_with_empty_body'] += 1
                    print(f"  Body is EMPTY STRING")
            else:
                print(f"  Body type: {type(body)} (UNEXPECTED)")

    print(f"\n{'='*80}")
    print("SUMMARY STATISTICS")
    print(f"{'='*80}")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Diagnosis
    print(f"\n{'='*80}")
    print("DIAGNOSIS")
    print(f"{'='*80}")

    if stats['pages_with_body_none'] > 0:
        print("  WARNING: Some pages have body=None. Check fetch_page_body() API response.")

    if stats['pages_with_empty_body'] > 0:
        print("  WARNING: Some pages have empty body strings. Possible causes:")
        print("    - API user lacks permission to read content")
        print("    - Pages use restricted content macros")
        print("    - Pages are empty or templates")

    if stats['pages_with_body_dict'] > 0 and stats['pages_with_body_string'] == 0:
        print("  ISSUE FOUND: Bodies are stored as dicts, not strings!")
        print("  This suggests sample_and_pickle_spaces.py saved the full body dict")
        print("  instead of extracting body['storage']['value']")
        print("  FIX: Update explore_clusters.py to handle dict bodies, OR")
        print("       Re-run pickle generation to extract the value correctly")

    if stats['pages_with_content'] == 0:
        print("  CRITICAL: No pages have actual content!")
        print("  Check API response in sample_and_pickle_spaces.py")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Diagnose pickle body structures')
    parser.add_argument('--dir', '-d', help='Pickle directory to inspect')
    parser.add_argument('--files', '-f', type=int, default=3, help='Max files to inspect')
    parser.add_argument('--pages', '-p', type=int, default=5, help='Max pages per file')

    args = parser.parse_args()
    inspect_pickles(args.dir, args.files, args.pages)
