#!/usr/bin/env python3
"""
Test script for explore_clusters.py functions
Tests the core functions without interactive menu
"""
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
import sys
import os

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from explorers.explore_clusters import (
    load_spaces,
    filter_spaces,
    get_vectors,
    cluster_spaces,
    calculate_avg_timestamps,
    search_spaces,
    TEMP_DIR,
    STOPWORDS
)

def test_load_spaces(temp_dir):
    """Test 1: Load spaces from pickle files"""
    print("\n" + "="*60)
    print("TEST 1: load_spaces()")
    print("="*60)

    try:
        spaces = load_spaces(temp_dir=temp_dir, min_pages=0)
        print(f"SUCCESS: Loaded {len(spaces)} spaces")

        if spaces:
            print(f"  First space: {spaces[0].get('space_key')}")
            print(f"  Pages in first space: {len(spaces[0].get('sampled_pages', []))}")
        return spaces
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        return []

def test_filter_spaces(spaces):
    """Test 2: Filter spaces by page count"""
    print("\n" + "="*60)
    print("TEST 2: filter_spaces()")
    print("="*60)

    try:
        filtered = filter_spaces(spaces, min_pages=10)
        print(f"SUCCESS: Filtered to {len(filtered)} spaces with >= 10 pages")
        return filtered
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        return spaces

def test_calculate_timestamps(spaces):
    """Test 3: Calculate average timestamps"""
    print("\n" + "="*60)
    print("TEST 3: calculate_avg_timestamps()")
    print("="*60)

    try:
        spaces_with_ts = calculate_avg_timestamps(spaces[:5])  # Test with first 5
        spaces_with_avg = [s for s in spaces_with_ts if s.get('avg', 0) > 0]
        print(f"SUCCESS: {len(spaces_with_avg)}/{len(spaces[:5])} spaces have timestamps")
        return True
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        return False

def test_get_vectors(spaces):
    """Test 4: Get TF-IDF vectors (tests HTML cleaning)"""
    print("\n" + "="*60)
    print("TEST 4: get_vectors() - This tests HTML cleaning")
    print("="*60)

    try:
        # Use subset for faster testing
        test_spaces = spaces[:10] if len(spaces) > 10 else spaces
        X, valid_spaces = get_vectors(test_spaces)
        print(f"SUCCESS: Generated vectors for {len(valid_spaces)}/{len(test_spaces)} spaces")
        print(f"  Vector shape: {X.shape}")

        if len(valid_spaces) == 0:
            print("  WARNING: No spaces had content after cleaning!")
            print("  This indicates the HTML cleaning issue")
        return X, valid_spaces
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None, []

def test_clustering(spaces):
    """Test 5: Run clustering"""
    print("\n" + "="*60)
    print("TEST 5: cluster_spaces()")
    print("="*60)

    try:
        # Need at least 2 spaces with content
        test_spaces = spaces[:20] if len(spaces) > 20 else spaces
        n_clusters = min(5, len(test_spaces))

        if n_clusters < 2:
            print("SKIPPED: Need at least 2 spaces for clustering")
            return None, []

        labels, valid_spaces = cluster_spaces(test_spaces, method='kmeans', n_clusters=n_clusters)
        print(f"SUCCESS: Clustered {len(valid_spaces)} spaces into {n_clusters} clusters")

        # Show cluster distribution
        from collections import Counter
        dist = Counter(labels)
        print(f"  Cluster distribution: {dict(dist)}")
        return labels, valid_spaces
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None, []

def test_search(spaces):
    """Test 6: Search spaces"""
    print("\n" + "="*60)
    print("TEST 6: search_spaces()")
    print("="*60)

    try:
        # Get a space key to search for
        if spaces:
            term = spaces[0].get('space_key', '')[:3]  # First 3 chars
            results = search_spaces(spaces, term)
            print(f"SUCCESS: Search for '{term}' returned {len(results)} results")
            for r in results[:3]:
                print(f"  - {r[0]} ({r[1]} pages)")
        return True
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        return False

def test_stopwords():
    """Test 7: Stopwords loaded"""
    print("\n" + "="*60)
    print("TEST 7: STOPWORDS loaded")
    print("="*60)

    print(f"SUCCESS: {len(STOPWORDS)} stopwords loaded")
    print(f"  Sample: {list(STOPWORDS)[:10]}")
    return True

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Test explore_clusters functions')
    parser.add_argument('--dir', '-d', default=TEMP_DIR, help='Pickle directory')
    args = parser.parse_args()

    print(f"Testing with pickle directory: {args.dir}")
    print(f"Default TEMP_DIR: {TEMP_DIR}")

    # Run tests
    results = {}

    # Test 1: Load
    spaces = test_load_spaces(args.dir)
    results['load_spaces'] = len(spaces) > 0

    if not spaces:
        print("\nCANNOT CONTINUE: No spaces loaded")
        return

    # Test 2: Filter
    filtered = test_filter_spaces(spaces)
    results['filter_spaces'] = True

    # Test 3: Timestamps
    results['timestamps'] = test_calculate_timestamps(spaces)

    # Test 4: Vectors (critical - tests HTML cleaning)
    X, valid = test_get_vectors(spaces)
    results['get_vectors'] = X is not None and len(valid) > 0

    # Test 5: Clustering
    if results['get_vectors']:
        labels, clustered = test_clustering(spaces)
        results['clustering'] = labels is not None
    else:
        print("\nSKIPPING clustering: get_vectors failed")
        results['clustering'] = False

    # Test 6: Search
    results['search'] = test_search(spaces)

    # Test 7: Stopwords
    results['stopwords'] = test_stopwords()

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {name}")

    if not results['get_vectors']:
        print("\nCRITICAL: get_vectors failed - this is the HTML cleaning issue")
        print("Check the debug output above for details")

if __name__ == '__main__':
    main()
