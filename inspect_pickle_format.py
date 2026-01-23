#!/usr/bin/env python3
"""Quick script to inspect pickle format"""
import pickle
import sys
import os

def inspect(path):
    with open(path, 'rb') as f:
        data = pickle.load(f)

    print(f"File: {path}")
    print(f"Type: {type(data).__name__}")

    if isinstance(data, dict):
        print(f"Keys: {list(data.keys())}")
        if 'space_key' in data:
            print(f"Space key: {data['space_key']}")
        if 'sampled_pages' in data:
            print(f"Num pages: {len(data['sampled_pages'])}")
    elif isinstance(data, list):
        print(f"List length: {len(data)}")
        if data:
            print(f"First item type: {type(data[0]).__name__}")
            if isinstance(data[0], dict):
                print(f"First item keys: {list(data[0].keys())}")
    else:
        print(f"Data: {repr(data)[:200]}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python inspect_pickle_format.py <pickle_file>")
        sys.exit(1)
    inspect(sys.argv[1])
