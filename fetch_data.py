# description: Fetches data from Confluence.

import pickle
import json
import sys # Import sys for stderr
import os
from viz import fetch_all_spaces, fetch_page_data_for_space, build_data

OUTPUT_PICKLE = "confluence_data.pkl"

def fetch_and_save_data():
    print("Starting data fetch process...")
    try:
        print("Fetching all spaces...")
        spaces = fetch_all_spaces()
        if not spaces:
            print("No spaces found. Exiting.", file=sys.stderr)
            return

        print(f"Found {len(spaces)} spaces. Fetching page data for each...")
        for idx, sp in enumerate(spaces, start=1):
            print(f"Processing space {idx}/{len(spaces)}: key={sp['key']} ({sp.get('name', 'N/A')})")
            count, ts = fetch_page_data_for_space(sp["key"])
            avg = (sum(ts) / len(ts)) if ts else 0
            sp["value"] = count
            sp["avg"] = avg
            print(f"  Finished processing space {sp['key']}. Pages: {count}, Avg Timestamp: {avg:.4f}")

        print("\nBuilding data structure...")
        data = build_data(spaces)

        print(f"Saving data to {OUTPUT_PICKLE}...")
        with open(OUTPUT_PICKLE, "wb") as f:
            pickle.dump(data, f)
        print(f"Data successfully saved to {OUTPUT_PICKLE}")

    except Exception as e:
        print(f"An error occurred during data fetching or saving: {e}", file=sys.stderr)

if __name__ == "__main__":
    fetch_and_save_data()