import pickle
import json
from viz import fetch_all_spaces, fetch_page_data_for_space, build_data

OUTPUT_PICKLE = "confluence_data.pkl"

def fetch_and_save_data():
    spaces = fetch_all_spaces()
    for sp in spaces:
        count, ts = fetch_page_data_for_space(sp["key"])
        avg = (sum(ts) / len(ts)) if ts else 0
        sp["value"] = count
        sp["avg"] = avg

    data = build_data(spaces)
    with open(OUTPUT_PICKLE, "wb") as f:
        pickle.dump(data, f)
    print(f"Data saved to {OUTPUT_PICKLE}")

if __name__ == "__main__":
    fetch_and_save_data()