import argparse
import pickle
import os
import sys
import numpy as np # For calculating mean, ignoring NaNs

OUTPUT_DIR = 'temp'
FULL_PICKLE_SUBDIR = 'full_pickles' # New line

def analyze_pickle(space_key):
    pickle_filename = f'{space_key}_full.pkl'
    # Updated path construction
    pickle_path = os.path.join(OUTPUT_DIR, FULL_PICKLE_SUBDIR, pickle_filename)

    if not os.path.exists(pickle_path):
        print(f"Error: Pickle file not found at {pickle_path}")
        print(f"Please ensure you have run 'python sample_and_pickle_spaces.py --pickle-space-full {space_key}' first.")
        sys.exit(1)

    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
    except Exception as e:
        print(f"Error loading pickle file {pickle_path}: {e}")
        sys.exit(1)

    print(f"--- Statistics for Space: {data.get('space_key', 'N/A')} ---")
    print(f"Space Name: {data.get('name', 'N/A')}")
    
    total_pages_in_space_metadata = data.get('total_pages_in_space', 0)
    print(f"Total Pages in Space (according to metadata in pickle): {total_pages_in_space_metadata}")

    sampled_pages = data.get('sampled_pages', [])
    num_pages_in_pickle = len(sampled_pages)
    print(f"Number of Pages in Pickle: {num_pages_in_pickle}")

    if not sampled_pages:
        print("No page data found in the pickle.")
        return

    pages_with_content = 0
    content_lengths = []
    for page in sampled_pages:
        body = page.get('body', '')
        if body and body.strip():
            pages_with_content += 1
            content_lengths.append(len(body))
        else:
            content_lengths.append(0) # Or use np.nan if you prefer to exclude from mean differently

    print(f"Number of Pages with Content (non-empty body): {pages_with_content}")
    
    if pages_with_content > 0:
        avg_content_length = np.mean([l for l in content_lengths if l > 0]) # Average for pages with content
        print(f"Average Content Length (for pages with content): {avg_content_length:.2f} characters")
    else:
        print("Average Content Length: N/A (no pages with content)")
        
    min_content_length = np.min(content_lengths) if content_lengths else 0
    max_content_length = np.max(content_lengths) if content_lengths else 0
    median_content_length = np.median(content_lengths) if content_lengths else 0
    
    print(f"Min Content Length: {min_content_length} characters")
    print(f"Max Content Length: {max_content_length} characters")
    print(f"Median Content Length: {median_content_length:.2f} characters")

    # Example: Print titles of top 5 largest pages
    if sampled_pages:
        sorted_pages_by_size = sorted(sampled_pages, key=lambda p: len(p.get('body', '')), reverse=True)
        print("\nTop 5 largest pages (by content length):")
        for i, page in enumerate(sorted_pages_by_size[:5]):
            print(f"  {i+1}. '{page.get('title', 'N/A')}' (ID: {page.get('id', 'N/A')}) - Length: {len(page.get('body', ''))}")

def main():
    parser = argparse.ArgumentParser(description='Analyze a "full" pickle file generated for a Confluence space.')
    parser.add_argument('space_key', type=str, help='The space key for which to analyze the _full.pkl file.')
    args = parser.parse_args()

    analyze_pickle(args.space_key)

if __name__ == '__main__':
    main()
