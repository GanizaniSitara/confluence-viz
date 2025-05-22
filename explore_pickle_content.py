import argparse
import pickle
import os
import sys
import numpy as np # For calculating mean, ignoring NaNs
from config_loader import load_visualization_settings # Added import
from datetime import datetime # Added for parsing dates if needed, though string comparison works for ISO

OUTPUT_DIR = 'temp'
FULL_PICKLE_SUBDIR = 'full_pickles' # New line

def link(uri, label=None):
    if label is None: 
        label = uri
    parameters = ''
    # OSC 8 ; params ; URI ST <name> OSC 8 ;; ST 
    escape_mask = '\\033]8;{};{}\\033\\\\{}\033]8;;\\033\\\\'
    return escape_mask.format(parameters, uri, label)


def analyze_pickle(pickle_data, confluence_base_url): # Modified arguments
    print(f"--- Statistics for Space: {pickle_data.get('space_key', 'N/A')} ---")
    print(f"Space Name: {pickle_data.get('name', 'N/A')}")
    
    total_pages_in_space_metadata = pickle_data.get('total_pages_in_space', 0)
    print(f"Total Pages in Space (according to metadata in pickle): {total_pages_in_space_metadata}")

    sampled_pages = pickle_data.get('sampled_pages', [])
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

    # Confluence base URL is now passed as an argument
    # No need to load settings here if confluence_base_url is provided and valid

    if sampled_pages:
        # Filter pages that have an 'updated' field.
        pages_with_date = [p for p in sampled_pages if p.get('updated')]

        if pages_with_date:
            # Sort by 'updated' field. ISO 8601 format strings can be compared directly.
            sorted_by_update_desc = sorted(pages_with_date, key=lambda p: p['updated'], reverse=True)
            sorted_by_update_asc = sorted(pages_with_date, key=lambda p: p['updated'])

            print("\nTop 5 most recently updated pages:")
            for i, page in enumerate(sorted_by_update_desc[:5]):
                page_id = page.get('id')
                page_title = page.get('title', 'N/A')
                page_updated = page.get('updated')
                link_text = ""
                if confluence_base_url and page_id:
                    page_url = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}"
                    link_text = f" - Link: {page_url}" # Reverted to plain text URL
                print(f"  {i+1}. '{page_title}' (Updated: {page_updated}){link_text}")

            print("\nTop 5 least recently updated pages:")
            for i, page in enumerate(sorted_by_update_asc[:5]):
                page_id = page.get('id')
                page_title = page.get('title', 'N/A')
                page_updated = page.get('updated')
                link_text = ""
                if confluence_base_url and page_id:
                    page_url = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}"
                    link_text = f" - Link: {page_url}" # Reverted to plain text URL
                print(f"  {i+1}. '{page_title}' (Updated: {page_updated}){link_text}")
        else:
            print("\nNo pages with update information found to sort by date.")

    # Example: Print titles of top 5 largest pages
    if sampled_pages:
        sorted_pages_by_size = sorted(sampled_pages, key=lambda p: len(p.get('body', '')), reverse=True)
        print("\nTop 5 largest pages (by content length):")
        for i, page in enumerate(sorted_pages_by_size[:5]):
            page_id = page.get('id')
            page_title = page.get('title', 'N/A')
            link_text = ""
            if confluence_base_url and page_id:
                page_url = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}"
                link_text = f" - Link: {page_url}" # Reverted to plain text URL
            print(f"  {i+1}. '{page_title}' (ID: {page.get('id', 'N/A')}) - Length: {len(page.get('body', ''))}{link_text}")

    # Print titles of top 5 pages with most collaborators (by update_count)
    if sampled_pages:
        pages_with_update_count = [p for p in sampled_pages if p.get('update_count') is not None]
        if pages_with_update_count:
            sorted_by_collaborators = sorted(pages_with_update_count, key=lambda p: p['update_count'], reverse=True)
            print("\nTop 5 pages with most collaborators (by update count/versions):")
            for i, page in enumerate(sorted_by_collaborators[:5]):
                page_id = page.get('id')
                page_title = page.get('title', 'N/A')
                update_count = page.get('update_count')
                link_text = ""
                if confluence_base_url and page_id:
                    page_url = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}"
                    link_text = f" - Link: {page_url}" # Reverted to plain text URL
                print(f"  {i+1}. '{page_title}' (Update Count: {update_count}){link_text}")
        else:
            print("\nNo pages with update count information found.")


def print_content_size_bar_chart(pickle_data):
    """Prints a text-based bar chart of page content sizes in KB."""
    sampled_pages = pickle_data.get('sampled_pages', [])
    if not sampled_pages:
        print("No page data found in the pickle to generate a bar chart.")
        return

    print("\\n--- Content Size per Page (KB) ---")
    
    page_sizes_kb = []
    for page in sampled_pages:
        title = page.get('title', 'N/A')
        content_length_bytes = len(page.get('body', ''))
        content_length_kb = content_length_bytes / 1024.0
        page_sizes_kb.append({'title': title, 'size_kb': content_length_kb})

    if not page_sizes_kb:
        print("No content found in pages.")
        return

    # Determine max size for scaling the bar chart (optional, for better visualization)
    # For simplicity, let's set a max bar width and scale accordingly.
    max_bar_width = 50 # characters
    max_kb_for_scaling = max(p['size_kb'] for p in page_sizes_kb) if page_sizes_kb else 1
    if max_kb_for_scaling == 0: # Avoid division by zero if all pages are empty
        max_kb_for_scaling = 1

    for page_info in sorted(page_sizes_kb, key=lambda x: x['size_kb'], reverse=True):
        title = page_info['title']
        size_kb = page_info['size_kb']
        
        bar_length = int((size_kb / max_kb_for_scaling) * max_bar_width)
        bar = '#' * bar_length
        
        # Truncate long titles
        max_title_len = 40
        display_title = (title[:max_title_len-3] + '...') if len(title) > max_title_len else title
        
        print(f"{display_title:<{max_title_len}} | {bar} ({size_kb:.2f} KB)")

    print("\\nNote: Bar length is proportional to content size.")


def print_content_size_list_sorted(pickle_data, smallest_first=True):
    """Prints a list of pages sorted by their content size in KB."""
    sampled_pages = pickle_data.get('sampled_pages', [])
    if not sampled_pages:
        print("No page data found in the pickle to generate a list.")
        return

    sort_order = "Smallest to Largest" if smallest_first else "Largest to Smallest"
    print(f"\\n--- Content Size per Page (KB) - Sorted: {sort_order} ---")
    
    page_sizes_kb = []
    for page in sampled_pages:
        title = page.get('title', 'N/A')
        content_length_bytes = len(page.get('body', ''))
        content_length_kb = content_length_bytes / 1024.0
        page_id = page.get('id', 'N/A')
        page_sizes_kb.append({'title': title, 'size_kb': content_length_kb, 'id': page_id})

    if not page_sizes_kb:
        print("No content found in pages.")
        return

    # Sort pages by size
    sorted_pages = sorted(page_sizes_kb, key=lambda x: x['size_kb'], reverse=not smallest_first)

    for page_info in sorted_pages:
        title = page_info['title']
        size_kb = page_info['size_kb']
        page_id = page_info['id']
        
        # Truncate long titles
        max_title_len = 70 # Adjusted for potentially longer lines
        display_title = (title[:max_title_len-3] + '...') if len(title) > max_title_len else title
        
        print(f"{display_title:<{max_title_len}} (ID: {page_id}) - {size_kb:.2f} KB")


def main():
    parser = argparse.ArgumentParser(description='Analyze a "full" pickle file generated for a Confluence space.')
    parser.add_argument('space_key', type=str, help='The space key for which to analyze the _full.pkl file.')
    args = parser.parse_args()

    pickle_filename = f'{args.space_key}_full.pkl'
    pickle_path = os.path.join(OUTPUT_DIR, FULL_PICKLE_SUBDIR, pickle_filename)

    if not os.path.exists(pickle_path):
        print(f"Error: Pickle file not found at {pickle_path}")
        print(f"Please ensure you have run 'python sample_and_pickle_spaces.py --pickle-space-full {args.space_key}' first.")
        sys.exit(1)

    try:
        with open(pickle_path, 'rb') as f:
            pickle_data = pickle.load(f)
    except Exception as e:
        print(f"Error loading pickle file {pickle_path}: {e}")
        sys.exit(1)

    # Load Confluence base URL for links
    confluence_base_url = None
    try:
        viz_settings = load_visualization_settings()
        confluence_base_url = viz_settings.get('confluence_base_url')
        if not confluence_base_url:
            print("\\nWarning: Confluence base URL not found in settings. Links in statistics might not be generated.")
    except FileNotFoundError:
        print("\\nWarning: settings.ini not found. Links in statistics might not be generated.")
    except Exception as e:
        print(f"\\nWarning: Error loading visualization settings: {e}. Links in statistics might not be generated.")

    while True:
        print("\\n--- Pickle Explorer Menu ---")
        print(f"Space: {pickle_data.get('space_key', 'N/A')} - {pickle_data.get('name', 'N/A')}")
        print("1. Display Page Statistics")
        print("2. Display Content Size Bar Chart (KB)")
        print("3. List Pages by Size (Smallest to Largest)")
        print("4. List Pages by Size (Largest to Smallest)")
        print("q. Quit")
        choice = input("Enter your choice: ").strip().lower()

        if choice == '1':
            analyze_pickle(pickle_data, confluence_base_url)
        elif choice == '2':
            print_content_size_bar_chart(pickle_data)
        elif choice == '3':
            print_content_size_list_sorted(pickle_data, smallest_first=True)
        elif choice == '4':
            print_content_size_list_sorted(pickle_data, smallest_first=False)
        elif choice == 'q':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()
