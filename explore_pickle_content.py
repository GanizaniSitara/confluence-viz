import os
import pickle
import argparse
from datetime import datetime
import textwrap
import shutil
import platform
if platform.system() == "Windows":
    import msvcrt
import numpy as np # Added import for numpy
import sys # Added import for sys
from bs4 import BeautifulSoup # Ensured bs4 is imported
from config_loader import load_confluence_settings, load_visualization_settings
from utils.html_cleaner import clean_confluence_html

OUTPUT_DIR = 'temp'
FULL_PICKLE_SUBDIR = 'full_pickles' # New line
SNIPPET_LINES = 10 # Number of lines for snippets

def clear_console():
    """Clears the terminal screen."""
    command = 'cls' if platform.system().lower() == 'windows' else 'clear'
    os.system(command)

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

def display_page_content(page, confluence_base_url, view_mode, cleaned_text_content=None):
    """Displays the content of a single page based on the view_mode."""
    page_id = page.get('id', 'N/A')
    title = page.get('title', 'N/A')
    updated = page.get('updated', 'N/A')
    content_length = len(page.get('body', ''))
    space_key = page.get('space_key', 'N/A')
    level = page.get('level', 'N/A')
    parent_id = page.get('parent_id', 'N/A')
    update_count = page.get('update_count', 'N/A')

    print(f"\n--- Page: {title} (ID: {page_id}) ---")
    print(f"Space Key: {space_key}")
    print(f"Last Updated: {updated}")
    print(f"Content Length (raw): {content_length} characters")
    print(f"Version (Update Count): {update_count}")
    print(f"Hierarchy Level: {level}")
    print(f"Parent ID: {parent_id}")
    
    if confluence_base_url and page_id != 'N/A':
        page_url = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}"
        print(f"Link: {page_url}")

    body = page.get('body', '')

    if view_mode == 'raw_snippet':
        print("\n--- Raw HTML Content (Snippet) ---")
        if body:
            lines = body.splitlines()
            snippet = "\n".join(lines[:SNIPPET_LINES])
            print(snippet)
            if len(lines) > SNIPPET_LINES:
                print("... (more content available)")
        else:
            print("[NO RAW CONTENT]")
    elif view_mode == 'raw_full':
        print("\n--- Raw HTML Content (Full) ---")
        print(body if body else "[NO RAW CONTENT]")
    elif view_mode == 'cleaned_snippet':
        print("\n--- Cleaned Text Content (Snippet) ---")
        if cleaned_text_content:
            lines = cleaned_text_content.splitlines()
            snippet = "\n".join(lines[:SNIPPET_LINES])
            print(snippet)
            if len(lines) > SNIPPET_LINES:
                print("... (more content available)")
        else:
            print("[NO CONTENT AFTER CLEANING or NO RAW CONTENT]")
    elif view_mode == 'cleaned_full':
        print("\n--- Cleaned Text Content (Full) ---")
        print(cleaned_text_content if cleaned_text_content else "[NO CONTENT AFTER CLEANING or NO RAW CONTENT]")
    
    print("--- End of Page ---")

def page_explorer(pickle_data, confluence_base_url):
    """Allows interactive exploration of pages within the pickle."""
    sampled_pages = pickle_data.get('sampled_pages', [])
    if not sampled_pages:
        print("No pages to explore in this pickle.")
        input("Press Enter to return...")
        return

    num_pages = len(sampled_pages)
    current_page_index = 0
    view_mode = 'raw_snippet' 
    current_cleaned_text = None

    while True:
        clear_console() # Clear console at the start of each page display
        page = sampled_pages[current_page_index]
        
        if view_mode in ['cleaned_snippet', 'cleaned_full']:
            if current_cleaned_text is None: 
                raw_html_body = page.get('body', '')
                if raw_html_body: 
                    current_cleaned_text = clean_confluence_html(raw_html_body)
                else:
                    current_cleaned_text = "" 
            display_page_content(page, confluence_base_url, view_mode, cleaned_text_content=current_cleaned_text)
        else: 
            display_page_content(page, confluence_base_url, view_mode)
        
        print(f"\nPage {current_page_index + 1} of {num_pages}")
        current_view_display = view_mode.replace('_', ' ')
        prompt_text = f"Options: (n)ext, (p)revious, (j)ump, (r)aw, (c)leaned, (f)ull/snippet, (q)uit [View: {current_view_display}]: "
        print(prompt_text, end='', flush=True)

        action = ''
        if platform.system() == "Windows":
            key_pressed_bytes = msvcrt.getch()
            try:
                action = key_pressed_bytes.decode().lower()
                print(action) # Echo the character pressed
            except UnicodeDecodeError:
                action = '' # Non-unicode key (e.g. arrow keys), ignore for simple commands
                print(" (key ignored)") # Optional: inform user
        else:
            # Fallback for non-Windows: still requires Enter
            # The prompt_text is already printed, so just get input.
            action = input("").strip().lower()


        if action == 'n':
            current_page_index = min(current_page_index + 1, num_pages - 1)
            if view_mode.startswith('cleaned'):
                view_mode = 'cleaned_snippet'
            else:
                view_mode = 'raw_snippet'
            current_cleaned_text = None 
        elif action == 'p':
            current_page_index = max(current_page_index - 1, 0)
            if view_mode.startswith('cleaned'):
                view_mode = 'cleaned_snippet'
            else:
                view_mode = 'raw_snippet'
            current_cleaned_text = None 
        elif action == 'j':
            clear_console() # Clear before asking for jump input
            try:
                page_num_str = input(f"Current Page: {current_page_index + 1}/{num_pages}. Enter page number to jump to (1-{num_pages}): ")
                page_num = int(page_num_str)
                if 1 <= page_num <= num_pages:
                    current_page_index = page_num - 1
                    if view_mode.startswith('cleaned'):
                        view_mode = 'cleaned_snippet'
                    else:
                        view_mode = 'raw_snippet'
                    current_cleaned_text = None 
                else:
                    print("Invalid page number.")
                    if platform.system() == "Windows":
                        print("Press any key to continue...")
                        msvcrt.getch()
                    else:
                        input("Press Enter to continue...")
            except ValueError:
                print("Invalid input. Please enter a number.")
                if platform.system() == "Windows":
                    print("Press any key to continue...")
                    msvcrt.getch()
                else:
                    input("Press Enter to continue...")
            # Loop will continue, clear console, and redraw the new page or current page if jump failed
        elif action == 'r': 
            if view_mode == 'cleaned_snippet':
                view_mode = 'raw_snippet'
            elif view_mode == 'cleaned_full':
                view_mode = 'raw_full'
            # print(f"Displaying {view_mode.replace('_', ' ')}.") # Message will be overwritten by clear_console
        elif action == 'c': 
            if view_mode == 'raw_snippet':
                view_mode = 'cleaned_snippet'
            elif view_mode == 'raw_full':
                view_mode = 'cleaned_full'
            
            if current_cleaned_text is None and page.get('body', ''):
                current_cleaned_text = clean_confluence_html(page.get('body', ''))
            elif not page.get('body', ''):
                 current_cleaned_text = ""
            # print(f"Displaying {view_mode.replace('_', ' ')}.")
        elif action == 'f': 
            if view_mode == 'raw_snippet':
                view_mode = 'raw_full'
            elif view_mode == 'raw_full':
                view_mode = 'raw_snippet'
            elif view_mode == 'cleaned_snippet':
                view_mode = 'cleaned_full'
            elif view_mode == 'cleaned_full':
                view_mode = 'cleaned_snippet'
            # print(f"Displaying {view_mode.replace('_', ' ')}.")
        elif action == 'q':
            clear_console()
            break
        # else: # No explicit "invalid option" needed for single key, or it flashes too fast
            # if platform.system() != "Windows": print("Invalid option.") # Only show for Enter-based input

def print_content_size_bar_chart(pickle_data):
    """Prints a text-based bar chart of page content sizes in KB."""
    sampled_pages = pickle_data.get('sampled_pages', [])
    if not sampled_pages:
        print("No page data found in the pickle to generate a bar chart.")
        return

    print("\n--- Content Size per Page (KB) ---")
    
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

    print("\nNote: Bar length is proportional to content size.")

def print_content_size_list_sorted(pickle_data, smallest_first=True):
    """Prints a list of pages sorted by their content size in KB."""
    sampled_pages = pickle_data.get('sampled_pages', [])
    if not sampled_pages:
        print("No page data found in the pickle to generate a list.")
        return

    sort_order = "Smallest to Largest" if smallest_first else "Largest to Smallest"
    print(f"\n--- Content Size per Page (KB) - Sorted: {sort_order} ---")
    
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

def list_and_select_pickled_space():
    """Lists available _full.pkl files and allows user to select one by number or space key."""
    # Determine the directory to scan for pickles
    pickle_dir_to_scan = os.path.join(OUTPUT_DIR, FULL_PICKLE_SUBDIR) # Default local path
    remote_pickle_dir = None
    try:
        viz_settings = load_visualization_settings()
        remote_pickle_dir = viz_settings.get('remote_full_pickle_dir')
    except Exception as e:
        print(f"Warning: Could not load remote_full_pickle_dir from settings: {e}")

    if remote_pickle_dir and os.path.exists(remote_pickle_dir) and os.path.isdir(remote_pickle_dir):
        print(f"Using remote pickle directory from settings: {remote_pickle_dir}")
        pickle_dir_to_scan = remote_pickle_dir
    elif remote_pickle_dir:
        print(f"Warning: remote_full_pickle_dir '{remote_pickle_dir}' not found or not a directory. Falling back to local path: {pickle_dir_to_scan}")
    else:
        print(f"Using local pickle directory: {pickle_dir_to_scan}")

    if not os.path.exists(pickle_dir_to_scan):
        print(f"Directory not found: {pickle_dir_to_scan}")
        print("Please ensure spaces have been pickled using the '--pickle-space-full' or '--pickle-all-spaces-full' options")
        print("in the sample_and_pickle_spaces.py script, or check your remote_full_pickle_dir setting.")
        return None

    pickle_files = [f for f in os.listdir(pickle_dir_to_scan) if f.endswith("_full.pkl")]

    if not pickle_files:
        print(f"No '*_full.pkl' files found in {pickle_dir_to_scan}.")
        return None

    print(f"\n--- Available Pickled Spaces in {pickle_dir_to_scan} ---")
    spaces = []
    for i, filename in enumerate(pickle_files):
        space_key = filename.replace("_full.pkl", "")
        spaces.append({'key': space_key, 'filename': filename, 'number': i + 1})
    
    # Sort spaces by key for consistent ordering
    spaces.sort(key=lambda s: s['key'])
    # Re-assign numbers after sorting if necessary, though selection logic uses key or original index if not re-numbered
    # For display, we'll use the sorted order and their new index + 1 for numbering in the list.

    terminal_width = shutil.get_terminal_size().columns
    item_width = 35  # Approximate width for "123. SPACEXYZ      "
    num_columns = max(1, terminal_width // item_width)

    for i in range(0, len(spaces), num_columns):
        line_items = []
        for j in range(num_columns):
            if i + j < len(spaces):
                space = spaces[i+j]
                # Use original number for selection, but display based on sorted order index for clarity
                display_number = i + j + 1 
                item_text = f"{display_number}. {space['key']}"
                line_items.append(f"{item_text:<{item_width-1}}") # -1 for a space between columns
            else:
                line_items.append(" " * (item_width-1))
        print(" ".join(line_items))
    
    print("q. Quit to main menu")

    while True:
        choice_str = input("Select a space by number (from the list above) or space key (or 'q' to quit to main menu): ").strip()
        if choice_str.lower() == 'q':
            return None
        
        selected_space = next((s for s in spaces if s['key'].lower() == choice_str.lower()), None)
        
        if selected_space:
            return os.path.join(pickle_dir_to_scan, selected_space['filename'])

        try:
            # Adjust choice_idx to match the displayed number which is 1-based index of the sorted list
            choice_idx = int(choice_str) - 1 
            if 0 <= choice_idx < len(spaces):
                selected_space = spaces[choice_idx] # Use the index from the sorted list
                return os.path.join(pickle_dir_to_scan, selected_space['filename'])
            else:
                print("Invalid number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number (from the list), a valid space key, or 'q'.")

def run_explorer_for_space(pickle_data, confluence_base_url):
    """Runs the main explorer menu loop for the loaded pickle data."""
    while True:
        print("\n--- Pickle Explorer Menu ---")
        space_key_display = pickle_data.get('space_key', 'N/A')
        space_name_display = pickle_data.get('name', 'N/A')
        # Handle cases where name might be None or empty, more gracefully
        if not space_name_display or str(space_name_display).strip() == "":
            space_name_display = "(Name not available)"
        
        print(f"Space: {space_key_display} - {space_name_display}")
        print("1. Display Page Statistics")
        print("2. Display Content Size Bar Chart (KB)")
        print("3. List Pages by Size (Smallest to Largest)")
        print("4. List Pages by Size (Largest to Smallest)")
        print("5. Explore Pages (Paginator)")
        print("q. Quit to main menu / Select another space") # Changed from 'b.'
        choice = input("Enter your choice: ").strip().lower()

        if choice == '1':
            analyze_pickle(pickle_data, confluence_base_url)
        elif choice == '2':
            print_content_size_bar_chart(pickle_data)
        elif choice == '3':
            print_content_size_list_sorted(pickle_data, smallest_first=True)
        elif choice == '4':
            print_content_size_list_sorted(pickle_data, smallest_first=False)
        elif choice == '5':
            page_explorer(pickle_data, confluence_base_url)
        elif choice == 'q': # Changed from 'b'
            print("Returning to main menu...")
            break
        else:
            print("Invalid choice. Please try again.")

def main():
    """Main function to handle argument parsing and initiate the space exploration."""
    parser = argparse.ArgumentParser(description="Explore content of pickled Confluence spaces.")
    parser.add_argument("space_key", nargs='?', help="Optional space key to load directly.")
    args = parser.parse_args()

    settings = load_visualization_settings()
    # Ensure pickle_dir is correctly determined (local 'temp' or from settings)
    # The list_and_select_pickled_space function handles remote_full_pickle_dir,
    # but direct loading via CLI arg needs a consistent base path.
    # For simplicity, direct CLI load will use the local 'temp' or settings-defined 'pickle_dir'.
    # If remote_full_pickle_dir is the primary source, this might need adjustment
    # or ensure CLI-passed keys are expected in the local/default pickle_dir.
    
    # Default to 'temp' if not in settings, then join with FULL_PICKLE_SUBDIR for CLI loading.
    local_base_pickle_dir = settings.get('pickle_dir', 'temp')
    # This is the directory where _full.pkl files are expected for direct CLI loading.
    # list_and_select_pickled_space has its own logic to check remote_full_pickle_dir.
    cli_load_pickle_dir = os.path.join(local_base_pickle_dir, FULL_PICKLE_SUBDIR)


    confluence_settings = load_confluence_settings()
    confluence_base_url = confluence_settings.get('base_url')

    if not confluence_base_url:
        print("Error: Confluence base URL not found in settings.ini.")
        sys.exit(1)

    if args.space_key:
        space_key_upper = args.space_key.upper()
        # Try loading from the path derived for CLI arguments first
        pickle_file_path_cli = os.path.join(cli_load_pickle_dir, f"{space_key_upper}_full.pkl")
        
        # Fallback: Check remote_full_pickle_dir if specified and file not in cli_load_pickle_dir
        remote_full_pickle_dir = settings.get('remote_full_pickle_dir')
        pickle_file_path_remote = None
        if remote_full_pickle_dir and os.path.isdir(remote_full_pickle_dir):
            pickle_file_path_remote = os.path.join(remote_full_pickle_dir, f"{space_key_upper}_full.pkl")

        actual_pickle_file_path = None
        if os.path.exists(pickle_file_path_cli):
            actual_pickle_file_path = pickle_file_path_cli
            print(f"Found pickle for '{space_key_upper}' in local/default directory: {cli_load_pickle_dir}")
        elif pickle_file_path_remote and os.path.exists(pickle_file_path_remote):
            actual_pickle_file_path = pickle_file_path_remote
            print(f"Found pickle for '{space_key_upper}' in remote directory: {remote_full_pickle_dir}")
        
        if actual_pickle_file_path:
            try:
                with open(actual_pickle_file_path, 'rb') as f:
                    pickle_data = pickle.load(f)
                space_display_name = pickle_data.get('name', pickle_data.get('space_key', 'Unknown Space'))
                space_key_display = pickle_data.get('space_key', 'N/A')
                print(f"Loaded data for space: {space_display_name} ({space_key_display})")
                run_explorer_for_space(pickle_data, confluence_base_url) # <--- MODIFIED HERE
            except Exception as e:
                print(f"Error loading pickle file {actual_pickle_file_path}: {e}")
                sys.exit(1)
        else:
            print(f"Pickle file for space key '{space_key_upper}' not found in specified local ('{cli_load_pickle_dir}') or remote ('{remote_full_pickle_dir or 'Not configured'}') directories.")
            # Fall through to listing available spaces
            selected_pickle_file = list_and_select_pickled_space() # This handles remote dir logic internally
            if selected_pickle_file:
                try:
                    with open(selected_pickle_file, 'rb') as f:
                        pickle_data = pickle.load(f)
                    space_display_name = pickle_data.get('name', pickle_data.get('space_key', 'Unknown Space'))
                    space_key_display = pickle_data.get('space_key', 'N/A')
                    print(f"Loaded data for space: {space_display_name} ({space_key_display})")
                    run_explorer_for_space(pickle_data, confluence_base_url) # <--- MODIFIED HERE
                except Exception as e:
                    print(f"Error loading selected pickle file {selected_pickle_file}: {e}")
                    sys.exit(1)
            else:
                print("No space selected. Exiting.")
                sys.exit(0)
    else:
        selected_pickle_file = list_and_select_pickled_space() # This handles remote dir logic internally
        if selected_pickle_file:
            try:
                with open(selected_pickle_file, 'rb') as f:
                    pickle_data = pickle.load(f)
                space_display_name = pickle_data.get('name', pickle_data.get('space_key', 'Unknown Space'))
                space_key_display = pickle_data.get('space_key', 'N/A')
                print(f"Loaded data for space: {space_display_name} ({space_key_display})")
                run_explorer_for_space(pickle_data, confluence_base_url) # <--- MODIFIED HERE
            except Exception as e:
                print(f"Error loading selected pickle file {selected_pickle_file}: {e}")
                sys.exit(1)
        else:
            print("No space selected. Exiting.")
            sys.exit(0)

if __name__ == "__main__":
    main()
