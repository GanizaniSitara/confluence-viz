#!/usr/bin/env python3
"""
HTML Content Extractor for Confluence Pickle Files

This script extracts just the HTML content from pickled Confluence space data.
It provides multiple output formats to help you understand and work with the HTML content
without needing to dump the entire pickle structure as JSON.

Usage:
    python extract_html_content.py SPACE_KEY [options]
    python extract_html_content.py --list-spaces
    
Examples:
    python extract_html_content.py DAENSTAT --format raw
    python extract_html_content.py DAENSTAT --format cleaned --output html_output/
    python extract_html_content.py DAENSTAT --page-id 971904 --format raw
"""

import pickle
import os
import argparse
import json
import sys
from pathlib import Path

# Import the HTML cleaner
try:
    from utils.html_cleaner import clean_confluence_html
    HTML_CLEANER_AVAILABLE = True
except ImportError:
    try:
        # Try importing from current directory
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from utils.html_cleaner import clean_confluence_html
        HTML_CLEANER_AVAILABLE = True
    except ImportError:
        # Simple fallback HTML cleaner without BeautifulSoup
        def clean_confluence_html(html_content):
            """Simple HTML cleaner that strips basic tags and extracts text."""
            import re
            if not html_content:
                return ""
            
            # First, extract text from rich-text-body sections before removing macros
            rich_text_pattern = r'<ac:rich-text-body[^>]*>(.*?)</ac:rich-text-body>'
            rich_text_matches = re.findall(rich_text_pattern, html_content, flags=re.DOTALL)
            extracted_text = '\n'.join(rich_text_matches)
            
            # Extract text from task bodies
            task_body_pattern = r'<ac:task-body[^>]*>(.*?)</ac:task-body>'
            task_matches = re.findall(task_body_pattern, html_content, flags=re.DOTALL)
            task_text = '\n'.join([f"- {task}" for task in task_matches])
            
            # Extract text from parameters (like titles)
            param_pattern = r'<ac:parameter[^>]*ac:name="title"[^>]*>(.*?)</ac:parameter>'
            param_matches = re.findall(param_pattern, html_content, flags=re.DOTALL)
            param_text = '\n'.join([f"Title: {param}" for param in param_matches])
            
            # Combine extracted text
            combined_text = '\n\n'.join(filter(None, [param_text, extracted_text, task_text]))
            
            # If we got meaningful content from extraction, use it
            if combined_text.strip():
                html_content = combined_text
            
            # Convert basic HTML tags
            html_content = re.sub(r'<h([1-6])[^>]*>(.*?)</h[1-6]>', r'\n\n# \2\n\n', html_content)
            html_content = re.sub(r'<p[^>]*>(.*?)</p>', r'\1\n\n', html_content, flags=re.DOTALL)
            html_content = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', html_content)
            html_content = re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', html_content)
            html_content = re.sub(r'<br[^>]*/?>', r'\n', html_content)
            
            # Remove remaining HTML/XML tags
            html_content = re.sub(r'<[^>]+>', '', html_content)
            
            # Clean up whitespace
            html_content = re.sub(r'\n\s*\n\s*\n+', '\n\n', html_content)
            html_content = re.sub(r'[ \t]+', ' ', html_content)
            lines = [line.strip() for line in html_content.split('\n')]
            lines = [line for line in lines if line]  # Remove empty lines
            return '\n'.join(lines)
        
        HTML_CLEANER_AVAILABLE = True  # We have a fallback cleaner
        print("Note: Using fallback HTML cleaner (BeautifulSoup not available).")

def find_pickle_files():
    """Find all available pickle files with space data."""
    pickle_files = {}
    
    # Check main directory
    for file in os.listdir('.'):
        if file.endswith('.pkl'):
            try:
                with open(file, 'rb') as f:
                    data = pickle.load(f)
                if isinstance(data, dict) and 'sampled_pages' in data:
                    space_key = data.get('space_key', file.replace('.pkl', ''))
                    pickle_files[space_key] = file
            except:
                continue
    
    # Check temp directory
    temp_dir = 'temp'
    if os.path.exists(temp_dir):
        for file in os.listdir(temp_dir):
            if file.endswith('.pkl'):
                filepath = os.path.join(temp_dir, file)
                try:
                    with open(filepath, 'rb') as f:
                        data = pickle.load(f)
                    if isinstance(data, dict) and 'sampled_pages' in data:
                        space_key = data.get('space_key', file.replace('.pkl', ''))
                        pickle_files[space_key] = filepath
                except:
                    continue
    
    # Check full pickle directories
    full_pickle_dirs = ['temp/full_pickles']
    
    # Try to load from settings if available
    try:
        from config_loader import load_visualization_settings
        settings = load_visualization_settings()
        remote_dir = settings.get('remote_full_pickle_dir')
        if remote_dir and os.path.exists(remote_dir):
            full_pickle_dirs.append(remote_dir)
    except:
        pass
    
    for pickle_dir in full_pickle_dirs:
        if os.path.exists(pickle_dir):
            for file in os.listdir(pickle_dir):
                if file.endswith('_full.pkl'):
                    filepath = os.path.join(pickle_dir, file)
                    try:
                        with open(filepath, 'rb') as f:
                            data = pickle.load(f)
                        if isinstance(data, dict) and 'sampled_pages' in data:
                            space_key = data.get('space_key', file.replace('_full.pkl', ''))
                            pickle_files[space_key] = filepath
                    except:
                        continue
    
    return pickle_files

def load_space_data(space_key):
    """Load pickle data for a specific space."""
    pickle_files = find_pickle_files()
    
    if space_key.upper() not in pickle_files:
        available_spaces = list(pickle_files.keys())
        print(f"Error: Space '{space_key}' not found.")
        print(f"Available spaces: {', '.join(available_spaces)}")
        return None
    
    filepath = pickle_files[space_key.upper()]
    
    try:
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        print(f"Loaded space data from: {filepath}")
        return data
    except Exception as e:
        print(f"Error loading pickle file {filepath}: {e}")
        return None

def extract_page_html(page, format_type='raw'):
    """Extract HTML content from a single page."""
    page_id = page.get('id', 'Unknown')
    title = page.get('title', 'Untitled')
    body = page.get('body', '')
    
    result = {
        'page_id': page_id,
        'title': title,
        'space_key': page.get('space_key', 'Unknown'),
        'updated': page.get('updated', 'Unknown'),
        'level': page.get('level', 0),
        'parent_id': page.get('parent_id'),
        'update_count': page.get('update_count', 0)
    }
    
    if format_type == 'raw':
        result['html_content'] = body
    elif format_type == 'cleaned' and HTML_CLEANER_AVAILABLE:
        result['text_content'] = clean_confluence_html(body)
        result['html_content'] = body  # Include both for reference
    elif format_type == 'cleaned':
        if not HTML_CLEANER_AVAILABLE:
            print("Warning: HTML cleaner not available. Using raw format.")
        result['html_content'] = body
    else:
        result['html_content'] = body
    
    return result

def extract_all_html(space_data, format_type='raw', page_id=None):
    """Extract HTML content from all pages or a specific page."""
    pages = space_data.get('sampled_pages', [])
    
    if page_id:
        # Find specific page
        target_page = None
        for page in pages:
            if str(page.get('id')) == str(page_id):
                target_page = page
                break
        
        if not target_page:
            print(f"Error: Page ID {page_id} not found in space.")
            available_ids = [str(p.get('id', 'Unknown')) for p in pages]
            print(f"Available page IDs: {', '.join(available_ids)}")
            return None
        
        return extract_page_html(target_page, format_type)
    else:
        # Extract all pages
        results = []
        for page in pages:
            results.append(extract_page_html(page, format_type))
        return results

def save_to_files(extracted_data, output_dir, space_key, format_type):
    """Save extracted data to individual files."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    if isinstance(extracted_data, dict):
        # Single page
        extracted_data = [extracted_data]
    
    for page_data in extracted_data:
        page_id = page_data['page_id']
        title = page_data['title']
        
        # Sanitize filename
        safe_title = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in title)
        safe_title = safe_title.replace(' ', '_')[:50]  # Limit length
        
        if format_type == 'raw':
            filename = f"{page_id}_{safe_title}_raw.html"
            content = page_data['html_content']
        elif format_type == 'cleaned':
            filename = f"{page_id}_{safe_title}_cleaned.txt"
            content = f"Title: {title}\nPage ID: {page_id}\nSpace: {space_key}\n\n"
            content += page_data.get('text_content', page_data.get('html_content', ''))
        else:
            filename = f"{page_id}_{safe_title}.html"
            content = page_data['html_content']
        
        filepath = output_path / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Saved: {filepath}")
        except Exception as e:
            print(f"Error saving {filepath}: {e}")

def print_extracted_data(extracted_data, format_type):
    """Print extracted data to console."""
    if isinstance(extracted_data, dict):
        # Single page
        extracted_data = [extracted_data]
    
    for i, page_data in enumerate(extracted_data):
        if i > 0:
            print("\n" + "="*80 + "\n")
        
        print(f"Page ID: {page_data['page_id']}")
        print(f"Title: {page_data['title']}")
        print(f"Space: {page_data['space_key']}")
        print(f"Updated: {page_data['updated']}")
        print(f"Level: {page_data['level']}")
        print(f"Parent ID: {page_data['parent_id']}")
        print(f"Update Count: {page_data['update_count']}")
        print("-" * 40)
        
        if format_type == 'cleaned' and 'text_content' in page_data:
            print("CLEANED TEXT CONTENT:")
            print(page_data['text_content'])
        else:
            print("RAW HTML CONTENT:")
            html_content = page_data['html_content']
            if len(html_content) > 1000:
                print(f"{html_content[:1000]}\n... (truncated, {len(html_content)} total characters)")
            else:
                print(html_content)

def list_available_spaces():
    """List all available spaces."""
    pickle_files = find_pickle_files()
    
    if not pickle_files:
        print("No pickle files with space data found.")
        return
    
    print("Available Confluence spaces:")
    print("-" * 50)
    
    for space_key, filepath in sorted(pickle_files.items()):
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            
            space_name = data.get('name', 'Unknown')
            page_count = len(data.get('sampled_pages', []))
            total_pages = data.get('total_pages_in_space', 'Unknown')
            
            print(f"Space Key: {space_key}")
            print(f"  Name: {space_name}")
            print(f"  Sampled Pages: {page_count}")
            print(f"  Total Pages in Space: {total_pages}")
            print(f"  File: {filepath}")
            print()
            
        except Exception as e:
            print(f"Space Key: {space_key}")
            print(f"  Error reading file: {e}")
            print(f"  File: {filepath}")
            print()

def main():
    parser = argparse.ArgumentParser(
        description="Extract HTML content from Confluence pickle files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s DAENSTAT --format raw
  %(prog)s DAENSTAT --format cleaned --output html_output/
  %(prog)s DAENSTAT --page-id 971904 --format raw
  %(prog)s --list-spaces
        """
    )
    
    parser.add_argument('space_key', nargs='?', help='Space key to extract content from')
    parser.add_argument('--format', choices=['raw', 'cleaned'], default='raw',
                       help='Output format: raw HTML or cleaned text (default: raw)')
    parser.add_argument('--page-id', help='Extract content from specific page ID only')
    parser.add_argument('--output', help='Output directory to save files (if not specified, prints to console)')
    parser.add_argument('--list-spaces', action='store_true', 
                       help='List all available spaces and exit')
    parser.add_argument('--json', action='store_true',
                       help='Output as JSON format')
    
    args = parser.parse_args()
    
    if args.list_spaces:
        list_available_spaces()
        return
    
    if not args.space_key:
        parser.print_help()
        print("\nUse --list-spaces to see available spaces.")
        return
    
    # Load space data
    space_data = load_space_data(args.space_key)
    if not space_data:
        return
    
    # Extract content
    extracted_data = extract_all_html(space_data, args.format, args.page_id)
    if not extracted_data:
        return
    
    # Output results
    if args.output:
        save_to_files(extracted_data, args.output, args.space_key.upper(), args.format)
    elif args.json:
        print(json.dumps(extracted_data, indent=2, ensure_ascii=False))
    else:
        print_extracted_data(extracted_data, args.format)

if __name__ == '__main__':
    main()