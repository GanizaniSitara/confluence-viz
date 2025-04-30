# filepath: c:\Solutions\PythonProject\confluence_visualization\explore_clusters.py
import os
import pickle
import sys
import numpy as np
import re
from sklearn.cluster import AgglomerativeClustering, KMeans, DBSCAN
import webbrowser
from collections import defaultdict
import matplotlib.pyplot as plt
from collections import Counter
import json
from datetime import datetime
import shutil
from config_loader import load_visualization_settings

# Try to import Whoosh (will be used for options 14 and 15)
try:
    import whoosh
    from whoosh.fields import Schema, TEXT, ID, KEYWORD
    from whoosh.analysis import StemmingAnalyzer
    from whoosh.index import create_in, open_dir, exists_in
    from whoosh.qparser import QueryParser, MultifieldParser
    WHOOSH_AVAILABLE = True
except ImportError:
    WHOOSH_AVAILABLE = False
    print("Whoosh library not found. Options 14 and 15 will not be available.")
    print("Install Whoosh with: pip install whoosh")

TEMP_DIR = 'temp'
WHOOSH_INDEX_DIR = 'whoosh_index'  # Directory to store Whoosh index
DEFAULT_MIN_PAGES = 0
VERSION = '1.4'  # Updated version

# Load stopwords from file
def load_stopwords():
    try:
        stopwords_path = os.path.join(os.path.dirname(__file__), 'stopwords.txt')
        with open(stopwords_path, 'r') as f:
            return set(line.strip().lower() for line in f if line.strip())
    except Exception as e:
        print(f"Warning: Could not load stopwords file: {e}")
        # Default stopwords if file can't be loaded
        return {'and', 'the', 'in', 'of', 'to', 'a', 'for', 'with', 'on', 'at', 
                'release', 'architecture', 'team', 'test', 'data', 'vcs', 'api', 
                'enterprise', 'new', 'status', 'migration', 'design', '2025', 
                'details', 'vision', 'sprint', 'requirements', 'management', 'home'}

# Initialize stopwords
STOPWORDS = load_stopwords()

# Color constants
GRADIENT_STEPS = 10  # Number of color steps
GREY_COLOR_HEX = '#cccccc'  # Color for spaces with no pages/timestamps
# Gradient colors: Red (oldest) -> Yellow (middle) -> Green (newest)
GRADIENT_COLORS_FOR_INTERP = ['#ffcccc', '#ffffcc', '#ccffcc']

# Color utility functions
def hex_to_rgb(hex_color):
    """Converts a hex color string (e.g., '#RRGGBB') to an RGB tuple."""
    hex_color = hex_color.lstrip('#')
    if len(hex_color) != 6:
        return (0, 0, 0)
    try:
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    except ValueError:
        return (0, 0, 0)

def rgb_to_hex(rgb_tuple):
    """Convert an RGB tuple to hex color string"""
    return f'#{int(rgb_tuple[0]):02x}{int(rgb_tuple[1]):02x}{int(rgb_tuple[2]):02x}'

def get_interpolated_color_from_fraction(fraction, gradient_colors_rgb_basis):
    """Interpolate between color basis points based on a fraction between 0-1"""
    if fraction <= 0:
        return gradient_colors_rgb_basis[0]
    if fraction >= 1:
        return gradient_colors_rgb_basis[-1]
    
    # Find the segment that contains the fraction
    segment_count = len(gradient_colors_rgb_basis) - 1
    segment_size = 1.0 / segment_count
    segment_index = min(int(fraction / segment_size), segment_count - 1)
    
    # Calculate the position within the segment
    segment_start = segment_index * segment_size
    segment_fraction = (fraction - segment_start) / segment_size
    
    # Interpolate between the two segment endpoints
    start_color = gradient_colors_rgb_basis[segment_index]
    end_color = gradient_colors_rgb_basis[segment_index + 1]
    
    return [
        start_color[0] + segment_fraction * (end_color[0] - start_color[0]),
        start_color[1] + segment_fraction * (end_color[1] - start_color[1]),
        start_color[2] + segment_fraction * (end_color[2] - start_color[2])
    ]

def calculate_color_data(spaces):
    """Calculate percentile thresholds and color range from space average timestamps"""
    # Extract all non-zero average timestamps
    avg_values = [s.get('avg', 0) for s in spaces if s.get('avg', 0) > 0]
    
    # Calculate percentile thresholds
    if avg_values and len(avg_values) > 1:
        percentile_thresholds = [
            np.percentile(avg_values, 100 * i / GRADIENT_STEPS)
            for i in range(1, GRADIENT_STEPS)
        ]
    else:
        percentile_thresholds = []
    
    # Generate color gradient
    gradient_colors_rgb_basis = [hex_to_rgb(c) for c in GRADIENT_COLORS_FOR_INTERP]
    color_range_hex = []
    
    for i in range(GRADIENT_STEPS):
        f = i / (GRADIENT_STEPS - 1) if GRADIENT_STEPS > 1 else 0.0
        rgb = get_interpolated_color_from_fraction(f, gradient_colors_rgb_basis)
        hex_color = rgb_to_hex(rgb)
        color_range_hex.append(hex_color)
    
    return percentile_thresholds, color_range_hex

# Load all pickles
def load_spaces(temp_dir=TEMP_DIR, min_pages=0, max_pages=None):
    spaces = []
    for fname in os.listdir(temp_dir):
        if fname.endswith('.pkl'):
            with open(os.path.join(temp_dir, fname), 'rb') as f:
                data = pickle.load(f)
                if 'space_key' in data and 'sampled_pages' in data:
                    # Use total_pages for filtering if available, otherwise fallback to sampled_pages length
                    page_count = data.get('total_pages', len(data['sampled_pages']))
                    # Apply both min and max filters
                    meets_min = page_count >= min_pages
                    meets_max = max_pages is None or page_count <= max_pages
                    if meets_min and meets_max:
                        spaces.append(data)
    return spaces

def filter_spaces(spaces, min_pages, max_pages=None):
    return [s for s in spaces if (
        s.get('total_pages', len(s['sampled_pages'])) >= min_pages and
        (max_pages is None or s.get('total_pages', len(s['sampled_pages'])) <= max_pages)
    )]

def filter_spaces_by_date(spaces, date_filter):
    """
    Filter spaces based on average date.
    date_filter should be a string in format '>YYYY-MM-DD' or '<YYYY-MM-DD'.
    Returns filtered spaces list.
    """
    # Check if spaces have avg timestamps, if not, calculate them
    if any('avg' not in s for s in spaces):
        spaces = calculate_avg_timestamps(spaces)
    
    filtered_spaces = []
    
    # Parse filter
    if not date_filter:
        return spaces  # No filter, return all spaces
    
    try:
        # Extract the operator and date string
        operator = date_filter[0]  # '>' or '<'
        date_str = date_filter[1:].strip()
        
        # Parse the target date string
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        target_timestamp = target_date.timestamp()
          # Apply filter
        for space in spaces:
            avg_ts = space.get('avg', 0)
            if avg_ts > 0:  # Only include spaces with valid timestamps
                if operator == '>' and avg_ts > target_timestamp:
                    filtered_spaces.append(space)
                elif operator == '<' and avg_ts < target_timestamp:
                    filtered_spaces.append(space)
        
        # Removed the print statement here as we now print this in ensure_data_loaded
        
    except (ValueError, IndexError) as e:
        print(f"Error parsing date filter: {e}")
        print("Format should be >YYYY-MM-DD or <YYYY-MM-DD")
        return spaces  # Return original spaces on error
    
    return filtered_spaces

def search_spaces(spaces, term):
    results = []
    for s in spaces:
        if term.lower() in s['space_key'].lower():
            results.append((s['space_key'], len(s['sampled_pages'])))
    return results

def get_vectors(spaces):
    # Semantic vectorization: concatenate all sampled page bodies for each space
    from sklearn.feature_extraction.text import TfidfVectorizer
    
    # Try to import BeautifulSoup, if not available fall back to regex
    try:
        from bs4 import BeautifulSoup
        
        # Function to clean HTML content
        def clean_html(html_content):
            if not html_content:
                return ''
            try:
                # Remove HTML tags and extract text
                soup = BeautifulSoup(html_content, 'html.parser')
                text = soup.get_text(separator=' ', strip=True)
                # Remove special characters and excessive whitespace
                text = re.sub(r'\s+', ' ', text)
                return text.strip()
            except Exception as e:
                print(f"Error cleaning HTML: {e}")
                return html_content
    except ImportError:
        print("BeautifulSoup not installed. Using simple regex for HTML cleaning.")
        def clean_html(html_content):
            if not html_content:
                return ''
            # Simple regex to remove HTML tags
            text = re.sub(r'<[^>]+>', ' ', html_content)
            # Remove special characters and excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
    
    texts = []
    valid_spaces = []
    spaces_with_content = 0
    total_spaces = len(spaces)
    for s in spaces:
        space_key = s.get('space_key', 'unknown')
        cleaned_texts = []
        
        # Process each page in the space
        page_count = 0
        pages_with_body = 0
        for p in s.get('sampled_pages', []):
            page_count += 1
            body = p.get('body', '')
            if body:
                pages_with_body += 1
                cleaned_text = clean_html(body)
                if cleaned_text:
                    cleaned_texts.append(cleaned_text)
        
        # Join all cleaned text for this space
        text = ' '.join(cleaned_texts).strip()
        
        if text:
            texts.append(text)
            valid_spaces.append(s)
            spaces_with_content += 1
        else:
            print(f"Space {space_key}: No usable text content after cleaning. Total pages: {page_count}, Pages with body: {pages_with_body}")
    
    print(f"Found {spaces_with_content} out of {total_spaces} spaces with text content.")
    
    if not texts:
        raise ValueError("No spaces with non-empty text content for vectorization. Check if BeautifulSoup is installed or if the page content contains actual text.")
    
    vectorizer = TfidfVectorizer(max_features=512)
    X = vectorizer.fit_transform(texts)
    return X, valid_spaces

def cluster_spaces(spaces, method='agglomerative', n_clusters=20):
    X, valid_spaces = get_vectors(spaces)
    if method == 'agglomerative':
        model = AgglomerativeClustering(n_clusters=n_clusters)
    elif method == 'kmeans':
        model = KMeans(n_clusters=n_clusters, n_init=10)
    elif method == 'dbscan':
        model = DBSCAN(eps=1.0, min_samples=2)
    else:
        raise ValueError('Unknown clustering method')
    labels = model.fit_predict(X.toarray())
    return labels, valid_spaces

def calculate_avg_timestamps(spaces):
    """Calculate average timestamp for each space from page timestamps if available"""
    for space in spaces:
        timestamps = []
        for page in space.get('sampled_pages', []):
            # Check 'updated' field which is set by sample_and_pickle_spaces.py
            when = page.get('updated')
            if not when and 'version' in page:
                # Fallback to version.when format if updated isn't available
                when = page.get('version', {}).get('when')
                
            if when:
                try:
                    # Parse ISO format timestamp and convert to unix timestamp
                    ts = datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
                    timestamps.append(ts)
                except ValueError:
                    print(f"Warning: Could not parse timestamp '{when}' for a page in space {space.get('space_key')}")
        
        # Calculate average timestamp if we have any valid timestamps
        avg_ts = sum(timestamps) / len(timestamps) if timestamps else 0
        space['avg'] = avg_ts  # Store avg timestamp for color mapping
        
        # Print debug info to verify we're getting timestamps
        if len(timestamps) > 0:
            print(f"Space {space.get('space_key')}: Found {len(timestamps)} timestamps, avg: {datetime.fromtimestamp(avg_ts).strftime('%Y-%m-%d')}")
        else:
            print(f"Space {space.get('space_key')}: No timestamps found")
    
    return spaces

def render_html(spaces, labels, method, tags=None):
    html = ['<html><head><title>Clustered Spaces</title>',
            '<style>',
            'table { border-collapse: collapse; width: 100%; }',
            'th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }',
            'th { background-color: #f2f2f2; }',
            'tr:nth-child(even) { background-color: #f9f9f9; }',
            '.age-old { background-color: #ffcccc; }',  # Light red for old
            '.age-mid { background-color: #ffffcc; }',  # Light yellow for middle age
            '.age-new { background-color: #ccffcc; }',  # Light green for new
            '.age-none { background-color: #cccccc; }', # Grey for no data
            '</style>',
            '</head><body>']
    html.append(f'<h2>Clustering method: {method}</h2>')
    
    clusters = defaultdict(list)
    for s, label in zip(spaces, labels):
        clusters[label].append(s)
    
    for label, group in clusters.items():
        tag_str = f"Tags: {', '.join(tags[label])}" if tags and label in tags else ""
        html.append(f'<h3>Cluster {label} {tag_str}</h3>')
        
        # Create table header
        html.append('<table>')
        html.append('<tr><th>Space Key</th><th>Pages</th><th>Last Edit Date</th></tr>')
        
        # Sort spaces by average timestamp (newest first) within each cluster
        sorted_group = sorted(group, key=lambda x: x.get('avg', 0), reverse=True)
        
        for s in sorted_group:
            # Get average timestamp and format as date if available
            avg_ts = s.get('avg', 0)
            if avg_ts > 0:
                try:
                    date_str = datetime.fromtimestamp(avg_ts).strftime('%Y-%m-%d')
                    
                    # Determine age class for color coding (simple 3-category approach)
                    # Extract all non-zero timestamps from all spaces for comparison
                    all_timestamps = [space.get('avg', 0) for space in spaces if space.get('avg', 0) > 0]
                    if all_timestamps:
                        oldest = min(all_timestamps)
                        newest = max(all_timestamps)
                        range_size = newest - oldest
                        
                        if range_size > 0:
                            position = (avg_ts - oldest) / range_size
                            if position < 0.33:
                                age_class = 'age-old'
                            elif position < 0.66:
                                age_class = 'age-mid'
                            else:
                                age_class = 'age-new'
                        else:
                            age_class = 'age-mid'
                    else:
                        age_class = 'age-none'
                except ValueError:
                    date_str = 'Invalid date'
                    age_class = 'age-none'
            else:
                date_str = 'No data'
                age_class = 'age-none'
                
            # Add row with color coding for the date cell
            html.append(f'<tr><td>{s["space_key"]}</td><td>{s.get("total_pages", len(s["sampled_pages"]))}</td>' +
                       f'<td class="{age_class}">{date_str}</td></tr>')
            
        html.append('</table>')
    
    html.append('</body></html>')
    out_path = 'clustered_spaces.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html))
    print(f'HTML written to {out_path}')
    webbrowser.open('file://' + os.path.abspath(out_path))

def preprocess_application_search_index(spaces):
    """
    Preprocess and index all spaces and pages using Whoosh for fast full-text search.
    This function creates a comprehensive index of all content, regardless of search terms.
    """
    if not WHOOSH_AVAILABLE:
        print("Error: Whoosh library is not installed. Please install it with:")
        print("pip install whoosh")
        return
        
    print(f"Indexing content from {len(spaces)} spaces...")
    
    # Create whoosh index directory if it doesn't exist
    if not os.path.exists(WHOOSH_INDEX_DIR):
        os.makedirs(WHOOSH_INDEX_DIR)
    else:
        # Clean existing index
        print("Cleaning existing index...")
        shutil.rmtree(WHOOSH_INDEX_DIR)
        os.makedirs(WHOOSH_INDEX_DIR)
    
    # Define schema for the index
    schema = Schema(
        space_key=ID(stored=True),
        page_id=ID(stored=True),
        page_title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        page_content=TEXT(analyzer=StemmingAnalyzer())
    )
    
    # Create the index
    ix = create_in(WHOOSH_INDEX_DIR, schema)
    
    # Import BeautifulSoup for HTML cleaning
    try:
        from bs4 import BeautifulSoup
        has_beautifulsoup = True
    except ImportError:
        print("BeautifulSoup not installed. Using simple regex for HTML cleaning.")
        has_beautifulsoup = False
    
    # Define HTML cleaning function
    def clean_html(html_content):
        if not html_content:
            return ''
        try:
            if has_beautifulsoup:
                # Use BeautifulSoup for better HTML cleaning
                soup = BeautifulSoup(html_content, 'html.parser')
                text = soup.get_text(separator=' ', strip=True)
            else:
                # Fallback to regex for HTML tag removal
                text = re.sub(r'<[^>]+>', ' ', html_content)
            
            # Remove special characters and excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception as e:
            print(f"Error cleaning HTML: {e}")
            return html_content if html_content else ''
    
    # Start indexing
    writer = ix.writer(limitmb=256, procs=1, multisegment=True)
    
    total_pages = 0
    pages_indexed = 0
    
    try:
        for space in spaces:
            space_key = space.get('space_key', 'unknown')
            print(f"Indexing space: {space_key}")
            
            for page in space.get('sampled_pages', []):
                total_pages += 1
                
                # Show progress periodically
                if total_pages % 1000 == 0:
                    print(f"Processed {total_pages} pages...")
                
                page_id = page.get('id', f"unknown_{total_pages}")
                page_title = page.get('title', 'Untitled')
                
                # Get and clean page content (body)
                body = page.get('body', '')
                cleaned_body = clean_html(body)
                
                # Index every page, regardless of content
                writer.add_document(
                    space_key=space_key,
                    page_id=str(page_id),
                    page_title=page_title,
                    page_content=cleaned_body
                )
                pages_indexed += 1
                
        # Commit the index
        print("Committing index...")
        writer.commit()
        
        print(f"\nIndexing complete!")
        print(f"Indexed {pages_indexed} pages across {len(spaces)} spaces.")
        print(f"Index stored in {os.path.abspath(WHOOSH_INDEX_DIR)}")
        
    except Exception as e:
        print(f"Error during indexing: {e}")
        # Try to commit whatever we have so far
        try:
            writer.commit()
        except:
            pass

def search_applications_indexed():
    """
    Search for applications using the Whoosh index (much faster than direct search).
    This function uses app_search.txt for the search terms but searches through the complete index.
    """
    if not WHOOSH_AVAILABLE:
        print("Error: Whoosh library is not installed. Please install it with:")
        print("pip install whoosh")
        return
    
    # Check if index exists
    if not os.path.exists(WHOOSH_INDEX_DIR) or not exists_in(WHOOSH_INDEX_DIR):
        print(f"Error: Whoosh index not found in {WHOOSH_INDEX_DIR}")
        print("Please run option 14 first to create the search index.")
        return
    
    # Load application search terms
    app_search_path = os.path.join(os.path.dirname(__file__), 'app_search.txt')
    if not os.path.exists(app_search_path):
        print(f"Error: app_search.txt not found at {app_search_path}")
        print("Please create this file with one application name per line.")
        return
        
    # Load search terms
    try:
        with open(app_search_path, 'r') as f:
            # Skip lines starting with # (comments) and empty lines
            search_terms = [line.strip() for line in f 
                           if line.strip() and not line.strip().startswith('#')]
    except Exception as e:
        print(f"Error reading app_search.txt: {e}")
        return
        
    if not search_terms:
        print("No search terms found in app_search.txt")
        print("Please add at least one application name per line.")
        return
    
    print(f"Loaded {len(search_terms)} application search terms from app_search.txt")
    
    # Open the index
    ix = open_dir(WHOOSH_INDEX_DIR)
    
    # Dictionary to hold results
    # Format: {app_term: [(space_key, hit_count, matched_pages), ...]}
    app_hits = defaultdict(list)
    
    # Track spaces that have at least one hit
    spaces_with_hits = set()
    
    # Set a large limit for query results (adjust based on your dataset size)
    QUERY_LIMIT = 10000
    
    # Search for each term
    print("Searching indexed data...")
    start_time = datetime.now()
    
    with ix.searcher() as searcher:
        for term in search_terms:
            print(f"Searching for: {term}")
            term_start_time = datetime.now()
            
            # Create a query that searches both title and content
            query_parser = MultifieldParser(["page_title", "page_content"], ix.schema)
            
            # Process the term to handle special characters
            # This makes sure terms with special characters like + or - don't cause query errors
            processed_term = term
            for char in '+-/\\()*&^%$#@!~`"\'|':
                if char in processed_term:
                    processed_term = processed_term.replace(char, f"\\{char}")
            
            # Optionally use phrase queries for multi-word terms to ensure exact matches
            if " " in processed_term:
                query_str = f'"{processed_term}"'  # Phrase query
            else:
                query_str = processed_term
                
            # Parse the query
            try:
                query = query_parser.parse(query_str)
            except Exception as e:
                print(f"Query error for term '{term}': {e}")
                print("Skipping this term.")
                continue
            
            # Execute the search with a large limit
            try:
                results = searcher.search(query, limit=QUERY_LIMIT)
                print(f"Found {len(results)} hits for '{term}'")
                
                # Process results for this term
                term_spaces = defaultdict(list)
                
                for result in results:
                    space_key = result["space_key"]
                    page_title = result["page_title"]
                    spaces_with_hits.add(space_key)
                    term_spaces[space_key].append(page_title)
                
                # Add to overall results
                for space_key, matched_pages in term_spaces.items():
                    hit_count = len(matched_pages)
                    app_hits[term].append((space_key, hit_count, matched_pages[:5]))
                
                term_elapsed = datetime.now() - term_start_time
                print(f"Processed in {term_elapsed.total_seconds():.2f} seconds")
                
            except Exception as e:
                print(f"Search error for term '{term}': {e}")
    
    total_elapsed = datetime.now() - start_time
    print(f"Total search time: {total_elapsed.total_seconds():.2f} seconds")
    
    # Generate HTML report
    html = ['<html><head><title>Indexed Application Search Results</title>',
            '<style>',
            'body { font-family: Arial, sans-serif; line-height: 1.6; margin: 20px; }',
            'h1 { color: #2c3e50; }',
            'h2 { color: #3498db; margin-top: 30px; }',
            'table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }',
            'th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }',
            'th { background-color: #f2f2f2; position: sticky; top: 0; }',
            'tr:nth-child(even) { background-color: #f9f9f9; }',
            'tr:hover { background-color: #f1f1f1; }',
            '.summary { margin-bottom: 30px; padding: 10px; background-color: #eaf2f8; border-radius: 5px; }',
            '.hit-count { font-weight: bold; color: #2980b9; }',
            '.matched-pages { font-size: 0.9em; color: #7f8c8d; max-width: 400px; }',
            '.search-time { font-style: italic; color: #7f8c8d; }',
            '</style>',
            '</head><body>']

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    html.append(f'<h1>Indexed Application Search Results</h1>')
    html.append(f'<p>Generated: {timestamp}</p>')
    html.append(f'<p class="search-time">Total search time: {total_elapsed.total_seconds():.2f} seconds</p>')
    
    # Summary section
    html.append('<div class="summary">')
    html.append(f'<p>Searched the Whoosh index for <b>{len(search_terms)}</b> application terms.</p>')
    html.append(f'<p>Found matches in <b>{len(spaces_with_hits)}</b> spaces.</p>')
    html.append('<p>Applications with most mentions:</p><ul>')
    
    # Show top 5 applications by total hit count
    sorted_apps = sorted(app_hits.items(), key=lambda x: sum(count for _, count, _ in x[1]), reverse=True)
    for app, hits in sorted_apps[:5]:
        total_hits = sum(count for _, count, _ in hits)
        html.append(f'<li><b>{app}</b>: {total_hits} mentions in {len(hits)} spaces</li>')
    
    html.append('</ul></div>')
    
    # First table: Application-centric view
    html.append('<h2>Applications and Where They Appear</h2>')
    html.append('<table>')
    html.append('<tr><th>Application</th><th>Space Key</th><th>Hit Count</th><th>Sample Matched Pages</th></tr>')
    
    for app, hits in sorted_apps:
        # Sort by hit count for this application
        sorted_hits = sorted(hits, key=lambda x: x[1], reverse=True)
        if sorted_hits:
            # First row includes application name
            space, count, pages = sorted_hits[0]
            html.append(f'<tr><td rowspan="{len(sorted_hits)}">{app}</td><td>{space}</td><td class="hit-count">{count}</td>')
            html.append(f'<td class="matched-pages">{", ".join(pages[:5])}')
            if len(pages) > 5:
                html.append(' <i>(and more...)</i>')
            html.append('</td></tr>')
            
            # Remaining rows for this application
            for space, count, pages in sorted_hits[1:]:
                html.append(f'<tr><td>{space}</td><td class="hit-count">{count}</td>')
                html.append(f'<td class="matched-pages">{", ".join(pages[:5])}')
                if len(pages) > 5:
                    html.append(' <i>(and more...)</i>')
                html.append('</td></tr>')
    
    html.append('</table>')
    
    # Second table: Space-centric view
    html.append('<h2>Spaces and Applications They Contain</h2>')
    html.append('<table>')
    html.append('<tr><th>Space Key</th><th>Applications Found</th><th>Total Mentions</th></tr>')
    
    # Build space-centric data
    space_data = defaultdict(list)
    for app, hits in app_hits.items():
        for space, count, _ in hits:
            space_data[space].append((app, count))
    
    # Sort spaces by total hit count
    sorted_spaces = sorted(space_data.items(), 
                          key=lambda x: sum(count for _, count in x[1]), 
                          reverse=True)
    
    for space, app_list in sorted_spaces:
        total_hits = sum(count for _, count in app_list)
        app_formatted = ', '.join([f"{app} ({count})" for app, count in 
                                  sorted(app_list, key=lambda x: x[1], reverse=True)])
        html.append(f'<tr><td>{space}</td><td>{app_formatted}</td><td>{total_hits}</td></tr>')
    
    html.append('</table>')
    html.append('</body></html>')
    
    # Write to file
    out_path = 'indexed_application_search_results.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html))
    
    print(f'\nSearch complete! Results written to {out_path}')
    print(f'Found {len(spaces_with_hits)} spaces with matches to your search terms.')
    
    # Open the HTML file in the browser
    webbrowser.open('file://' + os.path.abspath(out_path))

def main():
    min_pages = DEFAULT_MIN_PAGES
    max_pages = None
    date_filter = None  # New variable for date filtering
    spaces = []
    data_loaded = False
    n_clusters = 20  # Default number of clusters
    # Display a clear banner so we know the program is running
    print("\n" + "="*80)
    print("""
    ┌─────────────────────────────────────────────────────────┐
    │                                                         │
    │   CONFLUENCE CLUSTER EXPLORER                           │
    │   ===========================                           │
    │                                                         │
    │   Analyze and visualize Confluence spaces               │
    │   Version 1.4                                           │
    │                                                         │
    └─────────────────────────────────────────────────────────┘
    """)    
    print("="*80 + "\n")
    print("Welcome to the Confluence Cluster Explorer!")
    
    # Helper function to load data only when needed
    def ensure_data_loaded():
        nonlocal spaces, data_loaded, min_pages, max_pages, date_filter
        if not data_loaded:
            print(f"\nLoading space data with filter: >= {min_pages} pages" + 
                  (f" and <= {max_pages} pages" if max_pages else "") + 
                  (f" and date {date_filter}" if date_filter else "") + "...")
            spaces = load_spaces(min_pages=min_pages, max_pages=max_pages)
            print(f"Loaded {len(spaces)} spaces from {TEMP_DIR}.")
            
            # Apply date filter if specified
            if date_filter:
                initial_count = len(spaces)
                spaces = filter_spaces_by_date(spaces, date_filter)
                print(f"After date filter: {len(spaces)} out of {initial_count} spaces match.")
                
            data_loaded = True
        return spaces
    
    while True:
        print("\nMenu:")
        print("1. Set minimum pages filter (current: {} )".format(min_pages))
        print("2. Set maximum pages filter (current: {} )".format(max_pages if max_pages else "No limit"))
        print("3. Search for space key")
        print("4. Help (explain algorithms)")
        print("5. Visualize total pages per space (bar chart)")
        print(f"6. Set number of clusters manually (current: {n_clusters})")
        print("7. Semantic clustering and render HTML (Agglomerative)")
        print("8. Semantic clustering and render HTML (KMeans)")
        print("9. Semantic clustering and render HTML (DBSCAN)")
        print("10. Semantic clustering and D3 Circle Packing (Agglomerative)")        
        print("11. Semantic clustering and D3 Circle Packing (KMeans)")
        print("12. Semantic clustering and D3 Circle Packing (DBSCAN)")
        print("13. Search for applications in spaces (slow)")
        print("14. Preprocess application search index (Whoosh)")
        print("15. Search applications using indexed data (fast using Whoosh)")
        print(f"16. Set date filter (current: {date_filter if date_filter else 'None'} )")
        print("Q. Quit")
        
        choice = input("Select option: ").strip()
        
        if choice == '1':
            inp = input("Enter minimum pages per space (0 for all): ").strip()
            min_pages = int(inp) if inp else 0
            data_loaded = False  # Mark data as needing to be reloaded
            print(f"Minimum pages filter set to {min_pages}. Data will be loaded when needed.")
        elif choice == '2':
            inp = input("Enter maximum pages per space (leave empty for no limit): ").strip()
            max_pages = int(inp) if inp else None
            data_loaded = False  # Mark data as needing to be reloaded
            print(f"Maximum pages filter set to {max_pages if max_pages else 'No limit'}. Data will be loaded when needed.")
        elif choice == '3':
            ensure_data_loaded()  # Make sure data is loaded before searching
            term = input("Enter search term: ").strip()
            results = search_spaces(spaces, term)
            if results:
                print("\nSearch results:")
                for k, n in results:
                    print(f"{k}: {n} pages")
                print("\nPress Enter to continue...")
                input()
            else:
                print("No matches found.")
                print("\nPress Enter to continue...")
                input()
        elif choice == '4':
            explain_algorithms()
        elif choice == '5':
            ensure_data_loaded()  # Make sure data is loaded before visualizing
            visualize_total_pages(spaces)
        elif choice == '6':
            try:
                new_clusters = int(input("Enter number of clusters (5-50 recommended): ").strip())
                if new_clusters < 2:
                    print("Number of clusters must be at least 2")
                else:
                    n_clusters = new_clusters
                    print(f"Number of clusters set to {n_clusters}")
            except ValueError:
                print("Please enter a valid number")
        elif choice == '7':
            try:
                ensure_data_loaded()  # Make sure data is loaded before clustering
                print(f"Clustering {len(spaces)} spaces using semantic vectors (Agglomerative) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded or no spaces match the current filters.")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'agglomerative', n_clusters)
                tags = suggest_tags_for_clusters(valid_spaces, labels)
                render_html(valid_spaces, labels, 'Agglomerative', tags)
            except ValueError as e:
                if "No spaces with non-empty text content" in str(e):
                    print(f"Error: {e}")
                    print("This might be because the loaded spaces have no text content in their pages,")
                    print(f"or the 'min_pages' filter ({min_pages}) is too high, excluding spaces with content.")
                    print("Try adjusting the filter (Option 1) or check the data in the .pkl files.")
                else:
                    print(f"Clustering Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        elif choice == '8':
            try:
                ensure_data_loaded()  # Make sure data is loaded before clustering
                print(f"Clustering {len(spaces)} spaces using semantic vectors (KMeans) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded or no spaces match the current filters.")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'kmeans', n_clusters)
                tags = suggest_tags_for_clusters(valid_spaces, labels)
                render_html(valid_spaces, labels, 'KMeans', tags)
            except ValueError as e:
                if "No spaces with non-empty text content" in str(e):
                    print(f"Error: {e}")
                    print("This might be because the loaded spaces have no text content in their pages,")
                    print(f"or the 'min_pages' filter ({min_pages}) is too high, excluding spaces with content.")
                    print("Try adjusting the filter (Option 1) or check the data in the .pkl files.")
                else:
                    print(f"Clustering Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        elif choice == '9':
            try:
                ensure_data_loaded()  # Make sure data is loaded before clustering
                print(f"Clustering {len(spaces)} spaces using semantic vectors (DBSCAN)...")
                if not spaces:
                    print("Error: No spaces loaded or no spaces match the current filters.")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'dbscan')
                tags = suggest_tags_for_clusters(valid_spaces, labels)
                render_html(valid_spaces, labels, 'DBSCAN', tags)
            except ValueError as e:
                if "No spaces with non-empty text content" in str(e):
                    print(f"Error: {e}")
                    print("This might be because the loaded spaces have no text content in their pages,")
                    print(f"or the 'min_pages' filter ({min_pages}) is too high, excluding spaces with content.")
                    print("Try adjusting the filter (Option 1) or check the data in the .pkl files.")
                else:
                    print(f"Clustering Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        elif choice == '10':
            try:
                ensure_data_loaded()  # Make sure data is loaded before clustering
                print(f"Clustering {len(spaces)} spaces for D3 visualization (Agglomerative) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded or no spaces match the current filters.")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'agglomerative', n_clusters)
                tags = suggest_tags_for_clusters(valid_spaces, labels)
                render_d3_circle_packing(valid_spaces, labels, 'Agglomerative', tags)
            except ValueError as e:
                if "No spaces with non-empty text content" in str(e):
                    print(f"Error: {e}")
                    print("This might be because the loaded spaces have no text content in their pages,")
                    print(f"or the 'min_pages' filter ({min_pages}) is too high, excluding spaces with content.")
                    print("Try adjusting the filter (Option 1) or check the data in the .pkl files.")
                else:
                    print(f"Clustering Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        elif choice == '11':
            try:
                ensure_data_loaded()  # Make sure data is loaded before clustering
                print(f"Clustering {len(spaces)} spaces for D3 visualization (KMeans) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded or no spaces match the current filters.")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'kmeans', n_clusters)
                tags = suggest_tags_for_clusters(valid_spaces, labels)
                render_d3_circle_packing(valid_spaces, labels, 'KMeans', tags)
            except ValueError as e:
                if "No spaces with non-empty text content" in str(e):
                    print(f"Error: {e}")
                    print("This might be because the loaded spaces have no text content in their pages,")
                    print(f"or the 'min_pages' filter ({min_pages}) is too high, excluding spaces with content.")
                    print("Try adjusting the filter (Option 1) or check the data in the .pkl files.")
                else:
                    print(f"Clustering Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        elif choice == '12':
            try:
                ensure_data_loaded()  # Make sure data is loaded before clustering
                print(f"Clustering {len(spaces)} spaces for D3 visualization (DBSCAN)...")
                if not spaces:
                    print("Error: No spaces loaded or no spaces match the current filters.")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'dbscan')
                tags = suggest_tags_for_clusters(valid_spaces, labels)
                render_d3_circle_packing(valid_spaces, labels, 'DBSCAN', tags)
            except ValueError as e:
                if "No spaces with non-empty text content" in str(e):
                    print(f"Error: {e}")
                    print("This might be because the loaded spaces have no text content in their pages,")
                    print(f"or the 'min_pages' filter ({min_pages}) is too high, excluding spaces with content.")
                    print("Try adjusting the filter (Option 1) or check the data in the .pkl files.")               
                else:
                    print(f"Clustering Error: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")        
        elif choice == '13':
            ensure_data_loaded()  # Make sure data is loaded before searching for applications
            search_for_applications(spaces)
        elif choice == '14':
            ensure_data_loaded()  # Make sure data is loaded before building Whoosh index
            preprocess_application_search_index(spaces)
        elif choice == '15':
            search_applications_indexed()
        elif choice == '16':
            print("\nSet date filter to include spaces with average dates before/after a specific date.")
            print("Format: >YYYY-MM-DD (after date) or <YYYY-MM-DD (before date)")
            print("Examples: >2017-01-01 (spaces updated after Jan 1, 2017)")
            print("          <2020-03-15 (spaces updated before March 15, 2020)")
            print("Enter an empty string to clear the filter.")
            
            date_input = input("Enter date filter: ").strip()
            if date_input:
                if date_input[0] not in ['<', '>']:
                    print("Error: Date filter must start with < or >")
                    print("Format should be >YYYY-MM-DD or <YYYY-MM-DD")                
                else:
                    date_filter = date_input
                    data_loaded = False  # Mark data as needing to be reloaded
                    print(f"Date filter set to {date_filter}. Data will be loaded when needed.")
            else:
                date_filter = None
                data_loaded = False  # Mark data as needing to be reloaded
                print("Date filter cleared. Data will be loaded when needed.")
        elif choice.upper() == 'Q':
            print("Goodbye!")
            break
        else:
            print("Invalid option.")

if __name__ == '__main__':
    main()
