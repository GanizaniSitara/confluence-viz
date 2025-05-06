import os
import pickle
import sys
import numpy as np
import re
from sklearn.cluster import AgglomerativeClustering, KMeans, DBSCAN
from sklearn.manifold import TSNE  # Add TSNE import
import webbrowser
from collections import defaultdict
import matplotlib.pyplot as plt
from collections import Counter
import json
from datetime import datetime
import shutil
from config_loader import load_visualization_settings
import operator
from html import escape  # Added for HTML escaping

# Try to import Whoosh (will be used for options 14 and 15)
try:
    import whoosh
    from whoosh.fields import Schema, TEXT, ID, KEYWORD
    from whoosh.analysis import StemmingAnalyzer
    from whoosh.index import create_in, open_dir, exists_in
    from whoosh.qparser import QueryParser, MultifieldParser, OrGroup  # Added OrGroup
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
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))  # Corrected variable here
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

def explain_algorithms():
    print("\nClustering Algorithm Help:")
    print("1. Agglomerative: Hierarchical clustering that merges similar spaces into clusters based on their features (e.g., page count). Good for discovering nested/grouped structure.")
    print("2. KMeans: Partitions spaces into a fixed number of clusters by minimizing within-cluster variance. Good for even-sized, well-separated groups.")
    print("3. DBSCAN: Groups spaces based on density (how close together they are). Good for finding clusters of varying size and ignoring noise/outliers.")
    print("4. Visualization: Shows a bar chart of total number of pages per space, sorted descending.")
    print("5. Topic Naming/Tags: After clustering, the script will suggest tags for each cluster based on the most common words in the space keys (or you can extend to use semantic data).\n")

def visualize_total_pages(spaces):
    # Bar chart of total pages per space
    keys = [s['space_key'] for s in spaces]
    totals = [s.get('total_pages', len(s['sampled_pages'])) for s in spaces]
    sorted_pairs = sorted(zip(keys, totals), key=lambda x: x[1], reverse=True)
    keys_sorted, totals_sorted = zip(*sorted_pairs)
    plt.figure(figsize=(12, 6))
    plt.bar(keys_sorted, totals_sorted)
    plt.xticks(rotation=90, fontsize=8)
    plt.ylabel('Total Pages')
    plt.title('Total Number of Pages per Space')
    plt.tight_layout()
    plt.show()

def suggest_tags_for_clusters(spaces, labels):
    clusters = defaultdict(list)
    for s, label in zip(spaces, labels):
        clusters[label].append(s)
    tags = {}
    
    for label, group in clusters.items():
        # Get text from page titles and space names if available
        all_text = []
        
        for s in group:
            # Add space key (still useful as a backup)
            all_text.append(s['space_key'])
            
            # Extract titles from pages if available
            for page in s.get('sampled_pages', []):
                if 'title' in page and page['title']:
                    all_text.append(page['title'])
            
            # Look for a space name if present
            if 'name' in s and s['name']:
                all_text.append(s['name'])
        
        # Join all text and split into words
        text = ' '.join(all_text)
        # Remove special characters and convert to lowercase
        cleaned_text = re.sub(r'[^\w\s]', ' ', text.lower())
        words = cleaned_text.split()
        
        # Filter out stopwords and short words using the loaded STOPWORDS
        filtered_words = [w for w in words if w not in STOPWORDS and len(w) > 2]
        
        # Count word frequencies
        common = Counter(filtered_words).most_common(5)
        
        # Select most common words
        top_words = [w for w, _ in common][:3]  # Limit to top 3
        
        if not top_words and all('space_key' in s for s in group):
            # Fall back to space keys if no meaningful words found
            key_parts = []
            for s in group:
                key_parts.extend(s['space_key'].split('-'))
            fallback_common = Counter(key_parts).most_common(3)
            top_words = [w for w, _ in fallback_common]
        
        tags[label] = top_words
    
    return tags

def render_d3_semantic_scatter_plot(spaces, labels, method_name, tags, X_vectors):
    try:
        config = load_visualization_settings()
        confluence_base_url = config.get('confluence_base_url', '') # Use .get() for safety
        if not confluence_base_url:
            print("Warning: 'confluence_base_url' not found in visualization settings. Links in scatter plot may not work or will be relative.")
            confluence_base_url = "" # Default to empty string, makes links relative if base is missing
    except Exception as e:
        print(f"Warning: Could not load visualization settings: {e}. Links in scatter plot may not work or will be relative.")
        confluence_base_url = "" # Default to empty string

    # Ensure spaces have 'avg' timestamps for potential tooltip info
    spaces = calculate_avg_timestamps(spaces)

    # Dimensionality Reduction using t-SNE
    coordinates_2d = np.array([])
    if X_vectors is not None and X_vectors.shape[0] > 1:
        perplexity_value = min(30, X_vectors.shape[0] - 1)
        if perplexity_value <= 0: # Ensure perplexity is at least 1
            perplexity_value = 1
        
        n_iter_value = 1000 
        if X_vectors.shape[0] < 50 : # If very few samples, reduce iterations slightly
            n_iter_value = max(250, int(200 + X_vectors.shape[0] * 5))

        print(f"Running t-SNE with n_samples={X_vectors.shape[0]}, perplexity={perplexity_value}, n_iter={n_iter_value}")
        
        try:
            tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity_value, 
                        n_iter=n_iter_value, init='pca', learning_rate=200.0, method='auto')
            coordinates_2d = tsne.fit_transform(X_vectors.toarray() if hasattr(X_vectors, "toarray") else X_vectors)
            print(f"t-SNE completed. Shape of coordinates_2d: {coordinates_2d.shape}")
        except Exception as e:
            print(f"Error during t-SNE: {e}")
            if X_vectors is not None and X_vectors.shape[0] > 0:
                 coordinates_2d = np.random.rand(X_vectors.shape[0], 2) * 100 # Random 2D points
            else:
                 coordinates_2d = np.array([])

    elif X_vectors is not None and X_vectors.shape[0] == 1:
        print("Only one data point. Plotting at origin (0,0).")
        coordinates_2d = np.array([[0,0]])
    else:
        print("Not enough data points for 2D projection or X_vectors is None.")
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Semantic Scatter Plot - No Data</title>
</head>
<body>
    <h1>Semantic Scatter Plot ({method_name})</h1>
    <p>No data available to display. This might be due to filtering or lack of text content in spaces.</p>
</body>
</html>"""
        out_path = 'semantic_scatter_plot.html'
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f'HTML written to {out_path}')
        webbrowser.open('file://' + os.path.abspath(out_path))
        return

    plot_data = []
    if coordinates_2d.shape[0] == len(spaces) and len(labels) == len(spaces):
        for i, space in enumerate(spaces):
            label = labels[i]
            avg_ts = space.get('avg', 0)
            date_str = datetime.fromtimestamp(avg_ts).strftime('%Y-%m-%d') if avg_ts > 0 else "No date"
            
            space_key_original = space['space_key'] # Keep original for URL
            escaped_space_key = escape(space_key_original)
            escaped_space_name = escape(space.get('name', space_key_original))
            escaped_cluster_tags = escape(', '.join(tags.get(label, [])))

            plot_data.append({
                'key': escaped_space_key,
                'name': escaped_space_name,
                'x': float(coordinates_2d[i, 0]),
                'y': float(coordinates_2d[i, 1]),
                'cluster': int(label),
                'cluster_tags': escaped_cluster_tags,
                'value': space.get('total_pages', len(space['sampled_pages'])),
                'date': date_str,
                'url': f"{confluence_base_url}/display/{space_key_original}" # Use original space_key for URL
            })
    else:
        print(f"Warning: Mismatch between coordinate count ({coordinates_2d.shape[0]}), space count ({len(spaces)}), or label count ({len(labels)}). Plot data may be incomplete or incorrect.")

    data_json = json.dumps(plot_data)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Semantic Scatter Plot ({method_name})</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{ margin: 20px; font-family: sans-serif; }}
        .dot {{ stroke: #fff; stroke-width: 0.5px; }}
        .tooltip {{
            position: absolute;
            text-align: center;
            width: auto;
            height: auto;
            padding: 8px;
            font: 12px sans-serif;
            background: lightsteelblue;
            border: 0px;
            border-radius: 8px;
            pointer-events: none;
            opacity: 0;
        }}
        .axis-label {{ font-size: 10px; }}
        .legend {{ font-size: 10px; }}
    </style>
</head>
<body>
    <h1>Semantic Scatter Plot ({method_name})</h1>
    <div id="scatter-plot"></div>
    <script>
        const data = {data_json};
        console.log("Data for D3:", data);

        if (data && data.length > 0) {{
            const margin = {{top: 20, right: 200, bottom: 60, left: 60}};
            const width = 960 - margin.left - margin.right;
            const height = 600 - margin.top - margin.bottom;

            const svg = d3.select("#scatter-plot").append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
              .append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);

            const xScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d.x))
                .range([0, width]);

            const yScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d.y))
                .range([height, 0]);

            const colorScale = d3.scaleOrdinal(d3.schemeCategory10);
            colorScale.domain(Array.from(new Set(data.map(d => d.cluster))).sort((a,b) => a-b));

            const sizeScale = d3.scaleSqrt()
                .domain([0, d3.max(data, d => d.value)])
                .range([3, 20]);

            const tooltip = d3.select("body").append("div")
                .attr("class", "tooltip");

            svg.append("g")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(xScale))
                .append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", margin.bottom - 10)
                .attr("fill", "black")
                .style("text-anchor", "middle")
                .text("t-SNE Dimension 1");

            svg.append("g")
                .call(d3.axisLeft(yScale))
                .append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -margin.left + 20)
                .attr("fill", "black")
                .style("text-anchor", "middle")
                .text("t-SNE Dimension 2");

            svg.selectAll(".dot")
                .data(data)
                .enter().append("circle")
                .attr("class", "dot")
                .attr("cx", d => xScale(d.x))
                .attr("cy", d => yScale(d.y))
                .attr("r", d => sizeScale(d.value > 0 ? d.value : 1))
                .style("fill", d => colorScale(d.cluster))
                .on("mouseover", function(event, d) {{
                    tooltip.transition().duration(200).style("opacity", .9);
                    tooltip.html(
                        `<strong>${{d.key}}</strong><br/>
                        Cluster: ${{d.cluster}} (${{d.cluster_tags}})<br/>
                        Pages: ${{d.value}}<br/>
                        Date: ${{d.date}}`
                    )
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 30) + "px");
                }})
                .on("mouseout", function(d) {{
                    tooltip.transition().duration(500).style("opacity", 0);
                }})
                .on("click", function(event, d) {{
                    if (d.url) {{ window.open(d.url, '_blank'); }}
                }});

            const uniqueClusters = Array.from(new Set(data.map(d => d.cluster))).sort((a,b) => a-b);
            const legend = svg.selectAll(".legend-item")
                .data(uniqueClusters)
                .enter().append("g")
                .attr("class", "legend-item")
                .attr("transform", (d, i) => `translate(0,${{i * 20}})`);

            legend.append("rect")
                .attr("x", width + 10)
                .attr("y", 0)
                .attr("width", 18)
                .attr("height", 18)
                .style("fill", d => colorScale(d));

            legend.append("text")
                .attr("x", width + 35)
                .attr("y", 9)
                .attr("dy", ".35em")
                .attr("class", "legend")
                .style("text-anchor", "start")
                .text(d => {{
                    const firstSpaceInCluster = data.find(space => space.cluster === d);
                    const tags = firstSpaceInCluster ? firstSpaceInCluster.cluster_tags : '';
                    return `Cluster ${{d}}${{tags ? ' (' + tags + ')' : ''}}`;
                }});
            
            svg.append("text")
                .attr("x", width + 10)
                .attr("y", -10)
                .attr("class", "legend")
                .style("font-weight", "bold")
                .text("Clusters");

        }} else {{
            const plotDiv = document.getElementById("scatter-plot");
            if (plotDiv) {{
                 plotDiv.innerHTML = "<p>No data to display in scatter plot. This could be due to t-SNE failure, no text content in spaces, or filters being too restrictive.</p>";
            }}
        }}
    </script>
</body>
</html>"""

    out_path = 'semantic_scatter_plot.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f'Semantic scatter plot HTML written to {out_path}')
    webbrowser.open('file://' + os.path.abspath(out_path))

def semantic_clustering_2d_scatter_plot(spaces_arg, n_clusters_arg): # Accept spaces and n_clusters as arguments
    if not spaces_arg: # Use the passed argument
        print("Error: No spaces loaded. Please load data first (e.g., by setting filters).")
        render_d3_semantic_scatter_plot([], [], 'Agglomerative', {}, None)
        return

    print(f"Starting 2D Scatter Plot: Clustering {len(spaces_arg)} spaces using semantic vectors (Agglomerative) into {n_clusters_arg} clusters...")

    try:
        X_tfidf, valid_spaces_for_X = get_vectors(spaces_arg) # Use the passed argument

        if not valid_spaces_for_X or X_tfidf.shape[0] == 0:
            print("Error: No valid spaces with text content found for clustering after get_vectors.")
            render_d3_semantic_scatter_plot([], [], 'Agglomerative', {}, None)
            return
        
        print(f"Obtained {X_tfidf.shape[0]} valid spaces with TF-IDF vectors of shape {X_tfidf.shape}.")

        actual_n_clusters = n_clusters_arg # Use the passed argument
        if X_tfidf.shape[0] < n_clusters_arg: # Use the passed argument
            print(f"Warning: Number of samples ({X_tfidf.shape[0]}) is less than n_clusters ({n_clusters_arg}).")
            if X_tfidf.shape[0] < 2 and X_tfidf.shape[0] > 0:
                 actual_n_clusters = 1
            elif X_tfidf.shape[0] < 2:
                 print("Error: Not enough samples to cluster.")
                 render_d3_semantic_scatter_plot(valid_spaces_for_X, [], 'Agglomerative', {}, X_tfidf)
                 return
            else:
                 actual_n_clusters = min(n_clusters_arg, X_tfidf.shape[0])
            print(f"Using actual_n_clusters = {actual_n_clusters}")
        
        if actual_n_clusters < 1 and X_tfidf.shape[0] > 0:
            actual_n_clusters = 1
        elif actual_n_clusters == 0 and X_tfidf.shape[0] == 0:
             pass

        labels_for_plot = []
        if X_tfidf.shape[0] > 0 and actual_n_clusters > 0:
            if X_tfidf.shape[0] == 1:
                labels_for_plot = np.array([0])
            elif actual_n_clusters == 1 and X_tfidf.shape[0] > 1:
                labels_for_plot = np.zeros(X_tfidf.shape[0], dtype=int)
            elif actual_n_clusters > 1 and X_tfidf.shape[0] >= actual_n_clusters:
                agg_model = AgglomerativeClustering(n_clusters=actual_n_clusters)
                labels_for_plot = agg_model.fit_predict(X_tfidf.toarray())
            else:
                  print(f"Adjusting to 1 cluster due to sample/cluster count mismatch: Samples={X_tfidf.shape[0]}, Clusters={actual_n_clusters}")
                  labels_for_plot = np.zeros(X_tfidf.shape[0], dtype=int)

        else:
            labels_for_plot = []

        tags_for_plot = {}
        if valid_spaces_for_X and len(labels_for_plot) > 0:
             tags_for_plot = suggest_tags_for_clusters(valid_spaces_for_X, labels_for_plot)
        
        render_d3_semantic_scatter_plot(valid_spaces_for_X, labels_for_plot, 'Agglomerative', tags_for_plot, X_vectors=X_tfidf)

    except ValueError as e:
        if "No spaces with non-empty text content" in str(e) or "empty vocabulary" in str(e):
            print(f"Error during 2D scatter plot generation: {e}")
            print("This might be because the loaded spaces have no text content, the filters are too restrictive, or TF-IDF vectorization failed.")
            render_d3_semantic_scatter_plot([], [], 'Agglomerative', {}, None)
        else:
            print(f"Clustering or Plotting Error for 2D scatter plot: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in semantic_clustering_2d_scatter_plot: {e}")
        import traceback
        traceback.print_exc()
        render_d3_semantic_scatter_plot([], [], 'Agglomerative', {}, None)

def main():
    min_pages = DEFAULT_MIN_PAGES
    max_pages = None
    date_filter = None  # New variable for date filtering
    spaces = []
    data_loaded = False
    n_clusters = 20  # Default number of clusters
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
    
    def ensure_data_loaded():
        nonlocal spaces, data_loaded, min_pages, max_pages, date_filter
        if not data_loaded:
            print(f"\nLoading space data with filter: >= {min_pages} pages" + 
                  (f" and <= {max_pages} pages" if max_pages else "") + 
                  (f" and date {date_filter}" if date_filter else "") + "...")
            spaces = load_spaces(min_pages=min_pages, max_pages=max_pages)
            print(f"Loaded {len(spaces)} spaces from {TEMP_DIR}.")
            
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
        print("13. Search for applications in spaces (slow, direct scan)")
        print("14. Preprocess application search index (Whoosh - required for 15, 17, 18, 19)")
        print("15. Search applications using indexed data (Whoosh - full report)")
        print(f"16. Set date filter (current: {date_filter if date_filter else 'None'} )")
        print("17. Search applications using indexed data (Top 1 space overall, limit 3000)") 
        print("18. Find top space per application term (using Whoosh index)")
        print("19. Find all spaces per application term with counts (using Whoosh index)") # New option 19
        print("20. Semantic clustering 2D Scatter Plot (Agglomerative)") # New option 20
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
        elif choice == '17': 
            print("Option 17 (search_applications_indexed_top_space) is currently not implemented.")
        elif choice == '18': 
            search_applications_indexed_top_space_per_term()
        elif choice == '19': # New handler for option 19
            search_applications_indexed_all_spaces_per_term()
        elif choice == '20':
            ensure_data_loaded() # Ensure data is loaded
            semantic_clustering_2d_scatter_plot(spaces, n_clusters) # Pass spaces and n_clusters
        elif choice.upper() == 'Q':
            print("Goodbye!")
            break
        else:
            print("Invalid option.")

if __name__ == '__main__':
    main()
