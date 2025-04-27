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

TEMP_DIR = 'temp'
DEFAULT_MIN_PAGES = 0
VERSION = '1.2'  # Updated version

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
def load_spaces(temp_dir=TEMP_DIR, min_pages=0):
    spaces = []
    for fname in os.listdir(temp_dir):
        if fname.endswith('.pkl'):
            with open(os.path.join(temp_dir, fname), 'rb') as f:
                data = pickle.load(f)
                if 'space_key' in data and 'sampled_pages' in data:
                    if len(data['sampled_pages']) >= min_pages:
                        spaces.append(data)
    return spaces

def filter_spaces(spaces, min_pages):
    return [s for s in spaces if len(s['sampled_pages']) >= min_pages]

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

def render_d3_circle_packing(spaces, labels, method, tags=None):
    # Calculate average timestamps for spaces if they don't already have them
    spaces = calculate_avg_timestamps(spaces)
    
    # Calculate color thresholds and gradient
    percentile_thresholds, color_range_hex = calculate_color_data(spaces)
    
    # Build hierarchical data structure for D3
    clusters = defaultdict(list)
    for s, label in zip(spaces, labels):
        clusters[label].append(s)
    d3_data = {
        'key': 'root',
        'name': f'Clustered Spaces ({method})',
        'children': []
    }    
    for label, group in clusters.items():
        tag_str = ', '.join(tags[label]) if tags and label in tags else ''
        cluster_node = {
            'key': f'cluster_{label}',
            'name': f'Cluster {label}',  # Just the cluster ID
            'tags': tag_str,  # Store tags separately
            'children': [],
            'value': sum(s.get('total_pages', len(s['sampled_pages'])) for s in group)
        }
        for s in group:
            # Format the date if average timestamp is available
            avg_ts = s.get('avg', 0)
            date_str = ""
            if avg_ts > 0:
                try:
                    date_str = datetime.fromtimestamp(avg_ts).strftime('%Y-%m-%d')
                except (ValueError, OSError):
                    date_str = "Invalid date"
            else:
                date_str = "No date"
                
            cluster_node['children'].append({
                'key': s['space_key'],
                'name': s['space_key'],
                'value': s.get('total_pages', len(s['sampled_pages'])),
                'avg': avg_ts,  # Include avg timestamp for coloring
                'date': date_str  # Include formatted date for tooltip
            })
        d3_data['children'].append(cluster_node)
    data_json = json.dumps(d3_data)
    percentile_thresholds_json = json.dumps(percentile_thresholds)
    color_range_hex_json = json.dumps(color_range_hex)
    
    # Use triple quotes with normal string, then format at the end to avoid f-string issues with #
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Clustered Spaces Circle Packing</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body { margin:0; font-family:sans-serif; }
    .node text { text-anchor:middle; alignment-baseline:middle; font-size:6pt; pointer-events:none; }
    .group circle { stroke: #555; stroke-width: 1px; }
    /* Enhanced styles for cluster labels */
    .cluster-label { 
      font-size: 14pt; 
      font-weight: bold; 
      fill: #000; 
      text-anchor: middle; 
      dominant-baseline: middle;
    }
    .cluster-label-bg { 
      stroke: white; 
      stroke-width: 5px; 
      stroke-linejoin: round;
      paint-order: stroke;
      fill: #000;
    }
    /* Tooltip styling */
    .tooltip {
      position: absolute;
      background: #fff;
      border: 1px solid #ddd;
      border-radius: 4px;
      padding: 10px;
      pointer-events: none;
      opacity: 0;
      transition: opacity 0.3s;
    }
  </style>
</head>
<body>
<div id="chart"></div>
<script>
const data = DATA_JSON_PLACEHOLDER;
const PERCENTILE_THRESHOLDS = PERCENTILE_THRESHOLDS_PLACEHOLDER;
const COLOR_RANGE_HEX = COLOR_RANGE_HEX_PLACEHOLDER;
const GREY_COLOR_HEX = 'GREY_COLOR_HEX_PLACEHOLDER';

// Color scale based on thresholds
const colorScale = d3.scaleThreshold()
  .domain(PERCENTILE_THRESHOLDS)
  .range(COLOR_RANGE_HEX);

const width = 1800, height = 1200;
const root = d3.pack()
  .size([width, height])
  .padding(6)
  (d3.hierarchy(data)
  .sum(d => d.value));
const svg = d3.select('#chart').append('svg')
  .attr('width', width)
  .attr('height', height);

// Create tooltip div
const tooltip = d3.select("body").append("div")
  .attr("class", "tooltip");

const g = svg.selectAll('g')
  .data(root.descendants())
  .enter().append('g')
  .attr('transform', d => `translate(${d.x},${d.y})`)
  .on("mouseover", function(event, d) {
    if (!d.children && d.data.date) {
      tooltip.transition()
        .duration(200)
        .style("opacity", 0.9);
      tooltip.html(`<strong>${d.data.key}</strong><br>Pages: ${d.data.value}<br>Last Edit: ${d.data.date}`)
        .style("left", (event.pageX + 10) + "px")
        .style("top", (event.pageY - 28) + "px");
    }
  })
  .on("mouseout", function() {
    tooltip.transition()
      .duration(500)
      .style("opacity", 0);
  });

g.append('circle')
  .attr('r', d => d.r)
  .attr('fill', d => {
    // For non-leaf nodes (clusters), use light gray
    if (d.children) return '#f8f8f8';
    
    // For leaf nodes (spaces)
    if (!d.data.avg || d.data.avg <= 0) return GREY_COLOR_HEX;
    return colorScale(d.data.avg);
  })
  .attr('class', d => d.children ? 'group' : 'leaf');

const leafNodes = g.filter(d => !d.children);
leafNodes.append('text')
  .attr('dy','-0.35em')
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:6pt;')
  .text(d => d.data.key);

leafNodes.append('text')
  .attr('dy','0.75em')
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:6pt;')
  .text(d => d.data.value);

// Create separate layer for cluster labels to ensure they're on top
const clusterLabels = svg.append('g')
  .attr('class', 'cluster-labels')
  .attr('pointer-events', 'none');  // Make sure it doesn't block interactions

// Add labels for cluster nodes
g.filter(d => d.depth > 0 && d.children).each(function(d) {
  // Create a text element for the cluster ID (first line)
  clusterLabels.append('text')
    .attr('x', d.x)
    .attr('y', d.y)
    .attr('dy', '-0.4em')  // Position above center
    .attr('text-anchor', 'middle')
    .attr('class', 'cluster-label')
    .text(d.data.name || d.data.key)  // Simply use the name directly
    .style('font-size', '14pt')
    .style('font-weight', 'bold')
    .style('stroke', 'white')    
    .style('stroke-width', '3px')
    .style('stroke-linejoin', 'round')
    .style('paint-order', 'stroke')
    .style('fill', '#000000');
    
  // Add second line with just the tags in brackets
  if (d.data.tags && d.data.tags.length > 0) {
    clusterLabels.append('text')
      .attr('x', d.x)
      .attr('y', d.y)
      .attr('dy', '1.1em')  // Position below center
      .attr('text-anchor', 'middle')
      .attr('class', 'cluster-label')
      .text(`[${d.data.tags}]`)
      .style('font-size', '12pt')  // Slightly smaller font for tags
      .style('font-weight', 'bold')
      .style('stroke', 'white')    
      .style('stroke-width', '2px')
      .style('stroke-linejoin', 'round')
      .style('paint-order', 'stroke')
      .style('fill', '#000000');
  }
});

// Add legend for color scale
const legendWidth = 200;
const legendHeight = 20;
const legendX = width - legendWidth - 20;
const legendY = 20;

// Create legend title
svg.append('text')
  .attr('x', legendX)
  .attr('y', legendY - 7)
  .style('font-size', '12px')
  .text('Page Age (by Last Edit)');

// Create gradient for legend
const gradient = svg.append('linearGradient')
  .attr('id', 'legend-gradient')
  .attr('x1', '0%')
  .attr('x2', '100%')
  .attr('y1', '0%')
  .attr('y2', '0%');

COLOR_RANGE_HEX.forEach((color, i) => {
  gradient.append('stop')
    .attr('offset', `${i * 100 / (COLOR_RANGE_HEX.length - 1)}%`)
    .attr('stop-color', color);
});

// Add rectangle with gradient
svg.append('rect')
  .attr('x', legendX)
  .attr('y', legendY)
  .attr('width', legendWidth)
  .attr('height', legendHeight)
  .style('fill', 'url(#legend-gradient)');

// Add labels for oldest and newest
svg.append('text')
  .attr('x', legendX)
  .attr('y', legendY + legendHeight + 15)
  .style('font-size', '10px')
  .text('Oldest');

svg.append('text')
  .attr('x', legendX + legendWidth)
  .attr('y', legendY + legendHeight + 15)
  .style('font-size', '10px')
  .attr('text-anchor', 'end')
  .text('Newest');
</script>
</body>
</html>"""

    # Replace placeholders with actual data
    html = html.replace('DATA_JSON_PLACEHOLDER', data_json)
    html = html.replace('PERCENTILE_THRESHOLDS_PLACEHOLDER', percentile_thresholds_json)
    html = html.replace('COLOR_RANGE_HEX_PLACEHOLDER', color_range_hex_json)
    html = html.replace('GREY_COLOR_HEX_PLACEHOLDER', GREY_COLOR_HEX)
    
    out_path = 'clustered_spaces_d3.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'D3 circle packing HTML written to {out_path}')
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

def main():
    min_pages = None
    spaces = []
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
    │   Version 1.2                                           │
    │                                                         │
    └─────────────────────────────────────────────────────────┘
    """)
    print("="*80 + "\n")
    print("Welcome to the Confluence Cluster Explorer!")
    while True:
        if min_pages is None:
            print("\nYou must set a minimum pages filter before loading data.")
            inp = input("Enter minimum pages per space (0 for all): ").strip()
            min_pages = int(inp) if inp else 0
            spaces = load_spaces(min_pages=min_pages)
            print(f"Loaded {len(spaces)} spaces from {TEMP_DIR} with >= {min_pages} pages.")
            continue
        print("\nMenu:")
        print("1. Change minimum pages filter and reload data (current: {} )".format(min_pages))
        print("2. Search for space key")
        print("3. Help (explain algorithms)")
        print("4. Visualize total pages per space (bar chart)")
        print(f"5. Set number of clusters manually (current: {n_clusters})")
        print("6. Semantic clustering and render HTML (Agglomerative)")
        print("7. Semantic clustering and render HTML (KMeans)")
        print("8. Semantic clustering and render HTML (DBSCAN)")
        print("9. Semantic clustering and D3 Circle Packing (Agglomerative)")
        print("10. Semantic clustering and D3 Circle Packing (KMeans)")
        print("11. Semantic clustering and D3 Circle Packing (DBSCAN)")
        print("Q. Quit")
        
        choice = input("Select option: ").strip()
        
        if choice == '1':
            inp = input("Enter minimum pages per space (0 for all): ").strip()
            min_pages = int(inp) if inp else 0
            spaces = load_spaces(min_pages=min_pages)
            print(f"Loaded {len(spaces)} spaces from {TEMP_DIR} with >= {min_pages} pages.")
        elif choice == '2':
            term = input("Enter search term: ").strip()
            results = search_spaces(spaces, term)
            if results:
                for k, n in results:
                    print(f"{k}: {n} pages")
            else:
                print("No matches found.")
        elif choice == '3':
            explain_algorithms()
        elif choice == '4':
            visualize_total_pages(spaces)
        elif choice == '5':
            try:
                new_clusters = int(input("Enter number of clusters (5-50 recommended): ").strip())
                if new_clusters < 2:
                    print("Number of clusters must be at least 2")
                else:
                    n_clusters = new_clusters
                    print(f"Number of clusters set to {n_clusters}")
            except ValueError:
                print("Please enter a valid number")
        elif choice.upper() == 'Q':
            print("Goodbye!")
            break
        elif choice == '6':
            try:
                print(f"Clustering {len(spaces)} spaces using semantic vectors (Agglomerative) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
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
        elif choice == '7':
            try:
                print(f"Clustering {len(spaces)} spaces using semantic vectors (KMeans) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
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
        elif choice == '8':
            try:
                print(f"Clustering {len(spaces)} spaces using semantic vectors (DBSCAN)...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
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
        elif choice == '9':
            try:
                print(f"Clustering {len(spaces)} spaces for D3 visualization (Agglomerative) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
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
        elif choice == '10':
            try:
                print(f"Clustering {len(spaces)} spaces for D3 visualization (KMeans) into {n_clusters} clusters...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
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
        elif choice == '11':
            try:
                print(f"Clustering {len(spaces)} spaces for D3 visualization (DBSCAN)...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
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
        else:
            print("Invalid option.")

if __name__ == '__main__':
    main()
