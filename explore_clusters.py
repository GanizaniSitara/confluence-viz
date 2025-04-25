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

TEMP_DIR = 'temp'
DEFAULT_MIN_PAGES = 0

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
        for p in s.get('sampled_pages', []):
            body = p.get('body', '')
            if body:
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
            print(f"Space {space_key} has no usable text content after cleaning.")
    
    print(f"Found {spaces_with_content} out of {total_spaces} spaces with text content.")
    
    if not texts:
        raise ValueError("No spaces with non-empty text content for vectorization. Check if BeautifulSoup is installed or if the page content contains actual text.")
    
    vectorizer = TfidfVectorizer(max_features=512)
    X = vectorizer.fit_transform(texts)
    return X, valid_spaces

def cluster_spaces(spaces, method='agglomerative', n_clusters=5):
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

def render_html(spaces, labels, method, tags=None):
    html = ['<html><head><title>Clustered Spaces</title></head><body>']
    html.append(f'<h2>Clustering method: {method}</h2>')
    clusters = defaultdict(list)
    for s, label in zip(spaces, labels):
        clusters[label].append(s)
    for label, group in clusters.items():
        tag_str = f"Tags: {', '.join(tags[label])}" if tags and label in tags else ""
        html.append(f'<h3>Cluster {label} {tag_str}</h3><ul>')
        for s in group:
            html.append(f'<li>{s["space_key"]} (pages: {s.get("total_pages", len(s["sampled_pages"]))} )</li>')
        html.append('</ul>')
    html.append('</body></html>')
    out_path = 'clustered_spaces.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html))
    print(f'HTML written to {out_path}')
    webbrowser.open('file://' + os.path.abspath(out_path))

def render_d3_circle_packing(spaces, labels, method, tags=None):
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
            cluster_node['children'].append({
                'key': s['space_key'],
                'name': s['space_key'],
                'value': s.get('total_pages', len(s['sampled_pages'])),
            })
        d3_data['children'].append(cluster_node)
    data_json = json.dumps(d3_data)
    
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
  </style>
</head>
<body>
<div id="chart"></div>
<script>
const data = DATA_JSON_PLACEHOLDER;
const width = 1800, height = 1200;
const root = d3.pack()
  .size([width, height])
  .padding(6)
  (d3.hierarchy(data)
  .sum(d => d.value));
const svg = d3.select('#chart').append('svg')
  .attr('width', width)
  .attr('height', height);
const g = svg.selectAll('g')
  .data(root.descendants())
  .enter().append('g')
  .attr('transform', d => `translate(${d.x},${d.y})`);
g.append('circle')
  .attr('r', d => d.r)
  .attr('fill', d => d.children ? '#f8f8f8' : '#8ecae6')
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
</script>
</body>
</html>"""

    # Replace placeholder with actual data
    html = html.replace('DATA_JSON_PLACEHOLDER', data_json)
    
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
        
        # Filter out common stop words and short words
        stop_words = {'and', 'the', 'in', 'of', 'to', 'a', 'for', 'with', 'on', 'at'}
        filtered_words = [w for w in words if w not in stop_words and len(w) > 2]
        
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
    # Display a clear banner so we know the program is running
    print("\n" + "="*80)
    print("""
    ┌─────────────────────────────────────────────────────────┐
    │                                                         │
    │   CONFLUENCE CLUSTER EXPLORER                           │
    │   ===========================                           │
    │                                                         │
    │   Analyze and visualize Confluence spaces               │
    │   Version 1.0                                           │
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
        print("5. Semantic clustering and render HTML (Agglomerative)")
        print("6. Semantic clustering and render HTML (KMeans)")
        print("7. Semantic clustering and render HTML (DBSCAN)")
        print("8. Semantic clustering and D3 Circle Packing (Agglomerative)")
        print("9. Semantic clustering and D3 Circle Packing (KMeans)")
        print("10. Semantic clustering and D3 Circle Packing (DBSCAN)")
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
        elif choice.upper() == 'Q':
            print("Goodbye!")
            break
        elif choice == '5':
            try:
                print(f"Clustering {len(spaces)} spaces using semantic vectors (Agglomerative)...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'agglomerative')
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
        elif choice == '6':
            try:
                print(f"Clustering {len(spaces)} spaces using semantic vectors (KMeans)...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'kmeans')
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
        elif choice == '7':
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
        elif choice == '8':
            try:
                print(f"Clustering {len(spaces)} spaces for D3 visualization (Agglomerative)...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'agglomerative')
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
        elif choice == '9':
            try:
                print(f"Clustering {len(spaces)} spaces for D3 visualization (KMeans)...")
                if not spaces:
                    print("Error: No spaces loaded. Please load data first (Option 1).")
                    continue
                labels, valid_spaces = cluster_spaces(spaces, 'kmeans')
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
        elif choice == '10':
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
