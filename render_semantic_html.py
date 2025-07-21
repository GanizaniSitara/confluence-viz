# description: Renders semantic HTML for Confluence visualization.

import pickle
import json
import os
import sys
import webbrowser
import numpy as np
from datetime import datetime
from scipy.spatial import distance
import argparse
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config_loader import load_confluence_settings


OUTPUT_HTML = "confluence_semantic_treepack.html"
SEMANTIC_PICKLE = "confluence_semantic_data.pkl"

# Color constants
GRADIENT_STEPS = 10
GREY_COLOR_HEX = '#cccccc'

def rgb_to_hex(rgb_tuple):
    """Convert an RGB tuple to hex color string"""
    return f'#{int(rgb_tuple[0]):02x}{int(rgb_tuple[1]):02x}{int(rgb_tuple[2]):02x}'

def get_interpolated_color_from_fraction(fraction, gradient_colors_rgb_basis):
    """Interpolate between color basis points based on a fraction between 0-1"""
    if fraction <= 0:
        return gradient_colors_rgb_basis[0]
    if fraction >= 1:
        return gradient_colors_rgb_basis[-1]

    segment_count = len(gradient_colors_rgb_basis) - 1
    segment_size = 1.0 / segment_count
    segment_index = min(int(fraction / segment_size), segment_count - 1)
    segment_fraction = (fraction - segment_index * segment_size) / segment_size

    start_color = gradient_colors_rgb_basis[segment_index]
    end_color = gradient_colors_rgb_basis[segment_index + 1]

    return [
        start_color[0] + segment_fraction * (end_color[0] - start_color[0]),
        start_color[1] + segment_fraction * (end_color[1] - start_color[1]),
        start_color[2] + segment_fraction * (end_color[2] - start_color[2])
    ]

def calculate_color_data(data):
    """Extract avg timestamps and calculate color thresholds"""
    def extract_avg_values(node, avg_values):
        if 'avg' in node and node['avg'] > 0:
            avg_values.append(node['avg'])
        if 'children' in node:
            for child in node['children']:
                extract_avg_values(child, avg_values)
        return avg_values

    avg_values = extract_avg_values(data, [])

    if avg_values:
        percentile_thresholds = [
            np.percentile(avg_values, 100 * i / GRADIENT_STEPS)
            for i in range(1, GRADIENT_STEPS)
        ]
    else:
        percentile_thresholds = []

    # Pastel gradient: red → amber → green
    gradient_colors_rgb_basis = [
        [255, 179, 179],  # Oldest (pastel red)
        [255, 224, 179],  # Middle (pastel amber)
        [179, 255, 179]   # Newest (pastel green)
    ]

    color_range_hex = []
    for i in range(GRADIENT_STEPS):
        f = i / (GRADIENT_STEPS - 1) if GRADIENT_STEPS > 1 else 0.0
        rgb = get_interpolated_color_from_fraction(f, gradient_colors_rgb_basis)
        hex_color = rgb_to_hex(rgb)
        color_range_hex.append(hex_color)

    return percentile_thresholds, color_range_hex

def reorganize_data_by_similarity(data, vector_map):
    """Reorganize data structure to group similar spaces together"""
    # Extract spaces with vectors
    spaces_with_vectors = []

    def extract_spaces(node, parent_path=[]):
        current_path = parent_path + [node.get('key', 'root')]
        if 'key' in node and node['key'] in vector_map:
            spaces_with_vectors.append({
                'key': node['key'],
                'value': node.get('value', 0),
                'avg': node.get('avg', 0),
                'vector': vector_map[node['key']],
                'path': current_path
            })
        if 'children' in node:
            for child in node['children']:
                extract_spaces(child, current_path)

    extract_spaces(data)

    # No vectors, return original data
    if not spaces_with_vectors:
        return data

    # Calculate similarity matrix
    space_keys = [s['key'] for s in spaces_with_vectors]
    vectors = np.array([s['vector'] for s in spaces_with_vectors])

    # Compute pairwise distances
    dist_matrix = distance.pdist(vectors, 'cosine')
    dist_matrix = distance.squareform(dist_matrix)

    # Perform hierarchical clustering (simplified approximation)
    # This is a fast implementation that groups similar spaces
    groups = {}
    assigned = set()

    for i, space in enumerate(spaces_with_vectors):
        if space['key'] in assigned:
            continue

        # Create a new group
        group_key = f"group_{len(groups)}"
        groups[group_key] = {
            'key': group_key,
            'name': f"Topic Group {len(groups)+1}",
            'children': [space],
            'value': space['value'],
            'avg': space['avg']
        }
        assigned.add(space['key'])

        # Find similar spaces
        for j in range(len(spaces_with_vectors)):
            if i == j or spaces_with_vectors[j]['key'] in assigned:
                continue

            # If similarity is high enough, add to group
            if dist_matrix[i, j] < 0.5:  # Threshold for similarity
                groups[group_key]['children'].append(spaces_with_vectors[j])
                groups[group_key]['value'] += spaces_with_vectors[j]['value']
                # Update group average if both have timestamps
                if groups[group_key]['avg'] > 0 and spaces_with_vectors[j]['avg'] > 0:
                    groups[group_key]['avg'] = (groups[group_key]['avg'] + spaces_with_vectors[j]['avg']) / 2
                elif spaces_with_vectors[j]['avg'] > 0:
                    groups[group_key]['avg'] = spaces_with_vectors[j]['avg']
                assigned.add(spaces_with_vectors[j]['key'])

    # Create new hierarchical structure
    new_data = {
        'key': 'root',
        'name': 'Confluence Spaces',
        'children': list(groups.values())
    }

    # Add any unassigned spaces directly to root
    for space in spaces_with_vectors:
        if space['key'] not in assigned:
            new_data['children'].append({
                'key': space['key'],
                'name': space['key'],
                'value': space['value'],
                'avg': space['avg']
            })

    return new_data

def load_data_and_render():
    parser = argparse.ArgumentParser(description="Render semantic Confluence visualization as HTML.")
    parser.add_argument('--min-pages', type=int, default=0, help='Minimum number of pages for a space to be included')
    args = parser.parse_args()

    with open(SEMANTIC_PICKLE, "rb") as f:
        pickle_data = pickle.load(f)

    # Extract data components
    if isinstance(pickle_data, dict) and 'data' in pickle_data and 'vector_map' in pickle_data:
        data = pickle_data['data']
        vector_map = pickle_data['vector_map']
    else:
        data = pickle_data
        vector_map = {}

    # Filter spaces with less than min-pages
    def filter_spaces(node):
        if 'children' in node:
            node['children'] = [filter_spaces(child) for child in node['children'] if filter_spaces(child) is not None]
        if 'value' in node and node['value'] < args.min_pages:
            return None
        return node
    if args.min_pages > 0:
        data = filter_spaces(data)
        if data is None:
            print(f"No spaces with >= {args.min_pages} pages.")
            sys.exit(0)

    # Reorganize data based on semantic similarity
    semantic_data = reorganize_data_by_similarity(data, vector_map)

    # Calculate color thresholds and gradient
    percentile_thresholds, color_range_hex = calculate_color_data(semantic_data)

    # Prepare data for HTML embedding
    data_json = json.dumps(semantic_data)
    percentile_thresholds_json = json.dumps(percentile_thresholds)
    color_range_hex_json = json.dumps(color_range_hex)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Semantic Confluence Circle Packing</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body {{ margin:0; font-family:sans-serif; }}
    .node text {{ text-anchor:middle; alignment-baseline:middle; font-size:3pt; pointer-events:none; }}
    .group circle {{ stroke: #555; stroke-width: 1px; }}
  </style>
</head>
<body>
<div id="chart"></div>
<script>
const data = {data_json};
const PERCENTILE_THRESHOLDS = {percentile_thresholds_json};
const COLOR_RANGE_HEX = {color_range_hex_json};
const GREY_COLOR_HEX = '{GREY_COLOR_HEX}';

// Color scale based on thresholds
const colorScale = d3.scaleThreshold()
  .domain(PERCENTILE_THRESHOLDS)
  .range(COLOR_RANGE_HEX);

const width = 3000, height = 2000;
const root = d3.pack()
  .size([width, height])
  .padding(6)
  (d3.hierarchy(data)
  .sum(d => d.value));

const svg = d3.select('#chart').append('svg')
  .attr('width', width)
  .attr('height', height);

// Create groups for all nodes
const g = svg.selectAll('g')
  .data(root.descendants())
  .enter().append('g')
  .attr('transform', d => `translate(${d.x},${d.y})`);

// Add circles
g.append('circle')
  .attr('r', d => d.r)
  .attr('fill', d => {
    // Use grey for nodes with no avg
    if (!d.data.avg || d.data.avg <= 0) return GREY_COLOR_HEX;

    // Use color scale for leaf nodes and transparent for group nodes
    if (!d.children) return colorScale(d.data.avg);

    // For group nodes (that have children), use a very light fill
    return '#f8f8f8';
  })
  .attr('class', d => d.children ? 'group' : 'leaf');

// Add text labels for key and value, but only for leaf nodes
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

// Add group labels (only for groups that contain other nodes)
const groupNodes = g.filter(d => d.depth > 0 && d.children);

groupNodes.append('text')
  .attr('dy', 0)
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:8pt; font-weight:bold;')
  .text(d => d.data.name || d.data.key);
</script>
</body>
</html>"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML written to {OUTPUT_HTML}")

    # Open the HTML file in the default browser
    try:
        print(f"Opening {OUTPUT_HTML} in browser...")
        webbrowser.open('file://' + os.path.realpath(OUTPUT_HTML))
    except Exception as e:
        print(f"Could not automatically open browser: {e}", file=sys.stderr)
        print(f"Please open the file manually: {os.path.realpath(OUTPUT_HTML)}")

if __name__ == "__main__":
    load_data_and_render()