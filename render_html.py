import pickle
import json
import os
import sys
import webbrowser
import numpy as np
from datetime import datetime

OUTPUT_HTML = "confluence_treepack.html"
OUTPUT_PICKLE = "confluence_data.pkl"

# Color constants
GRADIENT_STEPS = 10  # Number of color steps
GREY_COLOR_HEX = '#cccccc'  # Color for spaces with no pages/timestamps


def rgb_to_hex(rgb_tuple):
    """Convert an RGB tuple to hex color string"""
    return f'#{int(rgb_tuple[0]):02x}{int(rgb_tuple[1]):02x}{int(rgb_tuple[2]):02x}'


def get_interpolated_color_from_fraction(fraction, gradient_colors_rgb_basis):
    """Interpolate between color basis points based on a fraction between 0-1"""
    if fraction <= 0:
        return gradient_colors_rgb_basis[0]
    if fraction >= 1:
        return gradient_colors_rgb_basis[-1]

    # Determine which segment of the gradient we're in
    segment_count = len(gradient_colors_rgb_basis) - 1
    segment_size = 1.0 / segment_count
    segment_index = min(int(fraction / segment_size), segment_count - 1)
    segment_fraction = (fraction - segment_index * segment_size) / segment_size

    # Interpolate between the two colors in this segment
    start_color = gradient_colors_rgb_basis[segment_index]
    end_color = gradient_colors_rgb_basis[segment_index + 1]

    return [
        start_color[0] + segment_fraction * (end_color[0] - start_color[0]),
        start_color[1] + segment_fraction * (end_color[1] - start_color[1]),
        start_color[2] + segment_fraction * (end_color[2] - start_color[2])
    ]

def calculate_color_data(data):
    """Extract avg timestamps and calculate color thresholds"""
    # Extract avg timestamps from all nodes with avg data
    def extract_avg_values(node, avg_values):
        if 'avg' in node and node['avg'] > 0:
            avg_values.append(node['avg'])
        if 'children' in node:
            for child in node['children']:
                extract_avg_values(child, avg_values)
        return avg_values

    avg_values = extract_avg_values(data, [])

    # Calculate percentile thresholds (if we have data)
    if avg_values:
        percentile_thresholds = [
            np.percentile(avg_values, 100 * i / GRADIENT_STEPS)
            for i in range(1, GRADIENT_STEPS)
        ]
    else:
        percentile_thresholds = []

    # Generate color gradient - pastel colors
    # RGB basis colors: oldest=pastel red, middle=pastel amber, newest=pastel green
    gradient_colors_rgb_basis = [
        [255, 179, 179],  # Oldest (pastel red)
        [255, 224, 179],  # Middle (pastel amber)
        [179, 255, 179]   # Newest (pastel green)
    ]

    # Generate color range
    color_range_hex = []
    for i in range(GRADIENT_STEPS):
        f = i / (GRADIENT_STEPS - 1) if GRADIENT_STEPS > 1 else 0.0
        rgb = get_interpolated_color_from_fraction(f, gradient_colors_rgb_basis)
        hex_color = rgb_to_hex(rgb)
        color_range_hex.append(hex_color)

    return percentile_thresholds, color_range_hex

def load_data_and_render():
    with open(OUTPUT_PICKLE, "rb") as f:
        data = pickle.load(f)

    # Calculate color thresholds and gradient
    percentile_thresholds, color_range_hex = calculate_color_data(data)

    # Prepare data for HTML embedding
    data_json = json.dumps(data)
    percentile_thresholds_json = json.dumps(percentile_thresholds)
    color_range_hex_json = json.dumps(color_range_hex)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Confluence Circle Packing</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body {{ margin:0; font-family:sans-serif; }}
    .node text {{ text-anchor:middle; alignment-baseline:middle; font-size:3pt; pointer-events:none; }}
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

const width = 1000, height = 800;
const root = d3.pack().size([width, height]).padding(3)(d3.hierarchy(data).sum(d => d.value));
const leaf = root.descendants().filter(d => d.data.avg !== undefined);
const svg = d3.select('#chart').append('svg').attr('width',width).attr('height',height);
const g = svg.selectAll('g').data(leaf).enter().append('g')
  .attr('transform', node => `translate(${{node.x}},${{node.y}})`);
g.append('circle')
  .attr('r', node => node.r)
  .attr('fill', node => node.data.avg > 0 ? colorScale(node.data.avg) : GREY_COLOR_HEX);
g.append('text')
  .attr('dy','-0.35em')
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:6pt;')
  .text(node => node.data.key);
g.append('text')
  .attr('dy','0.75em')
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:6pt;')
  .text(node => node.data.value);
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