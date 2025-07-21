#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File: viz.py
# description: Main script for Confluence visualization.

import requests
import time
import json
import sys
import os
import webbrowser
import urllib3
import math # Used for percentile calculation
from datetime import datetime

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
# Load Confluence settings
from config_loader import load_confluence_settings
settings = load_confluence_settings()

CONFLUENCE_BASE_URL = settings['base_url'] # Changed from api_base_url
API_ENDPOINT = "/rest/api" # Define the API endpoint suffix
USERNAME = settings['username']
PASSWORD = settings['password']
VERIFY_SSL = settings['verify_ssl']
SPACES_PAGE_LIMIT = 50
CONTENT_PAGE_LIMIT = 100
GRADIENT_STEPS = 10  # Number of percentile bins/color steps
OUTPUT_JSON = "confluence_data.json"
OUTPUT_HTML = "confluence_treepack.html"

# Suppress InsecureRequestWarning if VERIFY_SSL is False
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define the gradient colors: Red (oldest/lowest percentile) -> Yellow -> Green (newest/highest percentile)
# This list defines the colors assigned to the 10 bins.
# The first color is for the 0-10th percentile bin, the last color is for the 90-100th percentile bin.
GRADIENT_COLORS_HEX = ['#ffcccc', '#ffddcc', '#ffffcc', '#ddffcc', '#ccffcc',
                       '#ccffdd', '#ccffff', '#ccddee', '#ccddff', '#ccccff'] # Example 10-step gradient

# You could use a simpler 3-color gradient and let D3 interpolate across the 10 steps:
# GRADIENT_COLORS_FOR_INTERP = ['#ffcccc', '#ffffcc', '#ccffcc'] # Red (old) -> Yellow (mid) -> Green (new)
# If using this, D3's scaleThreshold range needs to interpolate:
# range(d3.range(STEPS).map(i => d3.interpolateRgbBasis(GRADIENT_COLORS_FOR_INTERP)(i/(STEPS-1))))
# Let's stick to defining 10 distinct colors for clarity with 10 bins.
# Reverting to a clear 3-color basis for simplicity as the requirement is 10 steps but a smooth gradient is implied.
# Let's generate 10 colors from the 3-color basis for the range of scaleThreshold.
GRADIENT_COLORS_FOR_INTERP = ['#ffcccc', '#ffffcc', '#ccffcc'] # Red (old) -> Yellow (mid) -> Green (new)
# These will be generated and passed to the range of d3.scaleThreshold

# Grey color for spaces with no pages (avg timestamp 0)
GREY_COLOR_HEX = '#cccccc'

# ------------------------------------------------------------------
# Color and Percentile Utility Functions
# ------------------------------------------------------------------
def hex_to_rgb(hex_color):
    """Converts a hex color string (e.g., '#RRGGBB') to an RGB tuple."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2 ,4))

def rgb_to_hex(rgb_color):
    """Converts an RGB tuple to a hex color string."""
    return '#{:02x}{:02x}{:02x}'.format(rgb_color[0], rgb_color[1], rgb_color[2])

def lerp_rgb(color1, color2, t):
    """Linear interpolation between two RGB colors. t is between 0 and 1."""
    return tuple(max(0, min(255, round(color1[i] + t * (color2[i] - color1[i])))) for i in range(3))

def get_interpolated_color_from_fraction(f, colors_rgb_stops):
    """
    Gets a color from a gradient defined by color stops, based on fraction f (0 to 1).
    Uses linear interpolation between stops. f=0 maps to first color, f=1 maps to last.
    """
    num_stops = len(colors_rgb_stops)
    if num_stops == 0:
        return (0, 0, 0) # Default black
    # Clamp f to [0, 1]
    f = max(0.0, min(1.0, f))

    if num_stops == 1:
        return colors_rgb_stops[0]
    if f == 1.0: # Handle f=1.0 explicitly to ensure we get the last color
        return colors_rgb_stops[-1]

    # Calculate which segment of the gradient f falls into
    # int(f * (num_stops - 1)) gives the index of the start color for interpolation
    segment_index = int(f * (num_stops - 1))
     # Ensure segment_index doesn't exceed the second-to-last stop
    segment_index = min(segment_index, num_stops - 2)

    # Normalize position within the segment (0 to 1)
    # The total distance is num_stops - 1 segments.
    # The fraction f covers `f * (num_stops - 1)` segments.
    # The fraction within the current segment is this total minus the completed segments (segment_index)
    t = (f * (num_stops - 1)) - segment_index

    return lerp_rgb(colors_rgb_stops[segment_index], colors_rgb_stops[segment_index + 1], t)


def calculate_percentile_thresholds(data, num_bins):
    """
    Calculates the threshold values that divide sorted data into num_bins parts.
    Returns num_bins - 1 threshold values. Data must be sorted.
    Returns an empty list if data is empty or has fewer elements than num_bins - 1.
    """
    data = sorted(data) # Ensure data is sorted
    n = len(data)
    if n == 0 or num_bins <= 1:
        return [] # Cannot calculate thresholds

    # We need num_bins - 1 thresholds to create num_bins bins.
    # Threshold i (0-indexed) is at the k-th percentile, where k = (i+1) * (100 / num_bins)
    # The index in the sorted list for the p-th percentile is (p / 100) * (n - 1)
    # For num_bins, we need thresholds at 1/num_bins, 2/num_bins, ..., (num_bins-1)/num_bins fractions.
    # Threshold i corresponds to the (i+1) / num_bins cumulative fraction.
    # Index in sorted data = ((i + 1) / num_bins) * (n - 1) for i from 0 to num_bins - 2.

    thresholds = []
    for i in range(num_bins - 1):
        rank = ((i + 1) / num_bins) * (n - 1)
        if rank < 0: # Should not happen with i >= 0
             rank = 0
        if rank >= n - 1: # Should not happen with i < num_bins - 1 unless n is too small
             rank = n - 1

        # Linear interpolation for fractional ranks
        lower_idx = math.floor(rank)
        upper_idx = math.ceil(rank)

        if lower_idx == upper_idx:
            thresholds.append(data[lower_idx])
        else:
            # Interpolate between the two values
            weight = rank - lower_idx
            interpolated_value = data[lower_idx] * (1 - weight) + data[upper_idx] * weight
            thresholds.append(interpolated_value)

    return thresholds

def get_color_for_avg_timestamp_percentile(avg_timestamp, percentile_thresholds, color_range_hex, default_color_hex=GREY_COLOR_HEX):
    """
    Calculates the hex color for an average timestamp based on which percentile
    bin it falls into defined by the thresholds.
    """
    if avg_timestamp == 0: # Special case for spaces with no pages
        return default_color_hex

    # If there are no thresholds (e.g., < 2 spaces with pages, or num_bins <= 1),
    # or if all non-zero timestamps are the same, they all fall into one bin.
    if not percentile_thresholds:
         # Assign the color for the highest percentile (Green/newest) if no thresholds can be calculated, assuming it's 'relatively' newest
         return color_range_hex[-1] if color_range_hex else GREY_COLOR_HEX

    # Find which bin the timestamp falls into
    # scaleThreshold domain [t0, t1, t2] maps
    # < t0 -> range[0]
    # >= t0 and < t1 -> range[1]
    # >= t1 and < t2 -> range[2]
    # >= t2 -> range[3]
    # So, bin index is the count of thresholds the timestamp is >=
    bin_index = 0
    for threshold in percentile_thresholds:
        if avg_timestamp >= threshold:
            bin_index += 1
        else:
            break # Found the bin

    # Clamp bin_index to the available color range indices
    bin_index = max(0, min(bin_index, len(color_range_hex) - 1))

    return color_range_hex[bin_index]


# ------------------------------------------------------------------
# HTTP helper
# ------------------------------------------------------------------
def get_with_retry(url, params=None, auth=None, verify=False):
    backoff = 1
    while True:
        resp = requests.get(url, params=params, auth=auth, verify=verify)
        if resp.status_code == 429:
            print(f"Warning: Rate limited (429). Retrying {url} in {backoff}s...", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        if resp.status_code >= 400:
            print(f"Error {resp.status_code} fetching {url}. Response: {resp.text}", file=sys.stderr) # Print response body on error
        return resp

# ------------------------------------------------------------------
# Fetch all spaces, exclude user spaces
# ------------------------------------------------------------------
def fetch_all_spaces():
    print("Fetching all spaces...")
    spaces = []
    start = 0
    idx = 0
    while True:
        url = f"{CONFLUENCE_BASE_URL}{API_ENDPOINT}/space" # Construct URL using CONFLUENCE_BASE_URL and API_ENDPOINT
        params = {"start": start, "limit": SPACES_PAGE_LIMIT}
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch spaces. Status code: {r.status_code}", file=sys.stderr)
            break
        results = r.json().get("results", [])
        if not results:
            break
        for sp in results:
            if sp.get("key", "").startswith("~"):  # Exclude user spaces
                print(f"Skipping user space: key={sp.get('key')}, name={sp.get('name')}")
                continue
            idx += 1
            print(f"[{idx}] Fetched space: key={sp.get('key')}, name={sp.get('name')}")
            spaces.append({"key": sp.get("key"), "name": sp.get("name")})
        if len(results) < SPACES_PAGE_LIMIT:
            break
        start += SPACES_PAGE_LIMIT
    print(f"Finished fetching spaces. Total fetched: {len(spaces)}")
    return spaces

# ------------------------------------------------------------------
# Fetch pages and timestamps
# ------------------------------------------------------------------
def fetch_page_data_for_space(space_key):
    count = 0
    timestamps = []
    start = 0
    print(f"  Fetching pages for space: {space_key}")
    while True:
        url = f"{CONFLUENCE_BASE_URL}{API_ENDPOINT}/content" # Construct URL using CONFLUENCE_BASE_URL and API_ENDPOINT
        params = {"type": "page", "spaceKey": space_key,
                  "start": start, "limit": CONTENT_PAGE_LIMIT, "expand": "version"}
        r = get_with_retry(url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
             print(f"  Failed to fetch pages for space {space_key}. Status code: {r.status_code}", file=sys.stderr)
             break # Exit loop on error
        pages = r.json().get("results", [])
        if not pages and start == 0:
             print(f"  No pages found for space: {space_key}")
             break # Exit loop if no pages at all
        if not pages:
             print(f"  No more pages found for space: {space_key} on subsequent pages.")
             break # Exit loop if no results on a page
        for p in pages:
            when = p.get("version", {}).get("when")
            if when:
                try:
                    ts = datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
                    timestamps.append(ts)
                except ValueError:
                    print(f"Warning: Could not parse timestamp '{when}' for page ID {p.get('id')} in space {space_key}", file=sys.stderr)
                    pass # Skip invalid timestamps
        count += len(pages)
        # print(f"  Fetched {len(pages)} pages for {space_key}. Total count for space: {count}") # Too verbose?
        if len(pages) < CONTENT_PAGE_LIMIT:
            break # Exit loop if this is the last page
        start += CONTENT_PAGE_LIMIT
    # print(f"  Finished fetching pages for space {space_key}. Total pages: {count}") # Keep main loop for summary
    return count, timestamps

# ------------------------------------------------------------------
# Prepare data for D3
# ------------------------------------------------------------------
def build_data(spaces):
    return {"name": "Confluence", "children": spaces}

# ------------------------------------------------------------------
# Main execution
# ------------------------------------------------------------------
def main():
    spaces = fetch_all_spaces()
    all_ts = [] # This list contains ALL individual page edit timestamps (used only for overall min/max print now)
    total_pages_fetched = 0 # Keep track of total pages

    if not spaces:
        print("No spaces fetched. Cannot proceed with page fetching or visualization. Exiting.")
        return

    print("\nStarting page data fetching for each space...")
    space_avg_timestamps = [] # List to store average timestamps for percentile calculation
    for idx, sp in enumerate(spaces, start=1):
        print(f"Processing space {idx}/{len(spaces)}: key={sp['key']} ({sp['name']})")
        count, ts = fetch_page_data_for_space(sp["key"])
        # Calculate AVERAGE timestamp for this space. Avg is 0 if no timestamps found.
        avg = (sum(ts) / len(ts)) if ts else 0
        sp["value"] = count # Use count as size
        sp["avg"] = avg     # Store avg timestamp for color mapping in D3
        all_ts.extend(ts) # Add ALL individual timestamps to the collective list (only for overall min/max print now)
        total_pages_fetched += count
        if avg > 0: # Only include spaces with pages/valid timestamps in percentile calculation
            space_avg_timestamps.append(avg)

        try:
            avg_iso = datetime.fromtimestamp(avg).isoformat(sep=' ', timespec='seconds') if avg > 0 else 'N/A'
        except (ValueError, OSError):
            avg_iso = "Invalid Timestamp"
        print(f"  Finished processing space {sp['key']}. Pages: {count}, Avg Last Edit Timestamp: {avg:.4f} ({avg_iso})")

    print(f"\nFinished processing all spaces. Total pages fetched across all spaces: {total_pages_fetched}")

    # Overall minT and maxT are from ALL page edit timestamps (still printed for info)
    minT_overall, maxT_overall = (min(all_ts), max(all_ts)) if all_ts else (0, 0)
    try:
        minT_iso = datetime.fromtimestamp(minT_overall).isoformat(sep=' ', timespec='seconds') if minT_overall > 0 else 'N/A'
        maxT_iso = datetime.fromtimestamp(maxT_overall).isoformat(sep=' ', timespec='seconds') if maxT_overall > 0 else 'N/A'
    except (ValueError, OSError):
         minT_iso = maxT_iso = "Invalid Timestamp"

    print(f"\nOverall Min Timestamp (Oldest Page Edit): {minT_overall:.4f} ({minT_iso})")
    print(f"Overall Max Timestamp (Newest Page Edit): {maxT_overall:.4f} ({maxT_iso})")


    # ------------------------------------------------------------------
    # Calculate Percentile Thresholds for Space Average Timestamps
    # ------------------------------------------------------------------
    print("\n--- Percentile Thresholds for Space Average Timestamps ---")
    num_spaces_with_avg = len(space_avg_timestamps)
    print(f"Calculating {GRADIENT_STEPS} color bins based on {num_spaces_with_avg} spaces with page activity.")

    if num_spaces_with_avg < GRADIENT_STEPS:
         print(f"Warning: Fewer spaces with page activity ({num_spaces_with_avg}) than gradient steps ({GRADIENT_STEPS}).")
         # In this case, each space gets a unique color from the gradient.
         # We still need thresholds for scaleThreshold, but they won't divide evenly.
         # Let's generate thresholds that place each space into its own bin relative to others.
         # Sort unique average timestamps and use them as thresholds.
         unique_avg_timestamps = sorted(list(set(space_avg_timestamps)))
         # We need len(unique_avg_timestamps) - 1 thresholds if each unique value is a bin boundary.
         # For n colors in range, scaleThreshold needs n-1 thresholds in domain.
         # If we want `GRADIENT_STEPS` colors/bins, we need `GRADIENT_STEPS - 1` thresholds.
         # If we have fewer than GRADIENT_STEPS unique timestamps, use those as thresholds.
         percentile_thresholds = unique_avg_timestamps[ : GRADIENT_STEPS - 1]
         print(f"Using {len(percentile_thresholds)} unique average timestamps as thresholds:")
         for i, ts in enumerate(percentile_thresholds):
              try:
                  iso_time = datetime.fromtimestamp(ts).isoformat(sep=' ', timespec='seconds')
              except (ValueError, OSError):
                  iso_time = "Invalid Timestamp"
              print(f"  Threshold {i+1}: {ts:.4f} ({iso_time})")


    elif num_spaces_with_avg == 0:
        percentile_thresholds = []
        print("No spaces with page activity found. No percentile thresholds calculated.")
    else:
         # Calculate num_bins - 1 thresholds (percentiles)
         # These thresholds divide the *sorted average timestamps* into num_bins sections.
         # For 10 bins (GRADIENT_STEPS=10), we need 9 thresholds (10th, 20th, ..., 90th percentiles).
         percentile_thresholds = calculate_percentile_thresholds(space_avg_timestamps, GRADIENT_STEPS)
         print(f"Calculated {len(percentile_thresholds)} percentile thresholds:")
         print("Index | Timestamp          | ISO Format")
         print("------|--------------------|----------------------")
         for i, ts in enumerate(percentile_thresholds):
              try:
                  iso_time = datetime.fromtimestamp(ts).isoformat(sep=' ', timespec='seconds')
              except (ValueError, OSError):
                  iso_time = "Invalid Timestamp"
              print(f"{i:<5} | {ts:<18.4f} | {iso_time}")

    # ------------------------------------------------------------------
    # Generate the actual 10 colors for the color range
    # These are derived from the Red->Yellow->Green basis
    # ------------------------------------------------------------------
    gradient_colors_rgb_basis = [hex_to_rgb(c) for c in GRADIENT_COLORS_FOR_INTERP]
    color_range_hex = []
    print(f"\nGenerating {GRADIENT_STEPS} colors for the gradient (Red=Old, Green=New):")
    print("Index | Color (RGB Hex)")
    print("------|----------------")
    # Calculate the 10 colors by interpolating the 3 basis colors
    for i in range(GRADIENT_STEPS):
        # f goes from 0 to 1 across the STEPS colors
        f = i / (GRADIENT_STEPS - 1) if GRADIENT_STEPS > 1 else 0.0 # Handle STEPS=1
        rgb = get_interpolated_color_from_fraction(f, gradient_colors_rgb_basis)
        hex_color = rgb_to_hex(rgb)
        color_range_hex.append(hex_color)
        print(f"{i:<5} | {hex_color}")
    print("----------------------------\n")


    # ------------------------------------------------------------------
    # Print Average Edit Time, Space Key, and Assigned Color per Space
    # (Based on Percentile Bin)
    # ------------------------------------------------------------------
    print("\n--- Space Coloring Details (Based on Percentile Bin) ---")
    if not spaces:
        print("No space data to display coloring details.")
    else:
        # Determine the color range to use for spaces with avg > 0
        effective_color_range = color_range_hex if color_range_hex else [GREY_COLOR_HEX] # Use grey if no gradient colors generated

        print("Avg Edit Timestamp   | Space Key   | Assigned Color (Hex)")
        print("---------------------|-------------|----------------------")
        # Sort spaces by average timestamp for easier review, newest first
        sorted_spaces = sorted(spaces, key=lambda x: x.get('avg', 0), reverse=True)

        for sp in sorted_spaces:
            avg_ts = sp.get('avg', 0)
            space_key = sp.get('key', 'N/A')
            try:
                avg_iso = datetime.fromtimestamp(avg_ts).isoformat(sep=' ', timespec='seconds') if avg_ts > 0 else 'N/A'
            except (ValueError, OSError):
                 avg_iso = "Invalid Timestamp"

            # Get the color assigned to this average timestamp based on its percentile bin
            # Use the calculated percentile thresholds
            assigned_color = get_color_for_avg_timestamp_percentile(
                avg_ts, percentile_thresholds, effective_color_range, default_color_hex=GREY_COLOR_HEX
            )

            print(f"{avg_ts:<20.4f} | {space_key:<11} | {assigned_color}")
        print("-----------------------------\n")


    # ------------------------------------------------------------------
    # Write JSON and Generate HTML
    # ------------------------------------------------------------------

    data = build_data(spaces)
    print(f"Writing data to {OUTPUT_JSON}...")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as jf:
        json.dump(data, jf, indent=2)
    print(f"Successfully created {OUTPUT_JSON}")

    print(f"Generating HTML output to {OUTPUT_HTML}...")
    data_json = json.dumps(data)
    percentile_thresholds_json = json.dumps(percentile_thresholds)
    color_range_hex_json = json.dumps(color_range_hex)

    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\">
  <title>Confluence Circle Packing</title>
  <script src=\"https://d3js.org/d3.v7.min.js\"></script>
  <style>
    body {{ margin:0; font-family:sans-serif; }}
    /* Ensure font is 6pt and text is centered */
    .node text {{ text-anchor:middle; alignment-baseline:middle; font-size:3pt; pointer-events:none; }}
  </style>
</head>
<body>
<div id=\"chart\"></div>
<script>
// Data embed
const data = {data_json};
// const minT = {minT_overall}, maxT = {maxT_overall}; // Not used for coloring scale anymore
const PERCENTILE_THRESHOLDS = {percentile_thresholds_json}; // Thresholds calculated from space average timestamps
const COLOR_RANGE_HEX = {color_range_hex_json}; // 10 colors for the 10 bins
const GREY_COLOR_HEX = '{GREY_COLOR_HEX}'; // Pass grey color to JS

// Color scale based on thresholds (percentiles of space average timestamps)
// This assigns colors to bins based on where a space's average timestamp falls
// compared to other spaces' average timestamps.
// scaleThreshold maps:
// value < threshold[0] -> range[0]
// value >= threshold[0] and < threshold[1] -> range[1]
// ...
// value >= threshold[n-1] -> range[n]
// It needs n thresholds for n+1 bins/colors. Our setup has STEPS-1 thresholds for STEPS bins.
// If PERCENTILE_THRESHOLDS has T thresholds, scaleThreshold().domain(T) range needs T+1 colors.
// We calculated STEPS-1 thresholds and generated STEPS colors. This fits.
const colorScale = d3.scaleThreshold().domain(PERCENTILE_THRESHOLDS).range(COLOR_RANGE_HEX);

// Pack layout
const width = 3000, height = 2000;
const root = d3.pack().size([width, height]).padding(6)(d3.hierarchy(data).sum(d => d.value));
const svg = d3.select('#chart').append('svg').attr('width', width).attr('height', height);


// Render - using node.data.avg for coloring
const g = svg.selectAll('g').data(leaf).enter().append('g')
  .attr('transform', node => `translate(${{node.x}},${{node.y}})`);

g.append('circle')
  .attr('r', node => node.r)
  // Use the space's average edit time (node.data.avg) to get color from the percentile scale
  // Use grey for nodes with avg_last_edit == 0 (spaces with no pages or no timestamp data)
  .attr('fill', node => node.data.avg > 0 ? colorScale(node.data.avg) : GREY_COLOR_HEX);

// Add text labels for key and value
g.append('text')
  // Text positioning attributes are handled by CSS (text-anchor:middle, alignment-baseline:middle)
  .attr('dy','-0.35em') // Vertical offset for the key
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:6pt;')
  .text(node => node.data.key);

g.append('text')
  // Text positioning attributes are handled by CSS
  .attr('dy','0.75em') // Vertical offset for the value
  .attr('text-anchor', 'middle')
  .attr('style', 'font-size:6pt;')
  .text(node => node.data.value);

// Optional: Add tooltips or legend if needed

</script>
</body>
</html>"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as hf:
        hf.write(html)
    print(f"Successfully created {OUTPUT_HTML}")

    # Open the HTML file automatically
    try:
        print(f"Opening {OUTPUT_HTML} in browser...")
        webbrowser.open('file://' + os.path.realpath(OUTPUT_HTML))
    except Exception as e:
        print(f"Could not automatically open browser: {e}", file=sys.stderr)
        print(f"Please open the file manually: {os.path.realpath(OUTPUT_HTML)}")


if __name__ == '__main__':
    main()