#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import json
import sys

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
CONFLUENCE_URL = "http://192.168.65.128:8090"  # e.g., "https://confluence.company.com"
USERNAME = "admin"
PASSWORD = "admin"

# Disable SSL certificate validation (set to False in production at your own risk)
VERIFY_SSL = False

# Number of spaces to fetch at a time
SPACES_PAGE_LIMIT = 50

# Output file names
OUTPUT_JSON = "confluence_data.json"
OUTPUT_HTML = "confluence_treepack.html"


# ------------------------------------------------------------------
# Helper: Handle 429 with a simple backoff
# ------------------------------------------------------------------
def get_with_retry(url, params=None, headers=None, auth=None, verify=False):
    """
    Makes a GET request, if 429 is encountered, waits and retries.
    """
    backoff = 1
    while True:
        resp = requests.get(url, params=params, headers=headers, auth=auth, verify=verify)
        if resp.status_code == 429:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        elif resp.status_code >= 400:
            print(f"Error {resp.status_code} fetching {url}", file=sys.stderr)
        return resp


# ------------------------------------------------------------------
# Step 1: Fetch all spaces
# ------------------------------------------------------------------
def fetch_all_spaces():
    """
    Returns a list of space objects: [{'key': ..., 'name': ...}, ...]
    """
    all_spaces = []
    start_index = 0

    while True:
        url = f"{CONFLUENCE_URL}/rest/api/space"
        params = {
            "start": start_index,
            "limit": SPACES_PAGE_LIMIT
        }
        r = get_with_retry(
            url=url,
            params=params,
            auth=(USERNAME, PASSWORD),
            verify=VERIFY_SSL
        )
        if r.status_code != 200:
            print(f"Failed to fetch spaces at start={start_index}, status={r.status_code}")
            break

        data = r.json()
        spaces = data.get("results", [])
        for sp in spaces:
            all_spaces.append({
                "key": sp.get("key", ""),
                "name": sp.get("name", "")
            })

        if len(spaces) < SPACES_PAGE_LIMIT:
            break
        start_index += SPACES_PAGE_LIMIT

    return all_spaces


# ------------------------------------------------------------------
# Step 2: For each space, fetch total page count
# ------------------------------------------------------------------
def fetch_page_count_for_space(space_key):
    """
    Uses /rest/api/content?type=page&spaceKey=XYZ&limit=0 to get total page count.
    Returns an integer count or 0 if error.
    """
    url = f"{CONFLUENCE_URL}/rest/api/content"
    params = {
        "type": "page",
        "spaceKey": space_key,
        "limit": 0
    }
    r = get_with_retry(
        url=url,
        params=params,
        auth=(USERNAME, PASSWORD),
        verify=VERIFY_SSL
    )
    if r.status_code == 200:
        data = r.json()
        return data.get("size", 0)
    return 0


# ------------------------------------------------------------------
# Step 3: Build data structure for circle packing
# ------------------------------------------------------------------
def build_circle_packing_data(spaces):
    """
    Returns a dict suitable for D3 circle packing:
    { 'name': 'Confluence', 'children': [ {'name': ..., 'value': ...}, ... ] }
    """
    root = {
        "name": "Confluence",
        "children": []
    }
    for sp in spaces:
        count = sp.get("pageCount", 0)
        if count > 0:
            root["children"].append({
                "name": sp["name"],
                "value": count
            })
    return root


# ------------------------------------------------------------------
# Step 4: Generate the HTML file referencing the JSON data
# ------------------------------------------------------------------
def write_html_file():
    """
    Writes an HTML file that loads and displays the circle packing from confluence_data.json.
    Filters to only leaf nodes so the root and intermediate circles don’t render.
    """
    html_content = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Confluence Circle Packing</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body {
      margin: 0;
      font-family: sans-serif;
    }
    .node text {
      text-anchor: middle;
      alignment-baseline: middle;
      font-size: 12px;
      pointer-events: none;
    }
    .node circle {
      stroke: #999;
      fill: #ccc;
    }
  </style>
</head>
<body>
<div id="chart"></div>

<script>
// Load data
fetch('confluence_data.json')
  .then(resp => resp.json())
  .then(data => {
    const width = 1000, height = 800;
    const root = d3.pack()
      .size([width, height])
      .padding(3)(
        d3.hierarchy(data)
          .sum(d => d.value)
      );

    const svg = d3.select("#chart")
      .append("svg")
      .attr("width", width)
      .attr("height", height);

    // Only leaf nodes
    const leaf = root.descendants().filter(d => !d.children);

    const nodes = svg.selectAll("g")
      .data(leaf)
      .enter()
      .append("g")
      .attr("class", "node")
      .attr("transform", d => `translate(${d.x},${d.y})`);

    nodes.append("circle")
      .attr("r", d => d.r);

    nodes.append("text")
      .text(d => d.data.name)
      .style("font-size", "12px")
      .attr("fill", "#000")
      .each(function(d) {
        const circleRadius = d.r;
        const textLength = this.getComputedTextLength();
        if (textLength > circleRadius * 1.7) d3.select(this).text("");
      });
  })
  .catch(err => console.error("Error loading JSON:", err));
</script>
</body>
</html>
"""
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)


# ------------------------------------------------------------------
# Main script
# ------------------------------------------------------------------
def main():
    if not VERIFY_SSL:
        requests.packages.urllib3.disable_warnings()

    print("Fetching list of spaces...")
    spaces = fetch_all_spaces()
    print(f"Found {len(spaces)} spaces.")

    print("Fetching page counts...")
    for idx, sp in enumerate(spaces, start=1):
        sp["pageCount"] = fetch_page_count_for_space(sp["key"])
        if idx % 10 == 0:
            print(f"Processed {idx} of {len(spaces)} spaces...")

    # Prepare data and write files
    data_for_d3 = build_circle_packing_data(spaces)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as jf:
        json.dump(data_for_d3, jf, indent=2)
    write_html_file()

    print("Done. Created confluence_data.json and confluence_treepack.html")


if __name__ == "__main__":
    main()
