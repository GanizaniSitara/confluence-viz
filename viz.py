#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import json
import sys
import os
import webbrowser

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

# Number of pages to fetch at a time when counting pages
CONTENT_PAGE_LIMIT = 100

# Output file names
OUTPUT_JSON = "confluence_data.json"
OUTPUT_HTML = "confluence_treepack.html"


# ------------------------------------------------------------------
# Helper: Handle 429 with a simple backoff
# ------------------------------------------------------------------
def get_with_retry(url, params=None, headers=None, auth=None, verify=False):
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
    all_spaces = []
    start = 0
    while True:
        url = f"{CONFLUENCE_URL}/rest/api/space"
        params = {"start": start, "limit": SPACES_PAGE_LIMIT}
        r = get_with_retry(url=url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch spaces at start={start}, status={r.status_code}", file=sys.stderr)
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
        start += SPACES_PAGE_LIMIT
    return all_spaces


# ------------------------------------------------------------------
# Step 2: For each space, count pages via pagination
# ------------------------------------------------------------------
def fetch_page_count_for_space(space_key):
    count = 0
    start = 0
    while True:
        url = f"{CONFLUENCE_URL}/rest/api/content"
        params = {
            "type": "page",
            "spaceKey": space_key,
            "limit": CONTENT_PAGE_LIMIT,
            "start": start
        }
        r = get_with_retry(url=url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch pages for space {space_key} at start={start}, status={r.status_code}", file=sys.stderr)
            break
        data = r.json()
        pages = data.get("results", [])
        count += len(pages)
        if len(pages) < CONTENT_PAGE_LIMIT:
            break
        start += CONTENT_PAGE_LIMIT
    return count


# ------------------------------------------------------------------
# Step 3: Build data structure for circle packing
# ------------------------------------------------------------------
def build_circle_packing_data(spaces):
    root = {"name": "Confluence", "children": []}
    for sp in spaces:
        pc = sp.get("pageCount", 0)
        if pc > 0:
            root["children"].append({
                # use the space key as the 'name' for layout
                "name": sp["key"],
                "key": sp["key"],
                "value": pc
            })
    return root


# ------------------------------------------------------------------
# Step 4: Generate the HTML file with inline data
# ------------------------------------------------------------------
def write_html_file(data):
    data_json = json.dumps(data)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Confluence Circle Packing</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body {{ margin: 0; font-family: sans-serif; }}
    .node text {{ text-anchor: middle; alignment-baseline: middle; font-size: 6pt; pointer-events: none; }}
    .node circle {{ stroke: #999; fill: #ccc; }}
  </style>
</head>
<body>
<div id="chart"></div>
<script>
// Embedded data
const data = {data_json};

const width = 1000, height = 800;
const root = d3.pack().size([width, height]).padding(3)(
  d3.hierarchy(data).sum(d => d.value)
);

const svg = d3.select("#chart").append("svg")
  .attr("width", width).attr("height", height);

const leaf = root.descendants().filter(d => !d.children);
const nodes = svg.selectAll("g").data(leaf).enter()
  .append("g")
  .attr("class", "node")
  .attr("transform", d => `translate(${{d.x}},${{d.y}})`);

// draw circle
nodes.append("circle").attr("r", d => d.r);

// first line: space key
nodes.append("text")
  .attr("dy", "-0.5em")
  .text(d => d.data.key);

// second line: page count
nodes.append("text")
  .attr("dy", "0.75em")
  .text(d => d.data.value);
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

    print("Counting pages per space...")
    for idx, sp in enumerate(spaces, start=1):
        sp["pageCount"] = fetch_page_count_for_space(sp["key"])
        if idx % 10 == 0:
            print(f"Processed {idx}/{len(spaces)} spaces...")

    data_for_d3 = build_circle_packing_data(spaces)

    # also write JSON for reference
    with open(OUTPUT_JSON, "w", encoding="utf-8") as jf:
        json.dump(data_for_d3, jf, indent=2)

    write_html_file(data_for_d3)
    print("Done. Created confluence_data.json and confluence_treepack.html")

    full_html_path = os.path.realpath(OUTPUT_HTML)
    print("Opening visualization in web browser...")
    webbrowser.open("file://" + full_html_path)


if __name__ == "__main__":
    main()
