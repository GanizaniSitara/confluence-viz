#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import json
import sys
import os
import webbrowser
from datetime import datetime

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
            all_spaces.append({"key": sp.get("key", ""), "name": sp.get("name", "")})
        if len(spaces) < SPACES_PAGE_LIMIT:
            break
        start += SPACES_PAGE_LIMIT
    return all_spaces


# ------------------------------------------------------------------
# Step 2: For each space, count pages and collect last-edit timestamps
# ------------------------------------------------------------------
def fetch_page_data_for_space(space_key):
    count = 0
    start = 0
    timestamps = []
    while True:
        url = f"{CONFLUENCE_URL}/rest/api/content"
        params = {
            "type": "page",
            "spaceKey": space_key,
            "limit": CONTENT_PAGE_LIMIT,
            "start": start,
            "expand": "version"
        }
        r = get_with_retry(url=url, params=params, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if r.status_code != 200:
            print(f"Failed to fetch pages for space {space_key} at start={start}, status={r.status_code}", file=sys.stderr)
            break
        data = r.json()
        pages = data.get("results", [])
        for p in pages:
            when = p.get("version", {}).get("when")
            if when:
                try:
                    dt = datetime.fromisoformat(when.replace("Z", "+00:00"))
                    timestamps.append(dt.timestamp())
                except ValueError:
                    pass
        count += len(pages)
        if len(pages) < CONTENT_PAGE_LIMIT:
            break
        start += CONTENT_PAGE_LIMIT
    return count, timestamps


# ------------------------------------------------------------------
# Step 3: Build data structure for circle packing
# ------------------------------------------------------------------
def build_circle_packing_data(spaces):
    root = {"name": "Confluence", "children": []}
    for sp in spaces:
        root["children"].append({
            "key": sp["key"],
            "value": sp["pageCount"],
            "avg": sp["avgLastEdit"]
        })
    return root


# ------------------------------------------------------------------
# Step 4: Generate the HTML file with inline data + 100-step pastel scale
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
  </style>
</head>
<body>
<div id="chart"></div>
<script>
// Embedded data
const data = {data_json};

// compute time bounds
const times = data.children.map(d => d.avg);
const [minT, maxT] = d3.extent(times);

// build 100-step pastel scale from red to green
const pastel = d3.range(100).map(i =>
  d3.interpolateRgb("#ffcccc", "#ccffcc")(i / 99)
);
const colorScale = d3.scaleQuantize()
  .domain([minT, maxT])
  .range(pastel);

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

// circles colored by 100-step pastel scale
nodes.append("circle")
  .attr("r", d => d.r)
  .attr("fill", d => colorScale(d.data.avg));

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

    spaces = fetch_all_spaces()
    print(f"Found {len(spaces)} spaces.")

    all_ts = []
    for idx, sp in enumerate(spaces, start=1):
        count, ts = fetch_page_data_for_space(sp["key"])
        sp["pageCount"] = count
        if ts:
            sp["avgLastEdit"] = sum(ts) / len(ts)
            all_ts.extend(ts)
        else:
            sp["avgLastEdit"] = 0
        if idx % 10 == 0:
            print(f"Processed {idx}/{len(spaces)} spaces...")

    if all_ts:
        oldest = datetime.fromtimestamp(min(all_ts)).isoformat()
        newest = datetime.fromtimestamp(max(all_ts)).isoformat()
        print(f"Oldest edit: {oldest}, Newest edit: {newest}")

    data_for_d3 = build_circle_packing_data(spaces)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as jf:
        json.dump(data_for_d3, jf, indent=2)

    write_html_file(data_for_d3)
    print("Done. Created confluence_data.json and confluence_treepack.html")
    webbrowser.open("file://" + os.path.realpath(OUTPUT_HTML))


if __name__ == "__main__":
    main()
