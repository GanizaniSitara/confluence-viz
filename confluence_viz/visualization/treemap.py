"""
Treemap visualization module for Confluence data.
Generates interactive D3.js circle packing visualizations.
"""

import json
import os
import webbrowser
import sys
from typing import Dict, Any, List, Optional

from ..utils.color_utils import GREY_COLOR_HEX


class TreemapGenerator:
    """
    Generates interactive treemap visualizations using D3.js circle packing.
    """
    
    def __init__(self, width: int = 3000, height: int = 2000):
        """
        Initialize the treemap generator.
        
        Args:
            width: SVG width in pixels
            height: SVG height in pixels
        """
        self.width = width
        self.height = height
    
    def generate_html(
        self, 
        data: Dict[str, Any], 
        percentile_thresholds: List[float], 
        color_range_hex: List[str],
        title: str = "Confluence Circle Packing"
    ) -> str:
        """
        Generate HTML content for the treemap visualization.
        
        Args:
            data: Hierarchical data structure
            percentile_thresholds: List of percentile threshold values
            color_range_hex: List of hex color strings for the gradient
            title: HTML page title
            
        Returns:
            Complete HTML content as string
        """
        data_json = json.dumps(data)
        percentile_thresholds_json = json.dumps(percentile_thresholds)
        color_range_hex_json = json.dumps(color_range_hex)
        
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{title}</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body {{ 
      margin: 0; 
      font-family: sans-serif; 
      background-color: #f5f5f5;
    }}
    #chart {{
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 100vh;
    }}
    .node text {{ 
      text-anchor: middle; 
      alignment-baseline: middle; 
      font-size: 6pt; 
      pointer-events: none; 
      fill: #333;
      font-weight: bold;
    }}
    .legend {{
      position: fixed;
      top: 20px;
      right: 20px;
      background: white;
      padding: 15px;
      border-radius: 5px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.1);
      font-family: sans-serif;
      font-size: 12px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      margin-bottom: 5px;
    }}
    .legend-color {{
      width: 20px;
      height: 15px;
      margin-right: 8px;
      border: 1px solid #ccc;
    }}
    .title {{
      position: fixed;
      top: 20px;
      left: 20px;
      background: white;
      padding: 15px;
      border-radius: 5px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.1);
      font-family: sans-serif;
      font-size: 18px;
      font-weight: bold;
    }}
  </style>
</head>
<body>
  <div class="title">{title}</div>
  <div class="legend">
    <div><strong>Color Legend</strong></div>
    <div class="legend-item">
      <div class="legend-color" style="background-color: {color_range_hex[0] if color_range_hex else GREY_COLOR_HEX}"></div>
      <span>Oldest Activity</span>
    </div>
    <div class="legend-item">
      <div class="legend-color" style="background-color: {color_range_hex[-1] if color_range_hex else GREY_COLOR_HEX}"></div>
      <span>Newest Activity</span>
    </div>
    <div class="legend-item">
      <div class="legend-color" style="background-color: {GREY_COLOR_HEX}"></div>
      <span>No Activity</span>
    </div>
  </div>
  <div id="chart"></div>
  
  <script>
    // Data embed
    const data = {data_json};
    const PERCENTILE_THRESHOLDS = {percentile_thresholds_json};
    const COLOR_RANGE_HEX = {color_range_hex_json};
    const GREY_COLOR_HEX = '{GREY_COLOR_HEX}';

    // Color scale based on thresholds (percentiles of space average timestamps)
    const colorScale = d3.scaleThreshold()
      .domain(PERCENTILE_THRESHOLDS)
      .range(COLOR_RANGE_HEX);

    // Pack layout
    const width = {self.width}, height = {self.height};
    const root = d3.pack()
      .size([width, height])
      .padding(6)(d3.hierarchy(data).sum(d => d.value));

    const svg = d3.select('#chart')
      .append('svg')
      .attr('width', width)
      .attr('height', height);

    // Create tooltip
    const tooltip = d3.select('body')
      .append('div')
      .style('position', 'absolute')
      .style('background', 'rgba(0,0,0,0.8)')
      .style('color', 'white')
      .style('padding', '8px')
      .style('border-radius', '4px')
      .style('font-size', '12px')
      .style('pointer-events', 'none')
      .style('opacity', 0);

    // Render nodes
    const node = svg.selectAll('g')
      .data(root.leaves())
      .enter().append('g')
      .attr('transform', d => `translate(${{d.x}},${{d.y}})`)
      .on('mouseover', function(event, d) {{
        const avgDate = d.data.avg > 0 ? new Date(d.data.avg * 1000).toLocaleDateString() : 'No activity';
        tooltip.transition().duration(200).style('opacity', .9);
        tooltip.html(`
          <strong>${{d.data.key}}</strong><br/>
          ${{d.data.name}}<br/>
          Pages: ${{d.data.value}}<br/>
          Last Activity: ${{avgDate}}
        `)
        .style('left', (event.pageX + 10) + 'px')
        .style('top', (event.pageY - 28) + 'px');
      }})
      .on('mouseout', function(d) {{
        tooltip.transition().duration(500).style('opacity', 0);
      }});

    // Add circles
    node.append('circle')
      .attr('r', d => d.r)
      .attr('fill', d => d.data.avg > 0 ? colorScale(d.data.avg) : GREY_COLOR_HEX)
      .attr('stroke', '#666')
      .attr('stroke-width', 0.5);

    // Add space key labels
    node.append('text')
      .attr('dy', '-0.35em')
      .attr('text-anchor', 'middle')
      .style('font-size', '6pt')
      .style('font-weight', 'bold')
      .text(d => d.data.key);

    // Add page count labels
    node.append('text')
      .attr('dy', '0.75em')
      .attr('text-anchor', 'middle')
      .style('font-size', '5pt')
      .text(d => d.data.value + ' pages');

  </script>
</body>
</html>"""
        
        return html_template
    
    def save_html(
        self, 
        data: Dict[str, Any], 
        percentile_thresholds: List[float], 
        color_range_hex: List[str],
        output_file: str = "confluence_treepack.html",
        title: str = "Confluence Circle Packing",
        auto_open: bool = True
    ):
        """
        Generate and save HTML visualization to file.
        
        Args:
            data: Hierarchical data structure
            percentile_thresholds: List of percentile threshold values
            color_range_hex: List of hex color strings for the gradient
            output_file: Output file path
            title: HTML page title
            auto_open: Whether to automatically open in browser
        """
        print(f"Generating HTML output to {output_file}...")
        
        html_content = self.generate_html(
            data=data,
            percentile_thresholds=percentile_thresholds,
            color_range_hex=color_range_hex,
            title=title
        )
        
        with open(output_file, "w", encoding="utf-8") as hf:
            hf.write(html_content)
        print(f"Successfully created {output_file}")
        
        if auto_open:
            self._open_in_browser(output_file)
    
    def _open_in_browser(self, file_path: str):
        """
        Open HTML file in the default browser.
        
        Args:
            file_path: Path to the HTML file
        """
        try:
            print(f"Opening {file_path} in browser...")
            webbrowser.open('file://' + os.path.realpath(file_path))
        except Exception as e:
            print(f"Could not automatically open browser: {e}", file=sys.stderr)
            print(f"Please open the file manually: {os.path.realpath(file_path)}")


def generate_treemap_visualization(
    data: Dict[str, Any], 
    percentile_thresholds: List[float], 
    color_range_hex: List[str],
    output_file: str = "confluence_treepack.html",
    width: int = 3000,
    height: int = 2000,
    auto_open: bool = True
) -> str:
    """
    Convenience function to generate a treemap visualization.
    
    Args:
        data: Hierarchical data structure
        percentile_thresholds: List of percentile threshold values
        color_range_hex: List of hex color strings for the gradient
        output_file: Output file path
        width: SVG width in pixels
        height: SVG height in pixels
        auto_open: Whether to automatically open in browser
        
    Returns:
        Path to the generated HTML file
    """
    generator = TreemapGenerator(width=width, height=height)
    generator.save_html(
        data=data,
        percentile_thresholds=percentile_thresholds,
        color_range_hex=color_range_hex,
        output_file=output_file,
        auto_open=auto_open
    )
    return os.path.realpath(output_file)