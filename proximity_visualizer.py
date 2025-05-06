import json
from datetime import datetime
from html import escape
from sklearn.manifold import TSNE
import numpy as np
import webbrowser
import os

from config_loader import load_visualization_settings

def render_d3_proximity_scatter_plot(
    spaces_for_plot_data, 
    X_vectors_for_tsne,
    calculate_avg_timestamps_unused, # Kept for signature consistency, but avg is pre-calculated
    percentile_thresholds_from_caller,
    color_range_hex_from_caller,
    grey_color_hex_from_caller
):
    config = load_visualization_settings()
    confluence_base_url = config.get('confluence_base_url', '')
    if confluence_base_url and not confluence_base_url.endswith('/'):
        confluence_base_url += '/'

    print(f"Performing t-SNE on {X_vectors_for_tsne.shape[0]} spaces for proximity plot...")
    
    # Ensure perplexity is less than n_samples
    perplexity_val = min(30, X_vectors_for_tsne.shape[0] - 1)
    if perplexity_val <= 0: # handles edge case of 1 sample
        print(f"Warning: Perplexity for t-SNE must be greater than 0. Got {perplexity_val} for {X_vectors_for_tsne.shape[0]} samples. Skipping t-SNE.")
        # Create a dummy plot or return early
        X_tsne = np.zeros((X_vectors_for_tsne.shape[0], 2)) # Dummy coordinates
    else:
        tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity_val, n_iter=1000, init='pca', learning_rate='auto')
        source_array = X_vectors_for_tsne.toarray() if hasattr(X_vectors_for_tsne, "toarray") else X_vectors_for_tsne
        X_tsne = tsne.fit_transform(source_array)

    plot_data = []
    for i, space in enumerate(spaces_for_plot_data):
        avg_ts = space.get('avg', 0) # 'avg' should be pre-calculated
        date_str = "No date"
        if avg_ts > 0:
            try:
                date_str = datetime.fromtimestamp(avg_ts).strftime('%Y-%m-%d')
            except (ValueError, OSError):
                date_str = "Invalid date"

        plot_data.append({
            'key': space['space_key'],
            'name': escape(space['space_key']),
            'x': float(X_tsne[i, 0]),
            'y': float(X_tsne[i, 1]),
            'value': space.get('total_pages', len(space.get('sampled_pages', []))),
            'avg': avg_ts,
            'date': date_str,
            'url': f"{confluence_base_url}display/{space['space_key']}" if confluence_base_url else ""
        })

    plot_data_json = json.dumps(plot_data)
    percentile_thresholds_json = json.dumps(percentile_thresholds_from_caller)
    color_range_hex_json = json.dumps(color_range_hex_from_caller)

    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Semantic Proximity Scatter Plot</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{ margin: 20px; font-family: sans-serif; }}
        .dot {{ stroke: #333; stroke-width: 0.5px; }}
        .tooltip {{
            position: absolute; background: rgba(255, 255, 255, 0.95);
            border: 1px solid #bbb; border-radius: 5px; padding: 10px;
            font-size: 12px; pointer-events: none; opacity: 0;
            box-shadow: 0 2px 5px rgba(0,0,0,0.15);
            transition: opacity 0.2s;
        }}
        /* Legend styles */
        .legend-title {{ font-size: 12px; font-weight: bold; }}
    </style>
</head>
<body>
    <h2>Semantic Proximity of Confluence Spaces (t-SNE only)</h2>
    <div id="scatter_chart_proximity"></div> 
    <script>
        const plotData = {plot_data_json};
        const percentileThresholds = {percentile_thresholds_json};
        const colorRangeHex = {color_range_hex_json};
        const greyColorHex = '{grey_color_hex_from_caller}';

        const margin = {{top: 20, right: 170, bottom: 60, left: 70}};
        const container = d3.select("#scatter_chart_proximity");
        const availableWidth = Math.max(900, parseInt(container.style("width")) || window.innerWidth);
        const availableHeight = Math.max(600, window.innerHeight * 0.7);

        const width = availableWidth - margin.left - margin.right;
        const height = availableHeight - margin.top - margin.bottom;

        const svg = container.append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${{margin.left}},${{margin.top}})`);

        const x = d3.scaleLinear()
            .domain(d3.extent(plotData, d => d.x)).nice()
            .range([0, width]);
        svg.append("g")
            .attr("transform", `translate(0,${{height}})`)
            .call(d3.axisBottom(x).ticks(10));
        svg.append("text")
            .attr("text-anchor", "middle")
            .attr("x", width / 2)
            .attr("y", height + margin.bottom - 10)
            .text("t-SNE Component 1");
            
        const y = d3.scaleLinear()
            .domain(d3.extent(plotData, d => d.y)).nice()
            .range([height, 0]);
        svg.append("g")
            .call(d3.axisLeft(y).ticks(10));
        svg.append("text")
            .attr("text-anchor", "middle")
            .attr("transform", "rotate(-90)")
            .attr("x", -height/2)
            .attr("y", -margin.left + 20)
            .text("t-SNE Component 2");

        const colorScale = d3.scaleThreshold()
            .domain(percentileThresholds)
            .range(colorRangeHex);

        const tooltip = d3.select("body").append("div")
            .attr("class", "tooltip");

        const sizeScale = d3.scaleSqrt()
            .domain([0, d3.max(plotData, d => d.value)])
            .range([3, 15]); // Min radius 3, max radius 15

        svg.append("g")
            .selectAll("dot")
            .data(plotData)
            .enter()
            .append("circle")
            .attr("class", "dot")
            .attr("cx", d => x(d.x))
            .attr("cy", d => y(d.y))
            .attr("r", d => sizeScale(d.value))
            .style("fill", d => {{
                if (!d.avg || d.avg <= 0) return greyColorHex;
                return colorScale(d.avg);
            }})
            .style("opacity", 0.7)
            .style("cursor", "pointer")
            .on("mouseover", function(event, d) {{
                d3.select(this).style("opacity", 1).style("stroke-width", 1.5);
                tooltip.transition().duration(100).style("opacity", .95);
                tooltip.html(
                    `<strong>${{d.name}}</strong><br/>` +
                    `Pages: ${{d.value}}<br/>` +
                    `Avg. Date: ${{d.date}}` +
                    (d.url ? `<br/><span style='font-size:10px; color:blue;'>Click to open</span>` : "")
                )
                .style("left", (event.pageX + 15) + "px")
                .style("top", (event.pageY - 10) + "px");
            }})
            .on("mouseout", function() {{
                d3.select(this).style("opacity", 0.7).style("stroke-width", 0.5);
                tooltip.transition().duration(300).style("opacity", 0);
            }})
            .on("click", (event, d) => {{
                if (d.url) window.open(d.url, "_blank");
            }});

        // Legend
        const legendWidth = 200;
        const legendHeight = 20;
        const legendX = width + 20; // Position to the right of the chart
        const legendY = margin.top;

        svg.append('text')
          .attr('x', legendX)
          .attr('y', legendY - 10)
          .attr('class', 'legend-title')
          .text('Page Age (Last Edit)');

        const gradient = svg.append('defs').append('linearGradient')
          .attr('id', 'legend-gradient-proximity')
          .attr('x1', '0%').attr('x2', '100%')
          .attr('y1', '0%').attr('y2', '0%');

        colorRangeHex.forEach((color, i) => {{
          gradient.append('stop')
            .attr('offset', `${{i * 100 / (colorRangeHex.length - 1)}}%`)
            .attr('stop-color', color);
        }});

        svg.append('rect')
          .attr('x', legendX)
          .attr('y', legendY)
          .attr('width', legendWidth)
          .attr('height', legendHeight)
          .style('fill', 'url(#legend-gradient-proximity)');

        svg.append('text')
          .attr('x', legendX)
          .attr('y', legendY + legendHeight + 12)
          .style('font-size', '10px')
          .text('Oldest');

        svg.append('text')
          .attr('x', legendX + legendWidth)
          .attr('y', legendY + legendHeight + 12)
          .style('font-size', '10px')
          .attr('text-anchor', 'end')
          .text('Newest');
        
        // Size Legend
        svg.append('text')
            .attr('x', legendX)
            .attr('y', legendY + legendHeight + 40)
            .attr('class', 'legend-title')
            .text('Page Count');

        const sizeLegendValues = [sizeScale.domain()[0], d3.quantile(plotData, 0.5, d => d.value), sizeScale.domain()[1]];
        const sizeLegend = svg.append("g")
            .attr("transform", `translate(${{legendX}}, ${{legendY + legendHeight + 60}})`);

        sizeLegend.selectAll("circle")
            .data(sizeLegendValues.filter(d => d !== undefined && d !== null))
            .enter().append("circle")
            .attr("cy", (d,i) => i * 25)
            .attr("r", d => sizeScale(d))
            .style("fill", "grey");

        sizeLegend.selectAll("text")
            .data(sizeLegendValues.filter(d => d !== undefined && d !== null))
            .enter().append("text")
            .attr("x", 30)
            .attr("y", (d,i) => i * 25 + 4)
            .text(d => Math.round(d))
            .style("font-size", "10px");

    </script>
</body>
</html>
"""
    output_filename = "semantic_proximity_scatter_plot.html"
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Proximity scatter plot HTML written to {os.path.abspath(output_filename)}")
    webbrowser.open("file://" + os.path.abspath(output_filename))


def generate_proximity_scatter_plot(
    spaces_arg, 
    get_vectors_func, 
    calculate_avg_timestamps_func, 
    calculate_color_data_func, 
    grey_color_hex_val 
):
    if not spaces_arg:
        print("No spaces data provided to generate_proximity_scatter_plot.")
        return

    print("Generating proximity scatter plot (t-SNE only)...")
    try:
        X_vectors, valid_spaces = get_vectors_func(spaces_arg)
        if not valid_spaces:
            print("No valid spaces with content found after vectorization for proximity plot.")
            return
        
        num_valid_spaces = X_vectors.shape[0]
        if num_valid_spaces < 2:
            print(f"Not enough data points ({num_valid_spaces}) for t-SNE. Need at least 2.")
            return

        # Calculate average timestamps for all valid spaces (for color and tooltips)
        # Pass a copy of valid_spaces to avoid modifying the original list from get_vectors_func
        valid_spaces_with_avg_ts = calculate_avg_timestamps_func(list(valid_spaces))

        # Get color scale data using the function passed from explore_clusters
        percentile_thresholds, color_range_hex = calculate_color_data_func(valid_spaces_with_avg_ts)

        render_d3_proximity_scatter_plot(
            valid_spaces_with_avg_ts,
            X_vectors,
            calculate_avg_timestamps_func, # Pass along, though not directly used in render if pre-calculated
            percentile_thresholds,
            color_range_hex,
            grey_color_hex_val
        )
    except Exception as e:
        print(f"Error in generate_proximity_scatter_plot: {e}")
        import traceback
        traceback.print_exc()
