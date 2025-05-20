# description: Visualizes Confluence data using scatter plots.

# scatter_plot_visualizer.py
import os
import numpy as np
import json
import webbrowser
from datetime import datetime
from html import escape
from sklearn.manifold import TSNE
from sklearn.cluster import AgglomerativeClustering
from config_loader import load_visualization_settings

def render_d3_semantic_scatter_plot(spaces, labels, method_name, tags, X_vectors, calculate_avg_timestamps_func):
    try:
        config = load_visualization_settings()
        confluence_base_url = config.get('confluence_base_url', '')
        if not confluence_base_url:
            print("Warning: 'confluence_base_url' not found in visualization settings. Links in scatter plot may not work or will be relative.")
            confluence_base_url = ""
    except Exception as e:
        print(f"Warning: Could not load visualization settings: {e}. Links in scatter plot may not work or will be relative.")
        confluence_base_url = ""

    # Use the passed function to calculate average timestamps
    # Ensure the passed spaces list is not modified if calculate_avg_timestamps_func modifies in-place
    # Making a shallow copy of the list of dictionaries. If calculate_avg_timestamps_func modifies dicts themselves, those changes will reflect.
    spaces_copy = [s.copy() for s in spaces]
    spaces_with_avg_timestamps = calculate_avg_timestamps_func(spaces_copy)

    coordinates_2d = np.array([])
    if X_vectors is not None and X_vectors.shape[0] > 1:
        perplexity_value = min(30, X_vectors.shape[0] - 1)
        if perplexity_value <= 0:
            perplexity_value = 1
        
        n_iter_value = 1000
        if X_vectors.shape[0] < 50:
            n_iter_value = max(250, int(200 + X_vectors.shape[0] * 5))

        print(f"Running t-SNE with n_samples={X_vectors.shape[0]}, perplexity={perplexity_value}, n_iter={n_iter_value}")
        
        try:
            X_dense = X_vectors.toarray() if hasattr(X_vectors, "toarray") else X_vectors
            if X_dense.shape[0] > 0:
                 tsne_model = TSNE(n_components=2, random_state=42, perplexity=perplexity_value, 
                                   n_iter=n_iter_value, init='pca', learning_rate=200.0, method='auto')
                 coordinates_2d = tsne_model.fit_transform(X_dense)
                 print(f"t-SNE completed. Shape of coordinates_2d: {coordinates_2d.shape}")
            else:
                print("Error: X_vectors resulted in an empty dense array for t-SNE.")
                coordinates_2d = np.array([])
        except Exception as e:
            print(f"Error during t-SNE: {e}")
            if X_vectors is not None and X_vectors.shape[0] > 0:
                 coordinates_2d = np.random.rand(X_vectors.shape[0], 2) * 100
            else:
                 coordinates_2d = np.array([])
    elif X_vectors is not None and X_vectors.shape[0] == 1:
        print("Only one data point. Plotting at origin (0,0).")
        coordinates_2d = np.array([[0,0]])
    else:
        print("Not enough data points for 2D projection or X_vectors is None/empty.")
        html_content_no_data = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Semantic Scatter Plot - No Data</title>
</head>
<body>
    <h1>Semantic Scatter Plot ({method_name})</h1>
    <p>No data available to display. This might be due to filtering, lack of text content in spaces, or t-SNE failure.</p>
</body>
</html>"""
        out_path_no_data = 'semantic_scatter_plot.html'
        with open(out_path_no_data, 'w', encoding='utf-8') as f:
            f.write(html_content_no_data)
        print(f'HTML (no data) written to {out_path_no_data}')
        webbrowser.open('file://' + os.path.abspath(out_path_no_data))
        return

    plot_data = []
    if coordinates_2d.ndim == 2 and coordinates_2d.shape[0] > 0 and \
       coordinates_2d.shape[0] == len(spaces_with_avg_timestamps) and \
       (len(labels) == 0 and coordinates_2d.shape[0] == 0 or len(labels) == len(spaces_with_avg_timestamps)): # handle empty labels if coords are also empty
        for i, space_item in enumerate(spaces_with_avg_timestamps):
            # If labels are empty (e.g. single point, no clustering), assign a default label
            label = labels[i] if len(labels) > 0 else 0
            
            avg_ts = space_item.get('avg', 0)
            date_str = datetime.fromtimestamp(avg_ts).strftime('%Y-%m-%d') if avg_ts > 0 else "No date"
            
            space_key_original = space_item['space_key']
            escaped_space_key = escape(space_key_original)
            escaped_space_name = escape(space_item.get('name', space_key_original))
            escaped_cluster_tags = escape(', '.join(tags.get(label, [])))

            plot_data.append({
                'key': escaped_space_key,
                'name': escaped_space_name,
                'x': float(coordinates_2d[i, 0]),
                'y': float(coordinates_2d[i, 1]),
                'cluster': int(label),
                'cluster_tags': escaped_cluster_tags,
                'value': space_item.get('total_pages', len(space_item.get('sampled_pages',[]))),
                'date': date_str,
                'url': f"{confluence_base_url}/display/{space_key_original}"
            })
    else:
        print(f"Warning: Mismatch or empty data for plotting. Coords: {coordinates_2d.shape}, Spaces: {len(spaces_with_avg_timestamps)}, Labels: {len(labels)}.")

    data_json = json.dumps(plot_data)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Semantic Scatter Plot ({method_name})</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{ margin: 20px; font-family: sans-serif; }}
        .dot {{ stroke: #fff; stroke-width: 0.5px; cursor: pointer; }}
        .tooltip {{
            position: absolute; text-align: center; width: auto; height: auto;
            padding: 8px; font: 12px sans-serif; background: lightsteelblue;
            border: 0px; border-radius: 8px; pointer-events: none; opacity: 0;
        }}
        .axis-label {{ font-size: 10px; }}
        .legend {{ font-size: 10px; }}
    </style>
</head>
<body>
    <h1>Semantic Scatter Plot ({method_name})</h1>
    <div id="scatter-plot"></div>
    <script>
        const data = {data_json};
        console.log("Data for D3:", data);

        if (data && data.length > 0) {{
            const margin = {{top: 20, right: 200, bottom: 60, left: 60}};
            const width = 960 - margin.left - margin.right;
            const height = 600 - margin.top - margin.bottom;

            const svg = d3.select("#scatter-plot").append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
              .append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);

            const xScale = d3.scaleLinear().domain(d3.extent(data, d => d.x)).range([0, width]);
            const yScale = d3.scaleLinear().domain(d3.extent(data, d => d.y)).range([height, 0]);
            const colorScale = d3.scaleOrdinal(d3.schemeCategory10);
            colorScale.domain(Array.from(new Set(data.map(d => d.cluster))).sort((a,b) => a-b));
            const sizeScale = d3.scaleSqrt().domain([0, d3.max(data, d => d.value)]).range([3, 20]);
            const tooltip = d3.select("body").append("div").attr("class", "tooltip");

            svg.append("g").attr("transform", `translate(0,${{height}})`).call(d3.axisBottom(xScale))
                .append("text").attr("class", "axis-label").attr("x", width / 2).attr("y", margin.bottom - 10)
                .attr("fill", "black").style("text-anchor", "middle").text("t-SNE Dimension 1");

            svg.append("g").call(d3.axisLeft(yScale))
                .append("text").attr("class", "axis-label").attr("transform", "rotate(-90)")
                .attr("x", -height / 2).attr("y", -margin.left + 20).attr("fill", "black")
                .style("text-anchor", "middle").text("t-SNE Dimension 2");

            svg.selectAll(".dot").data(data).enter().append("circle").attr("class", "dot")
                .attr("cx", d => xScale(d.x)).attr("cy", d => yScale(d.y))
                .attr("r", d => sizeScale(d.value > 0 ? d.value : 1))
                .style("fill", d => colorScale(d.cluster))
                .on("mouseover", function(event, d) {{
                    tooltip.transition().duration(200).style("opacity", .9);
                    tooltip.html(
                        `<strong>${{d.key}}</strong> (${{d.name}})<br/>` +
                        `Cluster: ${{d.cluster}} (${{d.cluster_tags}})<br/>` +
                        `Pages: ${{d.value}}<br/>` +
                        `Avg. Date: ${{d.date}}`
                    ).style("left", (event.pageX + 10) + "px").style("top", (event.pageY - 30) + "px");
                }})
                .on("mouseout", function(d) {{ tooltip.transition().duration(500).style("opacity", 0); }})
                .on("click", function(event, d) {{
                    if (d.url && d.url !== "{confluence_base_url}/display/None" && !d.url.endsWith("/display/None")) {{ // Check for valid URL more robustly
                         window.open(d.url, '_blank'); 
                    }}
                }});

            // Add text labels to dots
            svg.selectAll(".dot-label") // Use a new class for these labels
                .data(data)
                .enter().append("text")
                .attr("class", "dot-label")
                .attr("x", d => xScale(d.x))
                .attr("y", d => yScale(d.y))
                .attr("dy", ".35em") // Vertically center
                .attr("text-anchor", "middle") // Horizontally center
                .style("font-size", "5pt")
                .style("pointer-events", "none") // So they don't interfere with circle's mouse events
                .text(d => d.key);

            const uniqueClusters = Array.from(new Set(data.map(d => d.cluster))).sort((a,b) => a-b);
            const legend = svg.selectAll(".legend-item").data(uniqueClusters).enter().append("g")
                .attr("class", "legend-item").attr("transform", (d, i) => `translate(0,${{i * 20}})`);
            legend.append("rect").attr("x", width + 10).attr("y", 0).attr("width", 18).attr("height", 18)
                .style("fill", d => colorScale(d));
            legend.append("text").attr("x", width + 35).attr("y", 9).attr("dy", ".35em").attr("class", "legend")
                .style("text-anchor", "start").text(d => {{
                    const firstSpaceInCluster = data.find(space => space.cluster === d);
                    const tags = firstSpaceInCluster ? firstSpaceInCluster.cluster_tags : '';
                    return `Cluster ${{d}}${{tags ? ' (' + tags + ')' : ''}}`;
                }});
            svg.append("text").attr("x", width + 10).attr("y", -10).attr("class", "legend")
                .style("font-weight", "bold").text("Clusters");
        }} else {{
            const plotDiv = document.getElementById("scatter-plot");
            if (plotDiv) {{
                 plotDiv.innerHTML = "<p>No data to display in scatter plot. This could be due to t-SNE failure, no text content in spaces, or filters being too restrictive.</p>";
            }}
        }}
    </script>
</body>
</html>""";

    out_path = 'semantic_scatter_plot.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f'Semantic scatter plot HTML written to {out_path}')
    webbrowser.open('file://' + os.path.abspath(out_path))

def generate_2d_scatter_plot_agglomerative(
    spaces_arg,
    n_clusters_arg,
    get_vectors_func,
    suggest_tags_func,
    calculate_avg_timestamps_func
):
    if not spaces_arg:
        print("Error: No spaces loaded for scatter plot. Please load data first.")
        render_d3_semantic_scatter_plot([], [], 'Agglomerative (No Data)', {}, None, calculate_avg_timestamps_func)
        return

    print(f"Starting 2D Scatter Plot: Clustering {len(spaces_arg)} spaces (Agglomerative) into {n_clusters_arg} clusters...")

    try:
        X_tfidf, valid_spaces_for_X = get_vectors_func(spaces_arg)

        if not valid_spaces_for_X or (hasattr(X_tfidf, "shape") and X_tfidf.shape[0] == 0):
            print("Error: No valid spaces with text content found for clustering after get_vectors.")
            render_d3_semantic_scatter_plot([], [], 'Agglomerative (No Valid Spaces)', {}, None, calculate_avg_timestamps_func)
            return
        
        print(f"Obtained {X_tfidf.shape[0]} valid spaces with TF-IDF vectors of shape {X_tfidf.shape}.")

        actual_n_clusters = n_clusters_arg
        if X_tfidf.shape[0] < n_clusters_arg:
            print(f"Warning: Number of samples ({X_tfidf.shape[0]}) is less than n_clusters ({n_clusters_arg}).")
            if X_tfidf.shape[0] == 0: # No samples at all
                 print("Error: Zero samples available for clustering.")
                 labels_for_plot = []
                 actual_n_clusters = 0
            elif X_tfidf.shape[0] < 2: # Only 1 sample
                 print("Only 1 sample available. No clustering will be performed. Plotting single point.")
                 labels_for_plot = np.array([0]) 
                 actual_n_clusters = 1 
            else: # 2 or more samples, but less than n_clusters_arg
                 actual_n_clusters = X_tfidf.shape[0]
            print(f"Using actual_n_clusters = {actual_n_clusters}")
        
        labels_for_plot = []
        if X_tfidf.shape[0] > 0: # Proceed if there are any samples
            if actual_n_clusters <= 0 and X_tfidf.shape[0] > 0 : 
                actual_n_clusters = 1
                print(f"Corrected actual_n_clusters to {actual_n_clusters} as it was invalid.")

            if X_tfidf.shape[0] == 1: 
                labels_for_plot = np.array([0]) 
            elif actual_n_clusters == 1 and X_tfidf.shape[0] > 1: 
                labels_for_plot = np.zeros(X_tfidf.shape[0], dtype=int)
            elif actual_n_clusters > 1 and X_tfidf.shape[0] >= actual_n_clusters: 
                agg_model = AgglomerativeClustering(n_clusters=actual_n_clusters)
                labels_for_plot = agg_model.fit_predict(X_tfidf.toarray())
            # This case should be covered by the adjustment of actual_n_clusters already
            # elif actual_n_clusters > 1 and X_tfidf.shape[0] < actual_n_clusters: 
            #     print(f"Re-adjusting to {X_tfidf.shape[0]} clusters (should have been caught).")
            #     agg_model = AgglomerativeClustering(n_clusters=X_tfidf.shape[0])
            #     labels_for_plot = agg_model.fit_predict(X_tfidf.toarray())
            else: 
                 print(f"Unexpected condition for clustering: Samples={X_tfidf.shape[0]}, Clusters={actual_n_clusters}. Defaulting to 1 cluster.")
                 labels_for_plot = np.zeros(X_tfidf.shape[0], dtype=int) # Fallback to single cluster
        else: 
            labels_for_plot = []


        tags_for_plot = {}
        if valid_spaces_for_X and len(labels_for_plot) > 0:
             tags_for_plot = suggest_tags_func(valid_spaces_for_X, labels_for_plot)
        
        render_d3_semantic_scatter_plot(
            valid_spaces_for_X,
            labels_for_plot,
            'Agglomerative',
            tags_for_plot,
            X_vectors=X_tfidf,
            calculate_avg_timestamps_func=calculate_avg_timestamps_func
        )

    except ValueError as e:
        if "No spaces with non-empty text content" in str(e) or "empty vocabulary" in str(e):
            print(f"Error during 2D scatter plot (ValueError): {e}")
            render_d3_semantic_scatter_plot([], [], 'Agglomerative (Vectorization Error)', {}, None, calculate_avg_timestamps_func)
        else:
            print(f"Clustering or Plotting Value Error for 2D scatter plot: {e}")
            render_d3_semantic_scatter_plot([], [], 'Agglomerative (Value Error)', {}, None, calculate_avg_timestamps_func)
    except Exception as e:
        print(f"An unexpected error occurred in generate_2d_scatter_plot_agglomerative: {e}")
        import traceback
        traceback.print_exc()
        render_d3_semantic_scatter_plot([], [], 'Agglomerative (Unexpected Error)', {}, None, calculate_avg_timestamps_func)

