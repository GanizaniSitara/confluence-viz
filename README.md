
# Confluence Visualization Project

A comprehensive toolkit for extracting, analyzing, and visualizing data from Confluence spaces to gain insights into content organization, usage patterns, and semantic relationships.

## Overview

This project helps organizations understand how their Confluence instance is being used by:

1. Fetching metadata and content from Confluence spaces and pages
2. Performing various analyses including:
   - Hierarchical clustering of spaces based on content
   - Semantic analysis using natural language processing
   - Search capability across spaces and pages
3. Generating interactive visualizations:
   - Treemap visualizations of spaces and pages
   - Scatter plots showing semantic relationships
   - Proximity visualizations of related content
   - Cluster visualizations

## Key Features

- **Data Extraction**: Connect to Confluence API to fetch space and page data
- **Content Analysis**: 
  - Semantic analysis using TF-IDF and LSA (Latent Semantic Analysis)
  - Clustering using multiple algorithms (KMeans, Agglomerative, DBSCAN)
  - Content search and indexing with Whoosh
- **Visualizations**:
  - D3.js-based treemap visualizations
  - Scatter plot visualizations
  - Semantic relationship diagrams
  - Interactive HTML outputs
- **Application Search**: Search and analyze applications mentioned in Confluence

## Prerequisites

- Python 3.6+
- Confluence instance with API access
- Required Python packages (see installation)

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd confluence_visualization
   ```

2. Install required Python packages:
   ```
   pip install requests beautifulsoup4 numpy scikit-learn whoosh matplotlib
   ```

3. Create a settings.ini file (use settings.example.ini as a template):
   ```
   cp settings.example.ini settings.ini
   ```

4. Edit settings.ini with your Confluence credentials and settings:
   ```ini
   [confluence]
   api_base_url = https://your-confluence-instance.atlassian.net/rest/api
   username = your_username
   password = your_api_token
   verify_ssl = True
   base_url = https://your-confluence-instance.atlassian.net

   [visualization]
   default_clusters = 20
   default_min_pages = 5
   ```

## Usage

### Data Collection

1. Fetch data from Confluence:
   ```
   python fetch_data.py
   ```
   This creates a `confluence_data.pkl` file with space and page data.

2. Run semantic analysis (optional):
   ```
   python semantic_analysis.py
   ```
   This creates a `confluence_semantic_data.pkl` file with semantic analysis results.

### Visualizations

1. Generate treemap visualization:
   ```
   python render_html.py
   ```
   This creates `confluence_treepack.html`, an interactive visualization of spaces.

2. Explore clusters and generate additional visualizations:
   ```
   python explore_clusters.py
   ```
   This provides an interactive menu with multiple visualization and analysis options.

3. Generate semantic visualizations:
   ```
   python render_semantic_html.py
   ```
   This creates semantic-based visualizations.

### Exploration Tools

- **Space Explorer**: Browse and analyze individual Confluence spaces
  ```
  python space_explorer.py
  ```

- **Application Search**: Find and analyze applications mentioned in Confluence
  ```
  python seed_applications.py
  ```

## File Descriptions

- **config_loader.py**: Handles loading configuration from settings.ini
- **fetch_data.py**: Fetches data from Confluence API
- **semantic_analysis.py**: Performs semantic analysis on Confluence content
- **explore_clusters.py**: Interactive tool for exploring different clustering methods
- **render_html.py**: Generates treemap visualization
- **render_semantic_html.py**: Generates semantic-based visualizations
- **viz.py**: Core visualization and API interaction logic
- **proximity_visualizer.py**: Creates proximity-based visualizations
- **scatter_plot_visualizer.py**: Creates scatter plot visualizations

## Example Visualizations

- **confluence_treepack.html**: Treemap visualization of Confluence spaces
- **semantic_scatter_plot.html**: Scatter plot showing semantic relationships
- **clustered_spaces.html**: Visualization of clustered spaces
- **application_search_results.html**: Results from application search

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.


## SAMPLE_AND_PICKLE_SPACES.PY

Output Directory (temp) for .pkl files:

The script does not automatically delete the entire temp directory or its existing contents when it starts.

When the script processes a specific Confluence space (e.g., with key SPACEXYZ), it saves its sampled data to a pickle file (e.g., temp/SPACEXYZ.pkl).

If a file like temp/SPACEXYZ.pkl already exists from a previous run, and the script re-processes the SPACEXYZ space in the current run, the existing .pkl file for that specific space will be overwritten.

If a .pkl file exists in temp for a space that is not re-processed in the current run (for example, if the script resumes from a checkpoint and skips already processed spaces, or if the script is terminated before it reaches that space), that existing .pkl file will remain untouched.
Checkpoint File (confluence_checkpoint.json):

Normal Re-run (without --reset):
The script reads the confluence_checkpoint.json file to determine which spaces have already been processed and what the last position was.
It will skip processing spaces that are already listed in the checkpoint as completed.
The checkpoint file itself is updated as new spaces are processed.

Re-run with --reset flag:
The script accepts a --reset command-line argument (e.g., python [sample_and_pickle_spaces.py](http://_vscodecontentref_/8) --reset).

If this flag is used, the existing confluence_checkpoint.json file will be deleted at the beginning of the script's execution.

The script will then start processing all spaces from the beginning, as if it's a fresh run, and will create a new checkpoint file.

In summary:
Existing individual .pkl files for spaces in the temp directory are not automatically removed en masse. They are overwritten if and when the script re-processes that specific space.

The confluence_checkpoint.json file is used to manage resumption and can be removed if you use the --reset option, forcing a full re-processing of all spaces.