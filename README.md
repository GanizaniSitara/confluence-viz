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
  - **HTML Content Cleaning**: Raw Confluence HTML is processed to remove disruptive macros, replace others with informative placeholders (e.g., for attachments, JIRA issues), and extract clean, readable text. This functionality is primarily handled by the `utils/html_cleaner.py` module.
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
   # Base URL for your Confluence instance (e.g., http://localhost:8090 or https://your-domain.atlassian.net)
   base_url = https://your-confluence-instance.atlassian.net
   username = your_username
   password = your_api_token
   verify_ssl = True
   # base_url = https://your-confluence-instance.atlassian.net ; Note: This is a duplicate entry for base_url. The first one is used for API calls.

   [visualization]
   default_clusters = 20
   default_min_pages = 5
   # Optional: Path to a directory containing pre-generated full pickle files (e.g., from a remote server or shared location)
   # If set, explore_pickle_content.py will look for <SPACE_KEY>_full.pkl files here when 'full content' is requested.
   # Example: remote_full_pickle_dir = /mnt/shared_pickles/ or C:\shared_pickles\
   remote_full_pickle_dir = 
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
- **utils/html_cleaner.py**: A utility module responsible for cleaning HTML content fetched from Confluence. Its main function, `clean_confluence_html`, parses HTML, removes specified Confluence macros, replaces others (e.g., attachments, JIRA issues) with descriptive placeholders, converts horizontal rules to text separators, and extracts normalized, readable text.
- **explore_clusters.py**: Interactive tool for exploring different clustering methods
- **explore_pickle_content.py**: An interactive script to browse the content of pickled Confluence spaces (typically generated by `sample_and_pickle_spaces.py`). It allows users to list pages within a selected space and view their content. Key features include selection of spaces by number or space key, viewing raw HTML content (full or snippet), and viewing cleaned text content processed by `utils/html_cleaner.py`. Users can toggle between raw and cleaned views using menu options.
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

## Troubleshooting

### Empty Content After Cleaning (0 pages with content in explore_clusters)

If `explore_clusters.py` reports 0 or very few pages with content after cleaning, use `explore_pickle_content.py` to diagnose:

```bash
python explore_pickle_content.py SPACENAME
```

1. Select **Option 5** (Explore Pages) to paginate through pages
2. Press **`r`** to view **raw HTML** - see what's actually stored in the body
3. Press **`c`** to view **cleaned** text - see what remains after cleaning
4. Press **`f`** to toggle between full content and snippet view
5. Press **`n`/`p`** to navigate next/previous page

**Diagnosis:**
- **Raw HTML has content, cleaned is empty**: The HTML cleaner is stripping everything. This typically happens when Confluence content uses XML namespace tags (`ac:structured-macro`, `ac:rich-text-body`, etc.) that BeautifulSoup's `html.parser` doesn't traverse into properly. The fix is to normalize namespace prefixes before parsing (convert `ac:tag` to `ac-tag`). This fix has been applied to `explore_clusters.py` but may also need to be applied to `utils/html_cleaner.py`.

- **Raw HTML is empty**: The body content wasn't fetched. Check:
  - API user permissions (may lack read access to page content)
  - Confluence API version differences (Cloud vs Server)
  - Network/authentication issues during pickle generation

- **Raw HTML contains only macros**: Some pages are 100% macro content (diagrams, embeds, etc.) with no text. These will legitimately be empty after cleaning.

### Diagnosing Pickle Structure

Run the diagnostic script to inspect pickle file contents:

```bash
python diagnose_pickle_bodies.py --dir /path/to/pickles --files 3 --pages 5
```

This shows body types (string vs dict), content lengths, and identifies structural issues.