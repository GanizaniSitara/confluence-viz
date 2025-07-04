# Confluence Visualization Project - File Analysis

**Project Overview:** A comprehensive Python toolkit for analyzing, visualizing, and managing Confluence instances. The project provides data collection, semantic analysis, clustering, visualization, and administrative tools for Confluence spaces and pages.

---

## Configuration & Utilities

### `config_loader.py`
- **Purpose:** Configuration management for Confluence API settings and visualization parameters
- **Key Functions:**
  - `load_confluence_settings()`: Loads Confluence API credentials and settings
  - `load_visualization_settings()`: Loads visualization parameters like cluster defaults
- **Dependencies:** None (core utility)
- **Input Files:** `settings.ini`
- **Usage:** Imported by most other modules for configuration

### `utils/html_cleaner.py`
- **Purpose:** HTML content cleaning and processing for Confluence pages
- **Key Functions:**
  - `clean_confluence_html()`: Removes/replaces Confluence macros, converts to readable text
  - `get_attachment_placeholder()`: Generates placeholders for attachments
  - `_format_table_for_console()`: Converts HTML tables to text format
- **Dependencies:** BeautifulSoup4
- **Input/Output:** HTML content → cleaned text
- **Usage:** Used by analysis modules to process page content

### `utils/__init__.py`
- **Purpose:** Utils package initialization file
- **Content:** Empty package marker

---

## Data Collection

### `fetch_data.py`
- **Purpose:** Main data collection orchestrator
- **Key Functions:**
  - `fetch_and_save_data()`: Orchestrates the complete data fetching process
- **Dependencies:** `viz.py` for core fetching functions
- **Input/Output:** Confluence API → `confluence_data.pkl`
- **Usage:** `python fetch_data.py`

### `viz.py`
- **Purpose:** Core Confluence data fetching and visualization generation
- **Key Functions:**
  - `fetch_all_spaces()`: Retrieves all non-personal spaces
  - `fetch_page_data_for_space()`: Gets page metadata and timestamps
  - `build_data()`: Constructs hierarchical data structure
  - Color calculation and percentile functions
- **Dependencies:** `config_loader.py`
- **Input/Output:** Confluence API → `confluence_data.json` + HTML visualization
- **Usage:** Main data collection module

### `sample_and_pickle_spaces.py`
- **Purpose:** Advanced space sampling and full data collection with checkpointing
- **Key Functions:**
  - `fetch_page_metadata()`: Retrieves comprehensive page metadata
  - `sample_and_fetch_bodies()`: Samples pages or fetches all (full mode)
  - `pickle_space_details()`: Saves space data to pickle files
  - `download_attachments_for_space()`: Downloads space attachments
- **Dependencies:** `config_loader.py`
- **Input/Output:** Confluence API → pickle files in `temp/` or configured directories
- **Usage:** Interactive menu or command-line arguments for batch processing

### `space_explorer.py`
- **Purpose:** Interactive space exploration and detailed data extraction
- **Key Functions:**
  - `pickle_space_details()`: Detailed space data extraction
  - `get_page_id_from_url()`: Extracts page IDs from various URL formats
  - `check_page_user_status()`: Analyzes page contributors and their status
- **Dependencies:** `config_loader.py`
- **Input/Output:** Confluence API → `temp_space_explorer_*` directories
- **Usage:** Interactive menu system

### `get_confluence_watches_all.py`
- **Purpose:** Enumerates all watched content for the current user
- **Key Functions:**
  - `get_watched_content_cql()`: Uses CQL to find watched content
  - `export_to_json()`: Exports watch data to JSON
- **Dependencies:** `config.ini` (direct configuration)
- **Input/Output:** Confluence API → `confluence_watches.json`
- **Usage:** `python get_confluence_watches_all.py`

---

## Analysis & Processing

### `semantic_analysis.py`
- **Purpose:** Semantic vectorization of Confluence spaces using TF-IDF and LSA
- **Key Functions:**
  - `process_spaces_parallel()`: Parallel text extraction from spaces
  - `compute_semantic_vectors()`: TF-IDF and LSA transformation
- **Dependencies:** scikit-learn, `config_loader.py`
- **Input/Output:** `confluence_data.pkl` → `confluence_semantic_data.pkl`
- **Usage:** `python semantic_analysis.py`

### `explore_clusters.py`
- **Purpose:** Interactive clustering analysis and visualization tool
- **Key Functions:**
  - `cluster_spaces()`: Performs clustering (Agglomerative, KMeans, DBSCAN)
  - `suggest_tags_for_clusters()`: Generates cluster tags from content
  - `render_d3_circle_packing()`: Creates D3.js visualizations
  - Search functionality with Whoosh indexing
- **Dependencies:** scikit-learn, matplotlib, Whoosh (optional)
- **Input/Output:** Pickle files → HTML visualizations and search indexes
- **Usage:** Interactive menu with 21+ options

### `file_type_counter.py`
- **Purpose:** Analyzes and counts file types across Confluence spaces
- **Key Functions:** File type analysis from pickle data
- **Dependencies:** Pickle files from data collection
- **Usage:** File type statistics

### `extract_html_content.py`
- **Purpose:** HTML content extraction utilities
- **Dependencies:** HTML processing libraries
- **Usage:** Content extraction from Confluence pages

---

## Visualization

### `scatter_plot_visualizer.py`
- **Purpose:** 2D scatter plot visualizations using t-SNE
- **Key Functions:**
  - `generate_2d_scatter_plot_agglomerative()`: Creates clustered scatter plots
  - `render_d3_semantic_scatter_plot()`: D3.js interactive scatter plots
- **Dependencies:** scikit-learn, `config_loader.py`
- **Input/Output:** Clustered data → HTML scatter plot visualizations
- **Usage:** Called from `explore_clusters.py`

### `proximity_visualizer.py`
- **Purpose:** Semantic proximity visualization using t-SNE
- **Key Functions:**
  - `generate_proximity_scatter_plot()`: t-SNE proximity analysis
  - `render_d3_proximity_scatter_plot()`: D3.js proximity visualizations
- **Dependencies:** scikit-learn, `config_loader.py`
- **Input/Output:** Vector data → HTML proximity visualizations
- **Usage:** Called from `explore_clusters.py`

### `render_html.py`
- **Purpose:** Standard HTML circle packing visualization
- **Key Functions:**
  - `load_data_and_render()`: Creates D3.js circle packing from pickle data
  - Color calculation and filtering utilities
- **Dependencies:** numpy, `confluence_data.pkl`
- **Input/Output:** `confluence_data.pkl` → `confluence_treepack.html`
- **Usage:** `python render_html.py [--min-pages N]`

### `render_semantic_html.py`
- **Purpose:** Semantic similarity-based HTML visualizations
- **Key Functions:**
  - `reorganize_data_by_similarity()`: Groups similar spaces using semantic vectors
  - `load_data_and_render()`: Creates semantic circle packing visualization
- **Dependencies:** scipy, `confluence_semantic_data.pkl`
- **Input/Output:** `confluence_semantic_data.pkl` → `confluence_semantic_treepack.html`
- **Usage:** `python render_semantic_html.py [--min-pages N]`

---

## Administrative Tools

### `audit_admins.py`
- **Purpose:** Comprehensive space administrator audit tool
- **Key Functions:**
  - `audit_all_spaces()`: Audits admin permissions across all spaces
  - `check_current_user_admin()`: Checks current user's admin rights
  - `get_space_admins()`: Retrieves admin users for specific spaces
- **Dependencies:** `config_loader.py`, `contributors_current.csv`
- **Input/Output:** Confluence API → CSV output of admin audit results
- **Usage:** Interactive menu with 3 audit options

### `manage_users.py`
- **Purpose:** User management operations (create, delete, permissions)
- **Key Functions:**
  - `create_user()`: Creates new Confluence users
  - `delete_user()`: Deletes/deactivates users
  - `assign_space_admin()`: Assigns space admin permissions
  - `remove_space_admin()`: Removes space admin permissions
- **Dependencies:** `config_loader.py`, `contributors.csv`
- **Input/Output:** Confluence API operations based on CSV data
- **Usage:** Interactive menu for user management

### `check_empties.py`
- **Purpose:** Identifies empty pages that can be deleted
- **Key Functions:**
  - `check_pages_in_space()`: Analyzes specific space for empty pages
  - `check_all_spaces()`: Checks all spaces for deletable empty pages
  - `check_single_page()`: Analyzes individual page
- **Dependencies:** `config_loader.py`
- **Input/Output:** Confluence API → `deletable_pages.txt`
- **Usage:** Interactive menu or command-line arguments

---

## Analysis & Utilities

### `counter.py`
- **Purpose:** Comprehensive page and space counting with filtering
- **Key Functions:**
  - `count_using_cql()`: CQL-based counting
  - `get_all_items()`: Paginated item retrieval
  - `filter_pages_by_date()`: Date-based filtering
- **Dependencies:** `config_loader.py`
- **Input/Output:** Confluence API → pickle files, count statistics
- **Usage:** Interactive menu with date filtering

### `user_activity.py`
- **Purpose:** User activity tracking and visualization
- **Key Functions:**
  - `get_all_users()`: Retrieves all active users
  - `get_user_activity()`: Tracks user modification activity
  - `plot_top_users()`: Creates activity visualizations
- **Dependencies:** matplotlib, requests
- **Input/Output:** Confluence API → `user_activity.csv` + plot images
- **Usage:** `python user_activity.py`

### `explore_pickle_content.py`
- **Purpose:** Pickle file content exploration and analysis
- **Usage:** Analysis of pickle file contents

---

## Data Generation & Testing

### `seed.py`
- **Purpose:** Generates test data for Confluence instances
- **Key Functions:**
  - `create_space()`: Creates test spaces
  - `create_page()`: Creates test pages with generated content
  - `generate_content_with_corporate_lorem()`: Uses Corporate Lorem API
- **Dependencies:** requests, CorporateLorem API
- **Input/Output:** Confluence API (creates test spaces and pages)
- **Usage:** `python seed.py [arguments]` for bulk test data creation

### `seed_applications.py`
- **Purpose:** Application-specific seed data generation
- **Dependencies:** Similar to `seed.py`
- **Usage:** Specialized seeding for application testing

---

## Development & Testing Files

### Test and Development Files
- `test_explore_clusters.py`: Testing for cluster analysis
- `create_user_test.py`: User creation testing
- `proof_of_concept_admins.py`: Admin functionality proof of concept
- `proof_of_concept_playwright_admins.py`: Playwright-based admin testing
- `remove_first_admin.py`: Admin removal utilities
- `cleanup_data.py`: Data cleanup utilities
- `open-webui.py`: Web UI integration

---

## Dependencies Overview

**Core Dependencies:**
- `requests`: Confluence API communication
- `pickle`: Data serialization
- `configparser`: Configuration management
- `scikit-learn`: Machine learning (clustering, TF-IDF, t-SNE)
- `matplotlib`: Basic plotting
- `numpy`: Numerical operations
- `BeautifulSoup4`: HTML processing

**Optional Dependencies:**
- `Whoosh`: Full-text search indexing
- `scipy`: Scientific computing
- `playwright`: Browser automation (some POC files)

---

## Configuration Files

- `settings.ini`/`settings.example.ini`: Main configuration
- `app_search.txt`/`app_serach.example.txt`: Application search terms
- `stopwords.txt`: Custom stopwords for text analysis
- `contributors.csv`/`contributors_current.csv`: User management data

---

## Key Data Flow

1. **Collection:** `fetch_data.py` or `sample_and_pickle_spaces.py` → Raw data from Confluence
2. **Processing:** `semantic_analysis.py` → Semantic vectors
3. **Analysis:** `explore_clusters.py` → Clustering and insights
4. **Visualization:** Various render modules → Interactive HTML visualizations
5. **Administration:** Admin tools for user and space management

The project supports both quick sampling for analysis and comprehensive full-data collection for production use, with extensive filtering, clustering, and visualization capabilities.