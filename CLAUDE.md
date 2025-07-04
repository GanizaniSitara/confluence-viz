# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Confluence visualization toolkit that extracts, analyzes, and visualizes data from Confluence instances to provide insights into content organization, usage patterns, and semantic relationships.

## Common Commands

### Setup and Configuration
```bash
# Install dependencies
pip install requests beautifulsoup4 numpy scikit-learn whoosh matplotlib

# Setup configuration
cp settings.example.ini settings.ini
# Edit settings.ini with your Confluence credentials
```

### Data Collection
```bash
# Fetch data from Confluence API
python fetch_data.py

# Run semantic analysis (optional)
python semantic_analysis.py

# Sample specific spaces for detailed analysis
python sample_and_pickle_spaces.py
# Use --reset flag to start fresh: python sample_and_pickle_spaces.py --reset
```

### Visualization Generation
```bash
# Generate treemap visualization
python render_html.py

# Generate semantic visualizations
python render_semantic_html.py

# Main visualization with percentile coloring
python viz.py
```

### Analysis Tools
```bash
# Interactive cluster exploration
python explore_clusters.py

# Browse space content
python space_explorer.py

# Explore pickled data
python explore_pickle_content.py

# Application search and analysis
python seed_applications.py
```

## Architecture Overview

### Core Components

1. **Configuration System** (`config_loader.py`)
   - Loads settings from `settings.ini`
   - Handles Confluence API credentials and visualization parameters
   - Supports both Confluence and OpenWebUI configurations

2. **Data Extraction Layer** (`fetch_data.py`, `viz.py`)
   - `viz.py`: Core API interaction, space fetching, and page data collection
   - `fetch_data.py`: Simplified interface for data fetching and persistence
   - Uses exponential backoff for rate limiting (429 responses)
   - Supports SSL verification control

3. **Content Processing** (`utils/html_cleaner.py`)
   - Cleans raw Confluence HTML content
   - Removes or replaces Confluence macros with descriptive placeholders
   - Handles attachments, JIRA issues, diagrams, and tables
   - Converts HTML to readable text while preserving structure

4. **Analysis Engine**
   - **Semantic Analysis** (`semantic_analysis.py`): TF-IDF and LSA processing
   - **Clustering** (`explore_clusters.py`): Multiple algorithms (KMeans, Agglomerative, DBSCAN)
   - **Search** (`seed_applications.py`): Whoosh-based content indexing and search

5. **Visualization Layer**
   - **D3.js Integration**: Generates interactive HTML visualizations
   - **Treemap Visualizations** (`render_html.py`): Hierarchical space/page visualization
   - **Semantic Plots** (`render_semantic_html.py`): Relationship diagrams
   - **Scatter Plots** (`scatter_plot_visualizer.py`): Clustering visualizations
   - **Proximity Maps** (`proximity_visualizer.py`): Content similarity visualization

### Data Flow

1. **Collection**: Confluence API → Raw space/page data → `confluence_data.pkl`
2. **Processing**: HTML cleaning → Text extraction → Semantic analysis → `confluence_semantic_data.pkl`
3. **Sampling**: Detailed content extraction → Individual space pickles (`temp/*.pkl`)
4. **Visualization**: Processed data → Interactive HTML files

### Key Data Structures

- **Space Objects**: `{key, name, value (page count), avg (average edit timestamp)}`
- **Pickle Files**: 
  - `confluence_data.pkl`: Main dataset with all spaces and basic metrics
  - `confluence_semantic_data.pkl`: Semantic analysis results
  - `temp/<SPACE_KEY>.pkl`: Individual space content samples
  - `temp/<SPACE_KEY>_full.pkl`: Complete space content (when available)

### Configuration Files

- `settings.ini`: Main configuration (API credentials, visualization parameters)
- `settings.example.ini`: Template with all available options
- `confluence_checkpoint.json`: Checkpoint file for resumable space processing

### HTML Content Processing

The `utils/html_cleaner.py` module handles Confluence-specific HTML:
- Removes visual-only macros (carousel, gallery, profile-picture)
- Replaces functional macros with descriptive placeholders
- Extracts attachment filenames and JIRA issue keys
- Converts tables to text-based format
- Preserves heading structure with Markdown-style formatting

### Percentile-Based Coloring

The visualization system uses percentile-based coloring for spaces:
- Colors based on average page edit timestamps within each space
- Red (oldest) → Yellow (middle) → Green (newest) gradient
- Grey for spaces with no pages
- Configurable number of color bins (default: 10)

## Development Notes

- The codebase uses minimal dependencies (requests, beautifulsoup4, numpy, scikit-learn)
- Error handling includes retry logic for API rate limiting
- Checkpointing system allows resuming long-running data collection processes
- SSL verification can be disabled for internal/test Confluence instances
- Interactive exploration tools provide menu-driven interfaces for analysis