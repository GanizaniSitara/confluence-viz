# Confluence Visualization Project

A professional Python toolkit for extracting, analyzing, and visualizing data from Confluence instances to gain insights into content organization, usage patterns, and semantic relationships.

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Setup configuration  
cp settings.example.ini settings.ini
# Edit settings.ini with your Confluence credentials

# Run the main visualization tool
./confluence-viz
```

## 📋 Overview

This project helps organizations understand how their Confluence instance is being used by:

1. **Data Collection**: Fetching metadata and content from Confluence spaces and pages
2. **Analysis**: 
   - Hierarchical clustering of spaces based on content
   - Semantic analysis using TF-IDF and LSA (Latent Semantic Analysis)
   - Full-text search capability across spaces and pages
3. **Visualization**: 
   - Interactive D3.js treemap visualizations of spaces and pages
   - Scatter plots showing semantic relationships
   - Proximity visualizations of related content
   - Cluster analysis visualizations

## 🏗️ Project Structure

The project has been refactored into a clean, modular Python package:

```
confluence_visualization/
├── confluence-viz                    # Main CLI executable
├── setup.py                         # Package setup
├── requirements*.txt                 # Dependencies
├── settings.example.ini             # Configuration template
└── confluence_viz/                  # Main Python package
    ├── admin/                       # Administrative tools
    │   ├── audit_admins.py         # Space admin auditing
    │   ├── manage_users.py         # User management
    │   ├── check_empties.py        # Find empty/deletable pages
    │   └── cleanup_data.py         # Data cleanup utilities
    ├── analysis/                    # Analysis and clustering
    │   ├── semantic_analysis.py    # TF-IDF and LSA analysis
    │   ├── explore_clusters.py     # Interactive clustering tool
    │   ├── counter.py              # Page/space counting
    │   ├── file_type_counter.py    # File type analysis
    │   └── user_activity.py        # User activity tracking
    ├── api/                         # Confluence API client
    │   └── client.py               # HTTP client with retry logic
    ├── cli/                         # Command-line tools
    │   ├── main.py                 # Main CLI entry point
    │   ├── space_explorer.py       # Interactive space browser
    │   ├── explore_pickle_content.py # Data exploration tool
    │   └── get_confluence_watches_all.py # Watch tracking
    ├── config/                      # Configuration management
    │   ├── loader.py               # Legacy config loader
    │   └── settings.py             # Enhanced config with validation
    ├── data/                        # Data collection
    │   ├── collector.py            # Main data collection orchestrator
    │   ├── fetch_data.py           # Simple data fetcher
    │   ├── sample_and_pickle_spaces.py # Advanced sampling
    │   └── extract_html_content.py # HTML extraction
    ├── utils/                       # Utilities
    │   ├── html_cleaner.py         # Confluence HTML processing
    │   ├── color_utils.py          # Color calculations
    │   └── logging.py              # Structured logging
    ├── visualization/               # Visualization generation
    │   ├── treemap.py              # D3.js treemap generation
    │   ├── render_html.py          # Basic HTML rendering
    │   ├── render_semantic_html.py # Semantic visualizations
    │   ├── scatter_plot_visualizer.py # Scatter plots
    │   └── proximity_visualizer.py # Proximity visualizations
    └── testing/                     # Testing and development
        ├── seed.py                 # Test data generation
        ├── seed_applications.py    # Application seeding
        └── proof_of_concept/       # POC scripts
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- Confluence instance with API access
- Valid Confluence API credentials

### Setup

1. **Clone and install**:
   ```bash
   git clone <repository-url>
   cd confluence_visualization
   pip install -r requirements.txt
   ```

2. **Optional dependencies** (for extended features):
   ```bash
   pip install -r requirements-optional.txt
   ```

3. **Development dependencies** (for contributing):
   ```bash
   pip install -r requirements-dev.txt
   ```

4. **Configuration**:
   ```bash
   cp settings.example.ini settings.ini
   # Edit settings.ini with your Confluence credentials
   ```

## 📖 Usage

### Main CLI Tool

```bash
# Run with default settings
./confluence-viz

# Use custom config file
./confluence-viz --config my-settings.ini

# Custom output file
./confluence-viz --output my-visualization.html

# Don't open browser automatically  
./confluence-viz --no-browser
```

### Individual Modules

```bash
# Data collection
python -m confluence_viz.data.fetch_data

# Semantic analysis
python -m confluence_viz.analysis.semantic_analysis

# Interactive clustering exploration
python -m confluence_viz.analysis.explore_clusters

# Space exploration
python -m confluence_viz.cli.space_explorer

# Admin tools
python -m confluence_viz.admin.audit_admins
python -m confluence_viz.admin.manage_users
```

## 🎯 Key Features

### Data Collection
- **API Integration**: Robust Confluence REST API client with retry logic
- **Rate Limiting**: Automatic handling of API rate limits
- **Checkpointing**: Resume interrupted data collection processes
- **Sampling**: Smart sampling for large Confluence instances

### Content Analysis  
- **HTML Cleaning**: Processes raw Confluence HTML, removes macros, extracts clean text
- **Semantic Analysis**: TF-IDF vectorization and LSA dimensionality reduction
- **Clustering**: Multiple algorithms (KMeans, Agglomerative, DBSCAN)
- **Search**: Full-text search with Whoosh indexing
- **Activity Tracking**: User activity and contribution analysis

### Visualizations
- **Interactive Treemaps**: D3.js circle packing with hover details
- **Color Coding**: Percentile-based coloring by content freshness
- **Scatter Plots**: t-SNE projections of semantic relationships  
- **Proximity Maps**: Content similarity visualizations
- **Responsive Design**: Works across different screen sizes

### Administrative Tools
- **User Management**: Create, delete, manage user permissions
- **Space Auditing**: Audit space administrators and permissions
- **Content Cleanup**: Identify empty or deletable pages
- **Watch Tracking**: Track user content subscriptions

## 📁 Output Files

The toolkit generates several types of output:

- `confluence_data.pkl/json` - Raw space and page data
- `confluence_semantic_data.pkl` - Semantic analysis results  
- `confluence_treepack.html` - Interactive treemap visualization
- `temp/*.pkl` - Individual space data samples
- Various CSV files for admin audits

## 🔧 Configuration

The `settings.ini` file supports multiple sections:

```ini
[confluence]
base_url = https://your-confluence.atlassian.net
username = your_username  
password = your_api_token
verify_ssl = True

[visualization]
default_clusters = 20
default_min_pages = 5
gradient_steps = 10

[data_collection] 
spaces_page_limit = 50
content_page_limit = 100
enable_checkpointing = True
```

Environment variables are also supported:
- `CONFLUENCE_BASE_URL`
- `CONFLUENCE_USERNAME` 
- `CONFLUENCE_PASSWORD`

## 🧪 Development

### Running Tests
```bash
python -m pytest confluence_viz/testing/
```

### Code Quality
```bash
flake8 confluence_viz/
black confluence_viz/
mypy confluence_viz/
```

### Logging
Set log level via environment:
```bash
export LOG_LEVEL=DEBUG
./confluence-viz
```

## 📚 Advanced Usage

### Large Instances
For large Confluence instances:
1. Use sampling mode first: `python -m confluence_viz.data.sample_and_pickle_spaces`
2. Run analysis on samples: `python -m confluence_viz.analysis.explore_clusters`
3. Generate full collection if needed

### Custom Analysis
The modular structure allows custom analysis:
```python
from confluence_viz.data.collector import ConfluenceDataCollector
from confluence_viz.analysis.semantic_analysis import SemanticAnalyzer

collector = ConfluenceDataCollector()
spaces = collector.fetch_all_spaces()
# Custom processing...
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Ensure code quality checks pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

For detailed file-by-file documentation, see [FILE_ANALYSIS.md](FILE_ANALYSIS.md).
For cleanup recommendations, see [CLEANUP_RECOMMENDATIONS.md](CLEANUP_RECOMMENDATIONS.md).