# Proposed Project Structure

Based on the file analysis, here's a proposed new structure for the Confluence Visualization project:

## Current Issues
- 28 Python files in root directory
- No clear package organization
- Difficult to understand relationships between modules
- No separation of concerns

## Proposed Structure

```
confluence_visualization/
├── setup.py
├── requirements.txt
├── requirements-optional.txt
├── requirements-dev.txt
├── README.md
├── CLAUDE.md
├── FILE_ANALYSIS.md
├── settings.example.ini
├── app_search.example.txt
├── stopwords.txt
└── confluence_viz/                    # Main package
    ├── __init__.py
    ├── config/                        # Configuration management
    │   ├── __init__.py
    │   ├── loader.py                  # config_loader.py
    │   └── settings.py                # Configuration classes
    ├── api/                           # Confluence API interactions
    │   ├── __init__.py
    │   ├── client.py                  # Main API client
    │   ├── spaces.py                  # Space operations
    │   ├── pages.py                   # Page operations
    │   └── utils.py                   # API utilities (retry logic, etc.)
    ├── data/                          # Data collection and processing
    │   ├── __init__.py
    │   ├── collector.py               # fetch_data.py logic
    │   ├── sampler.py                 # sample_and_pickle_spaces.py
    │   └── processor.py               # Data processing utilities
    ├── analysis/                      # Analysis modules
    │   ├── __init__.py
    │   ├── semantic.py                # semantic_analysis.py
    │   ├── clustering.py              # clustering logic from explore_clusters.py
    │   ├── counters.py                # counter.py, file_type_counter.py
    │   └── activity.py                # user_activity.py
    ├── visualization/                 # Visualization modules
    │   ├── __init__.py
    │   ├── base.py                    # Common visualization utilities
    │   ├── treemap.py                 # render_html.py, render_semantic_html.py
    │   ├── scatter.py                 # scatter_plot_visualizer.py
    │   ├── proximity.py               # proximity_visualizer.py
    │   └── templates/                 # HTML templates
    │       ├── treemap.html
    │       ├── scatter.html
    │       └── proximity.html
    ├── admin/                         # Administrative tools
    │   ├── __init__.py
    │   ├── audit.py                   # audit_admins.py
    │   ├── users.py                   # manage_users.py
    │   └── cleanup.py                 # check_empties.py, cleanup_data.py
    ├── utils/                         # Utility modules
    │   ├── __init__.py
    │   ├── html_cleaner.py            # (keep as-is)
    │   ├── logging.py                 # New logging utilities
    │   └── helpers.py                 # Common helper functions
    ├── cli/                           # Command-line interfaces
    │   ├── __init__.py
    │   ├── main.py                    # Main CLI entry point
    │   ├── explore.py                 # explore_clusters.py, explore_pickle_content.py
    │   ├── space_explorer.py          # space_explorer.py
    │   └── watches.py                 # get_confluence_watches_all.py
    └── testing/                       # Testing and development
        ├── __init__.py
        ├── seed.py                    # seed.py, seed_applications.py
        ├── test_data.py               # create_user_test.py
        └── proof_of_concept/          # POC files
            ├── __init__.py
            ├── admins.py              # proof_of_concept_admins.py
            ├── playwright_admins.py   # proof_of_concept_playwright_admins.py
            └── webui.py               # open-webui.py
```

## Migration Strategy

### Phase 1: Create Package Structure
1. Create directory structure
2. Create `__init__.py` files
3. Move and rename files to new locations

### Phase 2: Refactor Large Files
1. Split `viz.py` into api/client.py, data/collector.py, visualization/treemap.py
2. Split `explore_clusters.py` into analysis/clustering.py and cli/explore.py
3. Refactor configuration management

### Phase 3: Update Imports
1. Update all import statements
2. Update entry points in setup.py
3. Test all functionality

### Phase 4: Add Improvements
1. Add proper logging
2. Add error handling
3. Add tests
4. Update documentation

## Benefits

1. **Clear Separation of Concerns**: Each module has a single responsibility
2. **Easier Navigation**: Related functionality is grouped together
3. **Better Testing**: Each module can be tested independently
4. **Maintainability**: Changes to one area don't affect others
5. **Extensibility**: Easy to add new features in the right place
6. **Professional Structure**: Follows Python packaging best practices

## Entry Points

The new structure will support these command-line tools:
- `confluence-fetch`: Data collection
- `confluence-analyze`: Semantic analysis
- `confluence-viz`: Visualization generation
- `confluence-explore`: Interactive exploration
- `confluence-admin`: Administrative tools
- `confluence-seed`: Test data generation

## Configuration

All configuration will be managed through the `confluence_viz.config` module, with:
- Single source of truth for settings
- Validation and error handling
- Environment variable support
- Multiple configuration file support