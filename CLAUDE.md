# Confluence Visualization - Claude Code Notes

## Environment Notes

- **WSL/Windows**: To open files in Windows default app, use `explorer.exe <filename>` - this opens a Windows dialog to select the application (e.g., PyCharm, VS Code, Notepad)

## Key Entry Points

| Task | Command |
|------|---------|
| Fetch data | `python collectors/sample_and_pickle_spaces.py` |
| Explore/analyze | `python explorers/explore_clusters.py` |
| Generate treemap | `python visualizers/render_html.py` |
| Browse SQL scripts | `python sql/browse_extracted_sql_web.py --db sql_queries.db` |
| Run tests | `pytest tests/test_browse_extracted_sql_web.py -v` |

## Project Structure

```
collectors/       - Data extraction (sample_and_pickle_spaces.py, etc.)
explorers/        - Interactive analysis (explore_clusters.py, etc.)
visualizers/      - Rendering (render_html.py, scatter_plot_visualizer.py, etc.)
uploaders/        - OpenWebUI uploaders (open-webui.py, etc.)
uploaders/qdrant/ - Qdrant uploaders (formerly GENERIC_SCRIPTS/)
sql/              - SQL extraction & browsing (browse_extracted_sql_web.py, etc.)
confluence_ops/   - Confluence operations (empty pages checker, delete, audit, etc.)
diagnostics/      - Check/diagnostic scripts (check_config.py, etc.)
tests/            - All test files
utils/            - Shared utilities (config_loader.py, html_cleaner.py)
temp/             - Pickle files from data collection
confluence-fast-mcp/ - MCP server (separate sub-project)
```

## Testing

### IMPORTANT: Test Modification Policy

**DO NOT modify test files without explicit user approval, even if "accept edits" is enabled.**

Test files in this project:
- `tests/test_browse_extracted_sql_web.py` - SQL browser tests

These tests verify critical functionality including search persistence across views.
Any changes to tests must be explicitly requested and approved by the user.

### Running Tests

```bash
# Run all tests
pytest tests/test_browse_extracted_sql_web.py -v

# Run specific test class
pytest tests/test_browse_extracted_sql_web.py::TestInsightsView -v

# Run with coverage
pytest tests/test_browse_extracted_sql_web.py -v --cov=sql.browse_extracted_sql_web
```

## Current Status

See `CONFLUENCE-VIZ-2-0.md` for cleanup progress and known issues.
