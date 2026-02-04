# Confluence Visualization - Claude Code Notes

## Environment Notes

- **WSL/Windows**: To open files in Windows default app, use `explorer.exe <filename>` - this opens a Windows dialog to select the application (e.g., PyCharm, VS Code, Notepad)

## Key Entry Points

| Task | Command |
|------|---------|
| Fetch data | `python sample_and_pickle_spaces.py` |
| Explore/analyze | `python explore_clusters.py` |
| Generate treemap | `python render_html.py` |
| Browse SQL scripts | `python browse_extracted_sql_web.py --db sql_queries.db` |
| Run tests | `pytest test_browse_extracted_sql_web.py -v` |

## Project Structure

- `temp/` - Pickle files from data collection
- `utils/` - Shared utilities (html_cleaner.py)
- `GENERIC_SCRIPTS/` - Qdrant/OpenWebUI uploaders
- `CONFLUENCE-VIZ-2-0.md` - Cleanup plan and status

## Testing

### IMPORTANT: Test Modification Policy

**DO NOT modify test files without explicit user approval, even if "accept edits" is enabled.**

Test files in this project:
- `test_browse_extracted_sql_web.py` - SQL browser tests

These tests verify critical functionality including search persistence across views.
Any changes to tests must be explicitly requested and approved by the user.

### Running Tests

```bash
# Run all tests
pytest test_browse_extracted_sql_web.py -v

# Run specific test class
pytest test_browse_extracted_sql_web.py::TestInsightsView -v

# Run with coverage
pytest test_browse_extracted_sql_web.py -v --cov=browse_extracted_sql_web
```

## Current Status

See `CONFLUENCE-VIZ-2-0.md` for cleanup progress and known issues.
