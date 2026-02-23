# Confluence Fast MCP Server

A high-performance Model Context Protocol (MCP) server that provides fast access to Confluence data by serving pre-pickled content instead of making live API calls.

## Features

- **10-100x faster** than live Confluence API calls
- **Offline capability** for previously-fetched spaces
- **Full-text search** using WHOOSH indexing
- **Compatible** with standard mcp-atlassian API
- **CQL query support** for familiar search syntax
- **HTML to ADF conversion** for proper rendering

## Quick Start

### Installation

```bash
pip install -e .
```

### Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `settings.ini` to point to your pickle directory:
```ini
[data]
pickle_dir = /path/to/your/confluence-viz/temp
```

3. Generate pickled data using confluence-viz's `sample_and_pickle_spaces.py`

### Usage

Start the MCP server:
```bash
python -m confluence_fast_mcp.server
```

Or use the installed script:
```bash
confluence-fast-mcp
```

## How It Works

1. **Pickle Loading**: Reads pre-pickled Confluence spaces from disk
2. **WHOOSH Indexing**: Builds a full-text search index on startup
3. **Fast Queries**: Serves data from memory and local index
4. **Format Conversion**: Converts HTML storage to ADF for compatibility
5. **Fallback**: Falls back to central Confluence for attachments

## Supported Tools

- `getAccessibleAtlassianResources()` - Get available resources
- `atlassianUserInfo()` - Get user info
- `getConfluenceSpaces()` - List all spaces
- `getConfluencePage()` - Get page by ID or title
- `getPagesInConfluenceSpace()` - List pages in a space
- `searchConfluenceUsingCql()` - Search with CQL syntax

## CQL Query Support

Basic CQL syntax is supported:

```
text ~ "kubernetes"                    # Search in all text
space = TECH                           # Filter by space key
title ~ "getting started"              # Search in titles
text ~ 'api' AND space = DOCS          # Combine filters
```

## Performance

- **Indexing**: 10-30s for large datasets (one-time cost)
- **Search**: <100ms for most queries
- **Page retrieval**: <10ms

## License

MIT
