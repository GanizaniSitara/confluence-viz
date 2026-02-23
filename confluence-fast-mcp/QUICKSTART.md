# Quick Start Guide

## Prerequisites

1. Python 3.8+ installed
2. Pickled Confluence data from `confluence-viz`

## Installation

### Option 1: With Virtual Environment (Recommended)

If you have `python3-venv` installed:

```bash
cd /home/user/git/confluence-fast-mcp
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

### Option 2: Without Virtual Environment

If venv is not available, you can install dependencies system-wide (not recommended) or use pipx:

```bash
# Install pipx if not available
apt install pipx

# Install the package
cd /home/user/git/confluence-fast-mcp
pipx install -e .
```

### Option 3: Development Mode (No Installation)

Run directly with PYTHONPATH:

```bash
cd /home/user/git/confluence-fast-mcp
export PYTHONPATH=/home/user/git/confluence-fast-mcp/src
python3 -m confluence_fast_mcp.server
```

## Configuration

1. Edit `settings.ini` to point to your pickle directory:

```ini
[data]
pickle_dir = /home/user/git/confluence-viz/temp
```

2. (Optional) Set up Confluence credentials for attachment fallback:

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Running the Server

### If installed:
```bash
confluence-fast-mcp
```

### Development mode:
```bash
cd /home/user/git/confluence-fast-mcp
PYTHONPATH=/home/user/git/confluence-fast-mcp/src python3 -m confluence_fast_mcp.server
```

## Testing

### Run unit tests:
```bash
# Install pytest first
pip install pytest pytest-asyncio

# Run tests
PYTHONPATH=/home/user/git/confluence-fast-mcp/src pytest tests/
```

### Manual testing:
```bash
# Test imports
PYTHONPATH=/home/user/git/confluence-fast-mcp/src python3 -c "
from confluence_fast_mcp.converters import html_to_adf
print(html_to_adf('<p>Hello</p>'))
"

# Test CQL parsing
PYTHONPATH=/home/user/git/confluence-fast-mcp/src python3 -c "
from confluence_fast_mcp.search import translate_cql
print(translate_cql('text ~ \"kubernetes\"'))
"
```

## Using with Claude Desktop

1. Configure Claude Desktop's MCP settings to use this server
2. Add the server configuration to your MCP client config

Example MCP config:
```json
{
  "mcpServers": {
    "confluence-fast": {
      "command": "python3",
      "args": ["-m", "confluence_fast_mcp.server"],
      "env": {
        "PYTHONPATH": "/home/user/git/confluence-fast-mcp/src",
        "PICKLE_DIR": "/home/user/git/confluence-viz/temp"
      }
    }
  }
}
```

## Generating Pickle Data

If you don't have pickled data yet, use the `confluence-viz` project:

```bash
cd /home/user/git/confluence-viz
python3 sample_and_pickle_spaces.py
```

This will create pickle files in the `temp/` directory (or as configured).

## Troubleshooting

### "Pickle directory does not exist"
- Make sure you've generated pickle files using `confluence-viz`
- Check that `pickle_dir` in `settings.ini` points to the correct location

### "No pickle files found"
- Run `sample_and_pickle_spaces.py` from confluence-viz first
- Verify the files are in the correct directory (*.pkl files)

### "Module not found" errors
- Make sure PYTHONPATH is set correctly
- Install missing dependencies: `pip install whoosh beautifulsoup4 lxml pydantic requests python-dateutil`

### Index is slow to build
- First-time indexing may take 10-30 seconds for large datasets
- Subsequent starts will use the existing index (much faster)
