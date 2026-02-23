# Installation Guide

## Dependencies

The project requires the following Python packages:

- `fastmcp>=0.1.0` - FastMCP framework
- `whoosh>=2.7.4` - Full-text search engine
- `beautifulsoup4>=4.12.0` - HTML parsing
- `lxml>=5.0.0` - XML/HTML processing
- `pydantic>=2.0.0` - Data validation
- `requests>=2.31.0` - HTTP client
- `python-dateutil>=2.8.0` - Date utilities

## Installation Methods

### Method 1: Using pip (Recommended)

```bash
# Install python3-venv if not available
sudo apt install python3-venv  # Ubuntu/Debian

# Create and activate virtual environment
cd /home/user/git/confluence-fast-mcp
python3 -m venv venv
source venv/bin/activate

# Install the package
pip install -e .

# Or install dependencies only
pip install fastmcp whoosh beautifulsoup4 lxml pydantic requests python-dateutil
```

### Method 2: Using system Python (if venv unavailable)

```bash
# Install dependencies system-wide
pip3 install --user fastmcp whoosh beautifulsoup4 lxml pydantic requests python-dateutil
```

### Method 3: Development without installation

```bash
# Install only the required dependencies
pip3 install --user whoosh beautifulsoup4 lxml pydantic requests python-dateutil fastmcp

# Run with PYTHONPATH
cd /home/user/git/confluence-fast-mcp
export PYTHONPATH=/home/user/git/confluence-fast-mcp/src
python3 -m confluence_fast_mcp.server
```

## Verification

Test the installation:

```bash
cd /home/user/git/confluence-fast-mcp
python3 test_basic.py
```

All tests should pass if dependencies are correctly installed.

## Next Steps

1. Configure the server - see [QUICKSTART.md](QUICKSTART.md)
2. Generate pickle data using confluence-viz
3. Run the server

## Troubleshooting

### "externally-managed-environment" error

This is a safety feature in newer Python versions. Options:

1. Use a virtual environment (recommended)
2. Use `--user` flag: `pip install --user <package>`
3. Use pipx for application installation
4. Override with `--break-system-packages` (not recommended)

### Missing python3-venv

```bash
sudo apt install python3-venv
```

### Import errors

Make sure all dependencies are installed:

```bash
pip3 install --user whoosh beautifulsoup4 lxml pydantic requests python-dateutil fastmcp
```
