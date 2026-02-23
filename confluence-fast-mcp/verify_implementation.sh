#!/bin/bash
# Verification script for Confluence Fast MCP implementation

echo "======================================================================"
echo "Confluence Fast MCP - Implementation Verification"
echo "======================================================================"
echo ""

cd "$(dirname "$0")"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

passed=0
failed=0

check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}[PASS]${NC} $1"
        ((passed++))
    else
        echo -e "${RED}[FAIL]${NC} $1 (missing)"
        ((failed++))
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}[PASS]${NC} $1/"
        ((passed++))
    else
        echo -e "${RED}[FAIL]${NC} $1/ (missing)"
        ((failed++))
    fi
}

echo "Checking directory structure..."
echo ""

check_dir "src"
check_dir "src/confluence_fast_mcp"
check_dir "tests"
check_dir "whoosh_index"

echo ""
echo "Checking core modules..."
echo ""

check_file "src/confluence_fast_mcp/__init__.py"
check_file "src/confluence_fast_mcp/server.py"
check_file "src/confluence_fast_mcp/config.py"
check_file "src/confluence_fast_mcp/models.py"
check_file "src/confluence_fast_mcp/pickle_loader.py"
check_file "src/confluence_fast_mcp/converters.py"
check_file "src/confluence_fast_mcp/indexer.py"
check_file "src/confluence_fast_mcp/search.py"
check_file "src/confluence_fast_mcp/fallback.py"

echo ""
echo "Checking test files..."
echo ""

check_file "tests/__init__.py"
check_file "tests/test_converters.py"
check_file "tests/test_search.py"
check_file "tests/test_pickle_loader.py"
check_file "test_basic.py"

echo ""
echo "Checking configuration files..."
echo ""

check_file "pyproject.toml"
check_file "settings.ini"
check_file ".env.example"
check_file ".gitignore"

echo ""
echo "Checking documentation..."
echo ""

check_file "README.md"
check_file "QUICKSTART.md"
check_file "INSTALL.md"
check_file "IMPLEMENTATION_SUMMARY.md"

echo ""
echo "======================================================================"
echo -e "Results: ${GREEN}${passed} passed${NC}, ${RED}${failed} failed${NC}"
echo "======================================================================"
echo ""

if [ $failed -eq 0 ]; then
    echo -e "${GREEN}[PASS] All files present!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Install dependencies: pip3 install --user fastmcp whoosh beautifulsoup4 lxml pydantic requests python-dateutil"
    echo "2. Generate pickle data using confluence-viz"
    echo "3. Run tests: python3 test_basic.py"
    echo "4. Start server: PYTHONPATH=\$PWD/src python3 -m confluence_fast_mcp.server"
    echo ""
    exit 0
else
    echo -e "${RED}[FAIL] Some files are missing${NC}"
    echo ""
    exit 1
fi
