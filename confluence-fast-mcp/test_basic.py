#!/usr/bin/env python3
"""Basic functionality test script."""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")

    try:
        from confluence_fast_mcp import config
        print("✓ config module")
    except ImportError as e:
        print(f"✗ config module: {e}")
        return False

    try:
        from confluence_fast_mcp import models
        print("✓ models module")
    except ImportError as e:
        print(f"✗ models module: {e}")
        return False

    try:
        from confluence_fast_mcp import pickle_loader
        print("✓ pickle_loader module")
    except ImportError as e:
        print(f"✗ pickle_loader module: {e}")
        return False

    try:
        from confluence_fast_mcp import converters
        print("✓ converters module")
    except ImportError as e:
        print(f"✗ converters module: {e}")
        return False

    try:
        from confluence_fast_mcp import search
        print("✓ search module")
    except ImportError as e:
        print(f"✗ search module: {e}")
        return False

    try:
        from confluence_fast_mcp import indexer
        print("✓ indexer module")
    except ImportError as e:
        print(f"✗ indexer module: {e}")
        return False

    try:
        from confluence_fast_mcp import fallback
        print("✓ fallback module")
    except ImportError as e:
        print(f"✗ fallback module: {e}")
        return False

    try:
        from confluence_fast_mcp import server
        print("✓ server module")
    except ImportError as e:
        print(f"✗ server module: {e}")
        return False

    return True


def test_converters():
    """Test HTML to ADF conversion."""
    print("\nTesting converters...")

    from confluence_fast_mcp.converters import html_to_adf, html_to_text

    # Test simple paragraph
    html = "<p>Hello world</p>"
    adf = html_to_adf(html)

    assert adf['type'] == 'doc', "ADF type should be 'doc'"
    assert adf['version'] == 1, "ADF version should be 1"
    assert len(adf['content']) > 0, "ADF should have content"

    print("✓ HTML to ADF conversion works")

    # Test text extraction
    text = html_to_text(html)
    assert "Hello world" in text, "Text extraction should work"

    print("✓ HTML to text conversion works")

    return True


def test_search():
    """Test CQL parsing."""
    print("\nTesting CQL parsing...")

    from confluence_fast_mcp.search import translate_cql

    # Test simple text search
    query, space = translate_cql('text ~ "kubernetes"')
    assert "kubernetes" in query, "Query should contain search term"
    assert space is None, "No space filter expected"

    print("✓ Simple text search parsing works")

    # Test space filter
    query, space = translate_cql('space = TECH')
    assert space == "TECH", "Space filter should be extracted"

    print("✓ Space filter parsing works")

    # Test combined query
    query, space = translate_cql('text ~ "api" AND space = DOCS')
    assert "api" in query, "Query should contain search term"
    assert space == "DOCS", "Space filter should be extracted"

    print("✓ Combined query parsing works")

    return True


def test_config():
    """Test configuration loading."""
    print("\nTesting configuration...")

    from confluence_fast_mcp.config import get_config

    config = get_config()
    assert config.pickle_dir is not None, "Pickle dir should be set"
    assert config.index_dir is not None, "Index dir should be set"

    print(f"✓ Config loaded (pickle_dir: {config.pickle_dir})")

    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("Confluence Fast MCP - Basic Functionality Tests")
    print("=" * 60)

    all_passed = True

    # Test imports
    if not test_imports():
        print("\n✗ Import tests failed")
        all_passed = False
    else:
        print("\n✓ All imports successful")

    # Test converters
    try:
        if not test_converters():
            print("\n✗ Converter tests failed")
            all_passed = False
    except Exception as e:
        print(f"\n✗ Converter tests failed with exception: {e}")
        all_passed = False

    # Test search
    try:
        if not test_search():
            print("\n✗ Search tests failed")
            all_passed = False
    except Exception as e:
        print(f"\n✗ Search tests failed with exception: {e}")
        all_passed = False

    # Test config
    try:
        if not test_config():
            print("\n✗ Config tests failed")
            all_passed = False
    except Exception as e:
        print(f"\n✗ Config tests failed with exception: {e}")
        all_passed = False

    # Summary
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED")
        print("=" * 60)
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        print("=" * 60)
        return 1


if __name__ == '__main__':
    sys.exit(main())
