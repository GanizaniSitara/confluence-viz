"""Tests for CQL parsing."""

import pytest
from confluence_fast_mcp.search import CQLParser, translate_cql


def test_parse_text_search():
    """Test parsing text search."""
    parser = CQLParser()
    query, space = parser.parse('text ~ "kubernetes"')

    assert "kubernetes" in query
    assert space is None


def test_parse_space_filter():
    """Test parsing space filter."""
    parser = CQLParser()
    query, space = parser.parse('space = TECH')

    assert space == "TECH"


def test_parse_title_search():
    """Test parsing title search."""
    parser = CQLParser()
    query, space = parser.parse('title ~ "getting started"')

    assert "getting started" in query
    assert "title:" in query  # Should boost title


def test_parse_combined_and():
    """Test parsing combined AND query."""
    parser = CQLParser()
    query, space = parser.parse('text ~ "api" AND space = DOCS')

    assert "api" in query
    assert space == "DOCS"


def test_parse_combined_or():
    """Test parsing combined OR query."""
    parser = CQLParser()
    query, space = parser.parse('text ~ "docker" OR text ~ "containers"')

    assert "docker" in query
    assert "containers" in query
    assert " OR " in query


def test_parse_empty():
    """Test parsing empty query."""
    parser = CQLParser()
    query, space = parser.parse('')

    assert query == "*"
    assert space is None


def test_parse_space_with_quotes():
    """Test parsing space with quotes."""
    parser = CQLParser()
    query, space = parser.parse('space = "DEV-TEAM"')

    assert space == "DEV-TEAM"


def test_translate_cql():
    """Test translate_cql function."""
    query, space = translate_cql('text ~ "test" AND space = KEY')

    assert "test" in query
    assert space == "KEY"
