"""Tests for pickle loader."""

import pytest
import pickle
import tempfile
import os
from confluence_fast_mcp.pickle_loader import PickleLoader


@pytest.fixture
def temp_pickle_dir():
    """Create a temporary directory with sample pickle files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample pickle data
        sample_space = {
            'space_key': 'TEST',
            'name': 'Test Space',
            'sampled_pages': [
                {
                    'id': '12345',
                    'title': 'Test Page 1',
                    'body': {
                        'storage': {
                            'value': '<p>Test content</p>'
                        }
                    }
                },
                {
                    'id': '67890',
                    'title': 'Test Page 2',
                    'body': {
                        'storage': {
                            'value': '<p>More content</p>'
                        }
                    }
                }
            ],
            'total_pages_in_space': 2
        }

        # Write pickle file
        pickle_path = os.path.join(tmpdir, 'TEST.pkl')
        with open(pickle_path, 'wb') as f:
            pickle.dump(sample_space, f)

        yield tmpdir


def test_load_pickles(temp_pickle_dir):
    """Test loading pickle files."""
    loader = PickleLoader(temp_pickle_dir)
    loader.load_all_pickles()

    spaces = loader.get_all_spaces()
    assert len(spaces) == 1
    assert spaces[0]['key'] == 'TEST'
    assert spaces[0]['name'] == 'Test Space'


def test_get_page_by_id(temp_pickle_dir):
    """Test getting page by ID."""
    loader = PickleLoader(temp_pickle_dir)
    loader.load_all_pickles()

    result = loader.get_page_by_id('12345')
    assert result is not None
    assert result['space_key'] == 'TEST'
    assert result['page']['title'] == 'Test Page 1'


def test_get_page_by_title(temp_pickle_dir):
    """Test getting page by title."""
    loader = PickleLoader(temp_pickle_dir)
    loader.load_all_pickles()

    result = loader.get_page_by_title('Test Page 2', 'TEST')
    assert result is not None
    assert result['space_key'] == 'TEST'
    assert result['page']['id'] == '67890'


def test_get_pages_in_space(temp_pickle_dir):
    """Test getting pages in a space."""
    loader = PickleLoader(temp_pickle_dir)
    loader.load_all_pickles()

    pages = loader.get_pages_in_space('TEST')
    assert len(pages) == 2
    assert pages[0]['title'] == 'Test Page 1'


def test_search_by_title(temp_pickle_dir):
    """Test searching by title."""
    loader = PickleLoader(temp_pickle_dir)
    loader.load_all_pickles()

    results = loader.search_by_title('Page 1')
    assert len(results) > 0
    assert results[0]['page']['title'] == 'Test Page 1'


def test_nonexistent_directory():
    """Test handling of nonexistent directory."""
    loader = PickleLoader('/nonexistent/path')
    loader.load_all_pickles()

    spaces = loader.get_all_spaces()
    assert len(spaces) == 0
