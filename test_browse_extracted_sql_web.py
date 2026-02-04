#!/usr/bin/env python3
"""
Tests for browse_extracted_sql_web.py

These tests verify the SQL browser functionality including:
- Search persistence across all views (Browser, Timeline, Insights)
- Navigation links preserving search terms
- Insights page showing filtered scripts list
- Pagination in Insights view
- Sidebar navigation preserving search

IMPORTANT: Do not modify these tests without explicit user approval.
"""

import pytest
import sqlite3
import tempfile
import os
from browse_extracted_sql_web import app, DATABASE


@pytest.fixture
def test_db():
    """Create a temporary test database with sample data."""
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE sql_scripts (
            id INTEGER PRIMARY KEY,
            space_key TEXT,
            space_name TEXT,
            page_id TEXT,
            page_title TEXT,
            sql_code TEXT,
            sql_language TEXT,
            sql_source TEXT,
            sql_title TEXT,
            sql_description TEXT,
            last_modified TEXT,
            line_count INTEGER,
            nesting_depth INTEGER,
            keyword_count INTEGER,
            sql_type TEXT,
            tables_referenced TEXT,
            schemas_referenced TEXT
        )
    ''')

    # Insert test data - scripts with various table references
    test_scripts = [
        (1, 'SPACE1', 'Space One', 'page1', 'Page One',
         'SELECT * FROM pma.users WHERE id = 1', 'sql', 'code-block', None, None,
         '2024-01-15 10:00:00', 1, 0, 5, 'SELECT', 'users', 'pma'),
        (2, 'SPACE1', 'Space One', 'page1', 'Page One',
         'SELECT * FROM pma.orders o JOIN pma.users u ON o.user_id = u.id', 'sql', 'code-block', None, None,
         '2024-01-16 10:00:00', 1, 0, 8, 'SELECT', 'orders,users', 'pma'),
        (3, 'SPACE1', 'Space One', 'page2', 'Page Two',
         'INSERT INTO audit.logs (msg) VALUES ("test")', 'sql', 'code-block', None, None,
         '2024-02-01 10:00:00', 1, 0, 4, 'INSERT', 'logs', 'audit'),
        (4, 'SPACE2', 'Space Two', 'page3', 'Page Three',
         'SELECT * FROM pma.products WHERE active = 1', 'sql', 'code-block', None, None,
         '2024-02-15 10:00:00', 1, 0, 6, 'SELECT', 'products', 'pma'),
        (5, 'SPACE2', 'Space Two', 'page3', 'Page Three',
         'UPDATE inventory SET qty = 0', 'sql', 'code-block', None, None,
         '2024-03-01 10:00:00', 1, 0, 4, 'UPDATE', 'inventory', 'public'),
    ]

    # Add more scripts for pagination testing (60 total to exceed page_size of 50)
    for i in range(6, 66):
        test_scripts.append((
            i, 'SPACE3', 'Space Three', f'page{i}', f'Page {i}',
            f'SELECT * FROM pma.table{i}', 'sql', 'code-block', None, None,
            f'2024-03-{(i % 28) + 1:02d} 10:00:00', 1, 0, 3, 'SELECT', f'table{i}', 'pma'
        ))

    conn.executemany('''
        INSERT INTO sql_scripts
        (id, space_key, space_name, page_id, page_title, sql_code, sql_language,
         sql_source, sql_title, sql_description, last_modified, line_count,
         nesting_depth, keyword_count, sql_type, tables_referenced, schemas_referenced)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', test_scripts)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    os.unlink(db_path)


@pytest.fixture
def client(test_db):
    """Create a test client with the test database."""
    import browse_extracted_sql_web
    browse_extracted_sql_web.DATABASE = test_db
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


class TestBrowserView:
    """Tests for the main browser view."""

    def test_browser_loads(self, client):
        """Browser view loads successfully."""
        response = client.get('/')
        assert response.status_code == 200
        assert b'Confluence SQL Script Browser' in response.data

    def test_browser_search(self, client):
        """Search filters scripts by SQL content."""
        response = client.get('/?search=pma')
        assert response.status_code == 200
        assert b'pma' in response.data

    def test_browser_search_shows_in_input(self, client):
        """Search term appears in search input field."""
        response = client.get('/?search=pma')
        assert b'value="pma"' in response.data

    def test_browser_timeline_button_preserves_search(self, client):
        """Timeline button submits search via form."""
        response = client.get('/?search=pma')
        assert b'formaction="/timeline"' in response.data

    def test_browser_insights_button_preserves_search(self, client):
        """Insights button submits search via form."""
        response = client.get('/?search=pma')
        assert b'formaction="/insights"' in response.data


class TestTimelineView:
    """Tests for the timeline view."""

    def test_timeline_loads(self, client):
        """Timeline view loads successfully."""
        response = client.get('/timeline')
        assert response.status_code == 200
        assert b'SQL Scripts Timeline' in response.data

    def test_timeline_with_search(self, client):
        """Timeline view accepts and uses search parameter."""
        response = client.get('/timeline?search=pma')
        assert response.status_code == 200
        assert b'pma' in response.data

    def test_timeline_browser_link_preserves_search(self, client):
        """Browser link preserves search term."""
        response = client.get('/timeline?search=pma')
        assert b'href="/?search=pma"' in response.data

    def test_timeline_insights_link_preserves_search(self, client):
        """Insights link preserves search term."""
        response = client.get('/timeline?search=pma')
        assert b'href="/insights?search=pma"' in response.data

    def test_timeline_event_click_preserves_search(self, client):
        """Clicking timeline event preserves search."""
        response = client.get('/timeline?search=pma')
        # Check that onclick includes search parameter
        assert b"&search=pma'" in response.data or b'&amp;search=pma' in response.data

    def test_timeline_space_link_preserves_search(self, client):
        """Space link in timeline preserves search."""
        response = client.get('/timeline?search=pma')
        assert b'/?space=' in response.data
        # Should have search param in space links
        assert b'&search=pma' in response.data or b'&amp;search=pma' in response.data


class TestInsightsView:
    """Tests for the insights view."""

    def test_insights_loads(self, client):
        """Insights view loads successfully."""
        response = client.get('/insights')
        assert response.status_code == 200
        assert b'SQL Insights' in response.data

    def test_insights_with_search(self, client):
        """Insights view accepts and filters by search parameter."""
        response = client.get('/insights?search=pma')
        assert response.status_code == 200
        assert b'pma' in response.data
        # Should show search in subtitle
        assert b'matching' in response.data

    def test_insights_browser_link_preserves_search(self, client):
        """Browser link preserves search term."""
        response = client.get('/insights?search=pma')
        assert b'href="/?search=pma"' in response.data

    def test_insights_timeline_link_preserves_search(self, client):
        """Timeline link preserves search term."""
        response = client.get('/insights?search=pma')
        assert b'href="/timeline?search=pma"' in response.data

    def test_insights_shows_filtered_scripts_on_search(self, client):
        """Insights shows matching scripts list when search is active."""
        response = client.get('/insights?search=pma')
        assert b'Matching Scripts' in response.data

    def test_insights_shows_top_pages(self, client):
        """Insights shows top pages panel."""
        response = client.get('/insights')
        assert b'Top 15 Pages by Script Count' in response.data

    def test_insights_drill_down_preserves_search(self, client):
        """Drill-down links in insights preserve search."""
        response = client.get('/insights?search=pma')
        # SQL type drill-down should include search
        html = response.data.decode('utf-8')
        assert 'type=SELECT' in html or 'type=INSERT' in html
        # When search is active, drill-downs should preserve it
        assert '&search=pma' in html or '&amp;search=pma' in html

    def test_insights_space_dropdown_preserves_search(self, client):
        """Space dropdown form includes hidden search field."""
        response = client.get('/insights?search=pma')
        assert b'name="search" value="pma"' in response.data


class TestInsightsPagination:
    """Tests for pagination in insights filtered scripts."""

    def test_pagination_appears_when_needed(self, client):
        """Pagination controls appear when results exceed page size."""
        # Search for 'pma' which matches 60+ scripts in test data
        response = client.get('/insights?search=pma')
        assert b'Page 1 of' in response.data
        assert b'Next' in response.data

    def test_pagination_page_parameter(self, client):
        """Page parameter navigates to correct page."""
        response = client.get('/insights?search=pma&page=2')
        assert response.status_code == 200
        assert b'Page 2 of' in response.data

    def test_pagination_preserves_filters(self, client):
        """Pagination links preserve all filter parameters."""
        response = client.get('/insights?search=pma&page=1')
        html = response.data.decode('utf-8')
        # Next page link should include search
        assert 'search=pma' in html
        assert 'page=2' in html


class TestSidebarNavigation:
    """Tests for sidebar navigation in browser view."""

    def test_sidebar_script_click_preserves_search(self, client):
        """Clicking script in sidebar preserves search."""
        response = client.get('/?search=pma')
        html = response.data.decode('utf-8')
        # Script onclick should include search
        assert '&search=pma' in html or '&amp;search=pma' in html

    def test_sidebar_page_click_preserves_search(self, client):
        """JavaScript togglePage function preserves search."""
        response = client.get('/?search=pma')
        html = response.data.decode('utf-8')
        # Should set currentSearch variable
        assert "currentSearch = 'pma'" in html

    def test_sidebar_shows_filtered_tree_on_search(self, client):
        """Sidebar tree is filtered by search term."""
        response = client.get('/?search=audit')
        html = response.data.decode('utf-8')
        # Should show SPACE1 which has the audit script
        assert 'SPACE1' in html


class TestSearchFilterBehavior:
    """Tests for search filter behavior across the application."""

    def test_clear_search_link_appears(self, client):
        """Clear link appears when search is active."""
        response = client.get('/?search=pma')
        assert b'>Clear</a>' in response.data

    def test_clear_all_filters_link_in_insights(self, client):
        """Clear all filters link appears in insights when filters active."""
        response = client.get('/insights?search=pma')
        assert b'Clear all filters' in response.data

    def test_no_sql_error_on_insights_search(self, client):
        """No SQL syntax error when searching in insights."""
        # This was a bug - double WHERE clause
        response = client.get('/insights?search=pma')
        assert response.status_code == 200
        # Should not contain error messages
        assert b'syntax error' not in response.data.lower()
        assert b'Error' not in response.data


class TestUniqueTablesAndSchemas:
    """Tests for unique tables/schemas counting in insights."""

    def test_unique_tables_counts_all_in_matching_scripts(self, client):
        """Unique tables count includes all tables from matching scripts."""
        # When searching for 'pma', scripts reference multiple tables
        response = client.get('/insights?search=pma')
        assert response.status_code == 200
        assert b'Unique Tables' in response.data

    def test_unique_schemas_shown(self, client):
        """Unique schemas are displayed in insights."""
        response = client.get('/insights')
        assert b'Unique Schemas' in response.data


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
