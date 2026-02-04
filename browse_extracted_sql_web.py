#!/usr/bin/env python3
"""
Web-based SQL Browser for Extracted Scripts

A simple Flask web interface to browse extracted SQL scripts.

Usage:
    python browse_extracted_sql_web.py --db sql_queries.db
    python browse_extracted_sql_web.py --db sql_queries.db --port 5080
"""

import sqlite3
import argparse
import os
import re
import configparser
from datetime import datetime
from collections import Counter, defaultdict
from flask import Flask, render_template_string, request, g

app = Flask(__name__)
DATABASE = None
CONFLUENCE_BASE_URL = None
SUGGESTION_EMAIL = None
DB_LAST_MODIFIED = None

def load_config():
    """Load configuration from settings.ini if it exists."""
    global SUGGESTION_EMAIL
    config_path = os.path.join(os.path.dirname(__file__), 'settings.ini')
    if os.path.exists(config_path):
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            if config.has_option('sql_browser', 'suggestion_email'):
                SUGGESTION_EMAIL = config.get('sql_browser', 'suggestion_email')
        except Exception:
            pass

def get_db_last_modified():
    """Get the last modified timestamp of the database file."""
    global DB_LAST_MODIFIED
    if DATABASE and os.path.exists(DATABASE):
        mtime = os.path.getmtime(DATABASE)
        DB_LAST_MODIFIED = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
    return DB_LAST_MODIFIED

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Confluence SQL Script Browser</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0; padding: 0; background: #f5f5f5;
            display: flex; height: 100vh;
        }

        /* Sidebar */
        .sidebar {
            width: 300px; background: #fff; border-right: 1px solid #ddd;
            overflow-y: auto; flex-shrink: 0;
        }
        .sidebar-header {
            padding: 15px; border-bottom: 1px solid #ddd;
            font-weight: 600; color: #333; background: #f8f9fa;
        }
        .sidebar-header a { color: #007bff; text-decoration: none; font-weight: normal; font-size: 13px; }
        .space-item {
            border-bottom: 1px solid #eee;
        }
        .space-header {
            padding: 10px 15px; cursor: pointer; display: flex;
            justify-content: space-between; align-items: center;
            background: #fafafa;
        }
        .space-header:hover { background: #f0f0f0; }
        .space-header.active { background: #e7f1ff; }
        .space-name { font-weight: 500; color: #333; font-size: 13px; }
        .space-count {
            background: #6c757d; color: white; padding: 2px 8px;
            border-radius: 10px; font-size: 11px;
        }
        .space-pages { display: none; background: #fff; }
        .space-pages.open { display: block; }
        .page-item {
            padding: 8px 15px 8px 25px; border-bottom: 1px solid #f0f0f0;
            font-size: 12px; color: #555; cursor: pointer;
            display: flex; justify-content: space-between;
        }
        .page-item:hover { background: #f8f9fa; }
        .page-item.active { background: #e7f1ff; color: #0056b3; }
        .page-title { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
        .page-count { color: #888; margin-left: 8px; }
        .script-item {
            padding: 6px 15px 6px 40px; border-bottom: 1px solid #f5f5f5;
            font-size: 11px; color: #666; cursor: pointer;
        }
        .script-item:hover { background: #f0f7ff; }
        .script-item.active { background: #cce5ff; color: #004085; }

        /* Main content */
        .main { flex: 1; overflow-y: auto; padding: 20px; }
        .container { max-width: 1000px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; font-size: 24px; }

        .nav {
            background: #fff; padding: 15px; border-radius: 8px;
            margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
        }
        .nav a, .nav button {
            padding: 8px 16px; background: #007bff; color: white;
            text-decoration: none; border-radius: 4px; border: none;
            cursor: pointer; font-size: 14px;
        }
        .nav a:hover, .nav button:hover { background: #0056b3; }
        .nav .disabled { background: #ccc; pointer-events: none; }
        .nav input[type="number"] { width: 80px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        .nav input[type="text"] { width: 200px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        .nav .spacer { flex-grow: 1; }
        .nav .info { color: #666; font-size: 14px; }

        .card {
            background: #fff; border-radius: 8px; padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px;
        }
        .meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .meta-item { }
        .meta-label { font-size: 12px; color: #666; text-transform: uppercase; }
        .meta-value { font-size: 14px; color: #333; word-break: break-word; }

        .sql-code {
            background: #1e1e1e; color: #d4d4d4; padding: 20px;
            border-radius: 8px; overflow-x: auto; font-family: "Fira Code", "Consolas", monospace;
            font-size: 13px; line-height: 1.5; white-space: pre-wrap; word-break: break-word;
        }

        .search-results { margin-bottom: 10px; color: #666; }
        .line-count { color: #888; font-size: 12px; }
        form { display: inline; }
    </style>
</head>
<body>
    <!-- Sidebar with tree -->
    <div class="sidebar">
        <div class="sidebar-header">
            Spaces & Pages ({{ total_scripts }} scripts)
            {% if space_filter or page_filter %}<br><a href="/">Clear filter</a>{% endif %}
        </div>
        {% for space in tree %}
        <div class="space-item">
            <div class="space-header {% if space.space_key == space_filter %}active{% endif %}"
                 onclick="toggleSpace(this)">
                <span class="space-name">{{ space.space_key }}</span>
                <span class="space-count">{{ space.script_count }}</span>
            </div>
            <div class="space-pages {% if space.space_key == space_filter %}open{% endif %}">
                {% for page in space.pages %}
                <div class="page-item {% if page.page_id == page_filter %}active{% endif %}"
                     onclick="togglePage(this, '{{ space.space_key }}', '{{ page.page_id }}')">
                    <span class="page-title" title="{{ page.page_title }}">{{ page.page_title or 'Untitled' }}</span>
                    <span class="page-count">{{ page.script_count }}</span>
                </div>
                <div class="page-scripts" style="display: {% if page.page_id == page_filter %}block{% else %}none{% endif %};">
                    {% for script in page.scripts %}
                    <div class="script-item {% if script.id == current_id %}active{% endif %}"
                         onclick="window.location='/?id={{ script.id }}{% if search %}&search={{ search }}{% endif %}'">
                        #{{ script.id }} - {{ script.line_count }} lines
                    </div>
                    {% endfor %}
                </div>
                {% endfor %}
            </div>
        </div>
        {% endfor %}
    </div>

    <!-- Main content -->
    <div class="main">
        <div class="container">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <h1 style="margin: 0;">Confluence SQL Script Browser</h1>
                <div style="display: flex; gap: 15px; align-items: center;">
                    {% if db_last_modified %}
                    <span style="color: #666; font-size: 13px;">Last rebuild: {{ db_last_modified }}</span>
                    {% endif %}
                    {% if suggestion_email %}
                    <a href="mailto:{{ suggestion_email }}?subject=SQL%20Browser%20Suggestion"
                       style="padding: 8px 16px; background: #17a2b8; color: white; text-decoration: none; border-radius: 4px; font-size: 14px;">
                        ✉ Submit Suggestion
                    </a>
                    {% endif %}
                </div>
            </div>

            <div class="nav">
                <a href="/?index=0" {% if index == 0 %}class="disabled"{% endif %}>First</a>
                <a href="/?index={{ index - 1 }}{% if search %}&search={{ search }}{% endif %}{% if space_filter %}&space={{ space_filter }}{% endif %}{% if page_filter %}&page={{ page_filter }}{% endif %}" {% if index == 0 %}class="disabled"{% endif %}>Prev</a>
                <a href="/?index={{ index + 1 }}{% if search %}&search={{ search }}{% endif %}{% if space_filter %}&space={{ space_filter }}{% endif %}{% if page_filter %}&page={{ page_filter }}{% endif %}" {% if index >= total - 1 %}class="disabled"{% endif %}>Next</a>
                <a href="/?index={{ total - 1 }}{% if space_filter %}&space={{ space_filter }}{% endif %}{% if page_filter %}&page={{ page_filter }}{% endif %}" {% if index >= total - 1 %}class="disabled"{% endif %}>Last</a>

                <form action="/" method="get" style="display: flex; gap: 5px; align-items: center;">
                    <input type="number" name="goto" value="{{ index + 1 }}" min="1" max="{{ total }}">
                    {% if space_filter %}<input type="hidden" name="space" value="{{ space_filter }}">{% endif %}
                    {% if page_filter %}<input type="hidden" name="page" value="{{ page_filter }}">{% endif %}
                    <button type="submit">Go</button>
                </form>

                <div class="spacer"></div>

                <form action="/" method="get" style="display: flex; gap: 5px; align-items: center;">
                    <input type="text" name="search" value="{{ search or '' }}" placeholder="Search SQL...">
                    <button type="submit">Search</button>
                    <button type="submit" formaction="/timeline" style="background: #28a745;">Timeline</button>
                    <button type="submit" formaction="/insights" style="background: #6f42c1;">Insights</button>
                    {% if search %}<a href="/">Clear</a>{% endif %}
                </form>

                <span class="info">{{ index + 1 }} of {{ total }}</span>
            </div>

            {% if search %}
            <div class="search-results">Found {{ total }} results for "{{ search }}"</div>
            {% endif %}
            {% if space_filter and not page_filter %}
            <div class="search-results">Showing scripts from space: {{ space_filter }}</div>
            {% endif %}
            {% if page_filter %}
            <div class="search-results">Showing scripts from page: {{ script.page_title if script else page_filter }}</div>
            {% endif %}

            {% if script %}
            <div class="card">
                <div class="meta">
                    <div class="meta-item">
                        <div class="meta-label">Space</div>
                        <div class="meta-value">
                            <a href="/?space={{ script.space_key }}">{{ script.space_key }}</a>
                            ({{ script.space_name or 'N/A' }})
                        </div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Page</div>
                        <div class="meta-value">
                            <a href="/?page={{ script.page_id }}">{{ script.page_title or 'Untitled' }}</a>
                            {% if confluence_url %}
                            <a href="{{ confluence_url }}/pages/viewpage.action?pageId={{ script.page_id }}" target="_blank" style="margin-left: 8px; font-size: 12px;">↗ Open in Confluence</a>
                            {% endif %}
                        </div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Page ID</div>
                        <div class="meta-value">{{ script.page_id }}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Modified</div>
                        <div class="meta-value">{{ script.last_modified }}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Language</div>
                        <div class="meta-value">{{ script.sql_language or 'N/A' }}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Source</div>
                        <div class="meta-value">{{ script.sql_source }}</div>
                    </div>
                    {% if script.sql_title %}
                    <div class="meta-item">
                        <div class="meta-label">Title</div>
                        <div class="meta-value">{{ script.sql_title }}</div>
                    </div>
                    {% endif %}
                    {% if script.sql_description %}
                    <div class="meta-item">
                        <div class="meta-label">Description</div>
                        <div class="meta-value">{{ script.sql_description }}</div>
                    </div>
                    {% endif %}
                </div>

                <div class="line-count">{{ script.line_count }} lines (Script ID: {{ script.id }})</div>
                <pre class="sql-code">{{ script.sql_code }}</pre>
            </div>
            {% else %}
            <div class="card">
                <p>No scripts found.</p>
            </div>
            {% endif %}
        </div>
    </div>

    <script>
        var currentSearch = '{{ search or '' }}';
        function toggleSpace(el) {
            const pages = el.nextElementSibling;
            pages.classList.toggle('open');
        }
        function togglePage(el, spaceKey, pageId) {
            // Navigate to filter by page, preserving search
            var url = '/?page=' + pageId;
            if (currentSearch) url += '&search=' + encodeURIComponent(currentSearch);
            window.location = url;
        }
    </script>
</body>
</html>
'''

TIMELINE_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>SQL Scripts Timeline - {{ search or 'All' }}</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0; padding: 20px; background: #f5f5f5;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 10px; }
        .subtitle { color: #666; margin-bottom: 20px; }

        .nav {
            background: #fff; padding: 15px; border-radius: 8px;
            margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            display: flex; gap: 10px; align-items: center;
        }
        .nav a, .nav button {
            padding: 8px 16px; background: #007bff; color: white;
            text-decoration: none; border-radius: 4px; border: none; cursor: pointer;
        }
        .nav a:hover, .nav button:hover { background: #0056b3; }
        .nav input[type="text"] { width: 300px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        .nav .spacer { flex-grow: 1; }

        .card {
            background: #fff; border-radius: 8px; padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px;
        }

        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }

        .timeline-cell { width: 60%; }
        .timeline {
            position: relative; height: 30px; background: #e9ecef; border-radius: 4px;
        }
        .event {
            position: absolute; height: 30px; width: 2px; background: #dc3545;
            cursor: pointer; transition: background 0.2s;
        }
        .event:hover { background: #007bff; width: 4px; margin-left: -1px; }

        .year-markers {
            position: relative; height: 20px; margin-top: 2px;
        }
        .year-marker {
            position: absolute; font-size: 10px; color: #666;
            transform: translateX(-50%);
        }

        .space-name { font-weight: 500; }
        .space-name a { color: #007bff; text-decoration: none; }
        .space-name a:hover { text-decoration: underline; }
        .script-count { color: #666; }

        .legend {
            display: flex; gap: 20px; margin-bottom: 15px; font-size: 13px; color: #666;
        }
        .legend-item { display: flex; align-items: center; gap: 5px; }
        .legend-bar { width: 20px; height: 12px; background: #dc3545; }
    </style>
</head>
<body>
    <div class="container">
        <h1>SQL Scripts Timeline</h1>
        <p class="subtitle">
            {% if search %}Search: "{{ search }}" - {% endif %}
            {{ total_scripts }} scripts across {{ spaces|length }} spaces
        </p>

        <div class="nav">
            <a href="/{% if search %}?search={{ search }}{% endif %}">← Browser</a>
            <a href="/insights{% if search %}?search={{ search }}{% endif %}">Insights</a>
            <div class="spacer"></div>
            <form action="/timeline" method="get" style="display: flex; gap: 5px;">
                <input type="text" name="search" value="{{ search or '' }}" placeholder="Search SQL content...">
                <button type="submit">Search</button>
                {% if search %}<a href="/timeline">Clear</a>{% endif %}
            </form>
        </div>

        <div class="card">
            <div class="legend">
                <div class="legend-item"><div class="legend-bar"></div> SQL script last modified date</div>
                <div class="legend-item">Timeline: {{ min_year }} - {{ max_year }}</div>
            </div>

            <table>
                <tr>
                    <th>Space</th>
                    <th>Scripts</th>
                    <th class="timeline-cell">Last Modified Timeline</th>
                </tr>
                {% for space in spaces %}
                <tr>
                    <td class="space-name"><a href="/?space={{ space.space_key }}{% if search %}&search={{ search }}{% endif %}">{{ space.space_key }}</a></td>
                    <td class="script-count">{{ space.script_count }}</td>
                    <td class="timeline-cell">
                        <div class="timeline">
                            {% for event in space.events %}
                            <div class="event" style="left: {{ event.position }}%;"
                                 title="{{ event.page_title }} ({{ event.date }})"
                                 onclick="window.location='/?id={{ event.id }}{% if search %}&search={{ search }}{% endif %}'"></div>
                            {% endfor %}
                        </div>
                        <div class="year-markers">
                            {% for year in years %}
                            <div class="year-marker" style="left: {{ year.position }}%;">{{ year.year }}</div>
                            {% endfor %}
                        </div>
                    </td>
                </tr>
                {% endfor %}
            </table>
        </div>
    </div>
</body>
</html>
'''

INSIGHTS_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>SQL Insights - Confluence SQL Script Browser</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0; padding: 20px; background: #f5f5f5;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 10px; }
        h2 { color: #555; margin-top: 30px; margin-bottom: 15px; font-size: 18px; }
        .subtitle { color: #666; margin-bottom: 20px; }

        .nav {
            background: #fff; padding: 15px; border-radius: 8px;
            margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            display: flex; gap: 10px; align-items: center;
        }
        .nav a {
            padding: 8px 16px; background: #007bff; color: white;
            text-decoration: none; border-radius: 4px;
        }
        .nav a:hover { background: #0056b3; }
        .nav a.active { background: #0056b3; }
        .nav .spacer { flex-grow: 1; }

        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }

        .card {
            background: #fff; border-radius: 8px; padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h3 { margin-top: 0; color: #333; font-size: 16px; border-bottom: 1px solid #eee; padding-bottom: 10px; }

        .stat-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px; }
        .stat-box {
            background: #fff; border-radius: 8px; padding: 20px; text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stat-value { font-size: 32px; font-weight: 700; color: #007bff; }
        .stat-label { font-size: 12px; color: #666; margin-top: 5px; }

        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }
        tr:hover { background: #f8f9fa; }

        .bar-cell { width: 40%; }
        .bar-container { background: #e9ecef; border-radius: 4px; height: 20px; }
        .bar { background: #007bff; height: 20px; border-radius: 4px; min-width: 2px; }
        .bar.green { background: #28a745; }
        .bar.orange { background: #fd7e14; }
        .bar.red { background: #dc3545; }

        a.table-link { color: #007bff; text-decoration: none; }
        a.table-link:hover { text-decoration: underline; }

        .clickable { cursor: pointer; }
        .clickable:hover { background: #e7f1ff; }

        .drill-link { color: #007bff; }
        .drill-link:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="container">
        <h1>SQL Insights</h1>
        <p class="subtitle">
            {{ total_scripts }} SQL scripts
            {% if search %} matching "<strong>{{ search }}</strong>"{% endif %}
            {% if space_filter %} in <strong>{{ space_filter }}</strong>{% else %} across {{ total_spaces }} spaces{% endif %}
            {% if type_filter %} | Type: <strong>{{ type_filter }}</strong>{% endif %}
            {% if source_filter %} | Source: <strong>{{ source_filter }}</strong>{% endif %}
            {% if size_filter %} | Size: <strong>{{ size_filter }}</strong>{% endif %}
            {% if nesting_filter %} | Nesting: <strong>{{ nesting_filter }}</strong>{% endif %}
            {% if search or space_filter or type_filter or source_filter or size_filter or nesting_filter %}
                | <a href="/insights">Clear all filters</a>
            {% endif %}
        </p>

        <div class="nav">
            <a href="/{% if search %}?search={{ search }}{% endif %}">← Browser</a>
            <a href="/timeline{% if search %}?search={{ search }}{% endif %}">Timeline</a>
            <a href="/insights{% if search %}?search={{ search }}{% endif %}" class="active">Insights</a>
            <div class="spacer"></div>
            <form action="/insights" method="get" style="display: flex; gap: 5px; align-items: center;">
                <input type="text" name="search" value="{{ search or '' }}" placeholder="Search SQL..." style="padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 200px;">
                <button type="submit" style="padding: 8px 16px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Search</button>
                {% if search %}<a href="/insights" style="padding: 8px 16px; background: #6c757d; color: white; text-decoration: none; border-radius: 4px;">Clear</a>{% endif %}
            </form>
            <form action="/insights" method="get" style="display: flex; gap: 5px;">
                {% if search %}<input type="hidden" name="search" value="{{ search }}">{% endif %}
                <select name="space" onchange="this.form.submit()" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                    <option value="">All Spaces</option>
                    {% for space in all_spaces %}
                    <option value="{{ space }}" {% if space == space_filter %}selected{% endif %}>{{ space }}</option>
                    {% endfor %}
                </select>
            </form>
            {% if type_filter or source_filter or size_filter or nesting_filter %}
            <a href="/insights?{% if search %}search={{ search }}&{% endif %}{% if space_filter %}space={{ space_filter }}{% endif %}" style="background: #dc3545; padding: 8px 16px; color: white; text-decoration: none; border-radius: 4px;">Clear dimension filters</a>
            {% endif %}
        </div>

        <!-- Summary Stats -->
        <div class="stat-grid">
            <div class="stat-box">
                <div class="stat-value">{{ total_scripts }}</div>
                <div class="stat-label">SQL Scripts</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{{ total_lines|default(0) }}</div>
                <div class="stat-label">Lines of SQL</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{{ unique_tables }}</div>
                <div class="stat-label">Unique Tables</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{{ unique_schemas }}</div>
                <div class="stat-label">Unique Schemas</div>
            </div>
        </div>

        <div class="grid">
            <!-- Top Tables -->
            <div class="card">
                <h3>Top 15 Tables Referenced</h3>
                <table>
                    <tr><th>Table</th><th>Count</th><th class="bar-cell">Usage</th></tr>
                    {% for table, count in top_tables %}
                    <tr class="clickable" onclick="window.location='/?search={{ table }}'">
                        <td><a class="table-link" href="/?search={{ table }}">{{ table }}</a></td>
                        <td>{{ count }}</td>
                        <td class="bar-cell">
                            <div class="bar-container">
                                <div class="bar" style="width: {{ (count / max_table_count * 100)|int }}%;"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- SQL Types -->
            <div class="card">
                <h3>SQL Statement Types</h3>
                <table>
                    <tr><th>Type</th><th>Count</th><th class="bar-cell">Distribution</th></tr>
                    {% for sql_type, count in sql_types %}
                    <tr class="clickable" onclick="window.location='/insights?type={{ sql_type }}{% if search %}&search={{ search }}{% endif %}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td><span class="drill-link">{{ sql_type }}</span></td>
                        <td>{{ count }}</td>
                        <td class="bar-cell">
                            <div class="bar-container">
                                <div class="bar green" style="width: {{ (count / total_scripts * 100)|int }}%;"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Top Schemas -->
            <div class="card">
                <h3>Top Schemas</h3>
                <table>
                    <tr><th>Schema</th><th>Count</th><th class="bar-cell">Usage</th></tr>
                    {% for schema, count in top_schemas %}
                    <tr class="clickable" onclick="window.location='/?search={{ schema }}'">
                        <td><a class="table-link" href="/?search={{ schema }}">{{ schema }}</a></td>
                        <td>{{ count }}</td>
                        <td class="bar-cell">
                            <div class="bar-container">
                                <div class="bar orange" style="width: {{ (count / max_schema_count * 100)|int if max_schema_count else 0 }}%;"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Extraction Sources -->
            <div class="card">
                <h3>Extraction Sources</h3>
                <table>
                    <tr><th>Source</th><th>Count</th><th class="bar-cell">Distribution</th></tr>
                    {% for source, count in sources %}
                    <tr class="clickable" onclick="window.location='/insights?source={{ source }}{% if search %}&search={{ search }}{% endif %}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td><span class="drill-link">{{ source }}</span></td>
                        <td>{{ count }}</td>
                        <td class="bar-cell">
                            <div class="bar-container">
                                <div class="bar" style="width: {{ (count / total_scripts * 100)|int }}%;"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Script Size Distribution -->
            <div class="card">
                <h3>Script Size Distribution</h3>
                <table>
                    <tr><th>Size</th><th>Count</th><th class="bar-cell">Distribution</th></tr>
                    {% for bucket, count in size_distribution %}
                    <tr class="clickable" onclick="window.location='/insights?size={{ bucket }}{% if search %}&search={{ search }}{% endif %}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td><span class="drill-link">{{ bucket }}</span></td>
                        <td>{{ count }}</td>
                        <td class="bar-cell">
                            <div class="bar-container">
                                <div class="bar green" style="width: {{ (count / total_scripts * 100)|int }}%;"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Complexity Distribution -->
            <div class="card">
                <h3>Nesting Depth Distribution</h3>
                <table>
                    <tr><th>Depth</th><th>Count</th><th class="bar-cell">Distribution</th></tr>
                    {% for bucket, count in nesting_distribution %}
                    <tr class="clickable" onclick="window.location='/insights?nesting={{ bucket }}{% if search %}&search={{ search }}{% endif %}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td><span class="drill-link">{{ bucket }}</span></td>
                        <td>{{ count }}</td>
                        <td class="bar-cell">
                            <div class="bar-container">
                                <div class="bar orange" style="width: {{ (count / total_scripts * 100)|int }}%;"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Top Spaces by Scripts -->
            <div class="card">
                <h3>Top 15 Spaces by Script Count</h3>
                <table>
                    <tr><th>Space</th><th>Scripts</th><th>Lines</th><th>Tables</th></tr>
                    {% for space in top_spaces %}
                    <tr class="clickable" onclick="window.location='/?space={{ space.space_key }}'">
                        <td><a class="table-link" href="/?space={{ space.space_key }}">{{ space.space_key }}</a></td>
                        <td>{{ space.count }}</td>
                        <td>{{ space.total_lines }}</td>
                        <td>{{ space.unique_tables }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Most Complex Scripts -->
            <div class="card">
                <h3>Most Complex Scripts (by keyword count)</h3>
                <table>
                    <tr><th>Page</th><th>Space</th><th>Keywords</th><th>Lines</th></tr>
                    {% for script in most_complex %}
                    <tr class="clickable" onclick="window.location='/?id={{ script.id }}'">
                        <td><a class="table-link" href="/?id={{ script.id }}">{{ script.page_title[:35] }}{% if script.page_title|length > 35 %}...{% endif %}</a></td>
                        <td>{{ script.space_key }}</td>
                        <td>{{ script.keywords }}</td>
                        <td>{{ script.line_count }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Top Pages by Script Count -->
            <div class="card">
                <h3>Top 15 Pages by Script Count</h3>
                <table>
                    <tr><th>Page</th><th>Space</th><th>Scripts</th><th>Lines</th></tr>
                    {% for page in top_pages %}
                    <tr class="clickable" onclick="window.location='/?page={{ page.page_id }}{% if search %}&search={{ search }}{% endif %}'">
                        <td><a class="table-link" href="/?page={{ page.page_id }}{% if search %}&search={{ search }}{% endif %}">{{ page.page_title[:35] }}{% if page.page_title|length > 35 %}...{% endif %}</a></td>
                        <td>{{ page.space_key }}</td>
                        <td>{{ page.script_count }}</td>
                        <td>{{ page.total_lines }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
        </div>

        {% if search or type_filter or source_filter or size_filter or nesting_filter %}
        <!-- Filtered Scripts List -->
        <h2>Matching Scripts ({{ total_scripts }} results)</h2>
        <div class="card">
            {% if total_scripts > page_size %}
            <div style="display: flex; gap: 10px; align-items: center; margin-bottom: 15px; flex-wrap: wrap;">
                <a href="/insights?{{ filter_params }}&page=1" style="padding: 6px 12px; background: {% if scripts_page == 1 %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">First</a>
                <a href="/insights?{{ filter_params }}&page={{ scripts_page - 1 }}" style="padding: 6px 12px; background: {% if scripts_page == 1 %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">Prev</a>
                <span style="color: #666;">Page {{ scripts_page }} of {{ total_pages }}</span>
                <a href="/insights?{{ filter_params }}&page={{ scripts_page + 1 }}" style="padding: 6px 12px; background: {% if scripts_page >= total_pages %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">Next</a>
                <a href="/insights?{{ filter_params }}&page={{ total_pages }}" style="padding: 6px 12px; background: {% if scripts_page >= total_pages %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">Last</a>
                <span style="color: #888; font-size: 13px;">(showing {{ (scripts_page - 1) * page_size + 1 }}-{{ [scripts_page * page_size, total_scripts]|min }} of {{ total_scripts }})</span>
            </div>
            {% endif %}
            <table>
                <tr><th>ID</th><th>Page</th><th>Space</th><th>Type</th><th>Source</th><th>Lines</th><th>Nesting</th></tr>
                {% for script in filtered_scripts %}
                <tr class="clickable" onclick="window.location='/?id={{ script.id }}'">
                    <td>{{ script.id }}</td>
                    <td><a class="table-link" href="/?id={{ script.id }}">{{ script.page_title[:30] }}{% if script.page_title|length > 30 %}...{% endif %}</a></td>
                    <td>{{ script.space_key }}</td>
                    <td>{{ script.sql_type }}</td>
                    <td>{{ script.source }}</td>
                    <td>{{ script.line_count }}</td>
                    <td>{{ script.nesting }}</td>
                </tr>
                {% endfor %}
            </table>
            {% if total_scripts > page_size %}
            <div style="display: flex; gap: 10px; align-items: center; margin-top: 15px; flex-wrap: wrap;">
                <a href="/insights?{{ filter_params }}&page=1" style="padding: 6px 12px; background: {% if scripts_page == 1 %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">First</a>
                <a href="/insights?{{ filter_params }}&page={{ scripts_page - 1 }}" style="padding: 6px 12px; background: {% if scripts_page == 1 %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">Prev</a>
                <span style="color: #666;">Page {{ scripts_page }} of {{ total_pages }}</span>
                <a href="/insights?{{ filter_params }}&page={{ scripts_page + 1 }}" style="padding: 6px 12px; background: {% if scripts_page >= total_pages %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">Next</a>
                <a href="/insights?{{ filter_params }}&page={{ total_pages }}" style="padding: 6px 12px; background: {% if scripts_page >= total_pages %}#ccc{% else %}#007bff{% endif %}; color: white; text-decoration: none; border-radius: 4px;">Last</a>
            </div>
            {% endif %}
        </div>
        {% endif %}
    </div>
</body>
</html>
'''


def get_db():
    """Get database connection for current request."""
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(error):
    """Close database connection at end of request."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


def get_tree(search=None):
    """Get hierarchical tree of spaces -> pages -> scripts, optionally filtered by search."""
    db = get_db()

    # Get all scripts grouped by space and page, with optional search filter
    if search:
        cursor = db.execute('''
            SELECT id, space_key, space_name, page_id, page_title, line_count
            FROM sql_scripts
            WHERE sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?
            ORDER BY space_key, page_title, id
        ''', (f'%{search}%', f'%{search}%', f'%{search}%'))
    else:
        cursor = db.execute('''
            SELECT id, space_key, space_name, page_id, page_title, line_count
            FROM sql_scripts
            ORDER BY space_key, page_title, id
        ''')

    tree = {}
    for row in cursor:
        space_key = row['space_key']
        page_id = row['page_id']

        if space_key not in tree:
            tree[space_key] = {
                'space_key': space_key,
                'space_name': row['space_name'],
                'pages': {},
                'script_count': 0
            }

        if page_id not in tree[space_key]['pages']:
            tree[space_key]['pages'][page_id] = {
                'page_id': page_id,
                'page_title': row['page_title'],
                'scripts': [],
                'script_count': 0
            }

        tree[space_key]['pages'][page_id]['scripts'].append({
            'id': row['id'],
            'line_count': row['line_count']
        })
        tree[space_key]['pages'][page_id]['script_count'] += 1
        tree[space_key]['script_count'] += 1

    # Convert to list format
    result = []
    for space_key in sorted(tree.keys()):
        space = tree[space_key]
        space['pages'] = sorted(space['pages'].values(), key=lambda p: p['page_title'] or '')
        result.append(space)

    return result


def get_total_count(search=None, space_filter=None, page_filter=None):
    """Get total count of scripts with filters."""
    db = get_db()

    conditions = []
    params = []

    if search:
        conditions.append('(sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?)')
        params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])
    if space_filter:
        conditions.append('space_key = ?')
        params.append(space_filter)
    if page_filter:
        conditions.append('page_id = ?')
        params.append(page_filter)

    where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    cursor = db.execute(f'SELECT COUNT(*) FROM sql_scripts{where}', params)
    return cursor.fetchone()[0]


def get_script_by_id(script_id):
    """Get script by ID."""
    db = get_db()
    cursor = db.execute('''
        SELECT id, space_key, space_name, page_id, page_title,
               last_modified, sql_language, sql_title, sql_description,
               sql_source, sql_code, line_count
        FROM sql_scripts WHERE id = ?
    ''', (script_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_script(index, search=None, space_filter=None, page_filter=None):
    """Get script at given index with filters."""
    db = get_db()

    conditions = []
    params = []

    if search:
        conditions.append('(sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?)')
        params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])
    if space_filter:
        conditions.append('space_key = ?')
        params.append(space_filter)
    if page_filter:
        conditions.append('page_id = ?')
        params.append(page_filter)

    where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    params.append(index)

    cursor = db.execute(f'''
        SELECT id, space_key, space_name, page_id, page_title,
               last_modified, sql_language, sql_title, sql_description,
               sql_source, sql_code, line_count
        FROM sql_scripts
        {where}
        ORDER BY id
        LIMIT 1 OFFSET ?
    ''', params)
    row = cursor.fetchone()
    return dict(row) if row else None


@app.route('/')
def browse():
    """Main browse endpoint."""
    search = request.args.get('search', '').strip() or None
    space_filter = request.args.get('space', '').strip() or None
    page_filter = request.args.get('page', '').strip() or None
    script_id = request.args.get('id', '').strip() or None

    # Get tree for sidebar (filtered by search if present)
    tree = get_tree(search)
    total_scripts = sum(s['script_count'] for s in tree)

    # If specific script ID requested
    if script_id:
        try:
            script = get_script_by_id(int(script_id))
            if script:
                # Find index for this script
                total = get_total_count(search, space_filter, page_filter)
                return render_template_string(
                    HTML_TEMPLATE,
                    script=script,
                    index=0,
                    total=1,
                    total_scripts=total_scripts,
                    search=search,
                    space_filter=script['space_key'],
                    page_filter=script['page_id'],
                    current_id=script['id'],
                    tree=tree,
                    confluence_url=CONFLUENCE_BASE_URL,
                    suggestion_email=SUGGESTION_EMAIL,
                    db_last_modified=get_db_last_modified()
                )
        except ValueError:
            pass

    total = get_total_count(search, space_filter, page_filter)

    # Handle "goto" form (1-based) vs navigation links (0-based "index")
    if 'goto' in request.args:
        try:
            index = int(request.args.get('goto', 1)) - 1
        except ValueError:
            index = 0
    else:
        try:
            index = int(request.args.get('index', 0))
        except ValueError:
            index = 0

    # Clamp to valid range
    index = max(0, min(index, total - 1)) if total > 0 else 0

    script = get_script(index, search, space_filter, page_filter) if total > 0 else None
    current_id = script['id'] if script else None

    return render_template_string(
        HTML_TEMPLATE,
        script=script,
        index=index,
        total=total,
        total_scripts=total_scripts,
        search=search,
        space_filter=space_filter,
        page_filter=page_filter,
        current_id=current_id,
        tree=tree,
        confluence_url=CONFLUENCE_BASE_URL,
        suggestion_email=SUGGESTION_EMAIL,
        db_last_modified=get_db_last_modified()
    )


def parse_date(date_str):
    """Parse date string to datetime, return None if invalid."""
    if not date_str:
        return None
    try:
        # Try common formats
        for fmt in ['%Y-%m-%d %H:%M:%S UTC', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
            try:
                return datetime.strptime(date_str.split('.')[0], fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None


@app.route('/timeline')
def timeline():
    """Timeline view of SQL scripts by space."""
    search = request.args.get('search', '').strip() or None
    db = get_db()

    # Build query
    conditions = []
    params = []
    if search:
        conditions.append('(sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?)')
        params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])

    where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''

    cursor = db.execute(f'''
        SELECT id, space_key, page_id, page_title, last_modified
        FROM sql_scripts
        {where}
        ORDER BY space_key, last_modified
    ''', params)

    # Group by space and collect dates
    spaces = {}
    all_dates = []

    for row in cursor:
        space_key = row['space_key']
        date = parse_date(row['last_modified'])

        if space_key not in spaces:
            spaces[space_key] = {'space_key': space_key, 'scripts': [], 'script_count': 0}

        spaces[space_key]['scripts'].append({
            'id': row['id'],
            'page_id': row['page_id'],
            'page_title': row['page_title'],
            'date': date,
            'date_str': row['last_modified']
        })
        spaces[space_key]['script_count'] += 1

        if date:
            all_dates.append(date)

    # Calculate timeline range (10 years by default, or based on data)
    now = datetime.now()
    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
        # Extend range a bit
        min_year = min_date.year
        max_year = max(max_date.year, now.year)
    else:
        min_year = now.year - 10
        max_year = now.year

    # Ensure at least 5 year span
    if max_year - min_year < 5:
        min_year = max_year - 5

    total_days = (datetime(max_year + 1, 1, 1) - datetime(min_year, 1, 1)).days

    # Calculate positions for each event
    for space in spaces.values():
        events = []
        for script in space['scripts']:
            if script['date']:
                days_from_start = (script['date'] - datetime(min_year, 1, 1)).days
                position = (days_from_start / total_days) * 100
                position = max(0, min(100, position))
                events.append({
                    'id': script['id'],
                    'position': position,
                    'page_title': script['page_title'] or 'Untitled',
                    'date': script['date_str']
                })
        space['events'] = events

    # Year markers
    years = []
    for year in range(min_year, max_year + 1):
        days_from_start = (datetime(year, 1, 1) - datetime(min_year, 1, 1)).days
        position = (days_from_start / total_days) * 100
        years.append({'year': year, 'position': position})

    # Sort spaces by script count
    spaces_list = sorted(spaces.values(), key=lambda s: -s['script_count'])
    total_scripts = sum(s['script_count'] for s in spaces_list)

    return render_template_string(
        TIMELINE_TEMPLATE,
        search=search,
        spaces=spaces_list,
        years=years,
        min_year=min_year,
        max_year=max_year,
        total_scripts=total_scripts
    )


# === Analytics helper functions (from analyze_extracted_sql.py) ===

def get_table_references(sql_code):
    """Extract table references from SQL code."""
    tables = set()
    patterns = [
        r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bINTO\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bTABLE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, sql_code, re.IGNORECASE)
        for match in matches:
            if match.upper() not in ('SELECT', 'FROM', 'WHERE', 'SET', 'VALUES', 'INTO', 'TABLE'):
                tables.add(match.upper())
    return tables


def get_schema_references(sql_code):
    """Extract schema references (schema.table patterns)."""
    schemas = set()
    pattern = r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b'
    matches = re.findall(pattern, sql_code)
    for schema, table in matches:
        if schema.upper() not in ('SYS', 'DUAL'):
            schemas.add(schema.upper())
    return schemas


def get_sql_type(sql_code):
    """Determine the primary type of SQL statement."""
    sql_upper = sql_code.strip().upper()
    if sql_upper.startswith('SELECT') or sql_upper.startswith('WITH'):
        return 'SELECT'
    elif sql_upper.startswith('INSERT'):
        return 'INSERT'
    elif sql_upper.startswith('UPDATE'):
        return 'UPDATE'
    elif sql_upper.startswith('DELETE'):
        return 'DELETE'
    elif sql_upper.startswith('CREATE'):
        if 'PROCEDURE' in sql_upper:
            return 'CREATE PROCEDURE'
        elif 'FUNCTION' in sql_upper:
            return 'CREATE FUNCTION'
        elif 'VIEW' in sql_upper:
            return 'CREATE VIEW'
        elif 'TABLE' in sql_upper:
            return 'CREATE TABLE'
        else:
            return 'CREATE OTHER'
    elif sql_upper.startswith('ALTER'):
        return 'ALTER'
    elif sql_upper.startswith('DROP'):
        return 'DROP'
    elif sql_upper.startswith('DECLARE') or sql_upper.startswith('BEGIN'):
        return 'PL/SQL BLOCK'
    else:
        return 'OTHER'


def count_nesting_level(sql_code):
    """Count the maximum nesting level."""
    max_depth = 0
    current_depth = 0
    in_string = False
    string_char = None
    for i, char in enumerate(sql_code):
        if char in ("'", '"') and (i == 0 or sql_code[i-1] != '\\'):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
        elif not in_string:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth = max(0, current_depth - 1)
    return max_depth


def count_keywords(sql_code):
    """Count SQL keywords for complexity estimation."""
    keywords = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
        'GROUP BY', 'ORDER BY', 'HAVING', 'UNION', 'INSERT', 'UPDATE', 'DELETE',
        'BEGIN', 'END', 'IF', 'THEN', 'ELSE', 'CASE', 'WHEN', 'LOOP', 'CURSOR',
    ]
    sql_upper = sql_code.upper()
    count = 0
    for kw in keywords:
        count += len(re.findall(r'\b' + kw.replace(' ', r'\s+') + r'\b', sql_upper))
    return count


@app.route('/insights')
def insights():
    """Insights view using pre-computed columns for fast aggregation."""
    db = get_db()

    # Get filter parameters
    search = request.args.get('search', '').strip() or None
    space_filter = request.args.get('space', '').strip() or None
    type_filter = request.args.get('type', '').strip() or None
    source_filter = request.args.get('source', '').strip() or None
    size_filter = request.args.get('size', '').strip() or None
    nesting_filter = request.args.get('nesting', '').strip() or None

    # Pagination for scripts list
    page_size = 50
    try:
        scripts_page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        scripts_page = 1

    # Get list of all spaces for dropdown
    all_spaces_cursor = db.execute('SELECT DISTINCT space_key FROM sql_scripts ORDER BY space_key')
    all_spaces = [row['space_key'] for row in all_spaces_cursor]

    # Build WHERE clause for filters
    def build_where_clause(extra_conditions=None):
        conditions = []
        params = []
        if search:
            conditions.append('sql_code LIKE ?')
            params.append(f'%{search}%')
        if space_filter:
            conditions.append('space_key = ?')
            params.append(space_filter)
        if type_filter:
            conditions.append('sql_type = ?')
            params.append(type_filter)
        if source_filter:
            conditions.append('sql_source = ?')
            params.append(source_filter)
        if size_filter:
            # Map size bucket to SQL condition
            size_conditions = {
                '1-5 lines': 'line_count <= 5',
                '6-20 lines': 'line_count > 5 AND line_count <= 20',
                '21-50 lines': 'line_count > 20 AND line_count <= 50',
                '51-100 lines': 'line_count > 50 AND line_count <= 100',
                '101-500 lines': 'line_count > 100 AND line_count <= 500',
                '500+ lines': 'line_count > 500',
            }
            if size_filter in size_conditions:
                conditions.append(f'({size_conditions[size_filter]})')
        if nesting_filter:
            # Map nesting bucket to SQL condition
            nesting_conditions = {
                'No nesting (0)': 'nesting_depth = 0',
                'Shallow (1-2)': 'nesting_depth >= 1 AND nesting_depth <= 2',
                'Moderate (3-5)': 'nesting_depth >= 3 AND nesting_depth <= 5',
                'Deep (6-10)': 'nesting_depth >= 6 AND nesting_depth <= 10',
                'Very deep (10+)': 'nesting_depth > 10',
            }
            if nesting_filter in nesting_conditions:
                conditions.append(f'({nesting_conditions[nesting_filter]})')
        if extra_conditions:
            conditions.extend(extra_conditions)
        where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
        return where, params

    where, params = build_where_clause()

    # Get summary stats using SQL aggregates (fast!)
    summary = db.execute(f'''
        SELECT COUNT(*) as total_scripts,
               COALESCE(SUM(line_count), 0) as total_lines,
               COUNT(DISTINCT space_key) as total_spaces
        FROM sql_scripts {where}
    ''', params).fetchone()
    total_scripts = summary['total_scripts']
    total_lines = summary['total_lines']
    total_spaces = summary['total_spaces']

    # SQL types aggregation
    sql_types_rows = db.execute(f'''
        SELECT sql_type, COUNT(*) as cnt
        FROM sql_scripts {where}
        GROUP BY sql_type
        ORDER BY cnt DESC
        LIMIT 15
    ''', params).fetchall()
    sql_types = [(row['sql_type'] or 'OTHER', row['cnt']) for row in sql_types_rows]

    # Source types aggregation
    sources_rows = db.execute(f'''
        SELECT COALESCE(sql_source, 'unknown') as source, COUNT(*) as cnt
        FROM sql_scripts {where}
        GROUP BY sql_source
        ORDER BY cnt DESC
    ''', params).fetchall()
    sources = [(row['source'], row['cnt']) for row in sources_rows]

    # Size distribution using SQL CASE
    size_dist_rows = db.execute(f'''
        SELECT
            CASE
                WHEN line_count <= 5 THEN '1-5 lines'
                WHEN line_count <= 20 THEN '6-20 lines'
                WHEN line_count <= 50 THEN '21-50 lines'
                WHEN line_count <= 100 THEN '51-100 lines'
                WHEN line_count <= 500 THEN '101-500 lines'
                ELSE '500+ lines'
            END as bucket,
            COUNT(*) as cnt
        FROM sql_scripts {where}
        GROUP BY bucket
    ''', params).fetchall()
    size_dict = {row['bucket']: row['cnt'] for row in size_dist_rows}
    size_buckets_order = ['1-5 lines', '6-20 lines', '21-50 lines', '51-100 lines', '101-500 lines', '500+ lines']
    size_distribution = [(b, size_dict.get(b, 0)) for b in size_buckets_order]

    # Nesting distribution using SQL CASE
    nesting_dist_rows = db.execute(f'''
        SELECT
            CASE
                WHEN nesting_depth = 0 THEN 'No nesting (0)'
                WHEN nesting_depth <= 2 THEN 'Shallow (1-2)'
                WHEN nesting_depth <= 5 THEN 'Moderate (3-5)'
                WHEN nesting_depth <= 10 THEN 'Deep (6-10)'
                ELSE 'Very deep (10+)'
            END as bucket,
            COUNT(*) as cnt
        FROM sql_scripts {where}
        GROUP BY bucket
    ''', params).fetchall()
    nest_dict = {row['bucket']: row['cnt'] for row in nesting_dist_rows}
    nest_buckets_order = ['No nesting (0)', 'Shallow (1-2)', 'Moderate (3-5)', 'Deep (6-10)', 'Very deep (10+)']
    nesting_distribution = [(b, nest_dict.get(b, 0)) for b in nest_buckets_order]

    # Top spaces by script count
    top_spaces_rows = db.execute(f'''
        SELECT space_key, COUNT(*) as cnt, SUM(line_count) as total_lines
        FROM sql_scripts {where}
        GROUP BY space_key
        ORDER BY cnt DESC
        LIMIT 15
    ''', params).fetchall()
    top_spaces = [{'space_key': row['space_key'], 'count': row['cnt'],
                   'total_lines': row['total_lines'], 'unique_tables': 0} for row in top_spaces_rows]

    # Most complex scripts (by keyword_count)
    most_complex_rows = db.execute(f'''
        SELECT id, space_key, page_title, line_count, nesting_depth, keyword_count
        FROM sql_scripts {where}
        ORDER BY keyword_count DESC
        LIMIT 10
    ''', params).fetchall()
    most_complex = [{'id': row['id'], 'space_key': row['space_key'],
                     'page_title': row['page_title'] or 'Untitled',
                     'line_count': row['line_count'], 'nesting': row['nesting_depth'],
                     'keywords': row['keyword_count']} for row in most_complex_rows]

    # Top pages by script count
    top_pages_rows = db.execute(f'''
        SELECT page_id, page_title, space_key, COUNT(*) as cnt, SUM(line_count) as total_lines
        FROM sql_scripts {where}
        GROUP BY page_id
        ORDER BY cnt DESC
        LIMIT 15
    ''', params).fetchall()
    top_pages = [{'page_id': row['page_id'], 'page_title': row['page_title'] or 'Untitled',
                  'space_key': row['space_key'], 'script_count': row['cnt'],
                  'total_lines': row['total_lines']} for row in top_pages_rows]

    # For filtered results, get script list with pagination
    filtered_scripts = []
    total_pages = 1
    if search or type_filter or source_filter or size_filter or nesting_filter:
        # Calculate total pages
        total_pages = max(1, (total_scripts + page_size - 1) // page_size)
        scripts_page = min(scripts_page, total_pages)  # Clamp to valid range
        offset = (scripts_page - 1) * page_size

        scripts_rows = db.execute(f'''
            SELECT id, space_key, page_id, page_title, line_count, nesting_depth, keyword_count, sql_type, sql_source
            FROM sql_scripts {where}
            ORDER BY id
            LIMIT {page_size} OFFSET {offset}
        ''', params).fetchall()
        filtered_scripts = [{'id': row['id'], 'space_key': row['space_key'],
                            'page_id': row['page_id'], 'page_title': row['page_title'] or 'Untitled',
                            'line_count': row['line_count'], 'nesting': row['nesting_depth'],
                            'keywords': row['keyword_count'], 'sql_type': row['sql_type'],
                            'source': row['sql_source']} for row in scripts_rows]

    # Tables and schemas - use pre-computed columns if available, otherwise parse SQL
    top_tables = []
    top_schemas = []
    max_table_count = 1
    max_schema_count = 1
    unique_tables = 0
    unique_schemas = 0

    # Check if pre-computed columns exist
    try:
        db.execute('SELECT tables_referenced FROM sql_scripts LIMIT 1')
        has_precomputed = True
    except Exception:
        has_precomputed = False

    if has_precomputed:
        # Use pre-computed tables_referenced and schemas_referenced columns (fast!)
        all_tables = Counter()
        all_schemas = Counter()
        tables_condition = "tables_referenced IS NOT NULL AND tables_referenced != ''"
        if where:
            tables_where = f"{where} AND {tables_condition}"
        else:
            tables_where = f"WHERE {tables_condition}"
        tables_rows = db.execute(f'''
            SELECT tables_referenced, schemas_referenced
            FROM sql_scripts {tables_where}
        ''', params).fetchall()
        for row in tables_rows:
            if row['tables_referenced']:
                all_tables.update(row['tables_referenced'].split(','))
            if row['schemas_referenced']:
                all_schemas.update(row['schemas_referenced'].split(','))
        top_tables = all_tables.most_common(15)
        top_schemas = all_schemas.most_common(10)
        max_table_count = top_tables[0][1] if top_tables else 1
        max_schema_count = top_schemas[0][1] if top_schemas else 1
        unique_tables = len(all_tables)
        unique_schemas = len(all_schemas)
    elif total_scripts <= 1000:
        # Fall back to parsing SQL for small result sets (legacy databases)
        all_tables = Counter()
        all_schemas = Counter()
        sql_codes = db.execute(f'SELECT sql_code FROM sql_scripts {where} LIMIT 1000', params).fetchall()
        for row in sql_codes:
            all_tables.update(get_table_references(row['sql_code']))
            all_schemas.update(get_schema_references(row['sql_code']))
        top_tables = all_tables.most_common(15)
        top_schemas = all_schemas.most_common(10)
        max_table_count = top_tables[0][1] if top_tables else 1
        max_schema_count = top_schemas[0][1] if top_schemas else 1
        unique_tables = len(all_tables)
        unique_schemas = len(all_schemas)

    # Build filter params string for pagination links
    filter_parts = []
    if search:
        filter_parts.append(f'search={search}')
    if space_filter:
        filter_parts.append(f'space={space_filter}')
    if type_filter:
        filter_parts.append(f'type={type_filter}')
    if source_filter:
        filter_parts.append(f'source={source_filter}')
    if size_filter:
        filter_parts.append(f'size={size_filter}')
    if nesting_filter:
        filter_parts.append(f'nesting={nesting_filter}')
    filter_params = '&'.join(filter_parts)

    return render_template_string(
        INSIGHTS_TEMPLATE,
        total_scripts=total_scripts,
        total_lines=total_lines,
        total_spaces=total_spaces,
        unique_tables=unique_tables,
        unique_schemas=unique_schemas,
        top_tables=top_tables,
        max_table_count=max_table_count,
        top_schemas=top_schemas,
        max_schema_count=max_schema_count,
        sql_types=sql_types,
        sources=sources,
        size_distribution=size_distribution,
        nesting_distribution=nesting_distribution,
        top_spaces=top_spaces,
        top_pages=top_pages,
        most_complex=most_complex,
        search=search,
        space_filter=space_filter,
        type_filter=type_filter,
        source_filter=source_filter,
        size_filter=size_filter,
        nesting_filter=nesting_filter,
        all_spaces=all_spaces,
        filtered_scripts=filtered_scripts,
        scripts_page=scripts_page,
        total_pages=total_pages,
        page_size=page_size,
        filter_params=filter_params
    )


def main():
    global DATABASE, CONFLUENCE_BASE_URL

    load_config()

    parser = argparse.ArgumentParser(
        description='Web-based SQL script browser',
        epilog='Example: python browse_extracted_sql_web.py --db sql_queries.db --port 5080'
    )
    parser.add_argument(
        '--db', required=True,
        help='Path to SQLite database file'
    )
    parser.add_argument(
        '--port', type=int, default=5080,
        help='Port to run server on (default: 5080)'
    )
    parser.add_argument(
        '--host', default='127.0.0.1',
        help='Host to bind to (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--confluence-url',
        help='Confluence base URL for click-through links (e.g., https://wiki.example.com)'
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database file not found: {args.db}")
        return 1

    DATABASE = args.db

    # Try to get Confluence URL from settings if not provided
    CONFLUENCE_BASE_URL = args.confluence_url
    if not CONFLUENCE_BASE_URL:
        try:
            import sys
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from config_loader import load_confluence_settings
            settings = load_confluence_settings()
            CONFLUENCE_BASE_URL = settings.get('base_url', '').rstrip('/')
        except Exception:
            pass

    # Quick count check
    conn = sqlite3.connect(DATABASE)
    count = conn.execute('SELECT COUNT(*) FROM sql_scripts').fetchone()[0]
    conn.close()

    print(f"Loaded {count} SQL scripts from {args.db}")
    print(f"Starting server at http://{args.host}:{args.port}")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()
