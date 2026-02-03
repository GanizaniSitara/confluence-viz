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
from datetime import datetime
from collections import Counter, defaultdict
from flask import Flask, render_template_string, request, g

app = Flask(__name__)
DATABASE = None
CONFLUENCE_BASE_URL = None

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
                         onclick="window.location='/?id={{ script.id }}'">
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
            <h1>Confluence SQL Script Browser</h1>

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
                    <a href="/insights" style="background: #6f42c1;">Insights</a>
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
        function toggleSpace(el) {
            const pages = el.nextElementSibling;
            pages.classList.toggle('open');
        }
        function togglePage(el, spaceKey, pageId) {
            // Navigate to filter by page
            window.location = '/?page=' + pageId;
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
            <a href="/">← Browser</a>
            <a href="/insights">Insights</a>
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
                    <td class="space-name"><a href="/?space={{ space.space_key }}">{{ space.space_key }}</a></td>
                    <td class="script-count">{{ space.script_count }}</td>
                    <td class="timeline-cell">
                        <div class="timeline">
                            {% for event in space.events %}
                            <div class="event" style="left: {{ event.position }}%;"
                                 title="{{ event.page_title }} ({{ event.date }})"
                                 onclick="window.location='/?id={{ event.id }}'"></div>
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
    </style>
</head>
<body>
    <div class="container">
        <h1>SQL Insights</h1>
        <p class="subtitle">
            {{ total_scripts }} SQL scripts
            {% if space_filter %} in <strong>{{ space_filter }}</strong>{% else %} across {{ total_spaces }} spaces{% endif %}
            {% if type_filter %} | Type: <strong>{{ type_filter }}</strong>{% endif %}
            {% if source_filter %} | Source: <strong>{{ source_filter }}</strong>{% endif %}
            {% if size_filter %} | Size: <strong>{{ size_filter }}</strong>{% endif %}
            {% if nesting_filter %} | Nesting: <strong>{{ nesting_filter }}</strong>{% endif %}
            {% if space_filter or type_filter or source_filter or size_filter or nesting_filter %}
                | <a href="/insights">Clear all filters</a>
            {% endif %}
        </p>

        <div class="nav">
            <a href="/">← Browser</a>
            <a href="/timeline">Timeline</a>
            <a href="/insights" class="active">Insights</a>
            <div class="spacer"></div>
            <form action="/insights" method="get" style="display: flex; gap: 5px;">
                <select name="space" onchange="this.form.submit()" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                    <option value="">All Spaces</option>
                    {% for space in all_spaces %}
                    <option value="{{ space }}" {% if space == space_filter %}selected{% endif %}>{{ space }}</option>
                    {% endfor %}
                </select>
            </form>
            {% if type_filter or source_filter or size_filter or nesting_filter %}
            <a href="/insights{% if space_filter %}?space={{ space_filter }}{% endif %}" style="background: #dc3545;">Clear dimension filters</a>
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
                    <tr class="clickable" onclick="window.location='/insights?type={{ sql_type }}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td>{{ sql_type }}</td>
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
                    <tr class="clickable" onclick="window.location='/insights?source={{ source }}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td>{{ source }}</td>
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
                    <tr class="clickable" onclick="window.location='/insights?size={{ bucket }}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td>{{ bucket }}</td>
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
                    <tr class="clickable" onclick="window.location='/insights?nesting={{ bucket }}{% if space_filter %}&space={{ space_filter }}{% endif %}'">
                        <td>{{ bucket }}</td>
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
        </div>

        {% if type_filter or source_filter or size_filter or nesting_filter %}
        <!-- Filtered Scripts List -->
        <h2>Matching Scripts ({{ total_scripts }} results)</h2>
        <div class="card">
            <table>
                <tr><th>ID</th><th>Page</th><th>Space</th><th>Type</th><th>Source</th><th>Lines</th><th>Nesting</th></tr>
                {% for script in filtered_scripts[:50] %}
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
                {% if total_scripts > 50 %}
                <tr><td colspan="7" style="text-align: center; color: #666;">... and {{ total_scripts - 50 }} more scripts</td></tr>
                {% endif %}
            </table>
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


def get_tree():
    """Get hierarchical tree of spaces -> pages -> scripts."""
    db = get_db()

    # Get all scripts grouped by space and page
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

    # Get tree for sidebar
    tree = get_tree()
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
                    confluence_url=CONFLUENCE_BASE_URL
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
        confluence_url=CONFLUENCE_BASE_URL
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
    """Insights view with tables, schemas, types, complexity. Supports filtering."""
    db = get_db()

    # Get filter parameters
    space_filter = request.args.get('space', '').strip() or None
    type_filter = request.args.get('type', '').strip() or None
    source_filter = request.args.get('source', '').strip() or None
    size_filter = request.args.get('size', '').strip() or None
    nesting_filter = request.args.get('nesting', '').strip() or None

    # Get list of all spaces for dropdown
    all_spaces_cursor = db.execute('SELECT DISTINCT space_key FROM sql_scripts ORDER BY space_key')
    all_spaces = [row['space_key'] for row in all_spaces_cursor]

    # Build query with optional space filter (SQL-level filtering for performance)
    if space_filter:
        cursor = db.execute('''
            SELECT id, space_key, space_name, page_id, page_title, sql_code, line_count, sql_source
            FROM sql_scripts
            WHERE space_key = ?
        ''', (space_filter,))
    else:
        cursor = db.execute('''
            SELECT id, space_key, space_name, page_id, page_title, sql_code, line_count, sql_source
            FROM sql_scripts
        ''')

    all_tables = Counter()
    all_schemas = Counter()
    sql_types = Counter()
    source_types = Counter()
    space_stats = defaultdict(lambda: {'count': 0, 'total_lines': 0, 'tables': set()})

    script_details = []
    total_lines = 0

    # Helper to get size bucket
    def get_size_bucket(lines):
        if lines <= 5:
            return '1-5 lines'
        elif lines <= 20:
            return '6-20 lines'
        elif lines <= 50:
            return '21-50 lines'
        elif lines <= 100:
            return '51-100 lines'
        elif lines <= 500:
            return '101-500 lines'
        else:
            return '500+ lines'

    # Helper to get nesting bucket
    def get_nesting_bucket(depth):
        if depth == 0:
            return 'No nesting (0)'
        elif depth <= 2:
            return 'Shallow (1-2)'
        elif depth <= 5:
            return 'Moderate (3-5)'
        elif depth <= 10:
            return 'Deep (6-10)'
        else:
            return 'Very deep (10+)'

    for row in cursor:
        sql_code = row['sql_code']
        line_count = row['line_count'] or (sql_code.count('\n') + 1)
        sql_type = get_sql_type(sql_code)
        nesting = count_nesting_level(sql_code)
        source = row['sql_source'] or 'unknown'

        # Apply filters (post-fetch filtering for computed fields)
        if type_filter and sql_type != type_filter:
            continue
        if source_filter and source != source_filter:
            continue
        if size_filter and get_size_bucket(line_count) != size_filter:
            continue
        if nesting_filter and get_nesting_bucket(nesting) != nesting_filter:
            continue

        total_lines += line_count
        tables = get_table_references(sql_code)
        schemas = get_schema_references(sql_code)
        keywords = count_keywords(sql_code)

        all_tables.update(tables)
        all_schemas.update(schemas)
        sql_types[sql_type] += 1
        source_types[source] += 1

        space_key = row['space_key']
        space_stats[space_key]['count'] += 1
        space_stats[space_key]['total_lines'] += line_count
        space_stats[space_key]['tables'].update(tables)
        space_stats[space_key]['name'] = row['space_name']

        script_details.append({
            'id': row['id'],
            'space_key': space_key,
            'page_title': row['page_title'] or 'Untitled',
            'line_count': line_count,
            'nesting': nesting,
            'keywords': keywords,
            'sql_type': sql_type,
            'source': source,
        })

    total_scripts = len(script_details)

    # Size distribution (using helper function)
    size_buckets = [('1-5 lines', 0), ('6-20 lines', 0), ('21-50 lines', 0),
                    ('51-100 lines', 0), ('101-500 lines', 0), ('500+ lines', 0)]
    size_dict = {b[0]: 0 for b in size_buckets}
    for s in script_details:
        bucket = get_size_bucket(s['line_count'])
        size_dict[bucket] += 1
    size_distribution = [(k, size_dict[k]) for k, _ in size_buckets]

    # Nesting distribution (using helper function)
    nest_buckets = [('No nesting (0)', 0), ('Shallow (1-2)', 0), ('Moderate (3-5)', 0),
                    ('Deep (6-10)', 0), ('Very deep (10+)', 0)]
    nest_dict = {b[0]: 0 for b in nest_buckets}
    for s in script_details:
        bucket = get_nesting_bucket(s['nesting'])
        nest_dict[bucket] += 1
    nesting_distribution = [(k, nest_dict[k]) for k, _ in nest_buckets]

    # Top spaces
    top_spaces = sorted([
        {
            'space_key': k,
            'count': v['count'],
            'total_lines': v['total_lines'],
            'unique_tables': len(v['tables'])
        }
        for k, v in space_stats.items()
    ], key=lambda x: -x['count'])[:15]

    # Most complex scripts
    most_complex = sorted(script_details, key=lambda x: -x['keywords'])[:10]

    top_tables = all_tables.most_common(15)
    top_schemas = all_schemas.most_common(10)
    max_table_count = top_tables[0][1] if top_tables else 1
    max_schema_count = top_schemas[0][1] if top_schemas else 1

    return render_template_string(
        INSIGHTS_TEMPLATE,
        total_scripts=total_scripts,
        total_lines=total_lines,
        total_spaces=len(space_stats),
        unique_tables=len(all_tables),
        unique_schemas=len(all_schemas),
        top_tables=top_tables,
        max_table_count=max_table_count,
        top_schemas=top_schemas,
        max_schema_count=max_schema_count,
        sql_types=sql_types.most_common(10),
        sources=source_types.most_common(),
        size_distribution=size_distribution,
        nesting_distribution=nesting_distribution,
        top_spaces=top_spaces,
        most_complex=most_complex,
        space_filter=space_filter,
        type_filter=type_filter,
        source_filter=source_filter,
        size_filter=size_filter,
        nesting_filter=nesting_filter,
        all_spaces=all_spaces,
        filtered_scripts=script_details
    )


def main():
    global DATABASE, CONFLUENCE_BASE_URL

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
