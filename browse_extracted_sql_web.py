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
from flask import Flask, render_template_string, request, g

app = Flask(__name__)
DATABASE = None

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>SQL Script Browser</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0; padding: 20px; background: #f5f5f5;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; }

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
        .sql-code .keyword { color: #569cd6; }
        .sql-code .function { color: #dcdcaa; }
        .sql-code .string { color: #ce9178; }
        .sql-code .comment { color: #6a9955; }
        .sql-code .number { color: #b5cea8; }

        .search-results { margin-bottom: 10px; color: #666; }
        .line-count { color: #888; font-size: 12px; }

        form { display: inline; }
    </style>
</head>
<body>
    <div class="container">
        <h1>SQL Script Browser</h1>

        <div class="nav">
            <a href="/?index=0" {% if index == 0 %}class="disabled"{% endif %}>First</a>
            <a href="/?index={{ index - 1 }}{% if search %}&search={{ search }}{% endif %}" {% if index == 0 %}class="disabled"{% endif %}>Prev</a>
            <a href="/?index={{ index + 1 }}{% if search %}&search={{ search }}{% endif %}" {% if index >= total - 1 %}class="disabled"{% endif %}>Next</a>
            <a href="/?index={{ total - 1 }}" {% if index >= total - 1 %}class="disabled"{% endif %}>Last</a>

            <form action="/" method="get" style="display: flex; gap: 5px; align-items: center;">
                <input type="number" name="goto" value="{{ index + 1 }}" min="1" max="{{ total }}">
                <button type="submit">Go</button>
            </form>

            <div class="spacer"></div>

            <form action="/" method="get" style="display: flex; gap: 5px; align-items: center;">
                <input type="text" name="search" value="{{ search or '' }}" placeholder="Search SQL...">
                <button type="submit">Search</button>
                {% if search %}<a href="/">Clear</a>{% endif %}
            </form>

            <span class="info">{{ index + 1 }} of {{ total }}</span>
        </div>

        {% if search %}
        <div class="search-results">
            Found {{ total }} results for "{{ search }}"
        </div>
        {% endif %}

        {% if script %}
        <div class="card">
            <div class="meta">
                <div class="meta-item">
                    <div class="meta-label">Space</div>
                    <div class="meta-value">{{ script.space_key }} ({{ script.space_name or 'N/A' }})</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Page</div>
                    <div class="meta-value">{{ script.page_title or 'Untitled' }}</div>
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

            <div class="line-count">{{ script.line_count }} lines</div>
            <pre class="sql-code">{{ script.sql_code }}</pre>
        </div>
        {% else %}
        <div class="card">
            <p>No scripts found.</p>
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


def get_total_count(search=None):
    """Get total count of scripts, optionally filtered by search."""
    db = get_db()
    if search:
        cursor = db.execute(
            'SELECT COUNT(*) FROM sql_scripts WHERE sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?',
            (f'%{search}%', f'%{search}%', f'%{search}%')
        )
    else:
        cursor = db.execute('SELECT COUNT(*) FROM sql_scripts')
    return cursor.fetchone()[0]


def get_script(index, search=None):
    """Get script at given index, optionally filtered by search."""
    db = get_db()
    if search:
        cursor = db.execute('''
            SELECT id, space_key, space_name, page_id, page_title,
                   last_modified, sql_language, sql_title, sql_description,
                   sql_source, sql_code, line_count
            FROM sql_scripts
            WHERE sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?
            ORDER BY id
            LIMIT 1 OFFSET ?
        ''', (f'%{search}%', f'%{search}%', f'%{search}%', index))
    else:
        cursor = db.execute('''
            SELECT id, space_key, space_name, page_id, page_title,
                   last_modified, sql_language, sql_title, sql_description,
                   sql_source, sql_code, line_count
            FROM sql_scripts
            ORDER BY id
            LIMIT 1 OFFSET ?
        ''', (index,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


@app.route('/')
def browse():
    """Main browse endpoint."""
    search = request.args.get('search', '').strip() or None
    total = get_total_count(search)

    # Handle "goto" form (1-based) vs navigation links (0-based "index")
    if 'goto' in request.args:
        # From the Go form - 1-based input
        try:
            index = int(request.args.get('goto', 1)) - 1
        except ValueError:
            index = 0
    else:
        # From navigation links - 0-based
        try:
            index = int(request.args.get('index', 0))
        except ValueError:
            index = 0

    # Clamp to valid range
    index = max(0, min(index, total - 1)) if total > 0 else 0

    script = get_script(index, search) if total > 0 else None

    return render_template_string(
        HTML_TEMPLATE,
        script=script,
        index=index,
        total=total,
        search=search
    )


def main():
    global DATABASE

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
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database file not found: {args.db}")
        return 1

    DATABASE = args.db

    # Quick count check
    conn = sqlite3.connect(DATABASE)
    count = conn.execute('SELECT COUNT(*) FROM sql_scripts').fetchone()[0]
    conn.close()

    print(f"Loaded {count} SQL scripts from {args.db}")
    print(f"Starting server at http://{args.host}:{args.port}")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()
