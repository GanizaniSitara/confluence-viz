#!/usr/bin/env python3
"""
Analyze Extracted SQL Scripts from SQLite Database

Provides statistics on SQL scripts extracted from Confluence pages:
- Overall counts and averages
- Per-space breakdown
- Table/schema usage analysis
- Complexity metrics (nesting, length, keywords)
- Top scripts by various criteria

Usage:
    python analyze_extracted_sql.py --db sql_queries.db
"""

import sqlite3
import os
import re
import argparse
from collections import Counter, defaultdict


def get_table_references(sql_code):
    """Extract table references from SQL code."""
    tables = set()
    sql_upper = sql_code.upper()

    # Common patterns for table references
    patterns = [
        r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bINTO\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bTABLE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
        r'\bTRUNCATE\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, sql_code, re.IGNORECASE)
        for match in matches:
            # Filter out SQL keywords that might match
            if match.upper() not in ('SELECT', 'FROM', 'WHERE', 'SET', 'VALUES', 'INTO', 'TABLE'):
                tables.add(match.upper())

    return tables


def get_schema_references(sql_code):
    """Extract schema references (schema.table patterns)."""
    schemas = set()
    pattern = r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b'
    matches = re.findall(pattern, sql_code)
    for schema, table in matches:
        if schema.upper() not in ('SYS', 'DUAL'):  # Common Oracle system refs
            schemas.add(schema.upper())
    return schemas


def count_nesting_level(sql_code):
    """Count the maximum nesting level (subqueries, parentheses)."""
    max_depth = 0
    current_depth = 0
    in_string = False
    string_char = None

    for i, char in enumerate(sql_code):
        # Track string literals to ignore parens inside them
        if char in ("'", '"') and (i == 0 or sql_code[i-1] != '\\'):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
        elif not in_string:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth = max(0, current_depth - 1)

    return max_depth


def count_subqueries(sql_code):
    """Count the number of subqueries (SELECT within SELECT)."""
    sql_upper = sql_code.upper()
    # Count SELECT keywords, subtract 1 for the main query
    select_count = len(re.findall(r'\bSELECT\b', sql_upper))
    return max(0, select_count - 1)


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
        elif 'PACKAGE' in sql_upper:
            return 'CREATE PACKAGE'
        elif 'TRIGGER' in sql_upper:
            return 'CREATE TRIGGER'
        elif 'VIEW' in sql_upper:
            return 'CREATE VIEW'
        elif 'TABLE' in sql_upper:
            return 'CREATE TABLE'
        elif 'INDEX' in sql_upper:
            return 'CREATE INDEX'
        else:
            return 'CREATE OTHER'
    elif sql_upper.startswith('ALTER'):
        return 'ALTER'
    elif sql_upper.startswith('DROP'):
        return 'DROP'
    elif sql_upper.startswith('MERGE'):
        return 'MERGE'
    elif sql_upper.startswith('DECLARE') or sql_upper.startswith('BEGIN'):
        return 'PL/SQL BLOCK'
    elif sql_upper.startswith('GRANT') or sql_upper.startswith('REVOKE'):
        return 'DCL'
    elif sql_upper.startswith('TRUNCATE'):
        return 'TRUNCATE'
    else:
        return 'OTHER'


def count_keywords(sql_code):
    """Count SQL keywords for complexity estimation."""
    keywords = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
        'GROUP BY', 'ORDER BY', 'HAVING', 'UNION', 'INTERSECT', 'MINUS',
        'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'ALTER', 'DROP',
        'BEGIN', 'END', 'IF', 'THEN', 'ELSE', 'ELSIF', 'CASE', 'WHEN',
        'LOOP', 'FOR', 'WHILE', 'CURSOR', 'FETCH', 'EXCEPTION', 'RAISE',
        'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'GRANT', 'REVOKE',
        'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE',
        'DISTINCT', 'ALL', 'ANY', 'SOME'
    ]
    sql_upper = sql_code.upper()
    count = 0
    for kw in keywords:
        count += len(re.findall(r'\b' + kw.replace(' ', r'\s+') + r'\b', sql_upper))
    return count


def print_separator(char='=', width=80):
    print(char * width)


def print_header(title):
    print()
    print_separator()
    print(f"  {title}")
    print_separator()


def main():
    parser = argparse.ArgumentParser(
        description='Analyze extracted SQL scripts from SQLite database.',
        epilog='Example: python analyze_extracted_sql.py --db sql_queries.db'
    )
    parser.add_argument(
        '--db', required=True,
        help='Path to SQLite database file (e.g., sql_queries.db)'
    )
    args = parser.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"ERROR: Database file not found: {db_path}")
        return

    print(f"Analyzing: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all scripts
    cursor.execute('SELECT id, space_key, space_name, page_id, page_title, sql_code, line_count, sql_source FROM sql_scripts')
    scripts = cursor.fetchall()

    if not scripts:
        print("No SQL scripts found in database.")
        return

    # Analyze each script
    all_tables = Counter()
    all_schemas = Counter()
    sql_types = Counter()
    source_types = Counter()
    space_stats = defaultdict(lambda: {
        'count': 0, 'total_lines': 0, 'total_nesting': 0,
        'total_subqueries': 0, 'total_keywords': 0, 'tables': Counter()
    })

    script_details = []

    for row in scripts:
        script_id, space_key, space_name, page_id, page_title, sql_code, line_count, sql_source = row

        tables = get_table_references(sql_code)
        schemas = get_schema_references(sql_code)
        nesting = count_nesting_level(sql_code)
        subqueries = count_subqueries(sql_code)
        sql_type = get_sql_type(sql_code)
        keywords = count_keywords(sql_code)

        all_tables.update(tables)
        all_schemas.update(schemas)
        sql_types[sql_type] += 1
        source_types[sql_source] += 1

        space_stats[space_key]['count'] += 1
        space_stats[space_key]['total_lines'] += line_count or 0
        space_stats[space_key]['total_nesting'] += nesting
        space_stats[space_key]['total_subqueries'] += subqueries
        space_stats[space_key]['total_keywords'] += keywords
        space_stats[space_key]['tables'].update(tables)
        space_stats[space_key]['name'] = space_name

        script_details.append({
            'id': script_id,
            'space_key': space_key,
            'page_title': page_title,
            'sql_code': sql_code,
            'line_count': line_count or sql_code.count('\n') + 1,
            'nesting': nesting,
            'subqueries': subqueries,
            'keywords': keywords,
            'sql_type': sql_type,
            'tables': tables
        })

    total_scripts = len(scripts)
    total_lines = sum(s['line_count'] for s in script_details)
    total_nesting = sum(s['nesting'] for s in script_details)
    total_subqueries = sum(s['subqueries'] for s in script_details)
    total_keywords = sum(s['keywords'] for s in script_details)

    # === OVERALL STATISTICS ===
    print_header("OVERALL STATISTICS")
    print(f"  Total SQL scripts:        {total_scripts:,}")
    print(f"  Total lines of SQL:       {total_lines:,}")
    print(f"  Average lines per script: {total_lines / total_scripts:.1f}")
    print(f"  Unique tables referenced: {len(all_tables):,}")
    print(f"  Unique schemas:           {len(all_schemas):,}")
    print(f"  Total subqueries:         {total_subqueries:,}")
    print(f"  Avg nesting depth:        {total_nesting / total_scripts:.2f}")
    print(f"  Avg keywords per script:  {total_keywords / total_scripts:.1f}")

    # === SQL TYPES ===
    print_header("SQL STATEMENT TYPES")
    for sql_type, count in sql_types.most_common(15):
        pct = count / total_scripts * 100
        bar = '#' * int(pct / 2)
        print(f"  {sql_type:20} {count:6,} ({pct:5.1f}%) {bar}")

    # === SOURCE TYPES ===
    print_header("EXTRACTION SOURCES")
    for source, count in source_types.most_common():
        pct = count / total_scripts * 100
        print(f"  {source:25} {count:6,} ({pct:5.1f}%)")

    # === TOP TABLES ===
    print_header("TOP 20 MOST REFERENCED TABLES")
    for i, (table, count) in enumerate(all_tables.most_common(20), 1):
        print(f"  {i:2}. {table:40} {count:,} references")

    # === TOP SCHEMAS ===
    if all_schemas:
        print_header("TOP 10 SCHEMAS")
        for i, (schema, count) in enumerate(all_schemas.most_common(10), 1):
            print(f"  {i:2}. {schema:30} {count:,} references")

    # === LONGEST SCRIPTS ===
    print_header("TOP 10 LONGEST SCRIPTS")
    longest = sorted(script_details, key=lambda x: x['line_count'], reverse=True)[:10]
    for i, s in enumerate(longest, 1):
        title = (s['page_title'] or 'Untitled')[:40]
        print(f"  {i:2}. [{s['space_key']}] {title:40} {s['line_count']:,} lines")

    # === MOST NESTED ===
    print_header("TOP 10 MOST NESTED SCRIPTS (deepest parentheses)")
    most_nested = sorted(script_details, key=lambda x: x['nesting'], reverse=True)[:10]
    for i, s in enumerate(most_nested, 1):
        title = (s['page_title'] or 'Untitled')[:40]
        print(f"  {i:2}. [{s['space_key']}] {title:40} depth: {s['nesting']}")

    # === MOST SUBQUERIES ===
    print_header("TOP 10 SCRIPTS WITH MOST SUBQUERIES")
    most_subqueries = sorted(script_details, key=lambda x: x['subqueries'], reverse=True)[:10]
    for i, s in enumerate(most_subqueries, 1):
        if s['subqueries'] > 0:
            title = (s['page_title'] or 'Untitled')[:40]
            print(f"  {i:2}. [{s['space_key']}] {title:40} {s['subqueries']} subqueries")

    # === MOST COMPLEX (by keyword count) ===
    print_header("TOP 10 MOST COMPLEX SCRIPTS (by keyword density)")
    most_complex = sorted(script_details, key=lambda x: x['keywords'], reverse=True)[:10]
    for i, s in enumerate(most_complex, 1):
        title = (s['page_title'] or 'Untitled')[:40]
        print(f"  {i:2}. [{s['space_key']}] {title:40} {s['keywords']} keywords")

    # === PER-SPACE STATISTICS ===
    print_header("TOP 20 SPACES BY SQL SCRIPT COUNT")
    sorted_spaces = sorted(space_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:20]
    print(f"  {'Space':<20} {'Scripts':>8} {'Lines':>10} {'Avg Len':>8} {'Avg Nest':>9} {'Tables':>8}")
    print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*8} {'-'*9} {'-'*8}")
    for space_key, stats in sorted_spaces:
        avg_lines = stats['total_lines'] / stats['count'] if stats['count'] > 0 else 0
        avg_nesting = stats['total_nesting'] / stats['count'] if stats['count'] > 0 else 0
        unique_tables = len(stats['tables'])
        print(f"  {space_key:<20} {stats['count']:>8,} {stats['total_lines']:>10,} {avg_lines:>8.1f} {avg_nesting:>9.2f} {unique_tables:>8}")

    # === LINE COUNT DISTRIBUTION ===
    print_header("SCRIPT SIZE DISTRIBUTION")
    size_buckets = {'1-5 lines': 0, '6-20 lines': 0, '21-50 lines': 0,
                    '51-100 lines': 0, '101-500 lines': 0, '500+ lines': 0}
    for s in script_details:
        lines = s['line_count']
        if lines <= 5:
            size_buckets['1-5 lines'] += 1
        elif lines <= 20:
            size_buckets['6-20 lines'] += 1
        elif lines <= 50:
            size_buckets['21-50 lines'] += 1
        elif lines <= 100:
            size_buckets['51-100 lines'] += 1
        elif lines <= 500:
            size_buckets['101-500 lines'] += 1
        else:
            size_buckets['500+ lines'] += 1

    for bucket, count in size_buckets.items():
        pct = count / total_scripts * 100
        bar = '#' * int(pct / 2)
        print(f"  {bucket:15} {count:6,} ({pct:5.1f}%) {bar}")

    # === NESTING DISTRIBUTION ===
    print_header("NESTING DEPTH DISTRIBUTION")
    nest_buckets = Counter()
    for s in script_details:
        depth = s['nesting']
        if depth == 0:
            nest_buckets['No nesting (0)'] += 1
        elif depth <= 2:
            nest_buckets['Shallow (1-2)'] += 1
        elif depth <= 5:
            nest_buckets['Moderate (3-5)'] += 1
        elif depth <= 10:
            nest_buckets['Deep (6-10)'] += 1
        else:
            nest_buckets['Very deep (10+)'] += 1

    for bucket in ['No nesting (0)', 'Shallow (1-2)', 'Moderate (3-5)', 'Deep (6-10)', 'Very deep (10+)']:
        count = nest_buckets.get(bucket, 0)
        pct = count / total_scripts * 100
        bar = '#' * int(pct / 2)
        print(f"  {bucket:18} {count:6,} ({pct:5.1f}%) {bar}")

    conn.close()

    print()
    print_separator()
    print("  Analysis complete!")
    print_separator()


if __name__ == '__main__':
    main()
