#!/usr/bin/env python3
"""
Simple SQL Browser for Extracted Scripts

A minimal terminal-based browser to navigate through extracted SQL scripts.
Use arrow keys or n/p to navigate, q to quit.

Usage:
    python browse_extracted_sql.py [database.db]
"""

import sqlite3
import sys
import os

# Try to find database
DB_PATHS = ['sql_scripts.db', 'temp/sql_scripts.db', 'extracted_sql.db']


def find_database():
    for path in DB_PATHS:
        if os.path.exists(path):
            return path
    return None


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def get_total_count(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM sql_scripts')
    return cursor.fetchone()[0]


def get_script(conn, index):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, space_key, space_name, page_id, page_title,
               last_modified, sql_language, sql_title, sql_description,
               sql_source, sql_code, line_count
        FROM sql_scripts
        ORDER BY id
        LIMIT 1 OFFSET ?
    ''', (index,))
    return cursor.fetchone()


def display_script(script, index, total):
    if not script:
        print("No script found.")
        return

    (id_, space_key, space_name, page_id, page_title,
     last_modified, sql_language, sql_title, sql_description,
     sql_source, sql_code, line_count) = script

    clear_screen()
    print("=" * 80)
    print(f"  SQL SCRIPT {index + 1} of {total}  (ID: {id_})")
    print("=" * 80)
    print(f"  Space:       {space_key} ({space_name or 'N/A'})")
    print(f"  Page:        {page_title or 'Untitled'}")
    print(f"  Page ID:     {page_id}")
    print(f"  Modified:    {last_modified}")
    print(f"  Language:    {sql_language or 'N/A'}")
    print(f"  Title:       {sql_title or 'N/A'}")
    print(f"  Description: {sql_description or 'N/A'}")
    print(f"  Source:      {sql_source}")
    print(f"  Lines:       {line_count}")
    print("-" * 80)

    # Show SQL (truncate if too long for terminal)
    lines = sql_code.split('\n') if sql_code else []
    max_lines = 30
    if len(lines) > max_lines:
        for line in lines[:max_lines]:
            print(line[:120])
        print(f"\n  ... ({len(lines) - max_lines} more lines)")
    else:
        for line in lines:
            print(line[:120])

    print("-" * 80)
    print("  [N]ext  [P]rev  [F]irst  [L]ast  [G]oto  [S]earch  [Q]uit")
    print("=" * 80)


def search_scripts(conn, search_term):
    """Search for scripts containing the term."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id FROM sql_scripts
        WHERE sql_code LIKE ? OR page_title LIKE ? OR space_key LIKE ?
        ORDER BY id
    ''', (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
    results = cursor.fetchall()
    return [r[0] for r in results]


def get_index_for_id(conn, script_id):
    """Get the 0-based index for a given script ID."""
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM sql_scripts WHERE id < ?', (script_id,))
    return cursor.fetchone()[0]


def main():
    # Find database
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = find_database()

    if not db_path or not os.path.exists(db_path):
        print("ERROR: Could not find SQL scripts database.")
        print(f"Looked in: {', '.join(DB_PATHS)}")
        print("\nUsage: python browse_extracted_sql.py [database.db]")
        print("\nRun extract_sql_from_pickles.py --sqlite sql_scripts.db first.")
        return

    conn = sqlite3.connect(db_path)
    total = get_total_count(conn)

    if total == 0:
        print("No SQL scripts found in database.")
        conn.close()
        return

    print(f"Loaded {total} SQL scripts from {db_path}")

    current_index = 0
    search_results = None
    search_idx = 0

    while True:
        script = get_script(conn, current_index)
        display_script(script, current_index, total)

        if search_results:
            print(f"  Search results: {search_idx + 1} of {len(search_results)}")

        try:
            cmd = input("\n> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if cmd in ('q', 'quit', 'exit'):
            break
        elif cmd in ('n', 'next', ''):
            if search_results:
                search_idx = min(search_idx + 1, len(search_results) - 1)
                current_index = get_index_for_id(conn, search_results[search_idx])
            else:
                current_index = min(current_index + 1, total - 1)
        elif cmd in ('p', 'prev', 'previous'):
            if search_results:
                search_idx = max(search_idx - 1, 0)
                current_index = get_index_for_id(conn, search_results[search_idx])
            else:
                current_index = max(current_index - 1, 0)
        elif cmd in ('f', 'first'):
            current_index = 0
            search_results = None
        elif cmd in ('l', 'last'):
            current_index = total - 1
            search_results = None
        elif cmd.startswith('g') or cmd.isdigit():
            # Goto specific number
            try:
                if cmd.isdigit():
                    num = int(cmd)
                else:
                    num = int(cmd[1:].strip())
                if 1 <= num <= total:
                    current_index = num - 1
                    search_results = None
                else:
                    print(f"Invalid number. Enter 1-{total}")
                    input("Press Enter...")
            except ValueError:
                print("Invalid number")
                input("Press Enter...")
        elif cmd.startswith('s') or cmd.startswith('/'):
            # Search
            if cmd.startswith('/'):
                term = cmd[1:].strip()
            else:
                term = cmd[1:].strip() if len(cmd) > 1 else input("Search for: ").strip()

            if term:
                results = search_scripts(conn, term)
                if results:
                    search_results = results
                    search_idx = 0
                    current_index = get_index_for_id(conn, search_results[0])
                    print(f"Found {len(results)} matches")
                else:
                    print("No matches found")
                    search_results = None
                input("Press Enter...")
        elif cmd == 'c':
            # Clear search
            search_results = None

    conn.close()
    clear_screen()
    print("Goodbye!")


if __name__ == '__main__':
    main()
