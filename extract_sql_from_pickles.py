#!/usr/bin/env python3
"""
Extract SQL Scripts from Confluence Pickles

This script scans all pickled Confluence pages looking for SQL scripts (Oracle SQL, MS SQL, etc.)
and outputs the page name, SQL body, description/name if available, space name, and last modified date.

Note: Last editor name is NOT currently stored in the pickles. To add this, modify
sample_and_pickle_spaces.py to expand 'version.by' in the API call.

Usage:
    python extract_sql_from_pickles.py [--pickle-dir PICKLE_DIR] [--output OUTPUT_FILE]
    python extract_sql_from_pickles.py --sqlite sql_scripts.db
"""

import os
import pickle
import argparse
import re
import sqlite3
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup, Tag, NavigableString
from config_loader import load_data_settings

# SQL-related language identifiers (case-insensitive matching)
SQL_LANGUAGES = [
    'sql', 'plsql', 'pl/sql', 'oracle', 'oraclesql', 'oracle-sql',
    'tsql', 't-sql', 'mssql', 'ms-sql', 'sqlserver', 'sql-server',
    'mysql', 'postgresql', 'postgres', 'sqlite', 'db2', 'sybase',
    'transact-sql', 'ansi-sql', 'ddl', 'dml'
]

# Keywords that strongly indicate SQL content (for detection in unlabeled code blocks)
SQL_KEYWORDS = [
    r'\bSELECT\b', r'\bFROM\b', r'\bWHERE\b', r'\bINSERT\b', r'\bUPDATE\b',
    r'\bDELETE\b', r'\bCREATE\s+TABLE\b', r'\bALTER\s+TABLE\b', r'\bDROP\s+TABLE\b',
    r'\bCREATE\s+INDEX\b', r'\bCREATE\s+VIEW\b', r'\bCREATE\s+PROCEDURE\b',
    r'\bCREATE\s+FUNCTION\b', r'\bCREATE\s+TRIGGER\b', r'\bCREATE\s+PACKAGE\b',
    r'\bBEGIN\b', r'\bEND\b', r'\bDECLARE\b', r'\bEXECUTE\b', r'\bEXEC\b',
    r'\bGRANT\b', r'\bREVOKE\b', r'\bCOMMIT\b', r'\bROLLBACK\b',
    r'\bMERGE\s+INTO\b', r'\bTRUNCATE\b', r'\bJOIN\b', r'\bLEFT\s+JOIN\b',
    r'\bINNER\s+JOIN\b', r'\bOUTER\s+JOIN\b', r'\bUNION\b', r'\bGROUP\s+BY\b',
    r'\bORDER\s+BY\b', r'\bHAVING\b', r'\bDISTINCT\b', r'\bCOUNT\s*\(',
    r'\bSUM\s*\(', r'\bAVG\s*\(', r'\bMAX\s*\(', r'\bMIN\s*\(',
    # Oracle-specific
    r'\bPLS_INTEGER\b', r'\bVARCHAR2\b', r'\bNUMBER\b', r'\bSYSDATE\b',
    r'\bNVL\b', r'\bDECODE\b', r'\bROWNUM\b', r'\bROWID\b', r'\bDBMS_',
    r'\bUTL_', r'\bCURSOR\b', r'\bFETCH\b', r'\bOPEN\b', r'\bCLOSE\b',
    r'\bLOOP\b', r'\bEXIT\s+WHEN\b', r'\bFOR\s+.*\s+IN\b', r'\bEXCEPTION\b',
    r'\bRAISE\b', r'\bPRAGMA\b', r'\bBULK\s+COLLECT\b', r'\bFORALL\b',
]

# Minimum number of SQL keywords to consider something as SQL (for unlabeled blocks)
MIN_SQL_KEYWORDS = 2

# Patterns that indicate the START of a SQL statement (for plain text extraction)
# NOTE: BEGIN, LOOP, EXCEPTION etc. are NOT starters - they're continuations within PL/SQL
SQL_STATEMENT_STARTERS = [
    r'^\s*SELECT\b',
    r'^\s*INSERT\b',
    r'^\s*UPDATE\b',
    r'^\s*DELETE\b',
    r'^\s*CREATE\b',
    r'^\s*ALTER\b',
    r'^\s*DROP\b',
    r'^\s*TRUNCATE\b',
    r'^\s*GRANT\b',
    r'^\s*REVOKE\b',
    r'^\s*MERGE\b',
    r'^\s*DECLARE\b',
    r'^\s*EXEC(UTE)?\b',
    r'^\s*WITH\b',  # CTE
    r'^\s*CALL\b',
    r'^\s*COMMIT\b',
    r'^\s*ROLLBACK\b',
]

# Lines that likely continue a SQL statement
SQL_CONTINUATION_PATTERNS = [
    r'^\s*FROM\b',
    r'^\s*WHERE\b',
    r'^\s*AND\b',
    r'^\s*OR\b',
    r'^\s*JOIN\b',
    r'^\s*(LEFT|RIGHT|INNER|OUTER|CROSS)\s+JOIN\b',
    r'^\s*ON\b',
    r'^\s*GROUP\s+BY\b',
    r'^\s*ORDER\s+BY\b',
    r'^\s*HAVING\b',
    r'^\s*UNION\b',
    r'^\s*INTERSECT\b',
    r'^\s*MINUS\b',
    r'^\s*INTO\b',
    r'^\s*VALUES\b',
    r'^\s*SET\b',
    r'^\s*RETURNING\b',
    r'^\s*WHEN\b',
    r'^\s*THEN\b',
    r'^\s*ELSE\b',
    r'^\s*END\b',
    r'^\s*LOOP\b',
    r'^\s*EXIT\b',
    r'^\s*FETCH\b',
    r'^\s*OPEN\b',
    r'^\s*CLOSE\b',
    r'^\s*RETURN\b',
    r'^\s*RAISE\b',
    r'^\s*EXCEPTION\b',
    r'^\s*PRAGMA\b',
    r'^\s*--',  # SQL comment
    r'^\s*/\*',  # Block comment start
    r'^\s*\*',   # Block comment continuation
    r'^\s*\(',   # Subquery or list
    r'^\s*\)',   # Closing
    r'.*;\s*$',  # Ends with semicolon
    r'^\s*,',    # Continuation with comma
    r'^[^a-zA-Z]*$',  # Lines with only symbols/numbers (likely part of SQL)
]


def normalize_sql_for_hash(sql_code):
    """Normalize SQL for duplicate detection: uppercase, collapse whitespace."""
    normalized = sql_code.upper()
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = normalized.strip()
    return normalized


def hash_sql(sql_code):
    """Compute hash of normalized SQL for fast duplicate detection."""
    normalized = normalize_sql_for_hash(sql_code)
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()


def is_sql_language(language_str):
    """Check if a language string indicates SQL."""
    if not language_str:
        return False
    lang_lower = language_str.lower().strip()
    return any(sql_lang in lang_lower for sql_lang in SQL_LANGUAGES)


def looks_like_sql(text):
    """Heuristically determine if text looks like SQL code."""
    if not text or len(text.strip()) < 20:
        return False

    text_upper = text.upper()
    keyword_count = sum(1 for pattern in SQL_KEYWORDS if re.search(pattern, text_upper))
    return keyword_count >= MIN_SQL_KEYWORDS


def is_sql_starter_line(line):
    """Check if a line starts a SQL statement."""
    line_upper = line.upper()
    for pattern in SQL_STATEMENT_STARTERS:
        if re.match(pattern, line_upper, re.IGNORECASE):
            return True
    return False


def is_sql_continuation_line(line):
    """Check if a line is likely part of an ongoing SQL statement."""
    if not line.strip():
        return False  # Blank lines might end a statement
    line_upper = line.upper()
    for pattern in SQL_CONTINUATION_PATTERNS:
        if re.match(pattern, line_upper, re.IGNORECASE):
            return True
    # Also check if line contains SQL keywords mid-line
    keyword_count = sum(1 for pattern in SQL_KEYWORDS if re.search(pattern, line_upper))
    return keyword_count >= 1


def looks_like_prose(line):
    """Check if a line looks like natural language prose rather than code."""
    line = line.strip()
    if not line:
        return False

    line_upper = line.upper()

    # First, check if this line contains SQL keywords - if so, it's not prose
    sql_words_in_line = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
        'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
        'ORDER BY', 'GROUP BY', 'HAVING', 'UNION', 'INTO', 'VALUES', 'SET',
        'BEGIN', 'END', 'DECLARE', 'EXECUTE', 'EXEC', 'COMMIT', 'ROLLBACK',
        'GRANT', 'REVOKE', 'CURSOR', 'FETCH', 'EXCEPTION', 'RAISE', 'LOOP',
        'VARCHAR', 'NUMBER', 'INTEGER', 'SYSDATE', 'NVL', 'DECODE', 'ROWNUM',
        'PROCEDURE', 'FUNCTION', 'TRIGGER', 'PACKAGE', 'VIEW', 'INDEX',
        'PRIMARY KEY', 'FOREIGN KEY', 'CONSTRAINT', 'NOT NULL', 'DEFAULT',
        'AND', 'OR', 'ON', 'AS', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN',
    ]

    # Check for SQL keywords at start of line or as significant part
    for sql_word in sql_words_in_line:
        if line_upper.startswith(sql_word + ' ') or line_upper.startswith(sql_word + '\t'):
            return False
        if line_upper == sql_word:
            return False

    # Check if line looks like it's part of SQL (has SQL operators/patterns)
    if re.search(r'\s+(AND|OR)\s+\w+\s*(=|<|>|!=|<>|LIKE|IN|IS)', line_upper):
        return False
    if re.search(r'\bJOIN\b.*\bON\b', line_upper):
        return False
    if re.search(r'\b(COUNT|SUM|AVG|MAX|MIN)\s*\(', line_upper):
        return False

    # Common prose patterns (only match these if no SQL keywords found)
    prose_patterns = [
        r'^(This|That|The|Here|There|But|If|What|How|Why|Please|Note|See)\s+\w+\s+\w+',
        r'.*\s(is|are|was|were|has|have|had|will|would|could|should|can|may|must|shall)\s+(a|an|the|this|that|these|those)\s',
        r'^[A-Z][a-z]+\s+[a-z]+\s+[a-z]+\s+[a-z]+',  # Sentence-like pattern (4+ words)
        r'\.\s*$',  # Ends with period (not semicolon)
        r':\s*$',   # Ends with colon (heading/label)
        r'^\d+\.\s+[A-Z]',  # Numbered list item starting with capital
        r'^[-*]\s+[A-Z]',   # Bullet list item starting with capital
    ]

    for pattern in prose_patterns:
        if re.search(pattern, line, re.IGNORECASE):
            # Final check - make sure it's not a SQL comment
            if not line.strip().startswith('--'):
                return True

    return False


def is_plsql_block_start(line):
    """Check if this line starts a PL/SQL block (procedure, function, package, trigger)."""
    line_upper = line.strip().upper()
    patterns = [
        r'^CREATE\s+(OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION|PACKAGE|TRIGGER|TYPE)\b',
        r'^DECLARE\b',
    ]
    for pattern in patterns:
        if re.match(pattern, line_upper):
            return True
    return False


def is_plsql_block_end(line):
    """Check if this line ends a PL/SQL block."""
    line_stripped = line.strip()
    # PL/SQL blocks typically end with END; followed by / on next line
    # Or just END; for anonymous blocks
    return line_stripped == '/' or line_stripped.upper() in ('END;', 'END')


def extract_sql_blocks_from_text(text):
    """
    Extract SQL blocks from plain text by looking for SQL statement patterns.

    Returns a list of extracted SQL blocks (strings).
    """
    if not text:
        return []

    lines = text.split('\n')
    sql_blocks = []
    current_block = []
    in_sql_block = False
    in_plsql_block = False  # Track if we're inside a PL/SQL block
    blank_line_count = 0
    prev_line_ended_with_comma = False
    prev_line_ended_with_semicolon = False
    open_parens = 0

    for i, line in enumerate(lines):
        line_stripped = line.strip()

        # Track parentheses balance
        if in_sql_block:
            open_parens += line_stripped.count('(') - line_stripped.count(')')
            open_parens = max(0, open_parens)  # Don't go negative

        # Check for PL/SQL block start
        if is_plsql_block_start(line_stripped):
            # Start a new PL/SQL block (save previous if valid)
            if current_block and looks_like_sql('\n'.join(current_block)):
                sql_blocks.append('\n'.join(current_block))
            current_block = [line]
            in_sql_block = True
            in_plsql_block = True
            blank_line_count = 0
            prev_line_ended_with_comma = line_stripped.endswith(',')
            prev_line_ended_with_semicolon = line_stripped.endswith(';')
            open_parens = line_stripped.count('(') - line_stripped.count(')')

        # Inside a PL/SQL block - don't start new blocks until END; /
        elif in_plsql_block:
            current_block.append(line)
            blank_line_count = 0 if line_stripped else blank_line_count + 1

            # Check for end of PL/SQL block
            if is_plsql_block_end(line_stripped):
                # Look ahead - if next non-blank line is '/', include it
                if line_stripped.upper() in ('END;', 'END'):
                    # Check if there's a / coming
                    for j in range(i + 1, min(i + 3, len(lines))):
                        next_line = lines[j].strip()
                        if next_line == '/':
                            continue  # Will be added in next iteration
                        elif next_line:
                            break  # Non-slash content, end here
                    else:
                        # No more lines or only blank/slash
                        pass
                elif line_stripped == '/':
                    # This is the end
                    if current_block and looks_like_sql('\n'.join(current_block)):
                        sql_blocks.append('\n'.join(current_block))
                    current_block = []
                    in_sql_block = False
                    in_plsql_block = False
                    open_parens = 0

        elif is_sql_starter_line(line_stripped):
            # Start a new SQL block (save previous if valid)
            if current_block and looks_like_sql('\n'.join(current_block)):
                sql_blocks.append('\n'.join(current_block))
            current_block = [line]
            in_sql_block = True
            blank_line_count = 0
            prev_line_ended_with_comma = line_stripped.endswith(',')
            prev_line_ended_with_semicolon = line_stripped.endswith(';')
            open_parens = line_stripped.count('(') - line_stripped.count(')')

        elif in_sql_block:
            if not line_stripped:
                # Blank line - might end the block
                blank_line_count += 1
                # End block after semicolon followed by blank line (unless in PL/SQL block)
                if prev_line_ended_with_semicolon and blank_line_count >= 1 and open_parens <= 0:
                    # Check if we might be in a PL/SQL block (has BEGIN but no END yet)
                    block_text = '\n'.join(current_block).upper()
                    if 'BEGIN' in block_text and 'END' not in block_text:
                        # Still in PL/SQL block, continue
                        current_block.append(line)
                    else:
                        if current_block and looks_like_sql('\n'.join(current_block)):
                            sql_blocks.append('\n'.join(current_block))
                        current_block = []
                        in_sql_block = False
                        open_parens = 0
                elif blank_line_count >= 2 and open_parens <= 0 and not prev_line_ended_with_comma:
                    # Two blank lines = end of SQL block
                    if current_block and looks_like_sql('\n'.join(current_block)):
                        sql_blocks.append('\n'.join(current_block))
                    current_block = []
                    in_sql_block = False
                    open_parens = 0
                else:
                    current_block.append(line)
            elif looks_like_prose(line_stripped):
                # This looks like natural language - end the SQL block
                if current_block and looks_like_sql('\n'.join(current_block)):
                    sql_blocks.append('\n'.join(current_block))
                current_block = []
                in_sql_block = False
                blank_line_count = 0
                open_parens = 0
            else:
                # Check various conditions for continuing the SQL block
                should_continue = False

                # Explicit SQL patterns
                if is_sql_continuation_line(line_stripped):
                    should_continue = True
                # Ends with semicolon (SQL terminator) - but might continue for PL/SQL
                elif line_stripped.endswith(';'):
                    should_continue = True
                # Ends with comma (list continuation)
                elif line_stripped.endswith(','):
                    should_continue = True
                # Previous line ended with comma - this is likely a list item
                elif prev_line_ended_with_comma:
                    should_continue = True
                # Inside parentheses
                elif open_parens > 0:
                    should_continue = True
                # Contains SQL keywords
                elif looks_like_sql(line_stripped):
                    should_continue = True
                # Indented lines (likely part of SQL structure)
                elif (line.startswith('    ') or line.startswith('\t')) and len(line_stripped) < 100:
                    should_continue = True
                # PL/SQL block terminators
                elif line_stripped in ('/', 'END;', 'END', 'BEGIN', 'EXCEPTION'):
                    should_continue = True
                # Short identifier-like lines (column names, etc.)
                elif len(line_stripped) < 60 and re.match(r'^[\w\s,\.\(\)\'\"_\-\*:=]+$', line_stripped):
                    # But not if it looks like prose
                    if not looks_like_prose(line_stripped):
                        should_continue = True

                if should_continue:
                    current_block.append(line)
                    blank_line_count = 0
                    prev_line_ended_with_comma = line_stripped.endswith(',')
                    prev_line_ended_with_semicolon = line_stripped.endswith(';')
                else:
                    # Check if this might be a terminator line for PL/SQL
                    if line_stripped == '/' or line_stripped.upper() == 'END;' or line_stripped.upper() == 'END':
                        current_block.append(line)
                        if current_block and looks_like_sql('\n'.join(current_block)):
                            sql_blocks.append('\n'.join(current_block))
                        current_block = []
                        in_sql_block = False
                        open_parens = 0
                    else:
                        # Likely end of SQL block
                        if current_block and looks_like_sql('\n'.join(current_block)):
                            sql_blocks.append('\n'.join(current_block))
                        current_block = []
                        in_sql_block = False
                        blank_line_count = 0
                        open_parens = 0

    # Don't forget the last block
    if current_block and looks_like_sql('\n'.join(current_block)):
        sql_blocks.append('\n'.join(current_block))

    return sql_blocks


def get_context_before_position(text, position, max_chars=200):
    """Get text before a position to use as context/description."""
    if position <= 0:
        return ''

    # Look backwards for a heading or meaningful context
    before_text = text[:position]
    lines = before_text.split('\n')

    # Get last few non-empty lines before the SQL
    context_lines = []
    for line in reversed(lines[-5:]):
        line = line.strip()
        if line and len(line) < 200:
            # Skip if it looks like SQL
            if not is_sql_starter_line(line) and not looks_like_sql(line):
                context_lines.insert(0, line)
                if len(' | '.join(context_lines)) > max_chars:
                    break

    return ' | '.join(context_lines) if context_lines else ''


def extract_text_from_element(element):
    """Extract text content from a BeautifulSoup element, handling CDATA and nested content."""
    if element is None:
        return ""

    # Check for CDATA (plain-text-body)
    if hasattr(element, 'string') and element.string:
        return str(element.string).strip()

    # Get all text content
    text = element.get_text(separator='\n', strip=True)
    return text


def extract_sql_from_code_macro(macro):
    """Extract SQL code and metadata from a Confluence code macro."""
    result = {
        'sql_code': '',
        'language': '',
        'title': '',
        'description': ''
    }

    # Get language parameter
    lang_param = macro.find('ac:parameter', attrs={'ac:name': 'language'})
    if lang_param:
        result['language'] = lang_param.get_text(strip=True)

    # Get title parameter
    title_param = macro.find('ac:parameter', attrs={'ac:name': 'title'})
    if title_param:
        result['title'] = title_param.get_text(strip=True)

    # Get the code body - try plain-text-body first (most common for code macro)
    plain_text_body = macro.find('ac:plain-text-body')
    if plain_text_body:
        result['sql_code'] = extract_text_from_element(plain_text_body)
    else:
        # Try rich-text-body
        rich_text_body = macro.find('ac:rich-text-body')
        if rich_text_body:
            result['sql_code'] = extract_text_from_element(rich_text_body)

    return result


def extract_sql_from_preformatted(element):
    """Extract SQL from pre or code tags."""
    return {
        'sql_code': element.get_text(separator='\n', strip=True),
        'language': '',
        'title': '',
        'description': ''
    }


def find_nearby_context(element, soup):
    """Try to find a description or context near the SQL element."""
    context_parts = []

    # Look at previous siblings for context (headings, paragraphs)
    for sibling in element.find_previous_siblings()[:3]:
        if isinstance(sibling, NavigableString):
            continue
        if sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']:
            text = sibling.get_text(strip=True)
            if text and len(text) < 200:  # Reasonable context length
                context_parts.insert(0, text)

    return ' | '.join(context_parts) if context_parts else ''


def extract_sql_from_table_cell(cell):
    """Extract SQL that might be in a table cell."""
    sql_blocks = []

    # Check for code macros within cell
    code_macros = cell.find_all(lambda tag: tag.name and tag.has_attr('ac:name') and
                                 tag.get('ac:name', '').lower() == 'code')
    for macro in code_macros:
        extracted = extract_sql_from_code_macro(macro)
        if extracted['sql_code']:
            # Check if it's labeled as SQL or looks like SQL
            if is_sql_language(extracted['language']) or looks_like_sql(extracted['sql_code']):
                sql_blocks.append(extracted)

    # Check for pre/code tags
    for pre in cell.find_all(['pre', 'code']):
        text = pre.get_text(separator='\n', strip=True)
        if text and looks_like_sql(text):
            sql_blocks.append({
                'sql_code': text,
                'language': 'detected',
                'title': '',
                'description': ''
            })

    # Check raw cell text if no structured code found
    if not sql_blocks:
        cell_text = cell.get_text(separator='\n', strip=True)
        if cell_text and looks_like_sql(cell_text):
            sql_blocks.append({
                'sql_code': cell_text,
                'language': 'detected-from-cell',
                'title': '',
                'description': ''
            })

    return sql_blocks


def extract_all_sql_from_page(html_content, page_title=''):
    """
    Extract all SQL scripts from a page's HTML content.

    Returns a list of dicts with keys: sql_code, language, title, description, source
    """
    if not html_content:
        return []

    sql_scripts = []
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Find code macros (ac:structured-macro with ac:name="code")
    try:
        code_macros = soup.find_all(lambda tag: tag and hasattr(tag, 'has_attr') and
                                     tag.has_attr('ac:name') and
                                     tag.get('ac:name', '').lower() == 'code')
    except Exception:
        code_macros = []

    for macro in code_macros:
        try:
            extracted = extract_sql_from_code_macro(macro)
            if extracted['sql_code']:
                # Check if explicitly labeled as SQL
                if is_sql_language(extracted['language']):
                    extracted['source'] = 'code-macro-labeled'
                    extracted['description'] = find_nearby_context(macro, soup)
                    sql_scripts.append(extracted)
                # Or if it looks like SQL even without label
                elif looks_like_sql(extracted['sql_code']):
                    extracted['source'] = 'code-macro-detected'
                    extracted['language'] = extracted['language'] or 'detected'
                    extracted['description'] = find_nearby_context(macro, soup)
                    sql_scripts.append(extracted)
        except Exception as e:
            continue

    # 2. Find noformat macros (often used for SQL too)
    try:
        noformat_macros = soup.find_all(lambda tag: tag and hasattr(tag, 'has_attr') and
                                         tag.has_attr('ac:name') and
                                         tag.get('ac:name', '').lower() == 'noformat')
    except Exception:
        noformat_macros = []

    for macro in noformat_macros:
        try:
            plain_text = macro.find('ac:plain-text-body')
            if plain_text:
                text = extract_text_from_element(plain_text)
                if text and looks_like_sql(text):
                    sql_scripts.append({
                        'sql_code': text,
                        'language': 'detected',
                        'title': '',
                        'description': find_nearby_context(macro, soup),
                        'source': 'noformat-macro'
                    })
        except Exception:
            continue

    # 3. Find standalone pre tags (outside macros)
    for pre in soup.find_all('pre'):
        # Skip if inside a macro we've already processed
        if pre.find_parent(lambda tag: tag and hasattr(tag, 'has_attr') and
                          tag.has_attr('ac:name')):
            continue

        text = pre.get_text(separator='\n', strip=True)
        if text and looks_like_sql(text):
            sql_scripts.append({
                'sql_code': text,
                'language': 'detected',
                'title': '',
                'description': find_nearby_context(pre, soup),
                'source': 'pre-tag'
            })

    # 4. Check tables for SQL content (common pattern: tables with script name + SQL)
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])

            # Try to get context from first cell if there are multiple
            row_context = ''
            if len(cells) >= 2:
                first_cell_text = cells[0].get_text(strip=True)
                if first_cell_text and len(first_cell_text) < 200:
                    row_context = first_cell_text

            for cell in cells:
                cell_sql = extract_sql_from_table_cell(cell)
                for sql_item in cell_sql:
                    sql_item['source'] = 'table-cell'
                    if row_context and not sql_item['description']:
                        sql_item['description'] = row_context
                    sql_scripts.append(sql_item)

    # 5. CRITICAL: Scan ALL plain text content for SQL patterns
    # This catches SQL that's not in any structured element (just written as text)
    # First, get the full text content of the page
    full_text = soup.get_text(separator='\n')

    # Track what SQL we've already found (to avoid duplicates)
    existing_sql_normalized = set()
    for script in sql_scripts:
        # Normalize: remove whitespace for comparison
        normalized = re.sub(r'\s+', ' ', script['sql_code'].strip().upper())
        existing_sql_normalized.add(normalized)

    # Extract SQL blocks from plain text
    plain_text_sql_blocks = extract_sql_blocks_from_text(full_text)

    for sql_block in plain_text_sql_blocks:
        # Check if this is a duplicate of something we already found
        normalized = re.sub(r'\s+', ' ', sql_block.strip().upper())
        if normalized in existing_sql_normalized:
            continue

        # Find position in text for context
        block_pos = full_text.find(sql_block[:50]) if len(sql_block) >= 50 else full_text.find(sql_block)
        context = get_context_before_position(full_text, block_pos) if block_pos > 0 else ''

        sql_scripts.append({
            'sql_code': sql_block,
            'language': 'detected-plain-text',
            'title': '',
            'description': context,
            'source': 'plain-text-scan'
        })
        existing_sql_normalized.add(normalized)

    return sql_scripts


def format_datetime(iso_string):
    """Format ISO 8601 datetime string to human-readable format."""
    if not iso_string:
        return 'Unknown date'
    try:
        # Handle Confluence format: 2024-01-15T10:30:00.000Z
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return iso_string


def init_sqlite_db(db_path):
    """Initialize SQLite database with schema for SQL scripts."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create main table for SQL scripts
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sql_scripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            space_key TEXT NOT NULL,
            space_name TEXT,
            page_id TEXT NOT NULL,
            page_title TEXT,
            last_modified TEXT,
            last_editor TEXT,
            sql_language TEXT,
            sql_title TEXT,
            sql_description TEXT,
            sql_source TEXT,
            sql_code TEXT NOT NULL,
            line_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create indexes for common queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_space_key ON sql_scripts(space_key)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_page_id ON sql_scripts(page_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sql_language ON sql_scripts(sql_language)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sql_source ON sql_scripts(sql_source)')

    # Create a summary view
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS sql_summary AS
        SELECT
            space_key,
            space_name,
            COUNT(*) as script_count,
            COUNT(DISTINCT page_id) as pages_with_sql,
            SUM(line_count) as total_lines
        FROM sql_scripts
        GROUP BY space_key, space_name
        ORDER BY script_count DESC
    ''')

    conn.commit()
    return conn


def insert_sql_to_db(conn, result):
    """Insert a single SQL result into the database."""
    cursor = conn.cursor()
    line_count = result['sql_code'].count('\n') + 1

    cursor.execute('''
        INSERT INTO sql_scripts (
            space_key, space_name, page_id, page_title, last_modified,
            last_editor, sql_language, sql_title, sql_description,
            sql_source, sql_code, line_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        result['space_key'],
        result['space_name'],
        result['page_id'],
        result['page_title'],
        result['last_modified'],
        result['last_editor'],
        result['sql_language'],
        result['sql_title'],
        result['sql_description'],
        result['sql_source'],
        result['sql_code'],
        line_count
    ))


def format_sql_result(result, script_num):
    """Format a single SQL result for output."""
    lines = []
    lines.append("=" * 80)
    lines.append(f"SQL SCRIPT #{script_num}")
    lines.append("=" * 80)
    lines.append(f"Space Key:      {result['space_key']}")
    lines.append(f"Space Name:     {result['space_name']}")
    lines.append(f"Page Title:     {result['page_title']}")
    lines.append(f"Page ID:        {result['page_id']}")
    lines.append(f"Last Modified:  {result['last_modified']}")
    lines.append(f"Last Editor:    {result['last_editor']}")
    lines.append(f"SQL Language:   {result['sql_language']}")
    lines.append(f"Script Title:   {result['sql_title'] or 'N/A'}")
    lines.append(f"Description:    {result['sql_description'] or 'N/A'}")
    lines.append(f"Source:         {result['sql_source']}")
    lines.append("-" * 40 + " SQL CODE " + "-" * 40)
    lines.append(result['sql_code'])
    lines.append("")
    return '\n'.join(lines)


def process_pickle_file_streaming(pickle_path, output_file, script_counter, min_lines=1, db_conn=None, seen_hashes=None, confluence_base_url=None):
    """
    Process a single pickle file and output SQL scripts in real-time.

    Args:
        pickle_path: Path to pickle file
        output_file: File handle for text output (or None for stdout)
        script_counter: List with single int for counting scripts
        min_lines: Minimum lines of SQL to include
        db_conn: SQLite connection (or None for text output)
        seen_hashes: Set of already seen SQL hashes (for duplicate detection)
        confluence_base_url: Base URL for Confluence (for generating page links)

    Returns: (page_count, sql_count, duplicate_count, pages_with_sql_set)
    """
    pages_with_sql = set()
    sql_count = 0
    duplicate_count = 0

    if seen_hashes is None:
        seen_hashes = set()

    try:
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
    except Exception as e:
        print(f"  ERROR loading pickle {pickle_path}: {e}")
        return 0, 0, 0, pages_with_sql

    space_key = data.get('space_key', os.path.basename(pickle_path).replace('.pkl', ''))
    space_name = data.get('name') or data.get('space_name') or space_key
    pages = data.get('sampled_pages', [])
    page_count = len(pages)

    for page in pages:
        page_id = page.get('id', 'unknown')
        page_title = page.get('title', 'Untitled')
        body = page.get('body', '')
        updated = page.get('updated', '')

        if not body:
            continue

        sql_scripts = extract_all_sql_from_page(body, page_title)

        # Flag pages with many SQL statements for manual review (potential double-counting)
        if len(sql_scripts) > 5:
            page_link = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}" if confluence_base_url else f"pageId={page_id}"
            print(f"  ** HIGH SQL COUNT: {len(sql_scripts)} statements in '{page_title[:50]}' - {page_link}")

        if sql_scripts:
            pages_with_sql.add((space_key, page_id))

        for script in sql_scripts:
            # Filter by minimum lines
            if min_lines > 1 and script['sql_code'].count('\n') + 1 < min_lines:
                continue

            # Check for duplicates using hash
            sql_hash = hash_sql(script['sql_code'])
            if sql_hash in seen_hashes:
                duplicate_count += 1
                continue
            seen_hashes.add(sql_hash)

            script_counter[0] += 1
            sql_count += 1

            result = {
                'space_key': space_key,
                'space_name': space_name,
                'page_id': page_id,
                'page_title': page_title,
                'last_modified': format_datetime(updated),
                'last_editor': 'Not available in pickle data',
                'sql_language': script.get('language', ''),
                'sql_title': script.get('title', ''),
                'sql_description': script.get('description', ''),
                'sql_source': script.get('source', ''),
                'sql_code': script.get('sql_code', '')
            }

            # Output to SQLite or text
            if db_conn:
                insert_sql_to_db(db_conn, result)
            elif output_file:
                formatted = format_sql_result(result, script_counter[0])
                output_file.write(formatted + '\n')
                output_file.flush()  # Ensure it's written immediately
            else:
                formatted = format_sql_result(result, script_counter[0])
                print(formatted)

    # Commit after each pickle file for SQLite
    if db_conn:
        db_conn.commit()

    return page_count, sql_count, duplicate_count, pages_with_sql


def main():
    parser = argparse.ArgumentParser(description='Extract SQL scripts from Confluence pickles')
    parser.add_argument('--pickle-dir', '-d', help='Directory containing pickle files')
    parser.add_argument('--output', '-o', help='Output text file path (default: stdout)')
    parser.add_argument('--sqlite', '--db', help='Output to SQLite database file')
    parser.add_argument('--summary', '-s', action='store_true',
                        help='Print summary statistics only')
    parser.add_argument('--min-lines', type=int, default=1,
                        help='Minimum lines of SQL to include (default: 1)')
    args = parser.parse_args()

    # Determine pickle directory
    if args.pickle_dir:
        pickle_dir = args.pickle_dir
    else:
        try:
            data_settings = load_data_settings()
            pickle_dir = data_settings.get('pickle_dir', 'temp')
        except Exception:
            pickle_dir = 'temp'

    if not os.path.isdir(pickle_dir):
        print(f"ERROR: Pickle directory not found: {pickle_dir}")
        print("Please specify a valid directory with --pickle-dir or configure pickle_dir in settings.ini")
        return

    # Find all pickle files
    pickle_files = sorted([f for f in os.listdir(pickle_dir) if f.endswith('.pkl')])

    if not pickle_files:
        print(f"No pickle files found in {pickle_dir}")
        return

    print(f"Found {len(pickle_files)} pickle files in {pickle_dir}")
    print("=" * 80)

    # Load Confluence base URL for generating page links
    confluence_base_url = None
    try:
        from config_loader import load_confluence_settings
        conf_settings = load_confluence_settings()
        confluence_base_url = conf_settings.get('base_url', '').rstrip('/')
    except Exception:
        pass

    # Open output file if specified
    output_file = None
    if args.output and not args.summary:
        output_file = open(args.output, 'w', encoding='utf-8')

    # Open SQLite database if specified
    db_conn = None
    if args.sqlite and not args.summary:
        db_conn = init_sqlite_db(args.sqlite)
        print(f"Writing to SQLite database: {args.sqlite}")

    total_pages = 0
    total_sql_found = 0
    total_duplicates = 0
    all_pages_with_sql = set()
    script_counter = [0]  # Use list to allow mutation in nested function
    seen_hashes = set()   # Track seen SQL hashes for duplicate detection

    try:
        for i, pkl_file in enumerate(pickle_files, 1):
            pkl_path = os.path.join(pickle_dir, pkl_file)

            if args.summary:
                # Summary mode - just count, don't output SQL
                try:
                    with open(pkl_path, 'rb') as f:
                        data = pickle.load(f)
                    page_count = len(data.get('sampled_pages', []))
                    total_pages += page_count

                    # Still need to count SQL for summary
                    space_key = data.get('space_key', pkl_file.replace('.pkl', ''))
                    pages = data.get('sampled_pages', [])
                    sql_in_space = 0
                    for page in pages:
                        body = page.get('body', '')
                        if body:
                            sql_scripts = extract_all_sql_from_page(body, page.get('title', ''))
                            if args.min_lines > 1:
                                sql_scripts = [s for s in sql_scripts if s['sql_code'].count('\n') + 1 >= args.min_lines]
                            if sql_scripts:
                                all_pages_with_sql.add((space_key, page.get('id', 'unknown')))
                                sql_in_space += len(sql_scripts)

                    total_sql_found += sql_in_space
                    print(f"[{i}/{len(pickle_files)}] {pkl_file}: {sql_in_space} SQL in {page_count} pages | "
                          f"Running total: {total_pages:,} pages examined, {total_sql_found:,} SQL found")
                except Exception as e:
                    print(f"[{i}/{len(pickle_files)}] {pkl_file}: ERROR - {e}")
            else:
                # Streaming mode - output SQL as found
                try:
                    with open(pkl_path, 'rb') as f:
                        data = pickle.load(f)
                    page_count_check = len(data.get('sampled_pages', []))
                except Exception as e:
                    print(f"[{i}/{len(pickle_files)}] {pkl_file}: ERROR - {e}")
                    continue

                page_count, sql_count, dup_count, pages_with_sql = process_pickle_file_streaming(
                    pkl_path, output_file, script_counter, args.min_lines, db_conn, seen_hashes, confluence_base_url
                )

                total_pages += page_count
                total_sql_found += sql_count
                total_duplicates += dup_count
                all_pages_with_sql.update(pages_with_sql)

                # Print progress after each pickle file
                dup_info = f", {dup_count} dups" if dup_count > 0 else ""
                print(f"[{i}/{len(pickle_files)}] {pkl_file}: {sql_count} SQL{dup_info} in {page_count} pages | "
                      f"Running total: {total_pages:,} pages, {total_sql_found:,} SQL, {total_duplicates:,} dups skipped")

    finally:
        if output_file:
            output_file.close()
        if db_conn:
            db_conn.close()

    print("=" * 80)
    print(f"\nSUMMARY:")
    print(f"  Total pickle files processed: {len(pickle_files)}")
    print(f"  Total pages scanned: {total_pages:,}")
    print(f"  Pages containing SQL: {len(all_pages_with_sql):,}")
    print(f"  Total SQL scripts found: {total_sql_found:,}")
    print(f"  Duplicates skipped: {total_duplicates:,}")

    if args.output and not args.summary:
        print(f"\nText results written to: {args.output}")
    if args.sqlite and not args.summary:
        print(f"\nSQLite database written to: {args.sqlite}")
        print("  Query examples:")
        print("    sqlite3 {} \"SELECT COUNT(*) FROM sql_scripts\"".format(args.sqlite))
        print("    sqlite3 {} \"SELECT * FROM sql_summary\"".format(args.sqlite))
        print("    sqlite3 {} \"SELECT page_title, sql_code FROM sql_scripts WHERE space_key='MYSPACE'\"".format(args.sqlite))


if __name__ == '__main__':
    main()
