#!/usr/bin/env python3
"""Test indexing a single page to diagnose failures.

Usage:
    python test_index_single_page.py <space_key> <page_id>
    python test_index_single_page.py DAENDATA 12345678

    # List all page IDs in a space:
    python test_index_single_page.py <space_key> --list

    # Try indexing every page in a space (find the ones that blow up):
    python test_index_single_page.py <space_key> --all
"""

import sys
import os
import pickle
import logging
import traceback
import time
from pathlib import Path

# Ensure we can import sibling modules
sys.path.insert(0, os.path.dirname(__file__))

from converters import html_to_text
from indexer import ConfluenceIndexer, MAX_BODY_HTML_BYTES

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_pickle(space_key: str) -> dict:
    """Load a single pickle file by space key."""
    pickle_dir = os.path.join(os.path.dirname(__file__), '..', 'temp')
    pickle_path = os.path.join(pickle_dir, f'{space_key}.pkl')

    if not os.path.exists(pickle_path):
        logger.error(f"Pickle not found: {pickle_path}")
        sys.exit(1)

    logger.info(f"Loading pickle: {pickle_path}")
    with open(pickle_path, 'rb') as f:
        data = pickle.load(f)

    pages = data.get('sampled_pages', [])
    logger.info(f"Space {space_key}: {len(pages)} pages in pickle")
    return data


def find_page(data: dict, page_id: str) -> dict:
    """Find a specific page by ID in the pickle data."""
    page_id_str = str(page_id)
    for page in data.get('sampled_pages', []):
        if str(page.get('id', '')) == page_id_str:
            return page
    return None


def diagnose_page(space_key: str, page: dict) -> dict:
    """Run the full indexing pipeline on a single page, reporting each step."""
    page_id = page.get('id', '?')
    title = page.get('title', '(no title)')
    result = {
        'page_id': page_id,
        'title': title,
        'steps': {},
        'success': False,
        'error': None,
    }

    print(f"\n{'='*70}")
    print(f"Page ID: {page_id}")
    print(f"Title:   {title}")
    print(f"{'='*70}")

    # Step 1: Extract body HTML
    print("\n[1] Extracting body HTML...")
    try:
        body_text = page.get('body_text', '')
        body_html = ''
        if not body_text:
            body_data = page.get('body', {})
            if isinstance(body_data, dict):
                storage = body_data.get('storage', {})
                if isinstance(storage, dict):
                    body_html = storage.get('value', '')
                elif isinstance(storage, str):
                    body_html = storage
            elif isinstance(body_data, str):
                body_html = body_data

        html_len = len(body_html)
        text_len = len(body_text)
        print(f"    Pre-extracted body_text: {text_len:,} chars")
        print(f"    Raw HTML body:           {html_len:,} chars")

        if html_len > MAX_BODY_HTML_BYTES:
            print(f"    WARNING: HTML exceeds MAX_BODY_HTML_BYTES ({MAX_BODY_HTML_BYTES:,}), will be truncated")

        result['steps']['extract_html'] = {
            'html_len': html_len,
            'body_text_len': text_len,
            'truncated': html_len > MAX_BODY_HTML_BYTES,
        }
    except Exception as e:
        print(f"    FAILED: {e}")
        result['steps']['extract_html'] = {'error': str(e)}
        result['error'] = f"Step 1 (extract HTML): {e}"
        traceback.print_exc()
        return result

    # Step 2: html_to_text conversion
    print("\n[2] Converting HTML to text...")
    try:
        if body_text:
            print(f"    Using pre-extracted body_text ({text_len:,} chars)")
            converted_text = body_text
        elif body_html:
            if len(body_html) > MAX_BODY_HTML_BYTES:
                body_html = body_html[:MAX_BODY_HTML_BYTES]
                print(f"    Truncated to {MAX_BODY_HTML_BYTES:,} bytes")

            t0 = time.time()
            converted_text = html_to_text(body_html)
            elapsed = time.time() - t0
            print(f"    Converted in {elapsed:.2f}s -> {len(converted_text):,} chars of text")

            if elapsed > 5:
                print(f"    WARNING: Conversion took {elapsed:.1f}s (slow!)")
        else:
            converted_text = ''
            print("    No body content to convert")

        result['steps']['html_to_text'] = {
            'output_len': len(converted_text),
            'elapsed': elapsed if body_html and not body_text else 0,
        }
    except Exception as e:
        print(f"    FAILED: {e}")
        result['steps']['html_to_text'] = {'error': str(e)}
        result['error'] = f"Step 2 (html_to_text): {e}"
        traceback.print_exc()
        return result

    # Step 3: Parse version/date
    print("\n[3] Parsing version/date...")
    try:
        from datetime import datetime
        updated = None
        version = page.get('version', {})
        if isinstance(version, dict):
            when = version.get('when')
            if when:
                updated = datetime.fromisoformat(when.replace('Z', '+00:00'))
                print(f"    Version date: {updated}")

        if not updated:
            history = page.get('history', {})
            if isinstance(history, dict):
                last_updated = history.get('lastUpdated', {})
                if isinstance(last_updated, dict):
                    when = last_updated.get('when')
                    if when:
                        updated = datetime.fromisoformat(when.replace('Z', '+00:00'))
                        print(f"    History date: {updated}")

        if not updated:
            updated = datetime.now()
            print(f"    No date found, using now: {updated}")

        result['steps']['parse_date'] = {'updated': str(updated)}
    except Exception as e:
        print(f"    FAILED: {e}")
        result['steps']['parse_date'] = {'error': str(e)}
        result['error'] = f"Step 3 (parse date): {e}"
        traceback.print_exc()
        return result

    # Step 4: Parse ancestors
    print("\n[4] Parsing ancestors...")
    try:
        ancestors = page.get('ancestors', [])
        parent_id = ''
        level = 0
        if ancestors:
            level = len(ancestors)
            if isinstance(ancestors[-1], dict):
                parent_id = str(ancestors[-1].get('id', ''))
        print(f"    Level: {level}, Parent ID: {parent_id or '(none)'}")
        result['steps']['parse_ancestors'] = {'level': level, 'parent_id': parent_id}
    except Exception as e:
        print(f"    FAILED: {e}")
        result['steps']['parse_ancestors'] = {'error': str(e)}
        result['error'] = f"Step 4 (ancestors): {e}"
        traceback.print_exc()
        return result

    # Step 5: Actual WHOOSH indexing (into a temp index)
    print("\n[5] Indexing into temp WHOOSH index...")
    import tempfile
    import shutil
    tmp_dir = tempfile.mkdtemp(prefix='whoosh_test_')
    try:
        indexer = ConfluenceIndexer(tmp_dir)
        t0 = time.time()
        count = indexer.index_all_pages([(space_key, page)], clear_first=True)
        elapsed = time.time() - t0
        print(f"    Indexed {count} page(s) in {elapsed:.2f}s")

        stats = indexer.get_stats()
        print(f"    Index stats: {stats}")
        result['steps']['whoosh_index'] = {'count': count, 'elapsed': elapsed}
    except Exception as e:
        print(f"    FAILED: {e}")
        result['steps']['whoosh_index'] = {'error': str(e)}
        result['error'] = f"Step 5 (WHOOSH index): {e}"
        traceback.print_exc()
        return result
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    result['success'] = True
    print(f"\n>>> ALL STEPS PASSED for page {page_id}")
    return result


def list_pages(data: dict):
    """List all page IDs and titles in a space."""
    pages = data.get('sampled_pages', [])
    print(f"\n{'ID':<12} {'Title'}")
    print(f"{'-'*12} {'-'*58}")
    for page in pages:
        pid = str(page.get('id', '?'))
        title = page.get('title', '(no title)')[:58]
        print(f"{pid:<12} {title}")
    print(f"\nTotal: {len(pages)} pages")


def test_all_pages(space_key: str, data: dict):
    """Try indexing every page, report failures."""
    pages = data.get('sampled_pages', [])
    failures = []
    slow = []

    print(f"\nTesting all {len(pages)} pages in {space_key}...\n")

    for i, page in enumerate(pages):
        page_id = str(page.get('id', '?'))
        title = page.get('title', '(no title)')

        # Quick inline test without the verbose diagnose output
        try:
            import tempfile, shutil
            tmp_dir = tempfile.mkdtemp(prefix='whoosh_test_')
            try:
                indexer = ConfluenceIndexer(tmp_dir)
                t0 = time.time()
                count = indexer.index_all_pages([(space_key, page)], clear_first=True)
                elapsed = time.time() - t0

                status = 'OK'
                if elapsed > 5:
                    status = f'SLOW ({elapsed:.1f}s)'
                    slow.append((page_id, title, elapsed))

                print(f"  [{i+1:>4}/{len(pages)}] {page_id:<12} {status:<16} {title[:50]}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception as e:
            print(f"  [{i+1:>4}/{len(pages)}] {page_id:<12} {'FAILED':<16} {title[:50]}")
            print(f"           Error: {e}")
            failures.append((page_id, title, str(e)))

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY: {len(pages)} pages tested, {len(failures)} failed, {len(slow)} slow")

    if failures:
        print(f"\nFAILED PAGES:")
        for pid, title, err in failures:
            print(f"  {pid:<12} {title[:40]}")
            print(f"               {err}")

    if slow:
        print(f"\nSLOW PAGES (>5s):")
        for pid, title, elapsed in sorted(slow, key=lambda x: -x[2]):
            print(f"  {pid:<12} {elapsed:>6.1f}s  {title[:40]}")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    space_key = sys.argv[1]
    page_id_or_flag = sys.argv[2]

    data = load_pickle(space_key)

    if page_id_or_flag == '--list':
        list_pages(data)
        return

    if page_id_or_flag == '--all':
        test_all_pages(space_key, data)
        return

    # Single page test
    page = find_page(data, page_id_or_flag)
    if not page:
        logger.error(f"Page ID {page_id_or_flag} not found in space {space_key}")
        print(f"\nAvailable page IDs (first 20):")
        for p in data.get('sampled_pages', [])[:20]:
            print(f"  {p.get('id')}: {p.get('title', '?')[:60]}")
        sys.exit(1)

    result = diagnose_page(space_key, page)

    if not result['success']:
        print(f"\n>>> FAILED at: {result['error']}")
        sys.exit(1)


if __name__ == '__main__':
    main()
