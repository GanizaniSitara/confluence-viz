#!/usr/bin/env python3
"""Convert pickled Confluence data to JSON for Go servers.

Usage:
    python convert_to_json.py [--pickle-dir ../temp] [--output-dir ./json_data]

Flattens the nested Confluence API structures into clean JSON that Go can
trivially deserialize. Run once after collecting pickles; both Go servers
read the resulting JSON files.
"""

import argparse
import json
import os
import pickle
import re
import sys
from pathlib import Path


def _body_html(page):
    body = page.get('body', {})
    if isinstance(body, dict):
        storage = body.get('storage', {})
        if isinstance(storage, dict):
            return storage.get('value', '')
        if isinstance(storage, str):
            return storage
    if isinstance(body, str):
        return body
    return ''


def _body_text(page):
    """Pre-extract plain text so Go doesn't need an HTML parser for search."""
    text = page.get('body_text', '')
    if text:
        return text
    html = _body_html(page)
    if not html:
        return ''
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, 'html.parser').get_text(separator=' ', strip=True)
    except Exception:
        return re.sub(r'<[^>]+>', ' ', html)


def _labels(page):
    labels = page.get('labels', [])
    if not labels:
        meta = page.get('metadata', {})
        if isinstance(meta, dict):
            ld = meta.get('labels', {})
            if isinstance(ld, dict):
                labels = ld.get('results', [])
            elif isinstance(ld, list):
                labels = ld
    return [
        l.get('name', str(l)) if isinstance(l, dict) else str(l)
        for l in labels
    ]


def _parent_id(page):
    pid = page.get('parent_id', '')
    if not pid:
        anc = page.get('ancestors', [])
        if anc and isinstance(anc[-1], dict):
            pid = str(anc[-1].get('id', ''))
    return str(pid) if pid else ''


def _ancestor_ids(page):
    return [
        str(a['id']) for a in page.get('ancestors', [])
        if isinstance(a, dict) and a.get('id')
    ]


def _comments(page):
    comments = page.get('comments', [])
    if not comments:
        children = page.get('children', {})
        if isinstance(children, dict):
            cd = children.get('comment', {})
            if isinstance(cd, dict):
                comments = cd.get('results', [])
    out = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        author = ''
        ad = c.get('author', {})
        if isinstance(ad, dict):
            author = ad.get('displayName', '')
        bh = ''
        bd = c.get('body', {})
        if isinstance(bd, dict):
            bh = bd.get('storage', {}).get('value', '')
        out.append({'author': author, 'body_html': bh})
    return out


def _version_field(page, key):
    v = page.get('version')
    if not isinstance(v, dict):
        return '' if key != 'number' else 1
    if key == 'number':
        return v.get('number', 1)
    if key == 'when':
        return v.get('when', '')
    if key == 'by':
        by = v.get('by', {})
        return by.get('displayName', '') if isinstance(by, dict) else ''
    return ''


def convert_file(pkl_path, output_dir):
    with open(pkl_path, 'rb') as f:
        data = pickle.load(f)
    sk = data.get('space_key', '')
    if not sk:
        return None
    pages = []
    for p in data.get('sampled_pages', []):
        pages.append({
            'id':             str(p.get('id', '')),
            'title':          p.get('title', ''),
            'parent_id':      _parent_id(p),
            'body_html':      _body_html(p),
            'body_text':      _body_text(p),
            'version_number': _version_field(p, 'number'),
            'version_when':   _version_field(p, 'when'),
            'version_by':     _version_field(p, 'by'),
            'labels':         _labels(p),
            'ancestor_ids':   _ancestor_ids(p),
            'comments':       _comments(p),
        })
    out = {
        'space_key':            sk,
        'name':                 data.get('name', sk),
        'total_pages_in_space': data.get('total_pages_in_space', len(pages)),
        'sampled_pages':        pages,
    }
    dest = os.path.join(output_dir, f'{sk}.json')
    with open(dest, 'w') as f:
        json.dump(out, f, ensure_ascii=False)
    return sk, len(pages)


def main():
    ap = argparse.ArgumentParser(description='Convert pickled Confluence data to JSON for Go servers')
    ap.add_argument('--pickle-dir', default='../temp', help='Directory with .pkl files (default: ../temp)')
    ap.add_argument('--output-dir', default='./json_data', help='Output directory (default: ./json_data)')
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    pkls = sorted(Path(args.pickle_dir).glob('*.pkl'))
    if not pkls:
        print(f'No .pkl files found in {args.pickle_dir}')
        sys.exit(1)

    print(f'Converting {len(pkls)} pickle files to JSON...')
    total = 0
    for p in pkls:
        result = convert_file(str(p), args.output_dir)
        if result:
            sk, n = result
            total += n
            print(f'  {sk}: {n} pages')

    print(f'\nDone! {total} pages written to {args.output_dir}/')


if __name__ == '__main__':
    main()
