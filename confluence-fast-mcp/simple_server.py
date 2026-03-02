#!/usr/bin/env python
"""Simple in-memory FastMCP server for Confluence - no WHOOSH indexing required.

Supports 30+ concurrent users for hackathons and team use.
"""

import sys
import os
import logging
import collections
import collections.abc
from typing import Optional, Dict, Any, List

# Python 3.10+ removed these aliases from collections.
# Patch them back so transitive dependencies that still use the old
# import path (e.g. "from collections import MutableMapping") don't crash.
for _attr in ("MutableMapping", "Mapping", "MutableSequence", "Sequence",
              "MutableSet", "Callable"):
    if not hasattr(collections, _attr) and hasattr(collections.abc, _attr):
        setattr(collections, _attr, getattr(collections.abc, _attr))

# Setup logging BEFORE importing FastMCP to prevent it from reconfiguring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── Work around fastmcp's hard dependency on cachetools.TLRUCache ──
# fastmcp 3.0+ unconditionally imports MemoryStore from
# key_value.aio.stores.memory, which requires cachetools with TLRUCache.
# If TLRUCache is missing (wrong cachetools version, broken install, etc.)
# we inject a simple dict-backed MemoryStore stub so fastmcp can load
# and operate normally — our server only needs basic session state storage.
import types as _types

def _ensure_memory_store_importable():
    """Inject a dict-backed MemoryStore if cachetools.TLRUCache is missing."""
    try:
        from cachetools import TLRUCache  # noqa: F401
        return  # real cachetools works fine, nothing to do
    except (ImportError, AttributeError):
        pass

    logger.info("cachetools.TLRUCache unavailable – injecting dict-backed MemoryStore stub")

    # Build a minimal MemoryStore that satisfies fastmcp's usage:
    #   store = MemoryStore()
    #   await store.setup()
    #   await store.get/put/delete(collection, key, ...)
    from key_value.aio.stores.base import (
        SEED_DATA_TYPE,
        BaseDestroyCollectionStore,
        BaseDestroyStore,
        BaseEnumerateCollectionsStore,
        BaseEnumerateKeysStore,
    )
    from key_value.aio._utils.managed_entry import ManagedEntry
    from typing_extensions import override

    class _DictMemoryStore(
        BaseDestroyStore,
        BaseDestroyCollectionStore,
        BaseEnumerateCollectionsStore,
        BaseEnumerateKeysStore,
    ):
        """Minimal dict-backed store (no TLRUCache / no TTL eviction)."""

        def __init__(self, *, max_entries_per_collection=None,
                     default_collection=None, seed=None):
            self._data: dict[str, dict[str, ManagedEntry]] = {}
            super().__init__(default_collection=default_collection,
                             seed=seed, stable_api=True)

        @override
        async def _setup(self):
            for col in self._seed:
                await self._setup_collection(collection=col)

        @override
        async def _setup_collection(self, *, collection):
            if collection not in self._data:
                self._data[collection] = {}

        def _col(self, collection):
            c = self._data.get(collection)
            if c is None:
                raise KeyError(f"Collection '{collection}' not set up")
            return c

        @override
        async def _get_managed_entry(self, *, key, collection):
            return self._col(collection).get(key)

        @override
        async def _put_managed_entry(self, *, key, collection, managed_entry):
            self._col(collection)[key] = managed_entry

        @override
        async def _delete_managed_entry(self, *, key, collection):
            return self._col(collection).pop(key, None) is not None

        @override
        async def _get_collection_keys(self, *, collection, limit=None):
            keys = list(self._col(collection).keys())
            return keys[:limit] if limit else keys

        @override
        async def _get_collection_names(self, *, limit=None):
            keys = list(self._data.keys())
            return keys[:limit] if limit else keys

        @override
        async def _delete_collection(self, *, collection):
            return self._data.pop(collection, None) is not None

        @override
        async def _delete_store(self):
            self._data.clear()
            return True

    _mod_key = "key_value.aio.stores.memory"
    _stub = _types.ModuleType(_mod_key)
    _stub.MemoryStore = _DictMemoryStore
    sys.modules[_mod_key] = _stub

    # Also patch the sub-module path so "from key_value.aio.stores.memory import MemoryStore" works
    _store_mod_key = _mod_key + ".store"
    _store_stub = _types.ModuleType(_store_mod_key)
    _store_stub.MemoryStore = _DictMemoryStore
    sys.modules[_store_mod_key] = _store_stub

_ensure_memory_store_importable()

# Disable FastMCP's rich logging to avoid tracebacks_max_frames errors
# Must be done before FastMCP() is called. Works on fastmcp 3.0+.
import fastmcp as _fastmcp_mod
_fastmcp_mod.settings.log_enabled = False
_fastmcp_mod.settings.enable_rich_logging = False

from fastmcp import FastMCP
from config import get_config
from pickle_loader import PickleLoader
from converters import html_to_markdown, html_to_text
from search import CQLParser

# Initialize FastMCP server
mcp = FastMCP("confluence-simple")

# Global instances
config = None
pickle_loader = None


# ---------------------------------------------------------------------------
# Tools – sooperset/mcp-atlassian compatible names & signatures
# See: https://github.com/sooperset/mcp-atlassian
# ---------------------------------------------------------------------------

@mcp.tool()
def confluence_search(
    query: str,
    limit: int = 10,
    spaces_filter: Optional[str] = None,
) -> str:
    """Search Confluence content.

    Args:
        query: Search query - simple text or CQL query string
        limit: Maximum number of results (1-50)
        spaces_filter: Comma-separated list of space keys to filter results
    """
    # Check if query looks like CQL (contains operators like ~, =, AND, OR)
    is_cql = any(op in query for op in (' ~ ', ' = ', ' AND ', ' OR '))

    if is_cql:
        parser = CQLParser()
        search_terms, cql_space = parser.parse(query)
        title_only = 'title:' in search_terms or query.strip().startswith('title')
        clean_query = search_terms.replace('title:', '').replace('^2', '')
        clean_query = clean_query.strip('() ')
        space_key = cql_space or (spaces_filter.split(',')[0].strip() if spaces_filter else None)

        if clean_query == '*' or not clean_query:
            if space_key:
                pages = pickle_loader.get_pages_in_space(space_key, limit=limit)
                return _format_search_results(
                    [{'space_key': space_key, 'page': p} for p in pages], query
                )
            return f"No results found for: {query}"

        matches = pickle_loader.search_content(
            clean_query, space_key=space_key, title_only=title_only, limit=limit
        )
    else:
        # Simple text search
        space_key = spaces_filter.split(',')[0].strip() if spaces_filter else None
        matches = pickle_loader.search_content(query, space_key=space_key, limit=limit)

    return _format_search_results(matches, query)


@mcp.tool()
def confluence_get_page(
    page_id: Optional[str] = None,
    title: Optional[str] = None,
    space_key: Optional[str] = None,
    include_metadata: bool = True,
    convert_to_markdown: bool = True,
) -> str:
    """Get a Confluence page by ID or title.

    Args:
        page_id: Numeric page ID from URL
        title: Exact page title
        space_key: Space key (required when using title)
        include_metadata: Whether to include creation date, version, labels
        convert_to_markdown: Whether to convert HTML body to markdown
    """
    result = None

    # Try page_id first
    if page_id:
        result = pickle_loader.get_page_by_id(page_id)

    # Fall back to title lookup
    if not result and title:
        result = pickle_loader.find_page_by_title_flexible(title, space_key=space_key)

    # Last resort: treat page_id as title
    if not result and page_id and not page_id.isdigit():
        result = pickle_loader.find_page_by_title_flexible(page_id, space_key=space_key)

    if not result:
        identifier = page_id or title or "unknown"
        return f"Page not found: {identifier}"

    page = result['page']
    page_space_key = result['space_key']

    return _format_page_text(page, page_space_key,
                             include_metadata=include_metadata,
                             convert_to_markdown=convert_to_markdown)


@mcp.tool()
def confluence_get_page_children(
    parent_id: str,
    expand: str = "version",
    limit: int = 25,
    include_content: bool = False,
    convert_to_markdown: bool = True,
    start: int = 0,
    include_folders: bool = True,
) -> str:
    """Get child pages of a specific page.

    Args:
        parent_id: ID of the parent page
        expand: Fields to expand in the response
        limit: Maximum child items to return (1-50)
        include_content: Whether to include page body content
        convert_to_markdown: Convert to markdown or return raw HTML
        start: Starting index for pagination
        include_folders: Whether to include child folders
    """
    children = pickle_loader.get_children(parent_id, limit=limit, start=start)

    if not children:
        return f"No child pages found for parent {parent_id}"

    lines = [f"Found {len(children)} child page(s) of page {parent_id}:\n"]

    for i, child in enumerate(children, 1):
        page = child['page']
        sk = child['space_key']
        page_id = page.get('id', '')
        page_title = page.get('title', '')
        lines.append(f"{i}. **{page_title}** (ID: {page_id}, Space: {sk})")

        if include_content:
            body_html = _extract_body_html(page)
            if convert_to_markdown:
                body = html_to_markdown(body_html)
            else:
                body = body_html
            if body:
                # Indent content under the list item
                for line in body.strip().split('\n')[:10]:
                    lines.append(f"   {line}")
                lines.append("")

    return "\n".join(lines)


@mcp.tool()
def confluence_get_comments(page_id: str) -> str:
    """Get comments on a Confluence page.

    Args:
        page_id: Confluence page ID

    Note: This cached server has limited comment data. Returns whatever
    comment data is available in the pickled page data.
    """
    result = pickle_loader.get_page_by_id(page_id)
    if not result:
        return f"Page not found: {page_id}"

    page = result['page']

    # Check if comments are stored in the pickle data
    comments = page.get('comments', [])
    if not comments:
        children = page.get('children', {})
        if isinstance(children, dict):
            comment_data = children.get('comment', {})
            if isinstance(comment_data, dict):
                comments = comment_data.get('results', [])

    if not comments:
        return f"No comments found for page {page_id} (page: {page.get('title', '')})"

    lines = [f"Comments on \"{page.get('title', '')}\" ({len(comments)} comment(s)):\n"]
    for i, comment in enumerate(comments, 1):
        author = ""
        if isinstance(comment, dict):
            author_data = comment.get('author', {})
            if isinstance(author_data, dict):
                author = author_data.get('displayName', '')
            body = comment.get('body', {})
            if isinstance(body, dict):
                text = body.get('storage', {}).get('value', '')
                text = html_to_text(text) if text else ''
            else:
                text = str(body)
            lines.append(f"{i}. [{author}] {text[:500]}")

    return "\n".join(lines)


@mcp.tool()
def confluence_get_labels(page_id: str) -> str:
    """Get labels for a Confluence page.

    Args:
        page_id: Content ID (page or attachment with att prefix)
    """
    result = pickle_loader.get_page_by_id(page_id)
    if not result:
        return f"Page not found: {page_id}"

    page = result['page']

    # Try various locations where labels might be stored
    labels = page.get('labels', [])
    if not labels:
        metadata = page.get('metadata', {})
        if isinstance(metadata, dict):
            label_data = metadata.get('labels', {})
            if isinstance(label_data, dict):
                labels = label_data.get('results', [])
            elif isinstance(label_data, list):
                labels = label_data

    if not labels:
        return f"No labels found for page {page_id} (page: {page.get('title', '')})"

    label_names = []
    for label in labels:
        if isinstance(label, dict):
            label_names.append(label.get('name', str(label)))
        else:
            label_names.append(str(label))

    return f"Labels for \"{page.get('title', '')}\": {', '.join(label_names)}"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_body_html(page: Dict[str, Any]) -> str:
    """Extract HTML body content from a page dict."""
    body_data = page.get('body', {})
    if isinstance(body_data, dict):
        storage = body_data.get('storage', {})
        if isinstance(storage, dict):
            return storage.get('value', '')
        elif isinstance(storage, str):
            return storage
    elif isinstance(body_data, str):
        return body_data
    return ''


def _format_page_text(page: Dict[str, Any], space_key: str,
                      include_metadata: bool = True,
                      convert_to_markdown: bool = True) -> str:
    """Format a page as readable text (sooperset style)."""
    page_id = str(page.get('id', ''))
    title = page.get('title', '')

    parts = [f"# {title}\n"]

    if include_metadata:
        parts.append(f"- **Page ID**: {page_id}")
        parts.append(f"- **Space**: {space_key}")
        version = page.get('version', {})
        if isinstance(version, dict):
            ver_num = version.get('number', 1)
            ver_when = version.get('when', '')
            parts.append(f"- **Version**: {ver_num}")
            if ver_when:
                parts.append(f"- **Last updated**: {ver_when}")
            by = version.get('by', {})
            if isinstance(by, dict) and by.get('displayName'):
                parts.append(f"- **Author**: {by['displayName']}")

        # Labels if present
        labels = page.get('labels', [])
        if labels:
            label_names = [
                l.get('name', str(l)) if isinstance(l, dict) else str(l)
                for l in labels
            ]
            parts.append(f"- **Labels**: {', '.join(label_names)}")

        parts.append("")  # blank line

    # Body content
    body_html = _extract_body_html(page)
    if body_html:
        parts.append("---\n")
        if convert_to_markdown:
            parts.append(html_to_markdown(body_html))
        else:
            parts.append(body_html)

    return "\n".join(parts)


def _format_search_results(matches: List[Dict[str, Any]], query: str) -> str:
    """Format search results as readable text (sooperset style)."""
    if not matches:
        return f"No results found for: {query}"

    lines = [f"Found {len(matches)} result(s) for \"{query}\":\n"]

    for i, match in enumerate(matches, 1):
        page = match['page']
        space_key = match['space_key']
        match_type = match.get('match_type', 'content')
        page_id = page.get('id', '')
        title = page.get('title', '')
        lines.append(
            f"{i}. **{title}** (ID: {page_id}, Space: {space_key}, Match: {match_type})"
        )

    return "\n".join(lines)


def initialize_server():
    """Initialize server components."""
    global config, pickle_loader

    logger.info("Initializing Simple Confluence MCP Server...")

    # Load configuration
    config = get_config()
    logger.info(f"Pickle directory: {config.pickle_dir}")

    # Initialize pickle loader
    logger.info("Loading pickle files into memory...")
    pickle_loader = PickleLoader(config.pickle_dir)
    pickle_loader.load_all_pickles()

    spaces = pickle_loader.get_all_spaces()
    total_pages = sum(s['sampled_pages'] for s in spaces)

    logger.info(f"Loaded {len(spaces)} spaces with {total_pages} pages")
    logger.info("Server initialization complete!")
    logger.info("Note: This server uses simple in-memory search (no WHOOSH indexing)")


def main():
    """Main entry point."""
    import sys

    initialize_server()

    # Check if running in HTTP mode
    if len(sys.argv) > 1 and sys.argv[1] == "--http":
        port = int(sys.argv[2]) if len(sys.argv) > 2 else 8070
        host = "0.0.0.0"
        logger.info(f"Starting Simple FastMCP server in HTTP mode on {host}:{port}...")
        mcp.run(transport="sse", host=host, port=port)
    else:
        logger.info("Starting Simple FastMCP server in stdio mode...")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
