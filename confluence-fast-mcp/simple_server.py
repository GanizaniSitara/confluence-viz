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

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

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
from confluence_fast_mcp.config import get_config
from confluence_fast_mcp.pickle_loader import PickleLoader
from confluence_fast_mcp.converters import html_to_adf
from confluence_fast_mcp.search import CQLParser

# Initialize FastMCP server
mcp = FastMCP("confluence-simple")

# Global instances
config = None
pickle_loader = None

# Fake cloud ID for local server
FAKE_CLOUD_ID = "local-confluence-simple"


@mcp.tool()
def getAccessibleAtlassianResources() -> List[Dict[str, Any]]:
    """Get accessible Atlassian resources."""
    return [{
        "id": FAKE_CLOUD_ID,
        "name": "Local Confluence (Simple)",
        "url": "http://localhost",
        "scopes": ["read:confluence-content.all"]
    }]


@mcp.tool()
def atlassianUserInfo() -> Dict[str, Any]:
    """Get current user info."""
    return {
        "accountId": "local-user",
        "accountType": "atlassian",
        "email": "local@example.com",
        "displayName": "Local User"
    }


@mcp.tool()
def getConfluenceSpaces(
    cloudId: Optional[str] = None,
    searchString: Optional[str] = None,
    maxResults: int = 50
) -> List[Dict[str, Any]]:
    """Get Confluence spaces."""
    spaces = pickle_loader.get_all_spaces()

    # Apply search filter
    if searchString:
        search_lower = searchString.lower()
        spaces = [
            s for s in spaces
            if search_lower in s['name'].lower() or search_lower in s['key'].lower()
        ]

    # Limit results
    spaces = spaces[:maxResults]

    return [
        {
            "id": space['key'],
            "key": space['key'],
            "name": space['name'],
            "type": "global",
            "status": "current"
        }
        for space in spaces
    ]


@mcp.tool()
def getConfluencePage(
    cloudId: str,
    pageIdOrTitleAndSpaceKey: str,
    spaceKey: Optional[str] = None
) -> Dict[str, Any]:
    """Get a Confluence page by ID or title.

    Supports page ID, exact title+spaceKey, or flexible title matching
    (case-insensitive, partial match).
    """
    # Try to get by ID first
    result = pickle_loader.get_page_by_id(pageIdOrTitleAndSpaceKey)

    # If not found by ID, try flexible title lookup
    if not result:
        result = pickle_loader.find_page_by_title_flexible(
            pageIdOrTitleAndSpaceKey, space_key=spaceKey
        )

    if not result:
        return {
            "isError": True,
            "message": f"Page not found: {pageIdOrTitleAndSpaceKey}"
        }

    page = result['page']
    page_space_key = result['space_key']

    return _format_page_response(page, page_space_key)


@mcp.tool()
def getPagesInConfluenceSpace(
    cloudId: str,
    spaceIdOrKey: str,
    limit: int = 25,
    start: int = 0
) -> Dict[str, Any]:
    """Get pages in a Confluence space."""
    pages = pickle_loader.get_pages_in_space(spaceIdOrKey, limit=limit, start=start)

    if not pages:
        return {
            "results": [],
            "start": start,
            "limit": limit,
            "size": 0
        }

    formatted_pages = [
        _format_page_response(page, spaceIdOrKey, include_body=False)
        for page in pages
    ]

    return {
        "results": formatted_pages,
        "start": start,
        "limit": limit,
        "size": len(formatted_pages)
    }


@mcp.tool()
def search(query: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Search across all pages by title and body content.

    Title matches are ranked higher than body content matches.
    """
    matches = pickle_loader.search_content(query, limit=limit)
    results = []

    for match in matches:
        page = match['page']
        space_key = match['space_key']
        match_type = match.get('match_type', 'title')
        results.append({
            "id": f"ari:cloud:confluence:{FAKE_CLOUD_ID}:page/{page.get('id')}",
            "title": page.get('title', ''),
            "url": f"http://localhost/spaces/{space_key}/pages/{page.get('id')}",
            "contentType": "page",
            "matchType": match_type,
            "space": {
                "key": space_key,
                "name": space_key
            },
            "excerpt": page.get('title', '')[:200]
        })

    return results


@mcp.tool()
def searchConfluenceUsingCql(
    cloudId: str,
    cql: str,
    limit: int = 25,
    start: int = 0
) -> Dict[str, Any]:
    """Search Confluence using CQL (Confluence Query Language).

    Supports in-memory CQL queries like:
    - text ~ "search term"
    - title ~ "title search"
    - space = KEY
    - text ~ "api" AND space = DOCS
    """
    parser = CQLParser()
    search_terms, space_key = parser.parse(cql)

    # Determine if this is a title-only search
    title_only = 'title:' in search_terms or cql.strip().startswith('title')

    # Clean WHOOSH-specific syntax from search terms for in-memory search
    clean_query = search_terms.replace('title:', '').replace('^2', '')
    clean_query = clean_query.strip('() ')

    if clean_query == '*' or not clean_query:
        # No text search, just space filter - return pages from space
        if space_key:
            pages = pickle_loader.get_pages_in_space(space_key, limit=limit, start=start)
            formatted = [
                _format_page_response(p, space_key, include_body=False) for p in pages
            ]
            return {"results": formatted, "start": start, "limit": limit, "size": len(formatted)}
        return {"results": [], "start": start, "limit": limit, "size": 0}

    matches = pickle_loader.search_content(
        clean_query, space_key=space_key, title_only=title_only, limit=limit
    )

    formatted = [
        _format_page_response(m['page'], m['space_key'], include_body=False)
        for m in matches
    ]

    return {
        "results": formatted,
        "start": start,
        "limit": limit,
        "size": len(formatted),
        "cqlQuery": cql
    }


@mcp.tool()
def getConfluencePageDescendants(
    cloudId: str,
    pageId: str,
    limit: int = 25,
    start: int = 0
) -> Dict[str, Any]:
    """Get child pages of a specific page."""
    children = pickle_loader.get_children(pageId, limit=limit, start=start)

    formatted = [
        _format_page_response(c['page'], c['space_key'], include_body=False)
        for c in children
    ]

    return {
        "results": formatted,
        "start": start,
        "limit": limit,
        "size": len(formatted)
    }


@mcp.tool()
def fetch(id: str) -> Dict[str, Any]:
    """Fetch details of a Confluence page by ARI."""
    # Parse ARI to extract page ID
    if id.startswith('ari:cloud:confluence:'):
        parts = id.split('/')
        if len(parts) >= 2:
            page_id = parts[-1]
            return getConfluencePage(FAKE_CLOUD_ID, page_id)

    return {
        "isError": True,
        "message": f"Invalid ARI format: {id}"
    }


def _format_page_response(page: Dict[str, Any], space_key: str,
                         include_body: bool = True) -> Dict[str, Any]:
    """Format a page as Confluence API response."""
    page_id = str(page.get('id', ''))
    title = page.get('title', '')

    response = {
        "id": page_id,
        "type": "page",
        "status": "current",
        "title": title,
        "space": {
            "key": space_key,
            "name": space_key
        }
    }

    # Add version info
    version = page.get('version', {})
    if isinstance(version, dict):
        response["version"] = {
            "number": version.get('number', 1),
            "when": version.get('when', ''),
        }

    # Add body if requested
    if include_body:
        body_html = ''
        body_data = page.get('body', {})
        if isinstance(body_data, dict):
            storage = body_data.get('storage', {})
            if isinstance(storage, dict):
                body_html = storage.get('value', '')
            elif isinstance(storage, str):
                body_html = storage
        elif isinstance(body_data, str):
            body_html = body_data

        # Convert to ADF
        adf = html_to_adf(body_html)

        response["body"] = {
            "storage": {
                "value": body_html,
                "representation": "storage"
            },
            "atlas_doc_format": {
                "value": adf,
                "representation": "atlas_doc_format"
            }
        }

    # Add links
    response["_links"] = {
        "self": f"/rest/api/content/{page_id}",
        "webui": f"/spaces/{space_key}/pages/{page_id}"
    }

    return response


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
