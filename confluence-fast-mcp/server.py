"""FastMCP server for Confluence data."""

import collections
import collections.abc
import logging
import os
import sys
import types as _types
from typing import Optional, Dict, Any, List

# Python 3.10+ removed these aliases from collections.
# Patch them back so transitive dependencies that still use the old
# import path (e.g. "from collections import MutableMapping") don't crash.
for _attr in ("MutableMapping", "Mapping", "MutableSequence", "Sequence",
              "MutableSet", "Callable"):
    if not hasattr(collections, _attr) and hasattr(collections.abc, _attr):
        setattr(collections, _attr, getattr(collections.abc, _attr))

# ── Work around fastmcp's hard dependency on cachetools.TLRUCache ──
# fastmcp 3.0+ unconditionally imports MemoryStore from
# key_value.aio.stores.memory, which requires cachetools with TLRUCache.
# If TLRUCache is missing we inject a simple dict-backed MemoryStore stub.
def _ensure_memory_store_importable():
    """Inject a dict-backed MemoryStore if cachetools.TLRUCache is missing."""
    try:
        from cachetools import TLRUCache  # noqa: F401
        return
    except (ImportError, AttributeError):
        pass

    from key_value.aio.stores.base import (
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
from indexer import ConfluenceIndexer
from converters import html_to_markdown, html_to_text
from search import translate_cql
from fallback import ConfluenceFallbackClient

# Setup logging — console + rotating file
_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(_log_dir, exist_ok=True)
_log_fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Console handler (INFO)
_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(_log_fmt)

# File handler (DEBUG, rotating 5 x 5 MB)
from logging.handlers import RotatingFileHandler
_file = RotatingFileHandler(
    os.path.join(_log_dir, 'server.log'),
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8',
)
_file.setLevel(logging.DEBUG)
_file.setFormatter(_log_fmt)

# Apply to root logger so all modules (indexer, pickle_loader, etc.) are captured
logging.root.setLevel(logging.DEBUG)
logging.root.addHandler(_console)
logging.root.addHandler(_file)

# Quieten noisy third-party loggers in the file log
for _noisy in ('httpcore', 'httpx', 'asyncio', 'watchfiles'):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("confluence-fast-mcp")

# Global instances (initialized on startup)
config = None
pickle_loader = None
indexer = None
fallback_client = None


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
    logger.info(f"Search: {query}")

    # Check if query looks like CQL
    is_cql = any(op in query for op in (' ~ ', ' = ', ' AND ', ' OR '))

    if is_cql:
        whoosh_query, cql_space = translate_cql(query)
        space_key = cql_space or (spaces_filter.split(',')[0].strip() if spaces_filter else None)
    else:
        whoosh_query = query
        space_key = spaces_filter.split(',')[0].strip() if spaces_filter else None

    # Use WHOOSH index for full-text search
    search_results = indexer.search(
        whoosh_query,
        space_key=space_key,
        limit=limit,
    )

    # Build formatted results
    matches = []
    for result in search_results:
        page_result = pickle_loader.get_page_by_id(result['page_id'])
        if page_result:
            matches.append({
                'space_key': page_result['space_key'],
                'page': page_result['page'],
                'match_type': 'content',
            })

    response = _format_search_results(matches, query)
    logger.debug(f"Search returned {len(matches)} matches, response {len(response)} chars")
    return response


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
    logger.info(f"GetPage: page_id={page_id}, title={title}, space_key={space_key}")
    result = None

    # Try page_id first
    if page_id:
        result = pickle_loader.get_page_by_id(page_id)

    # Fall back to title lookup
    if not result and title:
        if space_key:
            result = pickle_loader.get_page_by_title(title, space_key)
        if not result:
            result = pickle_loader.find_page_by_title_flexible(title, space_key=space_key)

    # Last resort: treat page_id as title
    if not result and page_id and not page_id.isdigit():
        result = pickle_loader.find_page_by_title_flexible(page_id, space_key=space_key)

    if not result:
        identifier = page_id or title or "unknown"
        logger.warning(f"GetPage: not found - {identifier}")
        return f"Page not found: {identifier}"

    page = result['page']
    page_space_key = result['space_key']
    logger.debug(f"GetPage: found page '{page.get('title', '')}' in space {page_space_key}")

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
    logger.info(f"GetChildren: parent_id={parent_id}, limit={limit}, start={start}")
    children = pickle_loader.get_children(parent_id, limit=limit, start=start)

    if not children:
        return f"No child pages found for parent {parent_id}"

    lines = [f"Found {len(children)} child page(s) of page {parent_id}:\n"]

    for i, child in enumerate(children, 1):
        page = child['page']
        sk = child['space_key']
        pid = page.get('id', '')
        page_title = page.get('title', '')
        lines.append(f"{i}. **{page_title}** (ID: {pid}, Space: {sk})")

        if include_content:
            body_html = _extract_body_html(page)
            if convert_to_markdown:
                body = html_to_markdown(body_html)
            else:
                body = body_html
            if body:
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
    logger.info(f"GetComments: page_id={page_id}")
    result = pickle_loader.get_page_by_id(page_id)
    if not result:
        return f"Page not found: {page_id}"

    page = result['page']
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
    logger.info(f"GetLabels: page_id={page_id}")
    result = pickle_loader.get_page_by_id(page_id)
    if not result:
        return f"Page not found: {page_id}"

    page = result['page']
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

        labels = page.get('labels', [])
        if labels:
            label_names = [
                l.get('name', str(l)) if isinstance(l, dict) else str(l)
                for l in labels
            ]
            parts.append(f"- **Labels**: {', '.join(label_names)}")

        parts.append("")

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
    global config, pickle_loader, indexer, fallback_client

    logger.info("Initializing Confluence Fast MCP Server...")

    # Load configuration
    config = get_config()
    logger.info(f"Pickle directory: {config.pickle_dir}")
    logger.info(f"Index directory: {config.index_dir}")

    # Initialize pickle loader
    pickle_loader = PickleLoader(config.pickle_dir)
    pickle_loader.load_all_pickles()

    # Initialize indexer
    indexer = ConfluenceIndexer(config.index_dir)

    # Check if index exists
    stats = indexer.get_stats()
    logger.info(f"Index stats: {stats}")

    if stats['total_docs'] == 0:
        logger.error("Index is empty. Please run 'python3 build_index.py' first.")
        raise RuntimeError("WHOOSH index not found. Run build_index.py to create it.")

    logger.info(f"Using existing index with {stats['total_docs']} documents")

    # Initialize fallback client (if configured)
    if config.confluence_url and config.confluence_username and config.confluence_api_token:
        fallback_client = ConfluenceFallbackClient(
            config.confluence_url,
            config.confluence_username,
            config.confluence_api_token
        )
        logger.info("Fallback client configured for attachment support")
    else:
        logger.info("Fallback client not configured (attachments unavailable)")

    logger.info("Server initialization complete!")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Confluence FastMCP server (WHOOSH search)")
    parser.add_argument("--stdio", action="store_true", help="Run in stdio mode (for Claude Desktop)")
    parser.add_argument("--port", type=int, default=8070, help="HTTP port (default: 8070)")
    parser.add_argument("--sse", action="store_true", help="Use legacy SSE transport instead of streamable HTTP")
    # Legacy support: --http [port]
    parser.add_argument("--http", nargs="?", type=int, const=8070, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.http is not None:
        args.port = args.http

    initialize_server()

    if args.stdio:
        logger.info("Starting FastMCP server in stdio mode...")
        mcp.run(transport="stdio")
    else:
        host = "0.0.0.0"
        transport = "sse" if args.sse else "http"
        endpoint = "/sse" if args.sse else "/mcp"
        logger.info(f"Starting FastMCP server on http://{host}:{args.port}{endpoint} (transport={transport})")
        mcp.run(transport=transport, host=host, port=args.port)


if __name__ == "__main__":
    main()
