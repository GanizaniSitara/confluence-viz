#!/usr/bin/env python3
"""Simple in-memory FastMCP server for Confluence - no WHOOSH indexing required."""

import sys
import os
import logging
from typing import Optional, Dict, Any, List

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from fastmcp import FastMCP
from confluence_fast_mcp.config import get_config
from confluence_fast_mcp.pickle_loader import PickleLoader
from confluence_fast_mcp.converters import html_to_adf

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    """Get a Confluence page by ID or title."""
    # Try to get by ID first
    result = pickle_loader.get_page_by_id(pageIdOrTitleAndSpaceKey)

    # If not found by ID and spaceKey provided, try title lookup
    if not result and spaceKey:
        result = pickle_loader.get_page_by_title(pageIdOrTitleAndSpaceKey, spaceKey)

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
    """Simple in-memory text search across all pages."""
    query_lower = query.lower()
    results = []

    # Search by title first
    title_matches = pickle_loader.search_by_title(query)

    for match in title_matches[:limit]:
        page = match['page']
        space_key = match['space_key']
        results.append({
            "id": f"ari:cloud:confluence:{FAKE_CLOUD_ID}:page/{page.get('id')}",
            "title": page.get('title', ''),
            "url": f"http://localhost/spaces/{space_key}/pages/{page.get('id')}",
            "contentType": "page",
            "space": {
                "key": space_key,
                "name": space_key
            },
            "excerpt": page.get('title', '')[:200]
        })

    return results


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
        logger.info(f"Starting Simple FastMCP server in HTTP mode on port {port}...")
        mcp.run(transport="sse", port=port)
    else:
        logger.info("Starting Simple FastMCP server in stdio mode...")
        mcp.run()


if __name__ == "__main__":
    main()
