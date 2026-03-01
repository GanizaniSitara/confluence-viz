"""Load and manage pickled Confluence data."""

import os
import pickle
import logging
import re
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


def _extract_body_text(page: Dict[str, Any]) -> str:
    """Extract plain text from page body for in-memory search.

    Uses BeautifulSoup for basic HTML stripping. Falls back gracefully.
    """
    body_data = page.get('body', {})
    body_html = ''
    if isinstance(body_data, dict):
        storage = body_data.get('storage', {})
        if isinstance(storage, dict):
            body_html = storage.get('value', '')
        elif isinstance(storage, str):
            body_html = storage
    elif isinstance(body_data, str):
        body_html = body_data

    if not body_html:
        return ''

    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(body_html, 'html.parser').get_text(separator=' ', strip=True)
    except Exception:
        # Fallback: strip tags with regex
        return re.sub(r'<[^>]+>', ' ', body_html)


class PickleLoader:
    """Manages loading and caching of pickled Confluence data."""

    def __init__(self, pickle_dir: str):
        """Initialize pickle loader.

        Args:
            pickle_dir: Directory containing .pkl files
        """
        self.pickle_dir = pickle_dir
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._pages_by_id: Dict[str, tuple] = {}  # page_id -> (space_key, page_data)
        self._pages_by_title: Dict[tuple, tuple] = {}  # (title, space_key) -> (space_key, page_data)
        self._children_by_parent: Dict[str, List[tuple]] = {}  # parent_id -> [(space_key, page_data), ...]
        self._loaded = False

        if not os.path.exists(pickle_dir):
            logger.warning(f"Pickle directory does not exist: {pickle_dir}")

    def load_all_pickles(self) -> None:
        """Load all pickle files from the configured directory."""
        if self._loaded:
            return

        if not os.path.exists(self.pickle_dir):
            logger.error(f"Pickle directory not found: {self.pickle_dir}")
            return

        pickle_files = list(Path(self.pickle_dir).glob('*.pkl'))
        logger.info(f"Found {len(pickle_files)} pickle files in {self.pickle_dir}")

        for pickle_file in pickle_files:
            try:
                self._load_pickle(str(pickle_file))
            except Exception as e:
                logger.error(f"Error loading {pickle_file}: {e}")

        self._loaded = True
        logger.info(f"Loaded {len(self._cache)} spaces with {len(self._pages_by_id)} total pages")

    def _load_pickle(self, filepath: str) -> None:
        """Load a single pickle file.

        Args:
            filepath: Path to the pickle file
        """
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)

            # Expected format: {'space_key': str, 'name': str, 'sampled_pages': list, ...}
            space_key = data.get('space_key')
            if not space_key:
                logger.warning(f"No space_key in {filepath}")
                return

            self._cache[space_key] = data

            # Index pages by ID and title
            pages = data.get('sampled_pages', [])
            for page in pages:
                page_id = page.get('id')
                page_title = page.get('title')

                if page_id:
                    self._pages_by_id[str(page_id)] = (space_key, page)

                if page_title:
                    # Index by (title, space_key) for lookups
                    self._pages_by_title[(page_title, space_key)] = (space_key, page)

                # Index parent-child relationships
                parent_id = page.get('parent_id')
                if not parent_id:
                    ancestors = page.get('ancestors', [])
                    if ancestors and isinstance(ancestors[-1], dict):
                        parent_id = str(ancestors[-1].get('id', ''))
                if parent_id:
                    parent_id = str(parent_id)
                    if parent_id not in self._children_by_parent:
                        self._children_by_parent[parent_id] = []
                    self._children_by_parent[parent_id].append((space_key, page))

            logger.debug(f"Loaded space {space_key} from {filepath} with {len(pages)} pages")

        except Exception as e:
            logger.error(f"Failed to load pickle {filepath}: {e}")
            raise

    def get_all_spaces(self) -> List[Dict[str, Any]]:
        """Get all loaded spaces.

        Returns:
            List of space dictionaries
        """
        self.load_all_pickles()
        return [
            {
                'key': key,
                'name': data.get('name', key),
                'total_pages': data.get('total_pages_in_space', len(data.get('sampled_pages', []))),
                'sampled_pages': len(data.get('sampled_pages', []))
            }
            for key, data in self._cache.items()
        ]

    def get_space(self, space_key: str) -> Optional[Dict[str, Any]]:
        """Get a specific space by key.

        Args:
            space_key: The space key

        Returns:
            Space data dictionary or None
        """
        self.load_all_pickles()
        return self._cache.get(space_key)

    def get_page_by_id(self, page_id: str) -> Optional[Dict[str, Any]]:
        """Get a page by its ID.

        Args:
            page_id: The page ID

        Returns:
            Tuple of (space_key, page_data) or None
        """
        self.load_all_pickles()
        result = self._pages_by_id.get(str(page_id))
        if result:
            return {'space_key': result[0], 'page': result[1]}
        return None

    def get_page_by_title(self, title: str, space_key: str) -> Optional[Dict[str, Any]]:
        """Get a page by title and space key.

        Args:
            title: Page title
            space_key: Space key

        Returns:
            Tuple of (space_key, page_data) or None
        """
        self.load_all_pickles()
        result = self._pages_by_title.get((title, space_key))
        if result:
            return {'space_key': result[0], 'page': result[1]}
        return None

    def get_pages_in_space(self, space_key: str, limit: int = 25, start: int = 0) -> List[Dict[str, Any]]:
        """Get pages in a specific space.

        Args:
            space_key: The space key
            limit: Maximum number of pages to return
            start: Starting index for pagination

        Returns:
            List of page dictionaries
        """
        self.load_all_pickles()
        space = self.get_space(space_key)
        if not space:
            return []

        pages = space.get('sampled_pages', [])
        return pages[start:start + limit]

    def get_all_pages(self) -> List[tuple]:
        """Get all pages from all spaces.

        Returns:
            List of (space_key, page_data) tuples
        """
        self.load_all_pickles()
        return [(space_key, page) for space_key, page in self._pages_by_id.values()]

    def get_children(self, page_id: str, limit: int = 25,
                     start: int = 0) -> List[Dict[str, Any]]:
        """Get child pages of a given page.

        Args:
            page_id: Parent page ID
            limit: Maximum results
            start: Pagination offset

        Returns:
            List of dicts with space_key and page
        """
        self.load_all_pickles()
        children = self._children_by_parent.get(str(page_id), [])
        return [
            {'space_key': sk, 'page': page}
            for sk, page in children[start:start + limit]
        ]

    def search_by_title(self, query: str) -> List[Dict[str, Any]]:
        """Simple title search across all pages.

        Args:
            query: Search query

        Returns:
            List of matching pages with space_key
        """
        self.load_all_pickles()
        query_lower = query.lower()
        results = []

        for (title, space_key), (sk, page) in self._pages_by_title.items():
            if query_lower in title.lower():
                results.append({'space_key': sk, 'page': page})

        return results

    def search_content(self, query: str, space_key: Optional[str] = None,
                       title_only: bool = False, limit: int = 50) -> List[Dict[str, Any]]:
        """Search pages by title and body content.

        Title matches are ranked higher than body matches.

        Args:
            query: Search query string
            space_key: Optional space key filter
            title_only: If True, only search titles
            limit: Maximum results to return

        Returns:
            List of matching pages with space_key and match_type
        """
        self.load_all_pickles()
        query_lower = query.lower()
        query_words = query_lower.split()
        title_results = []
        body_results = []

        for page_id, (sk, page) in self._pages_by_id.items():
            # Apply space filter
            if space_key and sk.upper() != space_key.upper():
                continue

            title = (page.get('title') or '').lower()

            # Check title match
            if all(w in title for w in query_words):
                title_results.append({
                    'space_key': sk,
                    'page': page,
                    'match_type': 'title'
                })
                continue

            # Check body match (skip if title_only)
            if not title_only:
                body_text = _extract_body_text(page).lower()
                if all(w in body_text for w in query_words):
                    body_results.append({
                        'space_key': sk,
                        'page': page,
                        'match_type': 'body'
                    })

        # Title matches first, then body matches
        return (title_results + body_results)[:limit]

    def find_page_by_title_flexible(self, title: str,
                                     space_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Flexible page lookup by title with fallbacks.

        Tries in order:
        1. Exact match (with space key if provided)
        2. Case-insensitive exact match across all spaces
        3. Partial title match (title contains query or query contains title)

        Args:
            title: Page title to search for
            space_key: Optional space key

        Returns:
            Dict with space_key and page, or None
        """
        self.load_all_pickles()

        # 1. Exact match with space key
        if space_key:
            result = self.get_page_by_title(title, space_key)
            if result:
                return result

        # 2. Case-insensitive exact match across all spaces (or filtered)
        title_lower = title.lower()
        for (t, sk), (space, page) in self._pages_by_title.items():
            if space_key and sk.upper() != space_key.upper():
                continue
            if t.lower() == title_lower:
                return {'space_key': space, 'page': page}

        # 3. Partial match - prefer shorter titles that contain the query
        candidates = []
        for (t, sk), (space, page) in self._pages_by_title.items():
            if space_key and sk.upper() != space_key.upper():
                continue
            if title_lower in t.lower() or t.lower() in title_lower:
                candidates.append({
                    'space_key': space,
                    'page': page,
                    'title_len': len(t)
                })

        if candidates:
            # Prefer closest length match
            candidates.sort(key=lambda c: abs(c['title_len'] - len(title)))
            return {'space_key': candidates[0]['space_key'], 'page': candidates[0]['page']}

        return None
