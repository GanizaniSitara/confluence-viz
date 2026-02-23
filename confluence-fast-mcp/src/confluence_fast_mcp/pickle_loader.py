"""Load and manage pickled Confluence data."""

import os
import pickle
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


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
