#!/usr/bin/env python3
"""Build WHOOSH search index from pickled Confluence data.

Usage:
    python build_index.py              # Rebuild entire index from all pickles
    python build_index.py --space XYZ  # Re-index just one space (delete + re-add)
"""

import sys
import os
import argparse
import logging

from config import get_config
from pickle_loader import PickleLoader
from indexer import ConfluenceIndexer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def rebuild_all(config):
    """Full rebuild of the WHOOSH index."""
    logger.info("Loading pickle files...")
    pickle_loader = PickleLoader(config.pickle_dir)
    pickle_loader.load_all_pickles()

    spaces = pickle_loader.get_all_spaces()
    logger.info(f"Loaded {len(spaces)} spaces")

    indexer = ConfluenceIndexer(config.index_dir)

    logger.info("Building search index (this may take 10-30 seconds)...")
    all_pages = pickle_loader.get_all_pages()
    indexed_count = indexer.index_all_pages(all_pages, clear_first=True)

    logger.info(f"Successfully indexed {indexed_count} pages")
    stats = indexer.get_stats()
    logger.info(f"Index statistics: {stats}")


def reindex_space(config, space_key: str):
    """Re-index a single space: delete old entries, add current ones."""
    pickle_loader = PickleLoader(config.pickle_dir)
    pickle_loader.load_all_pickles()

    space = pickle_loader.get_space(space_key)
    if not space:
        logger.error(f"Space {space_key} not found in pickle directory {config.pickle_dir}")
        available = [s['key'] for s in pickle_loader.get_all_spaces()]
        logger.info(f"Available spaces: {', '.join(sorted(available)[:20])}{'...' if len(available) > 20 else ''}")
        return 1

    indexer = ConfluenceIndexer(config.index_dir)

    # Get stats before
    stats_before = indexer.get_stats()
    logger.info(f"Index before: {stats_before['total_docs']} docs")

    # Delete existing pages for this space
    deleted = indexer.delete_space(space_key)
    logger.info(f"Deleted {deleted} existing pages for space {space_key}")

    # Get pages for this space and re-index
    pages = [(space_key, page) for page in space.get('sampled_pages', [])]
    logger.info(f"Re-indexing {len(pages)} pages for space {space_key}...")
    indexed_count = indexer.index_all_pages(pages, clear_first=False)

    stats_after = indexer.get_stats()
    logger.info(f"Re-indexed {indexed_count} pages for space {space_key}")
    logger.info(f"Index after: {stats_after['total_docs']} docs")
    return 0


def main():
    """Build the WHOOSH index."""
    parser = argparse.ArgumentParser(description="Build WHOOSH search index from pickled Confluence data.")
    parser.add_argument('--space', type=str, metavar='SPACE_KEY',
                        help='Re-index just one space (deletes old entries, adds current ones)')
    args = parser.parse_args()

    config = get_config()
    logger.info(f"Pickle directory: {config.pickle_dir}")
    logger.info(f"Index directory: {config.index_dir}")

    if args.space:
        logger.info(f"Re-indexing single space: {args.space}")
        return reindex_space(config, args.space)
    else:
        logger.info("Full index rebuild...")
        rebuild_all(config)
        return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error building index: {e}", exc_info=True)
        sys.exit(1)
