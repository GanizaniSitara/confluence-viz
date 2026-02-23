"""CQL (Confluence Query Language) to WHOOSH query translation."""

import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class CQLParser:
    """Simple CQL parser for basic query translation."""

    def parse(self, cql: str) -> Tuple[str, Optional[str]]:
        """Parse CQL query and extract search terms and filters.

        Supports:
        - text ~ "search term"
        - title ~ "title search"
        - space = KEY
        - type = page (always matches)
        - AND/OR operators (basic support)

        Args:
            cql: CQL query string

        Returns:
            Tuple of (whoosh_query_string, space_key_filter)
        """
        if not cql:
            return ("*", None)

        # Normalize whitespace
        cql = ' '.join(cql.split())

        # Extract space filter
        space_key = self._extract_space_filter(cql)

        # Extract text search
        search_terms = self._extract_search_terms(cql)

        # Build WHOOSH query string
        if search_terms:
            whoosh_query = search_terms
        else:
            whoosh_query = "*"

        logger.debug(f"Parsed CQL '{cql}' -> query='{whoosh_query}', space='{space_key}'")

        return (whoosh_query, space_key)

    def _extract_space_filter(self, cql: str) -> Optional[str]:
        """Extract space key from CQL.

        Args:
            cql: CQL query string

        Returns:
            Space key or None
        """
        # Match: space = KEY or space = "KEY"
        patterns = [
            r'space\s*=\s*["\']?([A-Z0-9_-]+)["\']?',
            r'space\s+in\s+\(([^)]+)\)',  # space in (KEY1, KEY2) - take first
        ]

        for pattern in patterns:
            match = re.search(pattern, cql, re.IGNORECASE)
            if match:
                space_keys = match.group(1).strip()
                # If multiple keys, take first (basic support)
                if ',' in space_keys:
                    space_keys = space_keys.split(',')[0].strip(' "\'"')
                return space_keys.strip(' "\'"')

        return None

    def _extract_search_terms(self, cql: str) -> str:
        """Extract and combine search terms from CQL.

        Args:
            cql: CQL query string

        Returns:
            Combined search terms for WHOOSH
        """
        terms = []

        # text ~ "search term"
        text_matches = re.finditer(r'text\s*~\s*["\']([^"\']+)["\']', cql, re.IGNORECASE)
        for match in text_matches:
            terms.append(match.group(1))

        # title ~ "title search"
        title_matches = re.finditer(r'title\s*~\s*["\']([^"\']+)["\']', cql, re.IGNORECASE)
        for match in title_matches:
            # Boost title searches
            terms.append(f"title:({match.group(1)})^2")

        # Handle simple AND operator
        if 'AND' in cql.upper():
            # WHOOSH uses AND by default, so just join
            return ' '.join(terms) if terms else ''

        # Handle simple OR operator
        if 'OR' in cql.upper():
            return ' OR '.join(terms) if terms else ''

        # Default: join with AND
        return ' '.join(terms) if terms else ''


def translate_cql(cql: str) -> Tuple[str, Optional[str]]:
    """Translate CQL to WHOOSH query.

    Args:
        cql: CQL query string

    Returns:
        Tuple of (whoosh_query, space_key_filter)
    """
    parser = CQLParser()
    return parser.parse(cql)


# Example usage and tests
if __name__ == '__main__':
    test_queries = [
        'text ~ "kubernetes"',
        'space = TECH',
        'title ~ "getting started"',
        'text ~ "api" AND space = DOCS',
        'text ~ "docker" OR text ~ "containers"',
        'space = DEV AND title ~ "setup"',
    ]

    parser = CQLParser()
    for query in test_queries:
        whoosh_q, space = parser.parse(query)
        print(f"CQL: {query}")
        print(f"  -> WHOOSH: {whoosh_q}, Space: {space}")
        print()
