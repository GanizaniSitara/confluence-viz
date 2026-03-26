"""Fallback client for central Confluence instance (for attachments, etc.)."""

import logging
import requests
from typing import Optional, Dict, Any, List
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


def _log_http_error(response: requests.Response, context: str, **extra) -> None:
    """Log an HTTP error with status code, URL, and response body."""
    detail_parts = [
        f"HTTP {response.status_code}",
        f"URL: {response.request.method} {response.url}",
    ]
    if extra:
        detail_parts.append(f"params: {extra}")
    # Capture first 500 chars of response body for diagnostics
    body = response.text[:500] if response.text else "(empty body)"
    detail_parts.append(f"response: {body}")
    detail = " | ".join(detail_parts)

    if response.status_code == 404:
        logger.warning(f"404 NOT FOUND — {context} | {detail}")
    elif response.status_code == 403:
        logger.warning(f"403 FORBIDDEN — {context} | {detail}")
    elif response.status_code == 401:
        logger.warning(f"401 UNAUTHORIZED — {context} | {detail}")
    else:
        logger.error(f"HTTP ERROR — {context} | {detail}")


class ConfluenceFallbackClient:
    """Client for falling back to central Confluence instance."""

    def __init__(self, base_url: str, username: str, api_token: str,
                 verify_ssl: bool = True):
        """Initialize fallback client.

        Args:
            base_url: Confluence base URL (e.g., https://your-instance.atlassian.net/wiki)
            username: Confluence username/email
            api_token: API token for authentication
            verify_ssl: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.api_token = api_token
        self.verify_ssl = verify_ssl
        self.auth = HTTPBasicAuth(username, api_token) if username and api_token else None

        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def is_configured(self) -> bool:
        """Check if fallback client is properly configured.

        Returns:
            True if credentials are configured
        """
        return bool(self.base_url and self.username and self.api_token)

    def get_page_attachments(self, page_id: str) -> Optional[Dict[str, Any]]:
        """Get attachments for a page.

        Args:
            page_id: Page ID

        Returns:
            Attachments data or None
        """
        if not self.is_configured():
            logger.warning("Fallback client not configured")
            return None

        try:
            url = f"{self.base_url}/rest/api/content/{page_id}/child/attachment"
            response = requests.get(
                url,
                auth=self.auth,
                verify=self.verify_ssl,
                timeout=10
            )
            if not response.ok:
                _log_http_error(response, f"get_page_attachments(page_id={page_id})")
                return None
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error fetching attachments for page {page_id}: {e}")
            return None

    def download_attachment(self, download_url: str) -> Optional[bytes]:
        """Download an attachment.

        Args:
            download_url: Attachment download URL

        Returns:
            Attachment bytes or None
        """
        if not self.is_configured():
            logger.warning("Fallback client not configured")
            return None

        try:
            # Handle relative URLs
            if download_url.startswith('/'):
                download_url = f"{self.base_url}{download_url}"

            response = requests.get(
                download_url,
                auth=self.auth,
                verify=self.verify_ssl,
                timeout=30
            )
            if not response.ok:
                _log_http_error(response, f"download_attachment(url={download_url})")
                return None
            return response.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error downloading attachment {download_url}: {e}")
            return None

    def get_page(self, page_id: str, expand: str = "body.storage,version") -> Optional[Dict[str, Any]]:
        """Get full page data from central instance.

        Args:
            page_id: Page ID
            expand: Fields to expand

        Returns:
            Page data or None
        """
        if not self.is_configured():
            logger.warning("Fallback client not configured")
            return None

        try:
            url = f"{self.base_url}/rest/api/content/{page_id}"
            params = {'expand': expand}
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                verify=self.verify_ssl,
                timeout=10
            )
            if not response.ok:
                _log_http_error(response, f"get_page(page_id={page_id})", expand=expand)
                return None
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error fetching page {page_id}: {e}")
            return None

    def search_cql(self, cql: str, limit: int = 25) -> Optional[Dict[str, Any]]:
        """Search using CQL on central instance.

        Args:
            cql: CQL query
            limit: Result limit

        Returns:
            Search results or None
        """
        if not self.is_configured():
            logger.warning("Fallback client not configured")
            return None

        try:
            url = f"{self.base_url}/rest/api/content/search"
            params = {'cql': cql, 'limit': limit}
            response = requests.get(
                url,
                auth=self.auth,
                params=params,
                verify=self.verify_ssl,
                timeout=15
            )
            if not response.ok:
                _log_http_error(response, f"search_cql(cql={cql!r}, limit={limit})")
                return None
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error searching CQL '{cql}': {e}")
            return None

    def get_page_children(self, page_id: str, limit: int = 25,
                          start: int = 0) -> Optional[List[Dict[str, Any]]]:
        """Get child pages of a page from the live Confluence API.

        Returns:
            List of child page dicts or None on error
        """
        if not self.is_configured():
            return None
        try:
            url = f"{self.base_url}/rest/api/content/{page_id}/child/page"
            params = {'limit': limit, 'start': start, 'expand': 'version,space'}
            response = requests.get(
                url, auth=self.auth, params=params,
                verify=self.verify_ssl, timeout=10,
            )
            if not response.ok:
                _log_http_error(response, f"get_page_children(page_id={page_id})")
                return None
            return response.json().get('results', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error fetching children for page {page_id}: {e}")
            return None

    def get_page_labels(self, page_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get labels for a page from the live Confluence API.

        Returns:
            List of label dicts or None on error
        """
        if not self.is_configured():
            return None
        try:
            url = f"{self.base_url}/rest/api/content/{page_id}/label"
            response = requests.get(
                url, auth=self.auth,
                verify=self.verify_ssl, timeout=10,
            )
            if not response.ok:
                _log_http_error(response, f"get_page_labels(page_id={page_id})")
                return None
            return response.json().get('results', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error fetching labels for page {page_id}: {e}")
            return None

    def get_page_comments(self, page_id: str,
                          limit: int = 25) -> Optional[List[Dict[str, Any]]]:
        """Get comments on a page from the live Confluence API.

        Returns:
            List of comment dicts or None on error
        """
        if not self.is_configured():
            return None
        try:
            url = f"{self.base_url}/rest/api/content/{page_id}/child/comment"
            params = {'limit': limit, 'expand': 'body.storage,version,author'}
            response = requests.get(
                url, auth=self.auth, params=params,
                verify=self.verify_ssl, timeout=10,
            )
            if not response.ok:
                _log_http_error(response, f"get_page_comments(page_id={page_id})")
                return None
            return response.json().get('results', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error fetching comments for page {page_id}: {e}")
            return None
