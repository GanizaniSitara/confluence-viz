"""Fallback client for central Confluence instance (for attachments, etc.)."""

import logging
import requests
from typing import Optional, Dict, Any
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


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
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching attachments for page {page_id}: {e}")
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
            response.raise_for_status()
            return response.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading attachment: {e}")
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
            response = requests.get(
                url,
                auth=self.auth,
                params={'expand': expand},
                verify=self.verify_ssl,
                timeout=10
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page_id}: {e}")
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
            response = requests.get(
                url,
                auth=self.auth,
                params={'cql': cql, 'limit': limit},
                verify=self.verify_ssl,
                timeout=15
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error searching with CQL '{cql}': {e}")
            return None
