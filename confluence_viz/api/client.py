"""
Confluence API client for handling HTTP requests and basic API operations.
"""

import requests
import time
import sys
import urllib3
from typing import Dict, Any, Optional, Tuple
from ..config.loader import load_confluence_settings
from ..utils.logging import get_logger


class ConfluenceAPIClient:
    """
    A client for interacting with the Confluence REST API.
    
    Handles authentication, rate limiting, and basic error handling.
    """
    
    def __init__(self, config_path: str = 'settings.ini'):
        """Initialize the API client with configuration."""
        self.logger = get_logger(__name__)
        self.settings = load_confluence_settings(config_path)
        self.base_url = self.settings['base_url']
        self.api_endpoint = "/rest/api"
        self.username = self.settings['username']
        self.password = self.settings['password']
        self.verify_ssl = self.settings['verify_ssl']
        
        self.logger.info(f"Initialized API client for {self.base_url}")
        
        # Suppress InsecureRequestWarning if VERIFY_SSL is False
        if not self.verify_ssl:
            self.logger.warning("SSL verification disabled")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    def get_with_retry(
        self, 
        url: str, 
        params: Optional[Dict[str, Any]] = None, 
        max_retries: int = 5
    ) -> requests.Response:
        """
        Make a GET request with exponential backoff retry logic.
        
        Args:
            url: The URL to request
            params: Query parameters
            max_retries: Maximum number of retries
            
        Returns:
            requests.Response object
        """
        backoff = 1
        retries = 0
        
        while retries < max_retries:
            try:
                resp = requests.get(
                    url, 
                    params=params, 
                    auth=(self.username, self.password), 
                    verify=self.verify_ssl,
                    timeout=30
                )
                
                if resp.status_code == 429:
                    self.logger.warning(f"Rate limited (429). Retrying {url} in {backoff}s... (attempt {retries + 1}/{max_retries})")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    retries += 1
                    continue
                    
                if resp.status_code >= 400:
                    self.logger.error(f"HTTP {resp.status_code} error fetching {url}: {resp.text[:200]}{'...' if len(resp.text) > 200 else ''}")
                
                return resp
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for {url}: {e}")
                if retries < max_retries - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    retries += 1
                    continue
                raise
        
        self.logger.error(f"Max retries ({max_retries}) exceeded for {url}")
        raise Exception(f"Max retries ({max_retries}) exceeded for {url}")
    
    def get_api_url(self, endpoint: str) -> str:
        """Construct full API URL from endpoint."""
        return f"{self.base_url}{self.api_endpoint}{endpoint}"
    
    def get_spaces(self, start: int = 0, limit: int = 50) -> requests.Response:
        """
        Fetch spaces from Confluence.
        
        Args:
            start: Starting index for pagination
            limit: Maximum number of results to return
            
        Returns:
            requests.Response containing space data
        """
        url = self.get_api_url("/space")
        params = {"start": start, "limit": limit}
        return self.get_with_retry(url, params=params)
    
    def get_pages_for_space(
        self, 
        space_key: str, 
        start: int = 0, 
        limit: int = 100,
        expand: str = "version"
    ) -> requests.Response:
        """
        Fetch pages for a specific space.
        
        Args:
            space_key: The space key to fetch pages for
            start: Starting index for pagination
            limit: Maximum number of results to return
            expand: Additional fields to expand in the response
            
        Returns:
            requests.Response containing page data
        """
        url = self.get_api_url("/content")
        params = {
            "type": "page",
            "spaceKey": space_key,
            "start": start,
            "limit": limit,
            "expand": expand
        }
        return self.get_with_retry(url, params=params)
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the connection to Confluence.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            url = self.get_api_url("/space")
            params = {"start": 0, "limit": 1}
            resp = self.get_with_retry(url, params=params)
            
            if resp.status_code == 200:
                return True, "Connection successful"
            else:
                return False, f"HTTP {resp.status_code}: {resp.text}"
                
        except Exception as e:
            return False, f"Connection failed: {str(e)}"


# Convenience function for backward compatibility
def get_with_retry(url: str, params: Optional[Dict[str, Any]] = None, auth: Optional[Tuple[str, str]] = None, verify: bool = False) -> requests.Response:
    """
    Backward compatibility function for the original get_with_retry.
    Consider using ConfluenceAPIClient instead for new code.
    """
    backoff = 1
    while True:
        resp = requests.get(url, params=params, auth=auth, verify=verify)
        if resp.status_code == 429:
            print(f"Warning: Rate limited (429). Retrying {url} in {backoff}s...", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        if resp.status_code >= 400:
            print(f"Error {resp.status_code} fetching {url}. Response: {resp.text}", file=sys.stderr)
        return resp