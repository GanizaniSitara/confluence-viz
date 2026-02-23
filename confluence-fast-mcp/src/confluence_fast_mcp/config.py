"""Configuration management for Confluence Fast MCP."""

import os
import configparser
from pathlib import Path
from typing import Optional


class Config:
    """Configuration loader for Confluence Fast MCP."""

    def __init__(self, settings_file: Optional[str] = None):
        """Initialize configuration.

        Args:
            settings_file: Path to settings.ini file. If None, looks in standard locations.
        """
        self.config = configparser.ConfigParser()

        # Find settings.ini
        if settings_file and os.path.exists(settings_file):
            self.settings_file = settings_file
        else:
            # Look in standard locations
            possible_locations = [
                'settings.ini',
                os.path.join(os.getcwd(), 'settings.ini'),
                os.path.join(Path(__file__).parent.parent.parent, 'settings.ini'),
            ]
            self.settings_file = None
            for location in possible_locations:
                if os.path.exists(location):
                    self.settings_file = location
                    break

        if self.settings_file:
            self.config.read(self.settings_file)

    def _get(self, section: str, key: str, default: str = '') -> str:
        """Get config value with environment variable override."""
        # Environment variables take precedence
        env_key = f"{section.upper()}_{key.upper()}"
        if env_key in os.environ:
            return os.environ[env_key]

        # Fall back to settings.ini
        try:
            return self.config.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    @property
    def pickle_dir(self) -> str:
        """Get pickle directory path."""
        # Check PICKLE_DIR env var first
        if 'PICKLE_DIR' in os.environ:
            return os.environ['PICKLE_DIR']
        return self._get('data', 'pickle_dir', '/tmp/confluence_pickles')

    @property
    def index_dir(self) -> str:
        """Get WHOOSH index directory path."""
        return self._get('data', 'index_dir',
                        os.path.join(Path(__file__).parent.parent.parent, 'whoosh_index'))

    @property
    def confluence_url(self) -> str:
        """Get Confluence base URL for fallback."""
        return os.environ.get('CONFLUENCE_URL',
                             self._get('confluence', 'url', ''))

    @property
    def confluence_username(self) -> str:
        """Get Confluence username for fallback."""
        return os.environ.get('CONFLUENCE_USERNAME',
                             self._get('confluence', 'username', ''))

    @property
    def confluence_api_token(self) -> str:
        """Get Confluence API token for fallback."""
        return os.environ.get('CONFLUENCE_API_TOKEN',
                             self._get('confluence', 'api_token', ''))

    @property
    def server_host(self) -> str:
        """Get server host."""
        return self._get('server', 'host', 'localhost')

    @property
    def server_port(self) -> int:
        """Get server port."""
        try:
            return int(self._get('server', 'port', '8080'))
        except ValueError:
            return 8080


# Global config instance
_config: Optional[Config] = None


def get_config(settings_file: Optional[str] = None) -> Config:
    """Get or create global config instance."""
    global _config
    if _config is None:
        _config = Config(settings_file)
    return _config
