"""
Enhanced configuration management for Confluence visualization.
Provides validation, defaults, and environment variable support.
"""

import os
import configparser
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from pathlib import Path


@dataclass
class ConfluenceConfig:
    """Configuration for Confluence API connection."""
    base_url: str
    username: str
    password: str
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 5
    
    def validate(self) -> None:
        """Validate configuration values."""
        if not self.base_url:
            raise ValueError("Confluence base_url is required")
        if not self.username:
            raise ValueError("Confluence username is required")
        if not self.password:
            raise ValueError("Confluence password is required")
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if self.max_retries < 0:
            raise ValueError("Max retries must be non-negative")


@dataclass
class VisualizationConfig:
    """Configuration for visualization settings."""
    default_clusters: int = 20
    default_min_pages: int = 5
    remote_full_pickle_dir: Optional[str] = None
    gradient_steps: int = 10
    treemap_width: int = 3000
    treemap_height: int = 2000
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.default_clusters <= 0:
            raise ValueError("Default clusters must be positive")
        if self.default_min_pages < 0:
            raise ValueError("Default min pages must be non-negative")
        if self.gradient_steps <= 0:
            raise ValueError("Gradient steps must be positive")
        if self.treemap_width <= 0 or self.treemap_height <= 0:
            raise ValueError("Treemap dimensions must be positive")


@dataclass
class DataCollectionConfig:
    """Configuration for data collection settings."""
    spaces_page_limit: int = 50
    content_page_limit: int = 100
    enable_checkpointing: bool = True
    checkpoint_file: str = "confluence_checkpoint.json"
    temp_directory: str = "temp"
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.spaces_page_limit <= 0:
            raise ValueError("Spaces page limit must be positive")
        if self.content_page_limit <= 0:
            raise ValueError("Content page limit must be positive")


@dataclass
class AppConfig:
    """Main application configuration container."""
    confluence: ConfluenceConfig
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)
    data_collection: DataCollectionConfig = field(default_factory=DataCollectionConfig)
    
    def validate(self) -> None:
        """Validate all configuration sections."""
        self.confluence.validate()
        self.visualization.validate()
        self.data_collection.validate()


class ConfigManager:
    """
    Enhanced configuration manager with validation and environment variable support.
    """
    
    def __init__(self):
        self._config: Optional[AppConfig] = None
    
    def load_config(self, config_path: str = 'settings.ini') -> AppConfig:
        """
        Load configuration from file with environment variable fallbacks.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Validated application configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If configuration is invalid
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file {config_path} not found.")
        
        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)
        
        # Load Confluence configuration
        confluence_config = self._load_confluence_config(config_parser)
        
        # Load visualization configuration
        visualization_config = self._load_visualization_config(config_parser)
        
        # Load data collection configuration
        data_collection_config = self._load_data_collection_config(config_parser)
        
        # Create and validate full configuration
        app_config = AppConfig(
            confluence=confluence_config,
            visualization=visualization_config,
            data_collection=data_collection_config
        )
        app_config.validate()
        
        self._config = app_config
        return app_config
    
    def _load_confluence_config(self, config_parser: configparser.ConfigParser) -> ConfluenceConfig:
        """Load Confluence-specific configuration."""
        if 'confluence' not in config_parser:
            raise ValueError("Missing [confluence] section in configuration")
        
        section = config_parser['confluence']
        
        return ConfluenceConfig(
            base_url=self._get_value(section, 'base_url', env_var='CONFLUENCE_BASE_URL'),
            username=self._get_value(section, 'username', env_var='CONFLUENCE_USERNAME'),
            password=self._get_value(section, 'password', env_var='CONFLUENCE_PASSWORD'),
            verify_ssl=section.getboolean('verify_ssl', fallback=True),
            timeout=section.getint('timeout', fallback=30),
            max_retries=section.getint('max_retries', fallback=5)
        )
    
    def _load_visualization_config(self, config_parser: configparser.ConfigParser) -> VisualizationConfig:
        """Load visualization-specific configuration."""
        section = config_parser.get('visualization', fallback={})
        
        return VisualizationConfig(
            default_clusters=int(section.get('default_clusters', '20')),
            default_min_pages=int(section.get('default_min_pages', '5')),
            remote_full_pickle_dir=section.get('remote_full_pickle_dir') or None,
            gradient_steps=int(section.get('gradient_steps', '10')),
            treemap_width=int(section.get('treemap_width', '3000')),
            treemap_height=int(section.get('treemap_height', '2000'))
        )
    
    def _load_data_collection_config(self, config_parser: configparser.ConfigParser) -> DataCollectionConfig:
        """Load data collection-specific configuration."""
        section = config_parser.get('data_collection', fallback={})
        
        return DataCollectionConfig(
            spaces_page_limit=int(section.get('spaces_page_limit', '50')),
            content_page_limit=int(section.get('content_page_limit', '100')),
            enable_checkpointing=section.get('enable_checkpointing', 'true').lower() == 'true',
            checkpoint_file=section.get('checkpoint_file', 'confluence_checkpoint.json'),
            temp_directory=section.get('temp_directory', 'temp')
        )
    
    def _get_value(self, section: configparser.SectionProxy, key: str, env_var: Optional[str] = None) -> str:
        """
        Get configuration value with environment variable fallback.
        
        Args:
            section: Configuration section
            key: Configuration key
            env_var: Environment variable name for fallback
            
        Returns:
            Configuration value
            
        Raises:
            ValueError: If value is not found
        """
        # Try configuration file first
        value = section.get(key)
        if value:
            return value
        
        # Try environment variable
        if env_var:
            env_value = os.getenv(env_var)
            if env_value:
                return env_value
        
        raise ValueError(f"Missing required configuration: {key}")
    
    def get_config(self) -> AppConfig:
        """
        Get the current configuration.
        
        Returns:
            Current application configuration
            
        Raises:
            RuntimeError: If configuration hasn't been loaded
        """
        if self._config is None:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        return self._config
    
    def create_example_config(self, output_path: str = 'settings.example.ini') -> None:
        """
        Create an example configuration file.
        
        Args:
            output_path: Path to write example configuration
        """
        example_config = """[confluence]
# Base URL for your Confluence instance (e.g., http://localhost:8090 or https://your-domain.atlassian.net)
base_url = https://your-confluence-instance.atlassian.net
username = your_username
password = your_api_token
verify_ssl = True
timeout = 30
max_retries = 5

[visualization]
# Default number of clusters for analysis
default_clusters = 20
# Minimum pages filter default
default_min_pages = 5
# Number of color gradient steps
gradient_steps = 10
# Treemap visualization dimensions
treemap_width = 3000
treemap_height = 2000
# Optional: Path to directory containing pre-generated full pickle files
# remote_full_pickle_dir = /mnt/shared_pickles/

[data_collection]
# Pagination limits for API calls
spaces_page_limit = 50
content_page_limit = 100
# Enable checkpointing for resumable operations
enable_checkpointing = True
checkpoint_file = confluence_checkpoint.json
temp_directory = temp
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(example_config)
        print(f"Example configuration written to {output_path}")


# Global configuration manager instance
_config_manager = ConfigManager()


def load_config(config_path: str = 'settings.ini') -> AppConfig:
    """
    Load application configuration.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Validated application configuration
    """
    return _config_manager.load_config(config_path)


def get_config() -> AppConfig:
    """
    Get the current application configuration.
    
    Returns:
        Current application configuration
    """
    return _config_manager.get_config()


def create_example_config(output_path: str = 'settings.example.ini') -> None:
    """
    Create an example configuration file.
    
    Args:
        output_path: Path to write example configuration
    """
    _config_manager.create_example_config(output_path)