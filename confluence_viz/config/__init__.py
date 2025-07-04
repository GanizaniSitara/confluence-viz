"""
Configuration package for Confluence visualization.
Provides centralized configuration management with validation and environment variable support.
"""

from .settings import (
    AppConfig,
    ConfluenceConfig,
    VisualizationConfig,
    DataCollectionConfig,
    ConfigManager,
    load_config,
    get_config,
    create_example_config
)

# Backward compatibility imports
from .loader import load_confluence_settings, load_visualization_settings

__all__ = [
    'AppConfig',
    'ConfluenceConfig', 
    'VisualizationConfig',
    'DataCollectionConfig',
    'ConfigManager',
    'load_config',
    'get_config',
    'create_example_config',
    # Backward compatibility
    'load_confluence_settings',
    'load_visualization_settings',
]