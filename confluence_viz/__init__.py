"""
Confluence Visualization Package

A comprehensive toolkit for analyzing, visualizing, and managing Confluence instances.
Provides data collection, semantic analysis, clustering, visualization, and administrative tools.
"""

__version__ = "0.1.0"
__author__ = "Confluence Analysis Team"
__email__ = "confluence-analysis@example.com"

# Import key classes and functions for easy access
from .config.loader import load_confluence_settings, load_visualization_settings
from .utils.html_cleaner import clean_confluence_html

__all__ = [
    "load_confluence_settings",
    "load_visualization_settings", 
    "clean_confluence_html",
]