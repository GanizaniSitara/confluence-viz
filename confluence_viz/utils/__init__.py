"""
Utility modules for Confluence visualization.
"""

from .html_cleaner import clean_confluence_html
from .color_utils import (
    generate_gradient_colors,
    calculate_percentile_thresholds,
    get_color_for_avg_timestamp_percentile,
    GRADIENT_STEPS,
    GREY_COLOR_HEX
)
from .logging import (
    setup_logging,
    get_logger,
    ProgressLogger,
    set_log_level
)

__all__ = [
    # HTML processing
    'clean_confluence_html',
    # Color utilities
    'generate_gradient_colors',
    'calculate_percentile_thresholds', 
    'get_color_for_avg_timestamp_percentile',
    'GRADIENT_STEPS',
    'GREY_COLOR_HEX',
    # Logging
    'setup_logging',
    'get_logger',
    'ProgressLogger',
    'set_log_level',
]