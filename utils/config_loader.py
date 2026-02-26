# Loads configuration for Confluence visualization.
# Config file is parsed once and cached for the lifetime of the process.

import configparser
import os
from functools import lru_cache


@lru_cache(maxsize=4)
def _parse_config(config_path):
    """Parse the config file once and cache the result."""
    config = configparser.ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)
    return config


def load_confluence_settings(config_path='settings.ini'):
    """Load Confluence API settings from the configuration file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    config = _parse_config(config_path)
    confluence = config['confluence']
    return {
        'base_url': confluence.get('base_url'),
        'username': confluence.get('username'),
        'password': confluence.get('password'),
        'verify_ssl': confluence.getboolean('verify_ssl', fallback=True)
    }


def load_data_settings(config_path='settings.ini'):
    """Load data storage settings with defaults."""
    config = _parse_config(config_path)
    defaults = {
        'pickle_dir': 'temp',
        'remote_full_pickle_dir': '',
    }
    if 'data' in config:
        for key in defaults:
            val = config['data'].get(key)
            if val is not None:
                defaults[key] = val
    return {
        'pickle_dir': defaults['pickle_dir'],
        'remote_full_pickle_dir': defaults['remote_full_pickle_dir'] or None
    }


def load_visualization_settings(config_path='settings.ini'):
    """Load visualization settings with defaults."""
    config = _parse_config(config_path)
    defaults = {
        'default_clusters': '20',
        'default_min_pages': '5',
        'spaces_dir': 'temp/full_pickles'
    }
    if 'visualization' in config:
        for key in defaults:
            val = config['visualization'].get(key)
            if val is not None:
                defaults[key] = val

    result = {}
    for key, value in defaults.items():
        if key in ('default_clusters', 'default_min_pages'):
            result[key] = int(value)
        else:
            result[key] = value

    # Also include confluence_base_url for visualization links
    if 'confluence' in config:
        result['confluence_base_url'] = config['confluence'].get('base_url', '')

    return result
