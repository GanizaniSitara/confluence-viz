# description: Loads configuration for Confluence visualization.

import configparser
import os

def load_confluence_settings(config_path='settings.ini'):
    """Load Confluence API settings from the configuration file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    config = configparser.ConfigParser()
    config.read(config_path)
    confluence = config['confluence']
    return {
        'base_url': confluence.get('base_url'),
        'username': confluence.get('username'),
        'password': confluence.get('password'),
        'verify_ssl': confluence.getboolean('verify_ssl', fallback=True)
    }

def load_visualization_settings(config_path='settings.ini'):
    """Load visualization settings including Confluence base URL from the configuration file."""
    # Try to load config file, use defaults if not available
    config = configparser.ConfigParser()
    
    # Set default values
    config['confluence'] = {
        'base_url': 'http://example.atlassian.net'
    }
    config['visualization'] = {
        'default_clusters': '20',
        'default_min_pages': '5'
    }
    
    # Override with values from config file if it exists
    if os.path.exists(config_path):
        config.read(config_path)
    
    # Return as dictionary
    return {
        'base_url': config['confluence'].get('base_url'),
        'default_clusters': int(config['visualization'].get('default_clusters')),
        'default_min_pages': int(config['visualization'].get('default_min_pages'))
    }