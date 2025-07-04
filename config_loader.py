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
    """Load visualization settings from the configuration file."""
    config = configparser.ConfigParser()
    
    # Set default values for visualization section only
    config['visualization'] = {
        'default_clusters': '20',
        'default_min_pages': '5',
        'remote_full_pickle_dir': '',  # Default to empty string, meaning not set
        'pickle_dir': 'temp'  # Default pickle directory
    }
    
    # Override with values from config file if it exists
    if os.path.exists(config_path):
        # Ensure we read into the existing config object to merge,
        # or handle sections carefully if they might not exist in defaults.
        # Reading directly might overwrite the whole config if not careful with sections.
        # A safer way is to read into a new parser and then copy values.
        temp_config = configparser.ConfigParser()
        temp_config.read(config_path)
        if 'visualization' in temp_config:
            for key, value in temp_config['visualization'].items():
                config['visualization'][key] = value
    
    # Return as dictionary
    return {
        'default_clusters': int(config['visualization'].get('default_clusters')),
        'default_min_pages': int(config['visualization'].get('default_min_pages')),
        'remote_full_pickle_dir': config['visualization'].get('remote_full_pickle_dir') if config['visualization'].get('remote_full_pickle_dir') else None,
        'pickle_dir': config['visualization'].get('pickle_dir')
    }