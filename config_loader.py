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

def load_data_settings(config_path='settings.ini'):
    """Load data storage settings from the configuration file."""
    config = configparser.ConfigParser()
    
    # Set default values for data section
    config['data'] = {
        'pickle_dir': 'temp',  # Default pickle directory
        'remote_full_pickle_dir': ''  # Default to empty string, meaning not set
    }
    
    # Override with values from config file if it exists
    if os.path.exists(config_path):
        temp_config = configparser.ConfigParser()
        temp_config.read(config_path)
        if 'data' in temp_config:
            for key, value in temp_config['data'].items():
                config['data'][key] = value
    
    # Return as dictionary
    return {
        'pickle_dir': config['data'].get('pickle_dir'),
        'remote_full_pickle_dir': config['data'].get('remote_full_pickle_dir') if config['data'].get('remote_full_pickle_dir') else None
    }

def load_visualization_settings(config_path='settings.ini'):
    """Load visualization settings from the configuration file."""
    config = configparser.ConfigParser()
    
    #Set default values for visualization section only
    config['visualization'] = {
        'default_clusters': '20',
        'default_min_pages': '5',
        'spaces_dir': 'temp/full_pickles'
    }
    
    # Override with values from config file if it exists
    if os.path.exists(config_path):
        temp_config = configparser.ConfigParser()
        temp_config.read(config_path)
        if 'visualization' in temp_config:
            for key, value in temp_config['visualization'].items():
                config['visualization'][key] = value
    
    # Return as dictionary with all settings
    result = {}
    for key, value in config['visualization'].items():
        # Convert numeric values to int
        if key in ['default_clusters', 'default_min_pages']:
            result[key] = int(value)
        else:
            result[key] = value
    return result