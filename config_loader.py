# File: config_loader.py
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
        'api_base_url': confluence.get('api_base_url'),
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
    
    # If base_url is not specified but api_base_url is, derive base_url from api_base_url
    if not config['confluence'].get('base_url') and config['confluence'].get('api_base_url'):
        api_base_url = config['confluence'].get('api_base_url')
        # Remove trailing /rest/api if present
        if '/rest/api' in api_base_url:
            base_url = api_base_url.split('/rest/api')[0]
        else:
            base_url = api_base_url
        config['confluence']['base_url'] = base_url
    
    # Return as dictionary
    return {
        'confluence_base_url': config['confluence'].get('base_url'),
        'default_clusters': int(config['visualization'].get('default_clusters')),
        'default_min_pages': int(config['visualization'].get('default_min_pages'))
    }