# File: config_loader.py
import configparser
import os

def load_confluence_settings(config_path='settings.ini'):
    """Load Confluence settings from the configuration file."""
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