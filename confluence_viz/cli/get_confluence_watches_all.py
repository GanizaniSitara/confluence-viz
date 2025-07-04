#!/usr/bin/env python3
"""
Confluence Watch Enumerator
Enumerates all watches for the current user in Confluence Data Center 9.2
Handles rate limiting, SSL issues, and loads credentials from config.ini
"""

import configparser
import requests
import json
import time
import sys
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlencode, quote_plus

# Suppress SSL warnings since we're not using SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class ConfluenceWatchEnumerator:
    def __init__(self, config_file='config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Load configuration
        try:
            self.base_url = self.config.get('confluence', 'base_url').rstrip('/')
            self.username = self.config.get('confluence', 'username')
            self.password = self.config.get('confluence', 'password')

            # Optional rate limiting settings
            self.max_retries = self.config.getint('rate_limiting', 'max_retries', fallback=5)
            self.base_delay = self.config.getfloat('rate_limiting', 'base_delay', fallback=1.0)
            self.max_delay = self.config.getfloat('rate_limiting', 'max_delay', fallback=60.0)

        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            print(f"Configuration error: {e}")
            print("Please ensure config.ini has the required sections and options.")
            sys.exit(1)

        # Setup session
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        # Disable SSL verification
        self.session.verify = False

    def exponential_backoff(self, attempt):
        """Calculate exponential backoff delay"""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        return delay

    def make_request(self, url, params=None):
        """Make HTTP request with rate limiting and retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    return response.json()

                elif response.status_code == 429:
                    # Rate limited - extract retry-after if available
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            delay = int(retry_after)
                        except ValueError:
                            delay = self.exponential_backoff(attempt)
                    else:
                        delay = self.exponential_backoff(attempt)

                    print(f"Rate limited (429). Waiting {delay} seconds before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(delay)
                    continue

                elif response.status_code == 401:
                    print("Authentication failed. Please check your credentials.")
                    sys.exit(1)

                elif response.status_code == 403:
                    print("Access forbidden. You may not have permission to access this resource.")
                    return None

                else:
                    print(f"HTTP {response.status_code}: {response.text}")
                    if attempt == self.max_retries - 1:
                        return None
                    time.sleep(self.exponential_backoff(attempt))

            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.exponential_backoff(attempt))

        return None

    def get_current_user(self):
        """Get current user information"""
        url = f"{self.base_url}/rest/api/user"
        print("Getting current user information...")

        user_data = self.make_request(url)
        if user_data:
            print(
                f"Authenticated as: {user_data.get('displayName', 'Unknown')} ({user_data.get('username', 'Unknown')})")
            return user_data
        else:
            print("Failed to get user information")
            return None

    def get_watched_content_cql(self, limit=50):
        """Get all watched content using CQL search"""
        url = f"{self.base_url}/rest/api/content/search"
        all_results = []
        start = 0

        print("Enumerating watched content using CQL search...")

        while True:
            # Use CQL to find content watched by current user
            cql_query = "watcher=currentUser()"
            params = {
                'cql': cql_query,
                'limit': limit,
                'start': start,
                'expand': 'space,version,ancestors'
            }

            print(f"Fetching results {start}-{start + limit - 1}...")
            data = self.make_request(url, params)

            if not data or 'results' not in data:
                break

            results = data['results']
            all_results.extend(results)

            print(f"Found {len(results)} items in this batch")

            # Check if we have more results
            if len(results) < limit or start + limit >= data.get('totalSize', 0):
                break

            start += limit

            # Small delay between requests to be nice to the server
            time.sleep(0.5)

        return all_results

    def get_watched_content_by_type(self, content_type='page', limit=50):
        """Get watched content filtered by type"""
        url = f"{self.base_url}/rest/api/content/search"
        all_results = []
        start = 0

        print(f"Enumerating watched {content_type} content...")

        while True:
            # Use CQL to find content watched by current user of specific type
            cql_query = f"type={content_type} AND watcher=currentUser()"
            params = {
                'cql': cql_query,
                'limit': limit,
                'start': start,
                'expand': 'space,version,ancestors'
            }

            print(f"Fetching {content_type} results {start}-{start + limit - 1}...")
            data = self.make_request(url, params)

            if not data or 'results' not in data:
                break

            results = data['results']
            all_results.extend(results)

            print(f"Found {len(results)} {content_type} items in this batch")

            # Check if we have more results
            if len(results) < limit or start + limit >= data.get('totalSize', 0):
                break

            start += limit

            # Small delay between requests
            time.sleep(0.5)

        return all_results

    def check_specific_watch(self, content_id):
        """Check if current user is watching specific content"""
        url = f"{self.base_url}/rest/api/user/watch/content/{content_id}"

        response_data = self.make_request(url)
        return response_data

    def format_content_info(self, content):
        """Format content information for display"""
        space_key = content.get('space', {}).get('key', 'Unknown')
        space_name = content.get('space', {}).get('name', 'Unknown')
        title = content.get('title', 'Untitled')
        content_type = content.get('type', 'unknown')
        content_id = content.get('id', 'unknown')

        # Get web URL if available
        web_url = content.get('_links', {}).get('webui', '')
        if web_url and not web_url.startswith('http'):
            web_url = f"{self.base_url}{web_url}"

        return {
            'id': content_id,
            'type': content_type,
            'title': title,
            'space_key': space_key,
            'space_name': space_name,
            'url': web_url
        }

    def export_to_json(self, data, filename='confluence_watches.json'):
        """Export watch data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Data exported to {filename}")
        except Exception as e:
            print(f"Failed to export data: {e}")

    def run(self):
        """Main execution method"""
        print("=" * 60)
        print("Confluence Watch Enumerator")
        print("=" * 60)

        # Get current user info
        user_info = self.get_current_user()
        if not user_info:
            return

        print("\n" + "-" * 40)

        # Get all watched content
        watched_content = self.get_watched_content_cql()

        if not watched_content:
            print("No watched content found or unable to retrieve watches.")
            return

        print(f"\nTotal watched items: {len(watched_content)}")
        print("\n" + "-" * 40)

        # Organize and display results
        formatted_data = []
        content_by_type = {}

        for content in watched_content:
            formatted = self.format_content_info(content)
            formatted_data.append(formatted)

            content_type = formatted['type']
            if content_type not in content_by_type:
                content_by_type[content_type] = []
            content_by_type[content_type].append(formatted)

        # Display summary by type
        print("Watch Summary by Content Type:")
        for content_type, items in content_by_type.items():
            print(f"  {content_type.title()}: {len(items)} items")

        print("\n" + "-" * 40)
        print("Detailed Watch List:")
        print("-" * 40)

        for item in formatted_data:
            print(f"Type: {item['type'].title()}")
            print(f"Title: {item['title']}")
            print(f"Space: {item['space_name']} ({item['space_key']})")
            print(f"ID: {item['id']}")
            if item['url']:
                print(f"URL: {item['url']}")
            print("-" * 40)

        # Export to JSON
        export_data = {
            'user_info': user_info,
            'watch_summary': {
                'total_watches': len(watched_content),
                'by_type': {k: len(v) for k, v in content_by_type.items()}
            },
            'watched_content': formatted_data,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }

        self.export_to_json(export_data)

        print(f"\nEnumeration complete! Found {len(watched_content)} watched items.")


def create_sample_config():
    """Create a sample config.ini file"""
    config_content = """[confluence]
base_url = https://confluence.barcapint.com
username = your_username
password = your_password

[rate_limiting]
# Maximum number of retries for rate-limited requests
max_retries = 5
# Base delay in seconds for exponential backoff
base_delay = 1.0
# Maximum delay in seconds
max_delay = 60.0
"""

    try:
        with open('config.ini', 'w') as f:
            f.write(config_content)
        print("Sample config.ini created. Please edit it with your credentials.")
    except Exception as e:
        print(f"Failed to create config.ini: {e}")


if __name__ == "__main__":
    import os

    # Check if config file exists
    if not os.path.exists('config.ini'):
        print("config.ini not found.")
        create_sample_config()
        print("Please edit config.ini with your Confluence credentials and run the script again.")
        sys.exit(1)

    try:
        enumerator = ConfluenceWatchEnumerator()
        enumerator.run()
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)