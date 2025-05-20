# description: Seeds data for Confluence visualization.

import argparse
import json
import os
import random
import string
import sys
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_key(existing_keys, length=5):
    while True:
        key = "".join(random.choices(string.ascii_uppercase, k=length))
        if key not in existing_keys:
            return key

def read_seeds(path: Path):
    if not path.exists():
        return ["Enterprise", "Application", "Management", "Operations"]
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

def sleep_with_backoff(attempt):
    time.sleep(min(2 ** attempt, 60))

def post_with_retry(url, payload, auth, verify, headers=None):
    attempt = 0
    while True:
        resp = requests.post(url, json=payload, auth=auth, verify=verify, headers=headers)
        if resp.status_code == 429:
            attempt += 1
            sleep_with_backoff(attempt)
            continue
        return resp

# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------

def create_space(base_url, auth, verify, key, name, desc=""):
    url = f"{base_url}/rest/api/space"
    payload = {
        "key": key,
        "name": name,
        "description": {
            "plain": {
                "value": desc or name,
                "representation": "plain",
            }
        },
    }
    r = post_with_retry(url, payload, auth, verify)
    return r.ok

def generate_content_with_corporate_lorem(paragraphs=2):
    """
    Generate inventive content using the CorporateLorem API.
    The API endpoint is:
    http://corporatelorem.kovah.de/api/[amount of paragraphs]?format=text
    Appending format=text returns plain text; if paragraph tags are needed,
    the API can be modified with an additional query parameter.
    """
    try:
        url = f"http://corporatelorem.kovah.de/api/{paragraphs}?format=text"
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error getting CorporateLorem content: status {response.status_code}", file=sys.stderr)
            return "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
    except Exception as e:
        print(f"Error generating content from CorporateLorem API: {e}", file=sys.stderr)
        return "Lorem ipsum dolor sit amet, consectetur adipiscing elit."

def create_page(base_url, auth, verify, space_key, title, content=None, use_ollama=False):
    """
    Create a page in Confluence.
    If use_ollama is True and no content is provided, generate content using CorporateLorem API.
    """
    if use_ollama and content is None:
        content = generate_content_with_corporate_lorem()
    newline_char = '\n'
    # Wrap plain text content in paragraph tags (replacing newlines)
    html_content = f"<p>{content.replace(newline_char, '</p><p>')}</p>" if use_ollama else f"<p>{content}</p>"
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "body": {
            "storage": {
                "value": html_content,
                "representation": "storage",
            }
        },
    }
    r = post_with_retry(f"{base_url}/rest/api/content", payload, auth, verify)
    return r.ok

# ---------------------------------------------------------------------------
# Main script
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="Confluence base URL, e.g. https://confluence.example.com")
    parser.add_argument("--user", help="Username")
    parser.add_argument("--password", help="Password")
    parser.add_argument("--spaces", type=int, help="Number of spaces to create")
    parser.add_argument("--min-pages", type=int, help="Minimum pages per space")
    parser.add_argument("--max-pages", type=int, help="Maximum pages per space")
    parser.add_argument("--seed-file", type=Path, help="Seed words file (txt or json)")
    parser.add_argument("--verify-ssl", action="store_true", help="Enable SSL verification (default off)")
    parser.add_argument("--use-ollama", action="store_true", help="Generate content using CorporateLorem API")
    args = parser.parse_args()

    # Default configuration
    DEFAULT_URL = "http://192.168.65.128:8090"
    DEFAULT_USER = "admin"
    DEFAULT_PASSWORD = "admin"
    DEFAULT_SPACES = 300
    DEFAULT_MIN_PAGES = 10
    DEFAULT_MAX_PAGES = 100
    DEFAULT_SEED_FILE = Path("seeds.txt")
    DEFAULT_VERIFY_SSL = False
    DEFAULT_USE_OLLAMA = True

    # Use provided arguments or fallback to defaults
    url = args.url or DEFAULT_URL
    user = args.user or DEFAULT_USER
    password = args.password or DEFAULT_PASSWORD
    spaces = args.spaces or DEFAULT_SPACES
    min_pages = args.min_pages or DEFAULT_MIN_PAGES
    max_pages = args.max_pages or DEFAULT_MAX_PAGES
    seed_file = args.seed_file or DEFAULT_SEED_FILE
    verify_ssl = args.verify_ssl or DEFAULT_VERIFY_SSL
    use_ollama = args.use_ollama or DEFAULT_USE_OLLAMA

    print(f"Starting content creation with the following settings:")
    print(f"URL: {url}")
    print(f"Spaces: {spaces}")
    print(f"Pages per space: {min_pages}-{max_pages}")
    print(f"Using CorporateLorem API for content: {use_ollama}")

    random.seed()
    auth = (user, password)
    base_url = url.rstrip("/")

    if not verify_ssl:
        requests.packages.urllib3.disable_warnings()

    seeds = read_seeds(seed_file)
    print(f"Loaded {len(seeds)} seed words for content generation")
    existing_keys = set()

    for i in range(spaces):
        key = rand_key(existing_keys)
        existing_keys.add(key)
        name = f"{random.choice(seeds)} Space {i + 1}"
        print(f"Creating space [{i + 1}/{spaces}]: {name} (key: {key})...")
        if not create_space(base_url, auth, verify_ssl, key, name):
            print(f"Failed to create space {name} ({key})", file=sys.stderr)
            continue

        page_total = random.randint(min_pages, max_pages)
        print(f"  Creating {page_total} pages in space {key}...")

        for p in range(page_total):
            title = f"{random.choice(seeds)} Page {p + 1}"
            print(f"    Creating page [{p + 1}/{page_total}]: {title}")

            if use_ollama:
                print(f"      Generating content with CorporateLorem API...")
                if not create_page(base_url, auth, verify_ssl, key, title, use_ollama=True):
                    print(f"Failed to create page {title} in space {key}", file=sys.stderr)
                else:
                    print(f"      Page created successfully")
            else:
                content = " ".join(random.choices(seeds, k=30))
                print(f"      Using random seed content")
                if not create_page(base_url, auth, verify_ssl, key, title, content):
                    print(f"Failed to create page {title} in space {key}", file=sys.stderr)
                else:
                    print(f"      Page created successfully")

    print("\nAll content creation completed successfully!")

if __name__ == "__main__":
    main()