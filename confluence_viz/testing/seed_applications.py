# description: Seeds application data for Confluence visualization.

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
seed_applications.py

Creates Confluence spaces and pages for each application in the provided CSV,
generating realistic enterprise content using Ollama with the granite3.1-moe model.
"""

import csv
import random
import sys
import time
import requests
import html
import json
from pathlib import Path
from config_loader import load_confluence_settings

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "granite3.1-moe"
CSV_FILE = "Application_Diagram_Builder.csv"

# --- Confluence API helpers ---
def post_with_retry(url, payload, auth, verify, headers=None):
    attempt = 0
    print(f"\nSending API request to: {url}")
    print(f"Request payload: {payload}")
    while True:
        resp = requests.post(url, json=payload, auth=auth, verify=verify, headers=headers)
        if resp.status_code == 429:
            print(f"Rate limited (429). Retrying attempt {attempt+1}...")
            attempt += 1
            time.sleep(min(2 ** attempt, 60))
            continue
        print(f"API Response Status: {resp.status_code}")
        if resp.ok:
            print("API request successful")
        else:
            print(f"API Error: {resp.text}")
        return resp

def create_space(base_url, auth, verify, key, name, desc=""):
    # Ensure we don't duplicate rest/api in the URL
    if base_url.endswith("/rest/api"):
        url = f"{base_url}/space"
    else:
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
    print(f"\n=== CREATING SPACE ===")
    print(f"Space Key: {key}")
    print(f"Space Name: {name}")
    print(f"Description: {desc}")
    r = post_with_retry(url, payload, auth, verify)
    if r.ok:
        print(f"✅ Successfully created space: {name} (key: {key})")
    else:
        print(f"❌ Failed to create space: {name} (key: {key})")
    return r.ok

def create_page(base_url, auth, verify, space_key, title, content):
    # Convert the LLM output to proper Confluence storage format (XHTML)
    # Process content line by line to create proper HTML structure
    lines = content.strip().split('\n')
    html_lines = []
    in_list = False
    in_code_block = False
    
    for line in lines:
        # Handle Markdown-style headers (# Header)
        if line.strip().startswith('# '):
            html_lines.append(f"<h1>{html.escape(line.strip()[2:])}</h1>")
        elif line.strip().startswith('## '):
            html_lines.append(f"<h2>{html.escape(line.strip()[3:])}</h2>")
        elif line.strip().startswith('### '):
            html_lines.append(f"<h3>{html.escape(line.strip()[4:])}</h3>")
        elif line.strip().startswith('#### '):
            html_lines.append(f"<h4>{html.escape(line.strip()[5:])}</h4>")
        # Handle lists
        elif line.strip().startswith('* ') or line.strip().startswith('- '):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{html.escape(line.strip()[2:])}</li>")
        elif line.strip().startswith('1. ') or (line.strip() and line.strip()[0].isdigit() and line.strip()[1:].startswith('. ')):
            if not in_list:
                html_lines.append("<ol>")
                in_list = True
            html_lines.append(f"<li>{html.escape(line.strip()[line.find('.')+1:].strip())}</li>")
        # Handle code blocks
        elif line.strip() == '```' or line.strip().startswith('```'):
            if in_code_block:
                html_lines.append("</code></pre>")
                in_code_block = False
            else:
                html_lines.append("<pre><code>")
                in_code_block = True
        # Handle empty lines as paragraph breaks
        elif line.strip() == '':
            if in_list:
                if html_lines[-1].endswith('</li>'):
                    html_lines.append("</ul>" if "</ul>" not in html_lines[-1] else "")
                    in_list = False
            html_lines.append("<br/>")
        # Regular paragraph content
        else:
            if in_code_block:
                html_lines.append(html.escape(line))
            else:
                html_lines.append(f"<p>{html.escape(line)}</p>")
      # Close any open lists - check which type of list needs to be closed
    if in_list:
        # Check if we have an unclosed ordered list
        if "<ol>" in "".join(html_lines) and "</ol>" not in "".join(html_lines[-10:]):
            html_lines.append("</ol>")
        # Check if we have an unclosed unordered list
        elif "<ul>" in "".join(html_lines) and "</ul>" not in "".join(html_lines[-10:]):
            html_lines.append("</ul>")
    
    # Close any open code blocks
    if in_code_block:
        html_lines.append("</code></pre>")
      # Join all HTML lines
    html_content = "".join(html_lines)
      # Check if content is already in HTML format
    # Simple check: if it contains multiple HTML tags (more than just a few <p> tags)
    html_tag_count = content.count('<h1>') + content.count('<h2>') + content.count('<ul>') + content.count('<ol>') + content.count('<li>') + content.count('<p>')
    is_already_html = html_tag_count >= 5 and '<' in content[:100]
    
    # Use the content directly if it's already HTML, otherwise use our converted HTML
    raw_content = content if is_already_html else html_content
    print(f"Content format: {'HTML (direct from LLM)' if is_already_html else 'Converted from Markdown to HTML'}")
    
    # Make sure the content is properly wrapped in a root element for Confluence XHTML
    # This ensures all content is properly contained and prevents XML parsing errors
    if not raw_content.strip().startswith("<ac:structured-macro") and not raw_content.strip().startswith("<html>"):
        final_content = f"<div>{raw_content}</div>"
    else:
        final_content = raw_content
    
    # Sanitize content to ensure it's valid XHTML
    # Remove any XML declaration or DOCTYPE that might be present
    final_content = final_content.replace('<?xml version="1.0"?>', '')
    final_content = final_content.replace('<!DOCTYPE html>', '')
    
    # Escape any potentially problematic characters that could be misinterpreted as XML directives
    final_content = final_content.replace('&', '&amp;').replace('<![', '&lt;![')
    
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "body": {
            "storage": {
                "value": final_content,
                "representation": "storage",
            }        },
    }
    
    print(f"\n=== CREATING PAGE ===")
    print(f"Space Key: {space_key}")
    print(f"Title: {title}")
    print(f"Content Length: {len(content)} characters")
    print(f"Content Preview (first 100 chars): {content[:100]}...")
    
    # Ensure we don't duplicate rest/api in the URL
    if base_url.endswith("/rest/api"):
        url = f"{base_url}/content"
    else:
        url = f"{base_url}/rest/api/content"
    
    r = post_with_retry(url, payload, auth, verify)
    if r.ok:
        # Get the page ID from the response and construct the direct URL
        response_data = r.json()
        page_id = response_data.get('id')
        
        # Extract the base Confluence URL (removing the /rest/api part)
        confluence_base_url = base_url.split('/rest/api')[0] if '/rest/api' in base_url else base_url
        
        # Construct the direct URL to the page
        page_url = f"{confluence_base_url}/pages/viewpage.action?pageId={page_id}"
        
        print(f"✅ Successfully created page: '{title}' in space '{space_key}'")
        print(f"   📄 Page URL: {page_url}")
    else:
        print(f"❌ Failed to create page '{title}' in space '{space_key}'")
        print(f"      Confluence API error: {r.status_code} {r.text}", file=sys.stderr)
        print(f"      Payload: {payload}", file=sys.stderr)
    return r.ok

# --- Ollama content generation ---
def generate_page_names(level1, level2, appname, num_pages=25):
    """Generate realistic page names for a Confluence space based on business context."""
    prompt = (
        f"Imagine we have a confluence server and are creating new space for '{appname}' "
        f"in context of enterprise technology in the business area '{level1}' and sub-area '{level2}'. "
        f"We need to name {num_pages} pages, generate the names. "
        f"Return ONLY the page names, one per line, with no additional text or explanations."
    )
    
    print(f"\nUsing prompt for page name generation:\n{prompt}\n")
    
    data = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
    try:
        resp = requests.post(OLLAMA_URL, json=data)
        if resp.status_code == 200:
            result = resp.json()
            # Print the full raw response from the LLM
            print(f"\nRaw LLM Response for page names:\n{result.get('response', '')}\n")
            page_names = result.get("response", "").strip().split('\n')
              # Enhanced sanitization of page names
            sanitized_names = []
            for name in page_names:
                if not name.strip():
                    continue
                
                # Remove leading numbers and dots (e.g., "1. ", "2.", etc.)
                name = name.strip()
                import re
                name = re.sub(r'^\d+\.?\s*"?', '', name)
                
                # Remove trailing and leading quotes
                name = name.strip('"\'')
                
                # Replace underscores with spaces
                name = name.replace('_', ' ')
                
                # Split words that are joined together (camelCase or PascalCase)
                # Pattern: look for a lowercase letter followed by uppercase letter and insert a space
                name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
                
                # Handle runs of uppercase letters followed by a lowercase letter
                # This helps with acronyms like "AI" in "AIinLending" -> "AI in Lending"
                name = re.sub(r'([A-Z])([A-Z][a-z])', r'\1 \2', name)
                
                # Fix capitalization (make each word start with capital letter)
                name = ' '.join(word.capitalize() for word in name.split())
                
                # If the name is not empty after sanitization, add it
                if name.strip():
                    sanitized_names.append(name)
            
            # Ensure we have the right number of page names
            if len(sanitized_names) < num_pages:
                # If we don't have enough names, add generic ones to reach the target
                for i in range(len(sanitized_names) + 1, num_pages + 1):
                    sanitized_names.append(f"{appname} - Documentation {i}")
            
            # Print the sanitized names for debugging
            print(f"\nSanitized page names for '{appname}':")
            for idx, name in enumerate(sanitized_names[:num_pages], 1):
                print(f"      {idx}. {name}")
            
            return sanitized_names[:num_pages]  # Return exactly the requested number of pages
        else:
            print(f"Ollama error: {resp.status_code} {resp.text}", file=sys.stderr)
            return [f"{appname} - Documentation {i}" for i in range(1, num_pages + 1)]
    except Exception as e:
        print(f"Ollama exception: {e}", file=sys.stderr)
        return [f"{appname} - Documentation {i}" for i in range(1, num_pages + 1)]

def generate_content(level1, level2, appname, page_name):
    """Generate content for a specific page in a Confluence space."""
    prompt = (
        f"You are an enterprise IT documentation assistant. "
        f"Write a detailed, realistic Confluence page with the title '{page_name}' "
        f"for the application '{appname}' in the business area '{level1}' and sub-area '{level2}'. "
        f"The content should match what would be expected for a page with this specific title "
        f"in an enterprise context. Include relevant business context, technical details, "
        f"and operational information appropriate for this specific page type. "
        f"Do not mention that this is generated or reference any seed data. "
        f"Use professional, clear language with appropriate structure. "
        f"FORMAT THE CONTENT IN HTML, not Markdown. Use proper HTML tags like <h1>, <h2>, <ul>, <li>, <p>, etc. "
        f"Do not use Markdown syntax like #, *, -, or ```."
    )
    
    print(f"\n=== GENERATING CONTENT FOR '{page_name}' ===")
    print(f"Business Area: {level1} / {level2}")
    print(f"Application: {appname}")
    print(f"Using prompt for content generation:\n{prompt}\n")
    print(f"Requesting content from Ollama ({OLLAMA_MODEL})...")
    
    data = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
    try:
        resp = requests.post(OLLAMA_URL, json=data)
        if resp.status_code == 200:
            result = resp.json()
            content = result.get("response", "(No content generated)")
            
            # Print the full content generated with clear formatting
            print(f"\n{'='*80}")
            print(f"GENERATED CONTENT FOR: '{page_name}'")
            print(f"LENGTH: {len(content)} characters")
            print(f"{'='*80}")
            print(content)
            print(f"{'='*80}")
            print(f"END OF CONTENT FOR: '{page_name}'")
            print(f"{'='*80}\n")
            
            return content
        else:
            print(f"❌ OLLAMA ERROR: {resp.status_code} {resp.text}", file=sys.stderr)
            return "(Content generation failed)"
    except Exception as e:
        print(f"❌ OLLAMA EXCEPTION: {e}", file=sys.stderr)
        return "(Content generation failed)"

# --- Main logic ---
def main():
    try:
        settings = load_confluence_settings()
        # Use base_url and append /rest/api for the API endpoint
        base_url = settings['base_url'] 
        api_endpoint = "/rest/api"
        username = settings['username']
        password = settings['password']
        verify_ssl = settings['verify_ssl']
    except Exception as e:
        print(f"Error loading Confluence settings: {e}", file=sys.stderr)
        sys.exit(1)
    auth = (username, password)

    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            level1 = row['Level1'].strip()
            level2 = row['Level2'].strip()
            appname = row['AppName'].strip()
            # Compose a unique space key (max 20 chars, uppercase, no spaces)
            key = (level1[:2] + level2[:2] + appname[:4]).upper().replace(' ', '')[:20]
            name = f"{level1} - {level2} - {appname}"
            desc = f"Enterprise application: {appname} in {level1} / {level2}"
            print(f"Creating space: {name} (key: {key})")
            
            if not create_space(base_url, auth, verify_ssl, key, name, desc):
                print(f"  Failed to create space {name} ({key})", file=sys.stderr)
                continue
                
            num_pages = random.randint(10, 100)
            print(f"  Creating {num_pages} pages for application '{appname}'...")
              # Generate realistic page names first
            print(f"    Generating page names for '{appname}'...")
            page_names = generate_page_names(level1, level2, appname, num_pages)
            
            # Print all generated page names
            print(f"\n    Generated page names for '{appname}':")
            for idx, name in enumerate(page_names, 1):
                print(f"      {idx}. {name}")
            print("")  # Empty line for better readability
            
            # Create pages with generated content
            for i, title in enumerate(page_names, 1):
                print(f"    Generating content for page {i}/{num_pages}: {title}")
                content = generate_content(level1, level2, appname, title)
                if not create_page(base_url, auth, verify_ssl, key, title, content):
                    print(f"      Failed to create page {title}", file=sys.stderr)
                else:
                    print(f"      Page created successfully")

if __name__ == "__main__":
    main()
