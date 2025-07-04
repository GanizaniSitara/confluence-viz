# Confluence Pickle File Structure Analysis

## Overview

This document provides a comprehensive understanding of how pickle files are structured in the confluence visualization project, specifically focusing on HTML content storage and extraction.

## Pickle File Structure

### Main Pickle Structure
The pickle files contain dictionaries with the following structure:

```python
{
    'space_key': 'SPACEKEY',           # Confluence space key (e.g., 'DAENSTAT')
    'name': 'Space Display Name',      # Human-readable space name
    'sampled_pages': [                 # List of page dictionaries
        {
            'id': 'page_id',          # Confluence page ID
            'title': 'Page Title',    # Page title
            'updated': 'ISO_DATE',    # Last updated timestamp
            'update_count': int,      # Number of updates/versions
            'parent_id': 'parent_id', # Parent page ID (None for root pages)
            'level': int,             # Hierarchy level (0 = root)
            'space_key': 'SPACEKEY', # Space key reference
            'body': 'HTML_CONTENT'   # Raw Confluence HTML content ← THIS IS WHAT YOU WANT
        },
        # ... more pages
    ],
    'total_pages_in_space': int       # Total pages in the space (metadata)
}
```

### Key Finding: HTML Content Location

**The HTML content from Confluence pages is stored in the `body` field of each page dictionary within the `sampled_pages` list.**

This `body` field contains:
- Raw Confluence HTML markup
- Confluence-specific XML tags (e.g., `<ac:structured-macro>`, `<ac:layout>`)
- Standard HTML elements embedded within the Confluence markup
- Rich text content, tables, images, links, etc.

## Scripts That Create Pickle Files

### 1. Primary Data Fetching Scripts

- **`fetch_data.py`**: Creates `confluence_data.pkl` (main data structure)
- **`sample_and_pickle_spaces.py`**: Creates individual space pickle files in `temp/` directory
  - Standard mode: Samples pages (root + recent + frequent updates)
  - Full mode: Saves ALL pages in a space (using `--pickle-space-full` or `--pickle-all-spaces-full`)

### 2. HTML Content Retrieval Process

In `sample_and_pickle_spaces.py`, the `fetch_page_body()` function:
1. Makes API call to `/rest/api/content/{page_id}?expand=body.storage`
2. Extracts HTML from `response.json()['body']['storage']['value']`
3. Stores this directly in the `body` field of the page dictionary

## HTML Content Processing

### Raw HTML Structure
The stored HTML contains Confluence-specific markup:
```html
<ac:layout>
  <ac:layout-section ac:type="single">
    <ac:layout-cell>
      <ac:structured-macro ac:name="tip">
        <ac:parameter ac:name="title">Welcome!</ac:parameter>
        <ac:rich-text-body>
          <p>This is the actual content you want to extract</p>
        </ac:rich-text-body>
      </ac:structured-macro>
    </ac:layout-cell>
  </ac:layout-section>
</ac:layout>
```

### HTML Cleaning Process
The project includes `utils/html_cleaner.py` which:
- Removes/replaces Confluence macros with placeholders
- Converts HTML to readable text
- Preserves formatting with Markdown-style elements
- Handles tables, lists, headings, etc.

## Solution: HTML Content Extraction Script

I've created `/mnt/c/Solutions/Python/confluence_visualization/extract_html_content.py` to help you extract just the HTML content without dumping entire pickle structures as JSON.

### Usage Examples

```bash
# List all available spaces
python extract_html_content.py --list-spaces

# Extract raw HTML from all pages in a space
python extract_html_content.py DAENSTAT --format raw

# Extract cleaned text from all pages in a space
python extract_html_content.py DAENSTAT --format cleaned

# Extract from a specific page
python extract_html_content.py DAENSTAT --page-id 971904 --format raw

# Save to files instead of console output
python extract_html_content.py DAENSTAT --format cleaned --output html_output/

# Get JSON output for programmatic use
python extract_html_content.py DAENSTAT --format raw --json
```

### Output Formats

1. **Raw Format**: Extracts the original Confluence HTML exactly as stored
2. **Cleaned Format**: Processes HTML through a text cleaner to extract readable content
3. **JSON Format**: Structured output for programmatic processing
4. **File Output**: Saves individual files per page for easy processing

### Example Extracted Content

From raw HTML containing complex Confluence markup, the cleaned format produces:
```
Title: Welcome to your new space!
Confluence spaces are great for sharing content and news with your team. This is your home page.

- **Edit this home page** - Click *Edit* in the top right of this screen
- **Create your first page** - Click the *Create* button in the header
- **Brand your Space** - Click *Configure Sidebar* in the left panel
- **Set permissions** - Click *Space Tools* in the left sidebar
```

## Key Findings Summary

1. **HTML Storage**: All Confluence page HTML content is stored in `pickle_data['sampled_pages'][n]['body']`

2. **No JSON Conversion Needed**: You can extract HTML directly from pickle files without converting the entire structure to JSON

3. **Two Processing Options**: 
   - Raw HTML (original Confluence markup)
   - Cleaned text (processed for readability)

4. **File Locations**: 
   - Main pickles: `temp/*.pkl` (sampled pages)
   - Full pickles: `temp/full_pickles/*_full.pkl` (all pages)
   - Remote pickles: Configurable via `settings.ini`

5. **Content Access**: Use the provided `extract_html_content.py` script for efficient HTML extraction without dealing with complex pickle structures

This approach gives you direct access to the HTML content while avoiding the overhead and complexity of JSON conversion of entire pickle files.