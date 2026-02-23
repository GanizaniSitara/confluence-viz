"""Tests for HTML to ADF conversion."""

import pytest
from confluence_fast_mcp.converters import html_to_adf, html_to_text


def test_html_to_text_basic():
    """Test basic HTML to text conversion."""
    html = "<p>Hello world</p>"
    text = html_to_text(html)
    assert "Hello world" in text


def test_html_to_adf_paragraph():
    """Test paragraph conversion."""
    html = "<p>Hello world</p>"
    adf = html_to_adf(html)

    assert adf['version'] == 1
    assert adf['type'] == 'doc'
    assert len(adf['content']) > 0
    assert adf['content'][0]['type'] == 'paragraph'


def test_html_to_adf_heading():
    """Test heading conversion."""
    html = "<h1>My Heading</h1>"
    adf = html_to_adf(html)

    assert len(adf['content']) > 0
    heading = adf['content'][0]
    assert heading['type'] == 'heading'
    assert heading['attrs']['level'] == 1


def test_html_to_adf_list():
    """Test list conversion."""
    html = """
    <ul>
        <li>Item 1</li>
        <li>Item 2</li>
    </ul>
    """
    adf = html_to_adf(html)

    assert len(adf['content']) > 0
    list_node = adf['content'][0]
    assert list_node['type'] == 'bulletList'
    assert len(list_node['content']) == 2


def test_html_to_adf_bold():
    """Test bold text conversion."""
    html = "<p>This is <strong>bold</strong> text</p>"
    adf = html_to_adf(html)

    para = adf['content'][0]
    assert para['type'] == 'paragraph'

    # Find the bold text node
    bold_found = False
    for node in para['content']:
        if node.get('text') == 'bold' and 'marks' in node:
            marks = node['marks']
            if any(m['type'] == 'strong' for m in marks):
                bold_found = True
                break

    assert bold_found, "Bold text not found in ADF"


def test_html_to_adf_link():
    """Test link conversion."""
    html = '<p>Check <a href="http://example.com">this link</a></p>'
    adf = html_to_adf(html)

    para = adf['content'][0]
    link_found = False

    for node in para['content']:
        if 'marks' in node:
            for mark in node['marks']:
                if mark['type'] == 'link' and mark['attrs']['href'] == 'http://example.com':
                    link_found = True
                    break

    assert link_found, "Link not found in ADF"


def test_html_to_adf_code_block():
    """Test code block conversion."""
    html = "<pre><code>print('hello')</code></pre>"
    adf = html_to_adf(html)

    assert len(adf['content']) > 0
    code_block = adf['content'][0]
    assert code_block['type'] == 'codeBlock'


def test_html_to_adf_empty():
    """Test empty HTML."""
    adf = html_to_adf("")

    assert adf['version'] == 1
    assert adf['type'] == 'doc'
    assert len(adf['content']) >= 0


def test_html_to_adf_complex():
    """Test complex HTML with multiple elements."""
    html = """
    <h2>Introduction</h2>
    <p>This is a <strong>test</strong> document.</p>
    <ul>
        <li>First item</li>
        <li>Second item</li>
    </ul>
    <p>With a <a href="http://example.com">link</a>.</p>
    """
    adf = html_to_adf(html)

    assert adf['version'] == 1
    assert adf['type'] == 'doc'
    assert len(adf['content']) >= 3  # At least heading, paragraph, list
