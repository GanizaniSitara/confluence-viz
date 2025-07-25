#!/usr/bin/env python3
"""
Test script to diagnose HTML cleaning issues with strikethrough and text size
"""

from utils.html_cleaner import clean_confluence_html
import json

# Test cases with various formatting
test_cases = [
    # Strikethrough
    ('<p>Normal text <del>strikethrough text</del> more text</p>', 'Strikethrough with <del>'),
    ('<p>Normal text <s>strikethrough text</s> more text</p>', 'Strikethrough with <s>'),
    ('<p>Normal text <strike>strikethrough text</strike> more text</p>', 'Strikethrough with <strike>'),
    ('<p>Normal text <span style="text-decoration: line-through">strikethrough text</span> more text</p>', 'Strikethrough with CSS'),
    
    # Font sizes
    ('<p><span style="font-size: 24px">Large text</span> normal text</p>', 'Font size with style'),
    ('<p><font size="5">Large text</font> normal text</p>', 'Font size with font tag'),
    ('<p><big>Big text</big> <small>small text</small></p>', 'Big and small tags'),
    
    # Confluence specific
    ('<p>Normal <ac:inline ac:type="mention">@user</ac:inline> text</p>', 'Confluence mention'),
    ('<p>Text with <span class="confluence-embedded-file-wrapper">file</span></p>', 'Confluence file wrapper'),
    
    # Complex formatting
    ('<p><strong>Bold <em>and italic <s>and strike</s></em></strong></p>', 'Nested formatting'),
    
    # Special characters that might cause issues
    ('<p>Text with — em dash and – en dash</p>', 'Special dashes'),
    ('<p>Text with "curly quotes" and \'single quotes\'</p>', 'Smart quotes'),
]

print("HTML Cleaning Test Results")
print("=" * 80)

for html, description in test_cases:
    cleaned = clean_confluence_html(html)
    print(f"\nTest: {description}")
    print(f"Input:   {html}")
    print(f"Output:  {cleaned}")
    print(f"Repr:    {repr(cleaned)}")
    print("-" * 40)

# Test a more complex Confluence page snippet
complex_html = """
<h2>Section with <span style="color: red">colored</span> heading</h2>
<p>This is a paragraph with <strong>bold</strong>, <em>italic</em>, and <s>strikethrough</s> text.</p>
<p><span style="font-size: 18px">Larger text</span> followed by <span style="font-size: 10px">smaller text</span>.</p>
<ul>
    <li>Item with <del>deleted text</del></li>
    <li>Item with <span style="text-decoration: underline">underlined text</span></li>
</ul>
<p>Text with <ac:structured-macro ac:name="status"><ac:parameter ac:name="colour">Green</ac:parameter><ac:parameter ac:name="title">DONE</ac:parameter></ac:structured-macro> status.</p>
"""

print("\n\nComplex Confluence HTML Test")
print("=" * 80)
print("Input HTML:")
print(complex_html)
print("\nCleaned output:")
cleaned = clean_confluence_html(complex_html)
print(cleaned)
print("\nRepr of cleaned:")
print(repr(cleaned))

# Test what happens when JSON encoded (like GPU script does)
print("\n\nJSON Encoding Test")
print("=" * 80)
data = {"content": cleaned}
json_encoded = json.dumps(data)
print("JSON encoded:")
print(json_encoded)
print("\nJSON decoded back:")
decoded = json.loads(json_encoded)
print(repr(decoded["content"]))

# Check for any markdown-like syntax that might cause issues
print("\n\nChecking for problematic patterns:")
problematic_patterns = [
    ('~~', 'Strikethrough syntax'),
    ('**', 'Bold syntax'),
    ('__', 'Underline/bold syntax'),
    ('*', 'Italic syntax'),
    ('_', 'Italic syntax'),
    ('```', 'Code block syntax'),
    ('`', 'Inline code syntax'),
    ('<', 'HTML tag opener'),
    ('>', 'HTML tag closer'),
    ('&lt;', 'Escaped HTML'),
    ('&gt;', 'Escaped HTML'),
]

for pattern, description in problematic_patterns:
    if pattern in cleaned:
        print(f"FOUND: {description} - '{pattern}' appears {cleaned.count(pattern)} times")