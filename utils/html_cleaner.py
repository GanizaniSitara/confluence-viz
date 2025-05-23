import re
from bs4 import BeautifulSoup, Tag

# Configuration for handling specific Confluence macros
MACROS_TO_REMOVE = [
    'carousel', 'gallery', 'profile-picture', 'user-profile', 'navmap', 
    # Add other purely visual or UI-heavy macros here
]

# Macros to replace with a descriptive placeholder.
# Some might have specific logic to extract more details.
MACRO_PLACEHOLDERS = {
    'drawio': '[DIAGRAM: Drawio]',
    'gliffy': '[DIAGRAM: Gliffy]',
    'lucidchart': '[DIAGRAM: Lucidchart]',
    'toc': '[TABLE_OF_CONTENTS]',
    'children': '[CHILDREN_LIST]',
    'pagetree': '[PAGE_TREE]',
    # 'jira': handled separately for key extraction
    # 'view-file', 'view-pdf': handled separately for filename extraction
}

def get_attachment_placeholder(macro_tag: Tag) -> str:
    """Generates a placeholder for attachment macros, trying to extract the filename."""
    try:
        name_param = macro_tag.find('ac:parameter', attrs={'ac:name': 'name'})
        if name_param:
            attachment_tag = name_param.find('ri:attachment', attrs={'ri:filename': True})
            if attachment_tag and attachment_tag.get('ri:filename'):
                filename = attachment_tag['ri:filename']
                macro_name = macro_tag.get('ac:name', 'attachment').lower()
                if 'pdf' in macro_name:
                    return f'[ATTACHMENT: {filename} (PDF)]'
                return f'[ATTACHMENT: {filename}]'
    except Exception:
        # Fallback if parsing fails
        pass
    if 'pdf' in macro_tag.get('ac:name', '').lower():
        return '[ATTACHMENT: PDF]'
    return '[ATTACHMENT]'

def get_jira_placeholder(macro_tag: Tag) -> str:
    """Generates a placeholder for JIRA issue macros, trying to extract the issue key."""
    try:
        key_param = macro_tag.find('ac:parameter', attrs={'ac:name': 'key'})
        if key_param and key_param.text:
            issue_key = key_param.text.strip()
            if issue_key:
                return f'[JIRA ISSUE: {issue_key}]'
    except Exception:
        # Fallback if parsing fails
        pass
    return '[JIRA ISSUE]'

def clean_confluence_html(html_content: str) -> str:
    """
    Cleans Confluence HTML content by removing/replacing specific macros 
    and then extracting text with improved readability.
    """
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all structured macros
    macros = soup.find_all(lambda tag: tag.has_attr('ac:name'))

    for macro in macros:
        macro_name = macro.get('ac:name', '').lower()

        if macro_name in MACROS_TO_REMOVE:
            macro.decompose()
            continue

        placeholder_text = None
        if macro_name in MACRO_PLACEHOLDERS:
            placeholder_text = MACRO_PLACEHOLDERS[macro_name]
        elif 'view-file' in macro_name or 'view-pdf' in macro_name or 'multimedia' in macro_name :
            placeholder_text = get_attachment_placeholder(macro)
        elif 'jira' in macro_name:
            placeholder_text = get_jira_placeholder(macro)
        
        if placeholder_text:
            placeholder_tag = soup.new_string(placeholder_text)
            macro.replace_with(placeholder_tag)
            continue
        
    for hr_tag in soup.find_all('hr'):
        hr_tag.replace_with(soup.new_string(' ---HR_PLACEHOLDER--- '))

    # Extract text using newline as a separator. strip=False is the default.
    text = soup.get_text(separator='\n')

    # Replace the HR_PLACEHOLDER with a visual line, surrounded by newlines.
    text = text.replace('---HR_PLACEHOLDER---', '\n-----\n')
    
    # Normalize whitespace for better readability:
    
    # 1. Split into lines, then strip whitespace from each line.
    #    This handles the fact that get_text(strip=False) might leave spaces around newlines.
    lines = [line.strip() for line in text.split('\n')]
    
    # 2. Filter out lines that are now empty after stripping.
    #    This prevents multiple newlines if original HTML had empty <p></p> or similar.
    lines = [line for line in lines if line] 
    text = '\n'.join(lines)
    
    # 3. Collapse multiple newlines (that might have been introduced by joining non-empty lines)
    #    into a maximum of two (to keep paragraph-like breaks).
    text = re.sub(r'\n{2,}', '\n\n', text) 
    
    # 4. Strip leading/trailing whitespace (including newlines) from the final text.
    text = text.strip()
    
    return text

if __name__ == '__main__':
    # Example Usage:
    sample_html_drawio = '<p>Some text</p><ac:structured-macro ac:name="drawio" ac:schema-version="1"><ac:parameter ac:name="diagramName">MyDiagram</ac:parameter></ac:structured-macro><p>More text</p>'
    sample_html_attachment = '<ac:structured-macro ac:name="view-file"><ac:parameter ac:name="name"><ri:attachment ri:filename="test.docx"/></ac:parameter></ac:structured-macro>'
    sample_html_panel = '<ac:structured-macro ac:name="panel"><ac:rich-text-body><p>Content of panel</p></ac:rich-text-body></ac:structured-macro>'
    sample_html_jira = '<ac:structured-macro ac:name="jira" ac:schema-version="1"><ac:parameter ac:name="key">EX-123</ac:parameter></ac:structured-macro>'
    sample_html_remove = '<ac:structured-macro ac:name="carousel">stuff</ac:structured-macro>Visible text.'


    print(f"Original: {sample_html_drawio}")
    print(f"Cleaned: {clean_confluence_html(sample_html_drawio)}")
    print("\n")
    print(f"Original: {sample_html_attachment}")
    print(f"Cleaned: {clean_confluence_html(sample_html_attachment)}")
    print("\n")
    print(f"Original: {sample_html_panel}")
    print(f"Cleaned: {clean_confluence_html(sample_html_panel)}")
    print("\n")
    print(f"Original: {sample_html_jira}")
    print(f"Cleaned: {clean_confluence_html(sample_html_jira)}")
    print("\n")
    print(f"Original: {sample_html_remove}")
    print(f"Cleaned: {clean_confluence_html(sample_html_remove)}")
    print("\n")
    complex_html = """
    <p>This is a test page.</p>
    <ac:structured-macro ac:name="info">
        <ac:rich-text-body>
            <p>This is an info panel with <em>important</em> text.</p>
        </ac:rich-text-body>
    </ac:structured-macro>
    <ac:structured-macro ac:name="view-file">
        <ac:parameter ac:name="name"><ri:attachment ri:filename="mydoc.pdf"/></ac:parameter>
    </ac:structured-macro>
    <p>Some text after attachment.</p>
    <ac:structured-macro ac:name="drawio" ac:macro-id="123">
        <ac:parameter ac:name="diagramName">Flowchart</ac:parameter>
    </ac:structured-macro>
    <ac:structured-macro ac:name="code" ac:schema-version="1">
        <ac:parameter ac:name="language">python</ac:parameter>
        <ac:plain-text-body><![CDATA[print("Hello World")]]></ac:plain-text-body>
    </ac:structured-macro>
    <ac:structured-macro ac:name="carousel"><p>Hidden by carousel</p></ac:structured-macro>
    """
    print(f"Original Complex: {complex_html.strip()}")
    print(f"Cleaned Complex: {clean_confluence_html(complex_html)}")
