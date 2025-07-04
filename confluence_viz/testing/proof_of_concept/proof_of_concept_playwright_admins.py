from playwright.sync_api import sync_playwright
from config_loader import load_confluence_settings
import sys

# Load Confluence settings
SETTINGS = load_confluence_settings()
CONFLUENCE_URL = SETTINGS['base_url']
USERNAME = SETTINGS['username']
PASSWORD = SETTINGS['password']
# VERIFY_SSL is not directly used by Playwright in the same way as requests,
# but good to have if any http requests were to be made separately.

# --- User's provided Playwright logic starts here, adapted for loaded settings ---

confluence_url = CONFLUENCE_URL
username = USERNAME
password = PASSWORD
space_key = "DOC"  # Default space key, can be changed for testing

print(f"Attempting to fetch admins for space '{space_key}' using Playwright...")
print(f"Target Confluence URL: {confluence_url}")

try:
    with sync_playwright() as p:
        # Launch browser (headless=True means no UI will be shown)
        # You can set headless=False for debugging to see what the browser is doing.
        browser = p.chromium.launch(headless=False)  # Changed to False
        page = browser.new_page()
        
        # 1. Log in to Confluence
        login_url = f"{confluence_url}/login.action"
        print(f"Navigating to login page: {login_url}")
        page.goto(login_url)

        # Pause for manual inspection if needed
        print("Pausing script. Press the 'Resume' button in the Playwright Inspector to continue.")
        page.pause()
        
        # Fill in Atlassian login form fields
        # These selectors might need adjustment based on your Confluence version/customization
        print("Waiting for username field to be visible...")
        page.wait_for_selector("input#username-field", timeout=10000) # Wait for the correct username field
        print("Filling username...")
        page.fill("input#username-field", username) # Changed selector
        print("Filling password...")
        page.fill("input#password-field", password) # Changed selector
        print("Clicking login button...")
        page.click("input#login")  # or input#loginButton, button#loginButton etc.
        
        # Wait for navigation after login. 
        # Waiting for networkidle can be robust. If it times out, 
        # a specific element on the dashboard page can be waited for.
        print("Waiting for login to complete (networkidle)...")
        page.wait_for_load_state("networkidle", timeout=60000) # 60s timeout
        print("Login likely successful.")

        # 2. Go to Space Directory
        space_directory_url = f"{confluence_url}/spacedirectory/view.action"
        print(f"Navigating to Space Directory: {space_directory_url}")
        page.goto(space_directory_url)
        print("Waiting for space directory table to load...")
        page.wait_for_selector("table.spacedirectory", timeout=30000) # 30s timeout
        print("Space directory loaded.")

        # 3. Click the "Space Details" icon (info icon) for the target space
        print(f"Looking for space row with key: {space_key}")
        # The selector for the row might need to be specific if space keys can appear elsewhere
        space_row_selector = f"tr:has(td a.space-name[data-space-key='{space_key}']), tr:has(td:has-text('{space_key}'))"
        space_row = page.locator(space_row_selector).first # Take the first match if multiple
        
        if not space_row.is_visible():
            print(f"Space row for '{space_key}' not found or not visible with selector: {space_row_selector}")
            print("Attempting to find by iterating rows if the direct locator failed...")
            # Fallback: Iterate through rows if the complex :has-text selector is problematic
            rows = page.locator("table.spacedirectory tbody tr")
            found_row = None
            for i in range(rows.count()):
                row = rows.nth(i)
                # Check if the space key is in the row's text content or a specific cell
                # This is a broad check; more specific cell targeting might be better
                if space_key in row.inner_text(): 
                    # Try to find a link with the space key for more certainty
                    if row.locator(f"a.space-name[data-space-key='{space_key}']").count() > 0 or \
                       row.locator(f"td:text-matches('^{space_key}$', 'i')").count() > 0:
                        found_row = row
                        break
            if found_row:
                space_row = found_row
                print(f"Found space row for '{space_key}' via iteration.")
            else:
                print(f"Could not find space '{space_key}' in the directory table after iteration.")
                browser.close()
                sys.exit(1)

        print(f"Found space row. Clicking info icon...")
        # The info icon selector might be .aui-iconfont-info, .icon-info, or similar
        info_icon_selector = "td.actions a[href*='/pages/viewinfo.action'], td.actions .icon-info, td.actions span.aui-icon-small.aui-iconfont-info"
        info_icon = space_row.locator(info_icon_selector).first
        
        if not info_icon.is_visible():
            print(f"Info icon not found or not visible with selector: {info_icon_selector}")
            # Attempt to take a screenshot for debugging
            page.screenshot(path="debug_screenshot_space_directory.png")
            print("Saved screenshot to debug_screenshot_space_directory.png")
            browser.close()
            sys.exit(1)

        info_icon.click()
        print("Info icon clicked.")

        # 4. Wait for the Space Details dialog and extract admins
        print("Waiting for Space Details dialog...")
        dialog_selector = ".space-details-dialog, #space-details-dialog, div[role='dialog'][aria-labelledby*='space-details']"
        page.wait_for_selector(dialog_selector, timeout=30000) # 30s timeout
        print("Space Details dialog loaded.")

        admins_text = page.locator(dialog_selector).inner_text()
        
        # Parse out the Administrators line
        # This parsing is highly dependent on the exact text format in the dialog
        admins_line = ""
        for line in admins_text.splitlines():
            if "Administrators:" in line or "administrators:" in line:
                admins_line = line
                break
        
        admins = []
        if admins_line:
            # Splitting logic: "Administrators: User A, User B, Group C (group)"
            # Needs to be robust to variations
            raw_admins_part = admins_line.split(":", 1)[1] # Get text after "Administrators:"
            # Split by comma, then strip whitespace from each name
            admins = [name.strip() for name in raw_admins_part.split(",") if name.strip()]
            print(f"Successfully parsed admins: {admins}")
        else:
            print("Could not find 'Administrators:' line in the dialog text.")
            print("Dialog content for debugging:")
            print(admins_text)
            # Attempt to take a screenshot for debugging
            page.screenshot(path="debug_screenshot_space_details_dialog.png")
            print("Saved screenshot to debug_screenshot_space_details_dialog.png")

        print(f"Space administrators for {space_key}: {admins}")
        browser.close()

except Exception as e:
    print(f"An error occurred: {e}")
    # If a browser instance exists, try to close it
    if 'browser' in locals() and browser:
        browser.close()
    # If a page instance exists, try to take a screenshot
    if 'page' in locals() and page:
        try:
            page.screenshot(path="error_screenshot.png")
            print("Saved error screenshot to error_screenshot.png")
        except Exception as se:
            print(f"Could not save error screenshot: {se}")


