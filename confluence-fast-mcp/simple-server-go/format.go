package main

import (
	"fmt"
	"strings"
)

// FormatSearchResults matches the Python _format_search_results output.
func FormatSearchResults(matches []SearchMatch, query string) string {
	if len(matches) == 0 {
		return fmt.Sprintf("No results found for: %s", query)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "Found %d result(s) for \"%s\":\n\n", len(matches), query)
	for i, m := range matches {
		fmt.Fprintf(&sb, "%d. **%s** (ID: %s, Space: %s, Match: %s)\n",
			i+1, m.Page.Title, m.Page.ID, m.SpaceKey, m.MatchType)
	}
	return sb.String()
}

// FormatPage matches the Python _format_page_text output.
func FormatPage(pr *PageResult, includeMeta, toMarkdown bool) string {
	p := pr.Page
	var sb strings.Builder

	fmt.Fprintf(&sb, "# %s\n", p.Title)

	if includeMeta {
		fmt.Fprintf(&sb, "- **Page ID**: %s\n", p.ID)
		fmt.Fprintf(&sb, "- **Space**: %s\n", pr.SpaceKey)
		if p.VersionNumber > 0 {
			fmt.Fprintf(&sb, "- **Version**: %d\n", p.VersionNumber)
		}
		if p.VersionWhen != "" {
			fmt.Fprintf(&sb, "- **Last updated**: %s\n", p.VersionWhen)
		}
		if p.VersionBy != "" {
			fmt.Fprintf(&sb, "- **Author**: %s\n", p.VersionBy)
		}
		if len(p.Labels) > 0 {
			fmt.Fprintf(&sb, "- **Labels**: %s\n", strings.Join(p.Labels, ", "))
		}
		sb.WriteString("\n")
	}

	if p.BodyHTML != "" {
		sb.WriteString("---\n\n")
		if toMarkdown {
			sb.WriteString(HTMLToMarkdown(p.BodyHTML))
		} else {
			sb.WriteString(p.BodyHTML)
		}
	}

	return sb.String()
}

// FormatChildren matches the Python confluence_get_page_children output.
func FormatChildren(children []PageResult, parentID string, includeContent, toMarkdown bool) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Found %d child page(s) of page %s:\n\n", len(children), parentID)

	for i, c := range children {
		fmt.Fprintf(&sb, "%d. **%s** (ID: %s, Space: %s)\n",
			i+1, c.Page.Title, c.Page.ID, c.SpaceKey)

		if includeContent && c.Page.BodyHTML != "" {
			var body string
			if toMarkdown {
				body = HTMLToMarkdown(c.Page.BodyHTML)
			} else {
				body = c.Page.BodyHTML
			}
			lines := strings.Split(strings.TrimSpace(body), "\n")
			if len(lines) > 10 {
				lines = lines[:10]
			}
			for _, line := range lines {
				fmt.Fprintf(&sb, "   %s\n", line)
			}
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// FormatComments matches the Python confluence_get_comments output.
func FormatComments(page *Page, comments []Comment) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Comments on \"%s\" (%d comment(s)):\n\n", page.Title, len(comments))

	for i, c := range comments {
		text := HTMLToText(c.BodyHTML)
		if len(text) > 500 {
			text = text[:500]
		}
		fmt.Fprintf(&sb, "%d. [%s] %s\n", i+1, c.Author, text)
	}
	return sb.String()
}

// FormatLabels matches the Python confluence_get_labels output.
func FormatLabels(title string, labels []string) string {
	return fmt.Sprintf("Labels for \"%s\": %s", title, strings.Join(labels, ", "))
}
