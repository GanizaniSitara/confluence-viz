package main

import (
	"fmt"
	"regexp"
	"strings"
)

// SearchMatch is a search result with match type info.
type SearchMatch struct {
	SpaceKey  string
	Page      *Page
	MatchType string // "title" or "body"
}

// Search uses the inverted index for full-text search, falling back to
// in-memory search for wildcard/listing queries.
func Search(ds *DataStore, idx *InvertedIndex, query, spacesFilter string, limit int) []SearchMatch {
	isCQL := strings.Contains(query, " ~ ") ||
		strings.Contains(query, " = ") ||
		strings.Contains(query, " AND ") ||
		strings.Contains(query, " OR ")

	var spaceKey, cleanQuery string
	var titleOnly bool

	if isCQL {
		cleanQuery, spaceKey, titleOnly = parseCQL(query)
		if spaceKey == "" && spacesFilter != "" {
			spaceKey = strings.TrimSpace(strings.Split(spacesFilter, ",")[0])
		}

		// Wildcard → list pages in space
		if (cleanQuery == "*" || cleanQuery == "") && spaceKey != "" {
			pages := ds.GetPagesInSpace(spaceKey, limit)
			matches := make([]SearchMatch, len(pages))
			for i, p := range pages {
				matches[i] = SearchMatch{SpaceKey: spaceKey, Page: p, MatchType: "title"}
			}
			return matches
		}
	} else {
		cleanQuery = query
		if spacesFilter != "" {
			spaceKey = strings.TrimSpace(strings.Split(spacesFilter, ",")[0])
		}
	}

	// Use inverted index for search
	results := idx.SearchIndex(cleanQuery, spaceKey, titleOnly, limit)

	// Convert to SearchMatch (resolve page references)
	matches := make([]SearchMatch, 0, len(results))
	for _, r := range results {
		pr := ds.GetPageByID(r.DocID)
		if pr == nil {
			continue
		}
		matches = append(matches, SearchMatch{
			SpaceKey:  r.SpaceKey,
			Page:      pr.Page,
			MatchType: r.MatchType,
		})
	}
	return matches
}

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

// ---------------------------------------------------------------------------
// CQL parser (mirrors search.py)
// ---------------------------------------------------------------------------

var (
	reSpaceEq     = regexp.MustCompile(`(?i)space\s*=\s*["']?([A-Z0-9_-]+)["']?`)
	reSpaceIn     = regexp.MustCompile(`(?i)space\s+in\s+\(([^)]+)\)`)
	reTextSearch  = regexp.MustCompile(`(?i)text\s*~\s*["']([^"']+)["']`)
	reTitleSearch = regexp.MustCompile(`(?i)title\s*~\s*["']([^"']+)["']`)
)

func parseCQL(cql string) (query, spaceKey string, titleOnly bool) {
	if m := reSpaceEq.FindStringSubmatch(cql); m != nil {
		spaceKey = strings.Trim(m[1], `"' `)
	} else if m := reSpaceIn.FindStringSubmatch(cql); m != nil {
		parts := strings.Split(m[1], ",")
		spaceKey = strings.Trim(parts[0], `"' `)
	}

	var terms []string
	for _, m := range reTextSearch.FindAllStringSubmatch(cql, -1) {
		terms = append(terms, m[1])
	}
	for _, m := range reTitleSearch.FindAllStringSubmatch(cql, -1) {
		terms = append(terms, m[1])
		titleOnly = true
	}

	query = strings.Join(terms, " ")
	if query == "" {
		query = "*"
	}
	return
}
