package main

import (
	"regexp"
	"strings"
)

// SearchMatch is a search result with match type info.
type SearchMatch struct {
	SpaceKey  string
	Page      *Page
	MatchType string // "title" or "body"
}

// Search performs in-memory search, matching the Python simple_server behavior.
// Supports simple text queries and basic CQL.
func Search(ds *DataStore, query, spacesFilter string, limit int) []SearchMatch {
	// Detect CQL
	isCQL := strings.Contains(query, " ~ ") ||
		strings.Contains(query, " = ") ||
		strings.Contains(query, " AND ") ||
		strings.Contains(query, " OR ")

	var spaceKey string
	var cleanQuery string
	var titleOnly bool

	if isCQL {
		cleanQuery, spaceKey, titleOnly = parseCQL(query)
		if spaceKey == "" && spacesFilter != "" {
			spaceKey = strings.TrimSpace(strings.Split(spacesFilter, ",")[0])
		}

		// Wildcard or empty query with space filter → list space pages
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

	return searchContent(ds, cleanQuery, spaceKey, titleOnly, limit)
}

// searchContent does word-based AND matching on title and body.
func searchContent(ds *DataStore, query, spaceKey string, titleOnly bool, limit int) []SearchMatch {
	words := strings.Fields(strings.ToLower(query))
	if len(words) == 0 {
		return nil
	}

	var titleResults, bodyResults []SearchMatch

	for _, pr := range ds.pagesByID {
		if spaceKey != "" && !strings.EqualFold(pr.SpaceKey, spaceKey) {
			continue
		}

		titleLower := strings.ToLower(pr.Page.Title)
		if allWordsIn(words, titleLower) {
			titleResults = append(titleResults, SearchMatch{
				SpaceKey: pr.SpaceKey, Page: pr.Page, MatchType: "title",
			})
			continue
		}

		if !titleOnly {
			bodyLower := strings.ToLower(pr.Page.BodyText)
			if allWordsIn(words, bodyLower) {
				bodyResults = append(bodyResults, SearchMatch{
					SpaceKey: pr.SpaceKey, Page: pr.Page, MatchType: "body",
				})
			}
		}
	}

	all := append(titleResults, bodyResults...)
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

func allWordsIn(words []string, text string) bool {
	for _, w := range words {
		if !strings.Contains(text, w) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Minimal CQL parser (mirrors search.py)
// ---------------------------------------------------------------------------

var (
	reSpaceEq     = regexp.MustCompile(`(?i)space\s*=\s*["']?([A-Z0-9_-]+)["']?`)
	reSpaceIn     = regexp.MustCompile(`(?i)space\s+in\s+\(([^)]+)\)`)
	reTextSearch  = regexp.MustCompile(`(?i)text\s*~\s*["']([^"']+)["']`)
	reTitleSearch = regexp.MustCompile(`(?i)title\s*~\s*["']([^"']+)["']`)
)

// parseCQL extracts search terms, space filter, and title-only flag from CQL.
func parseCQL(cql string) (query, spaceKey string, titleOnly bool) {
	// Extract space filter
	if m := reSpaceEq.FindStringSubmatch(cql); m != nil {
		spaceKey = strings.Trim(m[1], `"' `)
	} else if m := reSpaceIn.FindStringSubmatch(cql); m != nil {
		parts := strings.Split(m[1], ",")
		spaceKey = strings.Trim(parts[0], `"' `)
	}

	// Extract search terms
	var terms []string
	for _, m := range reTextSearch.FindAllStringSubmatch(cql, -1) {
		terms = append(terms, m[1])
	}
	for _, m := range reTitleSearch.FindAllStringSubmatch(cql, -1) {
		terms = append(terms, m[1])
		titleOnly = true
	}

	if strings.Contains(strings.ToUpper(cql), " OR ") {
		query = strings.Join(terms, " ")
	} else {
		query = strings.Join(terms, " ")
	}

	if query == "" {
		query = "*"
	}
	return
}
