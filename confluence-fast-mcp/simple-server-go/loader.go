package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Space represents a Confluence space loaded from JSON.
type Space struct {
	SpaceKey     string `json:"space_key"`
	Name         string `json:"name"`
	TotalPages   int    `json:"total_pages_in_space"`
	SampledPages []Page `json:"sampled_pages"`
}

// Page represents a Confluence page.
type Page struct {
	ID            string    `json:"id"`
	Title         string    `json:"title"`
	ParentID      string    `json:"parent_id"`
	BodyHTML      string    `json:"body_html"`
	BodyText      string    `json:"body_text"`
	VersionNumber int       `json:"version_number"`
	VersionWhen   string    `json:"version_when"`
	VersionBy     string    `json:"version_by"`
	Labels        []string  `json:"labels"`
	AncestorIDs   []string  `json:"ancestor_ids"`
	Comments      []Comment `json:"comments"`
}

// Comment represents a page comment.
type Comment struct {
	Author   string `json:"author"`
	BodyHTML string `json:"body_html"`
}

// PageResult wraps a page with its space key.
type PageResult struct {
	SpaceKey string
	Page     *Page
}

// DataStore holds all loaded data with in-memory indexes.
type DataStore struct {
	spaces           map[string]*Space
	pagesByID        map[string]PageResult
	pagesByTitle     map[string][]PageResult // lowercase title -> results
	childrenByParent map[string][]PageResult
}

// NewDataStore loads all JSON files from the given directory.
func NewDataStore(jsonDir string) (*DataStore, error) {
	ds := &DataStore{
		spaces:           make(map[string]*Space),
		pagesByID:        make(map[string]PageResult),
		pagesByTitle:     make(map[string][]PageResult),
		childrenByParent: make(map[string][]PageResult),
	}

	files, err := filepath.Glob(filepath.Join(jsonDir, "*.json"))
	if err != nil {
		return nil, fmt.Errorf("glob json files: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .json files found in %s (run convert_to_json.py first)", jsonDir)
	}

	for _, f := range files {
		if err := ds.loadFile(f); err != nil {
			log.Printf("Warning: skipping %s: %v", f, err)
		}
	}

	log.Printf("Loaded %d spaces with %d pages from %s", len(ds.spaces), len(ds.pagesByID), jsonDir)
	return ds, nil
}

func (ds *DataStore) loadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var space Space
	if err := json.Unmarshal(data, &space); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if space.SpaceKey == "" {
		return fmt.Errorf("no space_key in %s", path)
	}

	ds.spaces[space.SpaceKey] = &space

	for i := range space.SampledPages {
		page := &space.SampledPages[i]
		pr := PageResult{SpaceKey: space.SpaceKey, Page: page}

		if page.ID != "" {
			ds.pagesByID[page.ID] = pr
		}
		if page.Title != "" {
			key := strings.ToLower(page.Title)
			ds.pagesByTitle[key] = append(ds.pagesByTitle[key], pr)
		}
		if page.ParentID != "" {
			ds.childrenByParent[page.ParentID] = append(ds.childrenByParent[page.ParentID], pr)
		}
	}
	return nil
}

// GetPageByID looks up a page by its numeric ID.
func (ds *DataStore) GetPageByID(id string) *PageResult {
	pr, ok := ds.pagesByID[id]
	if !ok {
		return nil
	}
	return &pr
}

// GetPageByTitle does a flexible title lookup with fallbacks.
func (ds *DataStore) GetPageByTitle(title, spaceKey string) *PageResult {
	titleLower := strings.ToLower(title)

	// 1. Exact case-insensitive match
	if results, ok := ds.pagesByTitle[titleLower]; ok {
		for i := range results {
			if spaceKey == "" || strings.EqualFold(results[i].SpaceKey, spaceKey) {
				return &results[i]
			}
		}
	}

	// 2. Partial match — prefer closest length
	type candidate struct {
		pr   PageResult
		diff int
	}
	var best *candidate

	for key, results := range ds.pagesByTitle {
		if !strings.Contains(key, titleLower) && !strings.Contains(titleLower, key) {
			continue
		}
		for _, pr := range results {
			if spaceKey != "" && !strings.EqualFold(pr.SpaceKey, spaceKey) {
				continue
			}
			diff := len(pr.Page.Title) - len(title)
			if diff < 0 {
				diff = -diff
			}
			if best == nil || diff < best.diff {
				best = &candidate{pr: pr, diff: diff}
			}
		}
	}

	if best != nil {
		return &best.pr
	}
	return nil
}

// GetChildren returns child pages of a parent.
func (ds *DataStore) GetChildren(parentID string, limit, start int) []PageResult {
	children := ds.childrenByParent[parentID]
	if start >= len(children) {
		return nil
	}
	end := start + limit
	if end > len(children) {
		end = len(children)
	}
	return children[start:end]
}

// GetPagesInSpace returns pages from a specific space.
func (ds *DataStore) GetPagesInSpace(spaceKey string, limit int) []*Page {
	space, ok := ds.spaces[strings.ToUpper(spaceKey)]
	if !ok {
		// Try original case
		space, ok = ds.spaces[spaceKey]
		if !ok {
			return nil
		}
	}
	pages := space.SampledPages
	if limit > len(pages) {
		limit = len(pages)
	}
	result := make([]*Page, limit)
	for i := 0; i < limit; i++ {
		result[i] = &pages[i]
	}
	return result
}
