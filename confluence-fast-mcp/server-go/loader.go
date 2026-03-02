package main

import (
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/nlpodyssey/gopickle/pickle"
	"github.com/nlpodyssey/gopickle/types"
)

// Space represents a Confluence space.
type Space struct {
	SpaceKey   string
	Name       string
	TotalPages int
	Pages      []*Page
}

// Page represents a Confluence page.
type Page struct {
	ID            string
	Title         string
	ParentID      string
	BodyHTML      string
	BodyText      string // pre-extracted for search
	VersionNumber int
	VersionWhen   string
	VersionBy     string
	Labels        []string
	Comments      []Comment
}

// Comment represents a page comment.
type Comment struct {
	Author   string
	BodyHTML string
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

// NewDataStore loads all pickle files from the given directory.
func NewDataStore(pickleDir string) (*DataStore, error) {
	ds := &DataStore{
		spaces:           make(map[string]*Space),
		pagesByID:        make(map[string]PageResult),
		pagesByTitle:     make(map[string][]PageResult),
		childrenByParent: make(map[string][]PageResult),
	}

	files, err := filepath.Glob(filepath.Join(pickleDir, "*.pkl"))
	if err != nil {
		return nil, fmt.Errorf("glob pickle files: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .pkl files found in %s", pickleDir)
	}

	for _, f := range files {
		if err := ds.loadPickle(f); err != nil {
			log.Printf("Warning: skipping %s: %v", f, err)
		}
	}

	log.Printf("Loaded %d spaces with %d pages from %s", len(ds.spaces), len(ds.pagesByID), pickleDir)
	return ds, nil
}

func (ds *DataStore) loadPickle(path string) error {
	raw, err := pickle.Load(path)
	if err != nil {
		return fmt.Errorf("unpickle %s: %w", path, err)
	}

	d, ok := asDict(raw)
	if !ok {
		return fmt.Errorf("%s: root is not a dict", path)
	}

	spaceKey := dictStr(d, "space_key")
	if spaceKey == "" {
		return fmt.Errorf("%s: no space_key", path)
	}

	space := &Space{
		SpaceKey:   spaceKey,
		Name:       dictStr(d, "name"),
		TotalPages: dictInt(d, "total_pages_in_space"),
	}

	pagesList, _ := asList(dictVal(d, "sampled_pages"))
	if pagesList != nil {
		for _, raw := range *pagesList {
			pd, ok := asDict(raw)
			if !ok {
				continue
			}
			page := extractPage(pd)
			if page.BodyText == "" && page.BodyHTML != "" {
				page.BodyText = stripHTMLForSearch(page.BodyHTML)
			}

			space.Pages = append(space.Pages, page)
			pr := PageResult{SpaceKey: spaceKey, Page: page}

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
	}

	if space.TotalPages == 0 {
		space.TotalPages = len(space.Pages)
	}
	ds.spaces[spaceKey] = space
	return nil
}

// ---------------------------------------------------------------------------
// Page extraction from pickle dict
// ---------------------------------------------------------------------------

func extractPage(d *types.Dict) *Page {
	p := &Page{
		ID:    anyToStr(dictVal(d, "id")),
		Title: dictStr(d, "title"),
	}

	// Parent ID: try parent_id, then last ancestor
	p.ParentID = anyToStr(dictVal(d, "parent_id"))
	if p.ParentID == "" {
		if anc, ok := asList(dictVal(d, "ancestors")); ok && anc.Len() > 0 {
			if last, ok := asDict(anc.Get(anc.Len() - 1)); ok {
				p.ParentID = anyToStr(dictVal(last, "id"))
			}
		}
	}

	// Body HTML: body.storage.value
	if body, ok := asDict(dictVal(d, "body")); ok {
		if storage, ok := asDict(dictVal(body, "storage")); ok {
			p.BodyHTML = dictStr(storage, "value")
		} else if sv, ok := dictVal(body, "storage").(string); ok {
			p.BodyHTML = sv
		}
	} else if bv, ok := dictVal(d, "body").(string); ok {
		p.BodyHTML = bv
	}

	// Version
	if ver, ok := asDict(dictVal(d, "version")); ok {
		p.VersionNumber = dictInt(ver, "number")
		p.VersionWhen = dictStr(ver, "when")
		if by, ok := asDict(dictVal(ver, "by")); ok {
			p.VersionBy = dictStr(by, "displayName")
		}
	}
	if p.VersionNumber == 0 {
		p.VersionNumber = 1
	}

	// Labels: try page.labels, then page.metadata.labels.results
	p.Labels = extractLabels(d)

	// Comments: try page.comments, then page.children.comment.results
	p.Comments = extractComments(d)

	return p
}

func extractLabels(d *types.Dict) []string {
	var out []string

	if labels, ok := asList(dictVal(d, "labels")); ok {
		for i := 0; i < labels.Len(); i++ {
			if ld, ok := asDict(labels.Get(i)); ok {
				if n := dictStr(ld, "name"); n != "" {
					out = append(out, n)
				}
			} else if s, ok := labels.Get(i).(string); ok {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out
		}
	}

	// Fallback: metadata.labels.results
	if meta, ok := asDict(dictVal(d, "metadata")); ok {
		labelsRaw := dictVal(meta, "labels")
		if ld, ok := asDict(labelsRaw); ok {
			if results, ok := asList(dictVal(ld, "results")); ok {
				for i := 0; i < results.Len(); i++ {
					if rd, ok := asDict(results.Get(i)); ok {
						if n := dictStr(rd, "name"); n != "" {
							out = append(out, n)
						}
					}
				}
			}
		} else if ll, ok := asList(labelsRaw); ok {
			for i := 0; i < ll.Len(); i++ {
				if rd, ok := asDict(ll.Get(i)); ok {
					if n := dictStr(rd, "name"); n != "" {
						out = append(out, n)
					}
				}
			}
		}
	}
	return out
}

func extractComments(d *types.Dict) []Comment {
	var out []Comment

	tryList := func(raw interface{}) {
		if cl, ok := asList(raw); ok {
			for i := 0; i < cl.Len(); i++ {
				if cd, ok := asDict(cl.Get(i)); ok {
					c := Comment{}
					if ad, ok := asDict(dictVal(cd, "author")); ok {
						c.Author = dictStr(ad, "displayName")
					}
					if bd, ok := asDict(dictVal(cd, "body")); ok {
						if sd, ok := asDict(dictVal(bd, "storage")); ok {
							c.BodyHTML = dictStr(sd, "value")
						}
					}
					out = append(out, c)
				}
			}
		}
	}

	// Try page.comments directly
	tryList(dictVal(d, "comments"))
	if len(out) > 0 {
		return out
	}

	// Fallback: page.children.comment.results
	if children, ok := asDict(dictVal(d, "children")); ok {
		if commentD, ok := asDict(dictVal(children, "comment")); ok {
			tryList(dictVal(commentD, "results"))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Dict/List navigation helpers
// ---------------------------------------------------------------------------

func asDict(v interface{}) (*types.Dict, bool) {
	d, ok := v.(*types.Dict)
	return d, ok
}

func asList(v interface{}) (*types.List, bool) {
	l, ok := v.(*types.List)
	return l, ok
}

func dictVal(d *types.Dict, key string) interface{} {
	if d == nil {
		return nil
	}
	v, _ := d.Get(key)
	return v
}

func dictStr(d *types.Dict, key string) string {
	v := dictVal(d, key)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func dictInt(d *types.Dict, key string) int {
	v := dictVal(d, key)
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func anyToStr(v interface{}) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case int:
		return fmt.Sprintf("%d", s)
	case int64:
		return fmt.Sprintf("%d", s)
	default:
		return fmt.Sprintf("%v", s)
	}
}

var reHTMLTag = regexp.MustCompile(`<[^>]+>`)

func stripHTMLForSearch(html string) string {
	text := reHTMLTag.ReplaceAllString(html, " ")
	return strings.Join(strings.Fields(text), " ")
}

// ---------------------------------------------------------------------------
// Lookup methods
// ---------------------------------------------------------------------------

func (ds *DataStore) GetPageByID(id string) *PageResult {
	pr, ok := ds.pagesByID[id]
	if !ok {
		return nil
	}
	return &pr
}

func (ds *DataStore) GetPageByTitle(title, spaceKey string) *PageResult {
	titleLower := strings.ToLower(title)

	if results, ok := ds.pagesByTitle[titleLower]; ok {
		for i := range results {
			if spaceKey == "" || strings.EqualFold(results[i].SpaceKey, spaceKey) {
				return &results[i]
			}
		}
	}

	// Partial match
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

func (ds *DataStore) GetPagesInSpace(spaceKey string, limit int) []*Page {
	space, ok := ds.spaces[strings.ToUpper(spaceKey)]
	if !ok {
		space, ok = ds.spaces[spaceKey]
		if !ok {
			return nil
		}
	}
	pages := space.Pages
	if limit > len(pages) {
		limit = len(pages)
	}
	result := make([]*Page, limit)
	copy(result, pages[:limit])
	return result
}
