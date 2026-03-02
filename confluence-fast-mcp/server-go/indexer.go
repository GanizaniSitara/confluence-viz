package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

// InvertedIndex provides full-text search with TF-IDF scoring.
// Replaces WHOOSH for this use case.
type InvertedIndex struct {
	// term -> list of postings (docID + fields that matched)
	Postings map[string][]Posting `json:"postings"`
	// docID -> document metadata
	Docs map[string]DocMeta `json:"docs"`
	// total documents
	NumDocuments int `json:"num_documents"`
}

// Posting is a single term occurrence in a document.
type Posting struct {
	DocID      string  `json:"d"`
	TitleFreq  float64 `json:"tf,omitempty"` // frequency in title (boosted)
	BodyFreq   float64 `json:"bf,omitempty"` // frequency in body
}

// DocMeta stores document metadata in the index.
type DocMeta struct {
	SpaceKey string `json:"sk"`
	Title    string `json:"t"`
}

// BuildIndex creates a new inverted index from a DataStore.
func BuildIndex(ds *DataStore) *InvertedIndex {
	start := time.Now()

	idx := &InvertedIndex{
		Postings: make(map[string][]Posting),
		Docs:     make(map[string]DocMeta),
	}

	count := 0
	for id, pr := range ds.pagesByID {
		idx.indexPage(id, pr)
		count++
		if count%5000 == 0 {
			log.Printf("  Indexed %d pages...", count)
		}
	}

	idx.NumDocuments = count
	log.Printf("Index built in %v: %d documents, %d unique terms",
		time.Since(start).Round(time.Millisecond), count, len(idx.Postings))

	return idx
}

func (idx *InvertedIndex) indexPage(id string, pr PageResult) {
	idx.Docs[id] = DocMeta{SpaceKey: pr.SpaceKey, Title: pr.Page.Title}

	titleTokens := tokenize(pr.Page.Title)
	bodyTokens := tokenize(pr.Page.BodyText)

	// Count term frequencies
	titleFreqs := termFreqs(titleTokens)
	bodyFreqs := termFreqs(bodyTokens)

	// Merge all terms
	allTerms := make(map[string]struct{})
	for t := range titleFreqs {
		allTerms[t] = struct{}{}
	}
	for t := range bodyFreqs {
		allTerms[t] = struct{}{}
	}

	for term := range allTerms {
		p := Posting{DocID: id}
		if f, ok := titleFreqs[term]; ok {
			p.TitleFreq = f
		}
		if f, ok := bodyFreqs[term]; ok {
			p.BodyFreq = f
		}
		idx.Postings[term] = append(idx.Postings[term], p)
	}
}

// SearchResult is a scored search result.
type SearchResult struct {
	DocID     string
	SpaceKey  string
	Title     string
	Score     float64
	MatchType string // "title" or "body"
}

// SearchIndex queries the inverted index with TF-IDF scoring.
func (idx *InvertedIndex) SearchIndex(query string, spaceKey string, titleOnly bool, limit int) []SearchResult {
	terms := tokenize(query)
	if len(terms) == 0 {
		return nil
	}

	// Score accumulator per document
	scores := make(map[string]float64)
	titleMatch := make(map[string]bool)

	n := float64(idx.NumDocuments)

	for _, term := range terms {
		postings, ok := idx.Postings[term]
		if !ok {
			continue
		}

		// IDF = log(N / df)
		df := float64(len(postings))
		idf := math.Log(n / df)

		for _, p := range postings {
			// Space filter
			if spaceKey != "" {
				if doc, ok := idx.Docs[p.DocID]; ok {
					if !strings.EqualFold(doc.SpaceKey, spaceKey) {
						continue
					}
				}
			}

			// Title match gets 2x boost (matching WHOOSH schema)
			titleScore := p.TitleFreq * idf * 2.0
			bodyScore := 0.0
			if !titleOnly {
				bodyScore = p.BodyFreq * idf
			}

			score := titleScore + bodyScore
			scores[p.DocID] += score

			if p.TitleFreq > 0 {
				titleMatch[p.DocID] = true
			}
		}
	}

	// Sort by score
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		doc := idx.Docs[docID]
		mt := "body"
		if titleMatch[docID] {
			mt = "title"
		}
		results = append(results, SearchResult{
			DocID:     docID,
			SpaceKey:  doc.SpaceKey,
			Title:     doc.Title,
			Score:     score,
			MatchType: mt,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > limit {
		results = results[:limit]
	}
	return results
}

// Save writes the index to disk as JSON.
func (idx *InvertedIndex) Save(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}
	path := filepath.Join(dir, "index.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	log.Printf("Index saved to %s (%.1f MB)", path, float64(len(data))/1024/1024)
	return nil
}

// LoadIndex reads a previously saved index from disk.
func LoadIndex(dir string) (*InvertedIndex, error) {
	path := filepath.Join(dir, "index.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var idx InvertedIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}
	return &idx, nil
}

// NumTerms returns the number of unique terms.
func (idx *InvertedIndex) NumTerms() int { return len(idx.Postings) }

// NumDocs returns the number of indexed documents.
func (idx *InvertedIndex) NumDocs() int { return idx.NumDocuments }

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

func tokenize(s string) []string {
	s = strings.ToLower(s)
	var tokens []string
	var buf strings.Builder

	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			buf.WriteRune(r)
		} else {
			if buf.Len() > 0 {
				tok := buf.String()
				if len(tok) >= 2 && !isStopWord(tok) {
					tokens = append(tokens, tok)
				}
				buf.Reset()
			}
		}
	}
	if buf.Len() > 0 {
		tok := buf.String()
		if len(tok) >= 2 && !isStopWord(tok) {
			tokens = append(tokens, tok)
		}
	}
	return tokens
}

func termFreqs(tokens []string) map[string]float64 {
	freqs := make(map[string]float64)
	for _, t := range tokens {
		freqs[t]++
	}
	// Normalize by length
	n := float64(len(tokens))
	if n > 0 {
		for k := range freqs {
			freqs[k] /= n
		}
	}
	return freqs
}

var stopWords = map[string]bool{
	"the": true, "and": true, "is": true, "in": true, "to": true,
	"of": true, "it": true, "for": true, "on": true, "with": true,
	"as": true, "at": true, "by": true, "an": true, "be": true,
	"this": true, "that": true, "from": true, "or": true, "are": true,
	"was": true, "were": true, "has": true, "have": true, "had": true,
	"not": true, "but": true, "all": true, "can": true, "will": true,
	"just": true, "if": true, "we": true, "you": true, "do": true,
	"no": true, "so": true, "up": true, "out": true, "about": true,
}

func isStopWord(s string) bool { return stopWords[s] }
