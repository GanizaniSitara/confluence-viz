// Confluence Simple MCP Server (Go)
//
// In-memory search server that reads JSON files exported by convert_to_json.py.
// Drop-in replacement for simple_server.py — same MCP tools, same output format.
//
// Usage:
//
//	go build -o simple-server . && ./simple-server              # HTTP on :8070
//	./simple-server --port 9000                                  # custom port
//	./simple-server --stdio                                      # stdio for Claude Desktop
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

var store *DataStore

func main() {
	stdio := flag.Bool("stdio", false, "Run in stdio mode (for Claude Desktop)")
	port := flag.Int("port", 8070, "HTTP port (default: 8070)")
	dataDir := flag.String("data-dir", "../json_data", "Directory containing JSON files")
	flag.Parse()

	var err error
	store, err = NewDataStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}

	s := server.NewMCPServer("confluence-simple", "1.0.0")
	registerTools(s)

	if *stdio {
		log.Println("Starting Confluence Simple MCP server in stdio mode...")
		stdio := server.NewStdioServer(s)
		if err := stdio.Listen(context.Background(), os.Stdin, os.Stdout); err != nil {
			log.Fatalf("stdio server error: %v", err)
		}
	} else {
		addr := fmt.Sprintf(":%d", *port)
		log.Printf("Starting Confluence Simple MCP server on http://0.0.0.0%s ...", addr)
		sse := server.NewSSEServer(s)
		if err := sse.Start(addr); err != nil {
			log.Fatalf("SSE server error: %v", err)
		}
	}
}

func registerTools(s *server.MCPServer) {
	// confluence_search
	s.AddTool(
		mcp.NewTool("confluence_search",
			mcp.WithDescription("Search Confluence content."),
			mcp.WithString("query", mcp.Required(), mcp.Description("Search query - simple text or CQL query string")),
			mcp.WithNumber("limit", mcp.Description("Maximum number of results (1-50)")),
			mcp.WithString("spaces_filter", mcp.Description("Comma-separated list of space keys to filter results")),
		),
		handleSearch,
	)

	// confluence_get_page
	s.AddTool(
		mcp.NewTool("confluence_get_page",
			mcp.WithDescription("Get a Confluence page by ID or title."),
			mcp.WithString("page_id", mcp.Description("Numeric page ID from URL")),
			mcp.WithString("title", mcp.Description("Exact page title")),
			mcp.WithString("space_key", mcp.Description("Space key (required when using title)")),
			mcp.WithBoolean("include_metadata", mcp.Description("Whether to include creation date, version, labels")),
			mcp.WithBoolean("convert_to_markdown", mcp.Description("Whether to convert HTML body to markdown")),
		),
		handleGetPage,
	)

	// confluence_get_page_children
	s.AddTool(
		mcp.NewTool("confluence_get_page_children",
			mcp.WithDescription("Get child pages of a specific page."),
			mcp.WithString("parent_id", mcp.Required(), mcp.Description("ID of the parent page")),
			mcp.WithNumber("limit", mcp.Description("Maximum child items to return (1-50)")),
			mcp.WithBoolean("include_content", mcp.Description("Whether to include page body content")),
			mcp.WithBoolean("convert_to_markdown", mcp.Description("Convert to markdown or return raw HTML")),
			mcp.WithNumber("start", mcp.Description("Starting index for pagination")),
		),
		handleGetChildren,
	)

	// confluence_get_comments
	s.AddTool(
		mcp.NewTool("confluence_get_comments",
			mcp.WithDescription("Get comments on a Confluence page."),
			mcp.WithString("page_id", mcp.Required(), mcp.Description("Confluence page ID")),
		),
		handleGetComments,
	)

	// confluence_get_labels
	s.AddTool(
		mcp.NewTool("confluence_get_labels",
			mcp.WithDescription("Get labels for a Confluence page."),
			mcp.WithString("page_id", mcp.Required(), mcp.Description("Content ID")),
		),
		handleGetLabels,
	)
}

// ---------------------------------------------------------------------------
// Tool handlers
// ---------------------------------------------------------------------------

func handleSearch(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := stringArg(req, "query")
	limit := intArg(req, "limit", 10)
	spacesFilter := stringArg(req, "spaces_filter")

	if limit < 1 {
		limit = 1
	} else if limit > 50 {
		limit = 50
	}

	matches := Search(store, query, spacesFilter, limit)
	return mcp.NewToolResultText(FormatSearchResults(matches, query)), nil
}

func handleGetPage(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pageID := stringArg(req, "page_id")
	title := stringArg(req, "title")
	spaceKey := stringArg(req, "space_key")
	includeMeta := boolArg(req, "include_metadata", true)
	toMarkdown := boolArg(req, "convert_to_markdown", true)

	var pr *PageResult

	if pageID != "" {
		pr = store.GetPageByID(pageID)
	}
	if pr == nil && title != "" {
		pr = store.GetPageByTitle(title, spaceKey)
	}
	if pr == nil && pageID != "" && !isNumeric(pageID) {
		pr = store.GetPageByTitle(pageID, spaceKey)
	}
	if pr == nil {
		id := pageID
		if id == "" {
			id = title
		}
		if id == "" {
			id = "unknown"
		}
		return mcp.NewToolResultText(fmt.Sprintf("Page not found: %s", id)), nil
	}

	return mcp.NewToolResultText(FormatPage(pr, includeMeta, toMarkdown)), nil
}

func handleGetChildren(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	parentID := stringArg(req, "parent_id")
	limit := intArg(req, "limit", 25)
	start := intArg(req, "start", 0)
	includeContent := boolArg(req, "include_content", false)
	toMarkdown := boolArg(req, "convert_to_markdown", true)

	children := store.GetChildren(parentID, limit, start)
	if len(children) == 0 {
		return mcp.NewToolResultText(fmt.Sprintf("No child pages found for parent %s", parentID)), nil
	}

	return mcp.NewToolResultText(FormatChildren(children, parentID, includeContent, toMarkdown)), nil
}

func handleGetComments(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pageID := stringArg(req, "page_id")

	pr := store.GetPageByID(pageID)
	if pr == nil {
		return mcp.NewToolResultText(fmt.Sprintf("Page not found: %s", pageID)), nil
	}

	comments := pr.Page.Comments
	if len(comments) == 0 {
		return mcp.NewToolResultText(
			fmt.Sprintf("No comments found for page %s (page: %s)", pageID, pr.Page.Title),
		), nil
	}

	return mcp.NewToolResultText(FormatComments(pr.Page, comments)), nil
}

func handleGetLabels(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pageID := stringArg(req, "page_id")

	pr := store.GetPageByID(pageID)
	if pr == nil {
		return mcp.NewToolResultText(fmt.Sprintf("Page not found: %s", pageID)), nil
	}

	labels := pr.Page.Labels
	if len(labels) == 0 {
		return mcp.NewToolResultText(
			fmt.Sprintf("No labels found for page %s (page: %s)", pageID, pr.Page.Title),
		), nil
	}

	return mcp.NewToolResultText(FormatLabels(pr.Page.Title, labels)), nil
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

func stringArg(req mcp.CallToolRequest, key string) string {
	args := req.GetArguments()
	v, _ := args[key].(string)
	return v
}

func intArg(req mcp.CallToolRequest, key string, def int) int {
	args := req.GetArguments()
	v, ok := args[key].(float64)
	if !ok {
		return def
	}
	return int(v)
}

func boolArg(req mcp.CallToolRequest, key string, def bool) bool {
	args := req.GetArguments()
	v, ok := args[key].(bool)
	if !ok {
		return def
	}
	return v
}

func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
