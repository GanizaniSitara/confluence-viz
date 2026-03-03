package main

import (
	"fmt"
	"regexp"
	"strings"
)

// HTMLToText strips HTML tags and returns plain text.
func HTMLToText(s string) string {
	if s == "" {
		return ""
	}
	s = reTag.ReplaceAllString(s, " ")
	s = decodeEntities(s)
	return collapseWhitespace(s)
}

// HTMLToMarkdown converts Confluence HTML storage format to Markdown.
func HTMLToMarkdown(s string) string {
	if s == "" {
		return ""
	}
	return strings.TrimSpace(convertHTML(s))
}

// ---------------------------------------------------------------------------
// HTML-to-Markdown converter using regex-based tag matching.
// Confluence storage format is well-formed XML so this works reliably.
// ---------------------------------------------------------------------------

var (
	reTag       = regexp.MustCompile(`<[^>]+>`)
	reOpenTag   = regexp.MustCompile(`<(\w[\w:-]*)\b([^>]*)>`)
	reCloseTag  = regexp.MustCompile(`</(\w[\w:-]*)>`)
	reSelfClose = regexp.MustCompile(`<(\w[\w:-]*)\b([^>]*)/\s*>`)
	reAttr      = regexp.MustCompile(`(\w+)\s*=\s*"([^"]*)"`)
	reWS        = regexp.MustCompile(`\s+`)
)

func convertHTML(html string) string {
	// Pre-process: convert self-closing <br/>, <hr/>, <img .../> first
	html = reSelfClose.ReplaceAllStringFunc(html, func(m string) string {
		sub := reSelfClose.FindStringSubmatch(m)
		tag := strings.ToLower(sub[1])
		attrs := sub[2]
		switch tag {
		case "br":
			return "  \n"
		case "hr":
			return "\n\n---\n\n"
		case "img":
			alt := getAttrVal(attrs, "alt")
			src := getAttrVal(attrs, "src")
			return fmt.Sprintf("![%s](%s)", alt, src)
		}
		return ""
	})

	// Process block-level elements
	html = convertHeadings(html)
	html = convertCodeBlocks(html)
	html = convertBlockquotes(html)
	html = convertLists(html)
	html = convertTables(html)
	html = convertParagraphs(html)
	html = convertInline(html)

	// Strip remaining tags
	html = reTag.ReplaceAllString(html, "")
	html = decodeEntities(html)

	return html
}

// --- Block elements ---

var reHeading = regexp.MustCompile(`(?is)<(h[1-6])\b[^>]*>(.*?)</\1>`)

func convertHeadings(s string) string {
	return reHeading.ReplaceAllStringFunc(s, func(m string) string {
		sub := reHeading.FindStringSubmatch(m)
		level := int(sub[1][1] - '0')
		text := stripTags(sub[2])
		return fmt.Sprintf("\n\n%s %s\n\n", strings.Repeat("#", level), strings.TrimSpace(text))
	})
}

var rePre = regexp.MustCompile(`(?is)<pre\b[^>]*>(.*?)</pre>`)

func convertCodeBlocks(s string) string {
	return rePre.ReplaceAllStringFunc(s, func(m string) string {
		sub := rePre.FindStringSubmatch(m)
		code := stripTags(sub[1])
		code = decodeEntities(code)
		return fmt.Sprintf("\n\n```\n%s\n```\n\n", code)
	})
}

var reBlockquote = regexp.MustCompile(`(?is)<blockquote\b[^>]*>(.*?)</blockquote>`)

func convertBlockquotes(s string) string {
	return reBlockquote.ReplaceAllStringFunc(s, func(m string) string {
		sub := reBlockquote.FindStringSubmatch(m)
		inner := stripTags(sub[1])
		inner = strings.TrimSpace(inner)
		lines := strings.Split(inner, "\n")
		quoted := make([]string, len(lines))
		for i, l := range lines {
			quoted[i] = "> " + l
		}
		return "\n" + strings.Join(quoted, "\n") + "\n"
	})
}

var (
	reUL = regexp.MustCompile(`(?is)<ul\b[^>]*>(.*?)</ul>`)
	reOL = regexp.MustCompile(`(?is)<ol\b[^>]*>(.*?)</ol>`)
	reLI = regexp.MustCompile(`(?is)<li\b[^>]*>(.*?)</li>`)
)

func convertLists(s string) string {
	// Unordered
	s = reUL.ReplaceAllStringFunc(s, func(m string) string {
		sub := reUL.FindStringSubmatch(m)
		items := reLI.FindAllStringSubmatch(sub[1], -1)
		var lines []string
		for _, item := range items {
			text := strings.TrimSpace(stripTags(item[1]))
			lines = append(lines, "- "+text)
		}
		return "\n" + strings.Join(lines, "\n") + "\n"
	})

	// Ordered
	s = reOL.ReplaceAllStringFunc(s, func(m string) string {
		sub := reOL.FindStringSubmatch(m)
		items := reLI.FindAllStringSubmatch(sub[1], -1)
		var lines []string
		for i, item := range items {
			text := strings.TrimSpace(stripTags(item[1]))
			lines = append(lines, fmt.Sprintf("%d. %s", i+1, text))
		}
		return "\n" + strings.Join(lines, "\n") + "\n"
	})
	return s
}

var (
	reTable = regexp.MustCompile(`(?is)<table\b[^>]*>(.*?)</table>`)
	reTR    = regexp.MustCompile(`(?is)<tr\b[^>]*>(.*?)</tr>`)
	reTD    = regexp.MustCompile(`(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>`)
)

func convertTables(s string) string {
	return reTable.ReplaceAllStringFunc(s, func(m string) string {
		sub := reTable.FindStringSubmatch(m)
		rows := reTR.FindAllStringSubmatch(sub[1], -1)
		if len(rows) == 0 {
			return ""
		}

		var sb strings.Builder
		sb.WriteString("\n\n")
		for i, row := range rows {
			cells := reTD.FindAllStringSubmatch(row[1], -1)
			texts := make([]string, len(cells))
			for j, cell := range cells {
				texts[j] = strings.ReplaceAll(strings.TrimSpace(stripTags(cell[1])), "|", `\|`)
			}
			sb.WriteString("| " + strings.Join(texts, " | ") + " |\n")
			if i == 0 {
				sep := make([]string, len(texts))
				for j := range sep {
					sep[j] = "---"
				}
				sb.WriteString("| " + strings.Join(sep, " | ") + " |\n")
			}
		}
		sb.WriteString("\n")
		return sb.String()
	})
}

var reParagraph = regexp.MustCompile(`(?is)<p\b[^>]*>(.*?)</p>`)

func convertParagraphs(s string) string {
	return reParagraph.ReplaceAllStringFunc(s, func(m string) string {
		sub := reParagraph.FindStringSubmatch(m)
		inner := convertInline(sub[1])
		inner = reTag.ReplaceAllString(inner, "")
		inner = decodeEntities(strings.TrimSpace(inner))
		if inner == "" {
			return ""
		}
		return "\n\n" + inner + "\n\n"
	})
}

// --- Inline elements ---

var (
	reStrong = regexp.MustCompile(`(?is)<(strong|b)\b[^>]*>(.*?)</\1>`)
	reEm     = regexp.MustCompile(`(?is)<(em|i)\b[^>]*>(.*?)</\1>`)
	reCode   = regexp.MustCompile(`(?is)<code\b[^>]*>(.*?)</code>`)
	reStrike = regexp.MustCompile(`(?is)<(s|strike|del)\b[^>]*>(.*?)</\1>`)
	reAnchor = regexp.MustCompile(`(?is)<a\b([^>]*)>(.*?)</a>`)
)

func convertInline(s string) string {
	s = reStrong.ReplaceAllString(s, "**$2**")
	s = reEm.ReplaceAllString(s, "*$2*")
	s = reCode.ReplaceAllString(s, "`$1`")
	s = reStrike.ReplaceAllString(s, "~~$2~~")
	s = reAnchor.ReplaceAllStringFunc(s, func(m string) string {
		sub := reAnchor.FindStringSubmatch(m)
		href := getAttrVal(sub[1], "href")
		text := strings.TrimSpace(stripTags(sub[2]))
		if text == "" {
			text = href
		}
		return fmt.Sprintf("[%s](%s)", text, href)
	})
	return s
}

// --- Helpers ---

func stripTags(s string) string {
	return reTag.ReplaceAllString(s, "")
}

func getAttrVal(attrs, key string) string {
	for _, m := range reAttr.FindAllStringSubmatch(attrs, -1) {
		if m[1] == key {
			return m[2]
		}
	}
	return ""
}

func decodeEntities(s string) string {
	r := strings.NewReplacer(
		"&amp;", "&",
		"&lt;", "<",
		"&gt;", ">",
		"&quot;", `"`,
		"&#39;", "'",
		"&apos;", "'",
		"&nbsp;", " ",
	)
	return r.Replace(s)
}

func collapseWhitespace(s string) string {
	return strings.TrimSpace(reWS.ReplaceAllString(s, " "))
}
