package consumer

import (
	"bytes"
	"fmt"
	"net/url"
	"path"
	"strings"

	"golang.org/x/net/html"
)

type parsed struct {
	text string
	urls []string
}

func (c *Consumer) parseRawHTML(raw []byte, baseURL *url.URL) (*parsed, error) {
	doc, err := html.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse raw HTML %v", err)
	}

	var sb strings.Builder
	var urls []string
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		if n.Parent != nil {
			switch n.Type {
			case html.TextNode:
				switch n.Parent.Data {
				case "script", "style":
				// skip
				default:
					text := strings.TrimSpace(n.Data)
					if text != "" {
						if sb.Len() > 0 {
							sb.WriteByte(' ')
						}
						sb.WriteString(text)
					}
				}

			case html.ElementNode:
				if n.Data == "a" {
					for _, attr := range n.Attr {
						if attr.Key != "href" {
							continue
						}

						parsedHref, hrefErr := url.Parse(attr.Val)

						if hrefErr != nil || isResourceURL(parsedHref) {
							continue
						}

						switch parsedHref.Scheme {
						case "", "http", "https":
						default:
							continue
						}

						resolved := baseURL.ResolveReference(parsedHref)
						urls = append(urls, resolved.String())
					}
				}
			}
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}

	walk(doc)

	return &parsed{
		text: sb.String(),
		urls: urls,
	}, nil
}

func isResourceURL(u *url.URL) bool {
	switch strings.ToLower(path.Ext(u.Path)) {
	case ".js", ".css", ".woff", ".woff2", ".ttf", ".eot", ".otf",
		".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp":
		return true
	}

	return false
}
