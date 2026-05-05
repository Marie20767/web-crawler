package consumer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marie20767/web-crawler/shared/httperr"
)

type robots struct {
	CrawlDelay      string   `bson:"crawlDelay"`
	AllowedPaths    []string `bson:"allowedPaths"`
	DisallowedPaths []string `bson:"disallowedPaths"`
}

func (c *Consumer) fetchRobots(ctx context.Context, scheme, host string) (data []byte, err error) {
	robotsURL := scheme + "://" + host + "/robots.txt"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create robots request %v", err)
	}

	req.Header.Set("User-Agent", userAgent)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("make robots request %v", err)
	}
	defer res.Body.Close() //nolint:errcheck

	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if res.StatusCode >= minErrStatusCode {
		return nil, &httperr.Err{StatusCode: res.StatusCode}
	}

	robotsData, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read robots response: %v", err)
	}

	return robotsData, nil
}

func parseRobots(robotsData []byte) robots {
	scanner := bufio.NewScanner(bytes.NewReader(robotsData))

	const keyValLen = 2
	groupDelay := time.Duration(0)
	globalDelay := defaultCrawlDelay
	inGroup := false
	groupDone := false
	var allowed []string
	var disallowed []string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", keyValLen)
		if len(parts) != keyValLen {
			continue
		}

		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])

		switch key {
		case "user-agent":
			if groupDone {
				inGroup = false
				groupDone = false
			}
			if val == "*" {
				inGroup = true
			}

		case "allow", "disallow":
			if inGroup {
				groupDone = true
				val = stripComment(val)
				if val != "" {
					if key == "allow" {
						allowed = append(allowed, val)
					} else {
						disallowed = append(disallowed, val)
					}
				}
			}

		case "crawl-delay":
			delay, err := strconv.ParseFloat(val, 64)
			if err != nil {
				slog.Error("convert crawl delay", slog.Any("error", err))
			} else {
				delayDur := time.Duration(delay * float64(time.Second))
				if inGroup {
					groupDelay = delayDur
				} else {
					globalDelay = delayDur
				}
			}
		}
	}

	crawlDelay := globalDelay
	if groupDelay != 0 {
		crawlDelay = groupDelay
	}

	return robots{
		AllowedPaths:    allowed,
		DisallowedPaths: disallowed,
		CrawlDelay:      crawlDelay.String(),
	}
}

func isPathAllowed(path string, allowedPaths, disallowedPaths []string) bool {
	longestMatch := 0
	isAllowed := true

	for _, disallowed := range disallowedPaths {
		if disallowed == "*" {
			isAllowed = false
			break
		}

		if strings.HasPrefix(path, disallowed) && len(disallowed) > longestMatch {
			longestMatch = len(disallowed)
			isAllowed = false
		}
	}

	for _, allowed := range allowedPaths {
		if allowed == "*" {
			return true
		}

		if strings.HasPrefix(path, allowed) && len(allowed) >= longestMatch {
			longestMatch = len(allowed)
			isAllowed = true
		}
	}

	return isAllowed
}

func stripComment(val string) string {
	if i := strings.Index(val, "#"); i != -1 {
		return strings.TrimSpace(val[:i])
	}

	return val
}
