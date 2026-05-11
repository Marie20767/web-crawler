package consumer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/marie20767/web-crawler/shared/httperr"
)

const userAgent = "marie-web-crawler"

func (c *Consumer) fetchHTMLWithLimit(ctx context.Context, pageURL string) (data []byte, skipped bool, err error) {
	const maxContentSize = 2 * 1024 * 1024 // 2 MB

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, http.NoBody)
	if err != nil {
		return nil, false, fmt.Errorf("create web page request %v", err)
	}
	req.Header.Set("User-Agent", userAgent)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("make web page request %v", err)
	}
	defer res.Body.Close() //nolint:errcheck
	if res.StatusCode >= minErrStatusCode {
		return nil, false, &httperr.Err{StatusCode: res.StatusCode}
	}

	if res.ContentLength > maxContentSize {
		slog.Info("skipped large web page based on content-length header", slog.Int64("content-length", res.ContentLength))
		return nil, true, nil
	}

	// fallback if content-length header is absent/untrustworthy
	limited := io.LimitReader(res.Body, maxContentSize+1)
	data, err = io.ReadAll(limited)
	if err != nil {
		return nil, false, fmt.Errorf("read response: %v", err)
	}

	if int64(len(data)) > maxContentSize {
		slog.Info("skipped large web page request", slog.Int64("content-length", int64(len(data))))
		return nil, true, nil
	}

	return data, false, nil
}
