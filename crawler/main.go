package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/config"
)

const (
	kafkaTimeout      = 5 * time.Second
	kafkaMinBatchSize = 10e3            // 10KB
	kafkaMaxBatchSize = 10e6            // 10MB
	maxContentSize    = 2 * 1024 * 1024 // 2 MB
)

func main() {
	if err := run(); err != nil {
		slog.Error("crawler run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down crawler...")
}

func run() error {
	ctx := context.Background()

	cfg, err := config.ParseEnv()
	if err != nil {
		slog.Error("failed to parse env vars", slog.Any("error", err))
		return fmt.Errorf("failed to parse env vars: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	reader := newReader(cfg.Kafka.Broker, cfg.Kafka.Topic)
	defer reader.Close() //nolint:errcheck

	client := &http.Client{}

	for {
		readCtx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		msg, err := reader.ReadMessage(readCtx)
		cancel()

		if errors.Is(err, context.DeadlineExceeded) {
			break
		}

		if err != nil {
			slog.Error("error consuming from topic", slog.Any("error", err))
			return err
		}

		if err := processMessage(ctx, client, msg.Value); err != nil {
			continue
		}
	}

	slog.Info("consuming from topic complete")

	return nil
}

func newReader(broker, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,

		GroupID:  "crawler",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})
}

func processMessage(ctx context.Context, client *http.Client, raw []byte) error {
	var seedUrl string
	if err := json.Unmarshal(raw, &seedUrl); err != nil {
		slog.Error("failed to unmarshal url", slog.Any("error", err))
		return err
	}

	parsedUrl, err := url.Parse(seedUrl)
	if err != nil {
		slog.Error("failed to parse url", slog.Any("error", err))
		return err
	}

	res, ok, err := fetchWithLimit(ctx, client, parsedUrl.String())
	if ok {
		fmt.Println(">>> res:", string(res))
	}

	return err
}

func fetchWithLimit(ctx context.Context, client *http.Client, url string) ([]byte, bool, error) {
	skipped := false

	hReq, hErr := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if hErr != nil {
		slog.Error("failed to create HEAD request", slog.Any("error", hErr))
		return nil, skipped, fmt.Errorf("failed to create HEAD request %v", hErr)
	}

	res, err := client.Do(hReq)
	if err != nil {
		slog.Error("failed to make HEAD request", slog.Any("error", err))
		return nil, skipped, fmt.Errorf("failed to make HEAD request %v", err)
	}
	res.Body.Close()

	if cl := res.Header.Get("Content-Length"); cl != "" {
		size, err := strconv.ParseInt(cl, 10, 64)
		if err != nil {
			slog.Error("failed to parse content length", slog.Any("error", err))
			return nil, skipped, fmt.Errorf("failed to parse content length %v", err)
		}

		if size > maxContentSize {
			skipped = true
			slog.Info("skipped large web page request", slog.Int64("content-length", size))
			return nil, skipped, nil
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		slog.Error("failed to create web page request", slog.Any("error", err))
		return nil, skipped, fmt.Errorf("failed to create web page request %v", err)
	}
	res, err = client.Do(req)
	if err != nil {
		slog.Error("failed to make web page request", slog.Any("error", err))
		return nil, skipped, fmt.Errorf("failed to make web page request %v", err)
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		slog.Error("failed to read response", slog.Any("error", err))
		return nil, skipped, fmt.Errorf("failed to read response: %v", err)
	}

	return data, skipped, nil
}
