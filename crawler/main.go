package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/config"
)

const (
	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	maxContentSize = 2 * 1024 * 1024 // 2 MB

	httpTimeout = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("crawler run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down crawler gracefully...")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
			slog.Info("no messages available, waiting to poll again", slog.Duration("interval", kafkaPollInterval))
			select {
			case <-ctx.Done():
				slog.Info("context cancelled")
				return nil
			case <-time.After(kafkaPollInterval):
				continue
			}
		}

		if errors.Is(err, context.Canceled) {
			slog.Info("context cancelled")
			return nil
		}

		if err != nil {
			slog.Error("error consuming from topic", slog.Any("error", err))
			return err
		}

		slog.Info("processing message", slog.String("id", string(msg.Key)))

		if err := processMessage(ctx, client, msg.Value); err != nil {
			continue
		}
	}
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
	parsedUrl, err := url.Parse(string(raw))
	if err != nil {
		slog.Error("failed to parse url", slog.Any("error", err))
		return err
	}

	httpCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	res, ok, err := fetchWithLimit(httpCtx, client, parsedUrl.String())
	if ok {
		fmt.Println(">>> res:", string(res))
	}

	return err
}

func fetchWithLimit(ctx context.Context, client *http.Client, seedUrl string) (data []byte, skipped bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, seedUrl, http.NoBody)
	if err != nil {
		slog.Error("failed to create web page request", slog.Any("error", err))
		return nil, false, fmt.Errorf("failed to create web page request %v", err)
	}

	res, err := client.Do(req)
	if err != nil {
		slog.Error("failed to make web page request", slog.Any("error", err))
		return nil, false, fmt.Errorf("failed to make web page request %v", err)
	}
	defer res.Body.Close() //nolint:errcheck

	limited := io.LimitReader(res.Body, maxContentSize+1)
	data, err = io.ReadAll(limited)
	if err != nil {
		slog.Error("failed to read response", slog.Any("error", err))
		return nil, false, fmt.Errorf("failed to read response: %v", err)
	}

	if int64(len(data)) > maxContentSize {
		slog.Info("skipped large web page request", slog.Int64("content-length", int64(len(data))))
		return nil, true, nil
	}

	return data, false, nil
}
