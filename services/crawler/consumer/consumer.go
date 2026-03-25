package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/marie20767/web-crawler/config"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	maxContentSize = 2 * 1024 * 1024 // 2 MB

	httpTimeout = 30 * time.Second
)

type Consumer struct {
	httpClient *http.Client
	reader     *kafka.Reader
	ctx        context.Context
}

func New(ctx context.Context, cfg *config.Kafka) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Broker},
		Topic:   cfg.Topic,

		GroupID:  "crawler",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})

	return &Consumer{
		reader:     reader,
		httpClient: &http.Client{},
		ctx:        ctx,
	}
}

func (c *Consumer) Consume() error {
	for {
		readCtx, cancel := context.WithTimeout(c.ctx, kafkaTimeout)
		msg, err := c.reader.ReadMessage(readCtx)
		cancel()

		if errors.Is(err, context.DeadlineExceeded) {
			slog.Info("no messages available, waiting to poll again", slog.Int64("interval_seconds", int64(kafkaPollInterval.Seconds())))
			select {
			case <-c.ctx.Done():
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
		if err := c.processMessage(msg.Value); err != nil {
			slog.Error("failed to process message", slog.Any("error", err))
			continue
		}
	}
}

func (c *Consumer) processMessage(raw []byte) error {
	parsedUrl, err := url.Parse(string(raw))
	if err != nil {
		return fmt.Errorf("failed to parse url %v", err)
	}

	httpCtx, cancel := context.WithTimeout(c.ctx, httpTimeout)
	defer cancel()

	res, skipped, err := c.fetchWithLimit(httpCtx, parsedUrl.String())
	if !skipped {
		slog.Info("web page res", slog.String("res", string(res)))
	}

	return err
}

func (c *Consumer) fetchWithLimit(httpCtx context.Context, seedUrl string) (data []byte, skipped bool, err error) {
	req, err := http.NewRequestWithContext(httpCtx, http.MethodGet, seedUrl, http.NoBody)
	if err != nil {
		slog.Error("failed to create web page request", slog.Any("error", err))
		return nil, false, fmt.Errorf("failed to create web page request %v", err)
	}

	res, err := c.httpClient.Do(req)
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

func (c *Consumer) Close() {
	c.reader.Close()
}
