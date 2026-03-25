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

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/objectstorage"
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
	httpClient  *http.Client
	reader      *kafka.Reader
	ctx         context.Context
	objectStore *objectstorage.Store
}

func New(ctx context.Context, broker, topic, bucketName, prefix string) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,

		GroupID:  "crawler",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})

	objectStore, err := objectstorage.New(ctx, bucketName, prefix)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		reader:      reader,
		httpClient:  &http.Client{},
		ctx:         ctx,
		objectStore: objectStore,
	}, nil
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
			slog.Error("consume from topic", slog.Any("error", err))
			return err
		}

		slog.Info("processing message", slog.String("id", string(msg.Key)))
		if err := c.processMessage(&msg); err != nil {
			slog.Error("process message", slog.Any("error", err))
			continue
		}
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) error {
	parsedURL, err := url.Parse(string(msg.Value))
	if err != nil {
		return fmt.Errorf("parse URL %w", err)
	}

	httpCtx, cancel := context.WithTimeout(c.ctx, httpTimeout)
	defer cancel()

	res, skipped, err := c.fetchWithLimit(httpCtx, parsedURL.String())
	if !skipped {
		return c.objectStore.StoreHTML(string(msg.Key), res)
	}

	return err
}

func (c *Consumer) fetchWithLimit(httpCtx context.Context, seedURL string) (data []byte, skipped bool, err error) {
	req, err := http.NewRequestWithContext(httpCtx, http.MethodGet, seedURL, http.NoBody)
	if err != nil {
		slog.Error("create web page request", slog.Any("error", err))
		return nil, false, fmt.Errorf("create web page request %w", err)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		slog.Error("make web page request", slog.Any("error", err))
		return nil, false, fmt.Errorf("make web page request %w", err)
	}
	defer res.Body.Close() //nolint:errcheck

	limited := io.LimitReader(res.Body, maxContentSize+1)
	data, err = io.ReadAll(limited)

	if err != nil {
		slog.Error("read response", slog.Any("error", err))
		return nil, false, fmt.Errorf("read response: %w", err)
	}

	if int64(len(data)) > maxContentSize {
		slog.Info("skipped large web page request", slog.Int64("content-length", int64(len(data))))
		return nil, true, nil
	}

	return data, false, nil
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		slog.Error("close consumer", slog.Any("error", err))
	}
}
