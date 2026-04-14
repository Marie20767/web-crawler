package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/producer"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	workerCount = 10

	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	maxContentSize = 2 * 1024 * 1024 // 2 MB

	httpTimeout         = 30 * time.Second
	minErrStatusCode    = 400
	maxIdleConns        = 200
	maxIdleConnsPerHost = 20
	idleConnTimeout     = 90 * time.Second
)

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("unexpected status: %d", e.StatusCode)
}

type Consumer struct {
	httpClient *http.Client
	reader     *kafka.Reader
	ctx        context.Context
	objStore   *objstorage.Store
	producer   *producer.Producer
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, prod *producer.Producer) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaCfg.Broker},
		Topic:   kafkaCfg.InitTopic,

		GroupID:  "crawler",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix, "")
	if err != nil {
		return nil, err
	}

	return &Consumer{
		reader: reader,
		httpClient: &http.Client{
			Timeout: httpTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleConns,
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     idleConnTimeout,
			},
		},
		ctx:      ctx,
		objStore: objStore,
		producer: prod,
	}, nil
}

func (c *Consumer) Consume() error {
	jobs := make(chan kafka.Message, workerCount)
	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			for job := range jobs {
				slog.Info("processing message", slog.String("id", string(job.Key)))
				if err := c.processMessage(&job); err != nil {
					slog.Error("process message", slog.Any("error", err))
					var hErr *HTTPError
					errStatusCode := 0
					if errors.As(err, &hErr) {
						errStatusCode = hErr.StatusCode
					}

					c.producer.PublishDLQ(&job, errStatusCode)
				}

				if err := c.reader.CommitMessages(context.WithoutCancel(c.ctx), job); err != nil {
					slog.Error("commit message offset", slog.Any("error", err))
				}
			}
		})
	}

	defer wg.Wait()
	defer close(jobs)

	for {
		readCtx, cancel := context.WithTimeout(c.ctx, kafkaTimeout)
		msg, err := c.reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(kafkaPollInterval)

				slog.Info("no messages available, waiting to poll again", slog.Int64("interval_seconds", int64(kafkaPollInterval.Seconds())))
				select {
				case <-c.ctx.Done():
					timer.Stop()
					slog.Info("context cancelled")
					return nil
				case <-timer.C:
					continue
				}
			}

			if errors.Is(err, context.Canceled) {
				slog.Info("context cancelled")
				return nil
			}

			slog.Error("consume from topic", slog.Any("error", err))
			return err
		}

		select {
		case jobs <- msg:
		case <-c.ctx.Done():
			slog.Warn("context cancelled, dropping unqueued message", slog.String("id", string(msg.Key)))
			return nil
		}
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) error {
	parsedURL, err := url.Parse(string(msg.Value))
	if err != nil {
		return err
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("invalid URL: %q", string(msg.Value))
	}

	ctx := context.WithoutCancel(c.ctx)

	res, skipped, err := c.fetchWithLimit(ctx, parsedURL.String())
	if err != nil {
		return err
	}
	if skipped {
		return nil
	}

	link, err := c.objStore.StoreRawHTML(ctx, string(msg.Key), res)
	if err != nil {
		return err
	}

	c.producer.PublishParser(string(msg.Key), link)
	return nil
}

func (c *Consumer) fetchWithLimit(ctx context.Context, seedURL string) (data []byte, skipped bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, seedURL, http.NoBody)
	if err != nil {
		return nil, false, fmt.Errorf("create web page request %w", err)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("make web page request %w", err)
	}
	defer res.Body.Close() //nolint:errcheck
	if res.StatusCode >= minErrStatusCode {
		return nil, false, &HTTPError{StatusCode: res.StatusCode}
	}

	if res.ContentLength > maxContentSize {
		slog.Info("skipped large web page based on content-length header", slog.Int64("content-length", res.ContentLength))
		return nil, true, nil
	}

	// fallback if content-length header is absent/untrustworthy
	limited := io.LimitReader(res.Body, maxContentSize+1)
	data, err = io.ReadAll(limited)

	if err != nil {
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
