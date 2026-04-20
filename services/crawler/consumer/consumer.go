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

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/producer"
	"github.com/marie20767/web-crawler/shared/httperr"
	sharedconsumer "github.com/marie20767/web-crawler/shared/kafka/consumer"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	maxContentSize = 2 * 1024 * 1024 // 2 MB

	httpTimeout         = 30 * time.Second
	minErrStatusCode    = 400
	maxIdleConns        = 200
	maxIdleConnsPerHost = 20
	idleConnTimeout     = 90 * time.Second
)

type Consumer struct {
	*sharedconsumer.Consumer
	httpClient *http.Client
	objStore   *objstorage.Store
	producer   *producer.Producer
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, prod *producer.Producer) (*Consumer, error) {
	readers := make([]*kafka.Reader, 0, kafkaCfg.Partitions)
	for range kafkaCfg.Partitions {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaCfg.Broker},
			Topic:    kafkaCfg.InitTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		}))
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix, "")
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Consumer: sharedconsumer.New(readers),
		httpClient: &http.Client{
			Timeout: httpTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleConns,
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     idleConnTimeout,
			},
		},
		objStore: objStore,
		producer: prod,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	return c.Consumer.Consume(ctx, c.handle)
}

func (c *Consumer) handle(ctx context.Context, msg *kafka.Message) error {
	err := c.processMessage(ctx, msg)
	if err != nil {
		var hErr *httperr.Err
		errStatusCode := 0
		if errors.As(err, &hErr) {
			errStatusCode = hErr.StatusCode
		}
		c.producer.ProduceDLQ(ctx, msg, errStatusCode)
	}
	return err
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) error {
	parsedURL, err := url.Parse(string(msg.Value))
	if err != nil {
		return err
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %q", parsedURL.Scheme)
	}

	res, skipped, err := c.fetchWithLimit(ctx, parsedURL.String())
	if err != nil {
		return err
	}
	if skipped {
		return nil
	}

	storageLink, err := c.objStore.StoreRawHTML(ctx, string(msg.Key), res)
	if err != nil {
		return err
	}

	return c.producer.ProduceParser(ctx, string(msg.Key), parsedURL.String(), storageLink)
}

func (c *Consumer) fetchWithLimit(ctx context.Context, seedURL string) (data []byte, skipped bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, seedURL, http.NoBody)
	if err != nil {
		return nil, false, fmt.Errorf("create web page request %v", err)
	}

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
