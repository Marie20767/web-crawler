package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/producer"
	shareddb "github.com/marie20767/web-crawler/shared/db"
	"github.com/marie20767/web-crawler/shared/httperr"
	sharedconsumer "github.com/marie20767/web-crawler/shared/kafka/consumer"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	httpTimeout         = 30 * time.Second
	minErrStatusCode    = 400
	maxIdleConns        = 200
	maxIdleConnsPerHost = 20
	idleConnTimeout     = 90 * time.Second

	dbTimeout = 5 * time.Second

	createdAtTTL = 24 * time.Hour
)

type Consumer struct {
	*sharedconsumer.Consumer
	httpClient *http.Client
	objStore   *objstorage.Store
	producer   *producer.Producer
	db         *db
}

func New(
	ctx context.Context,
	kafkaCfg *config.Kafka,
	awsCfg *config.AWS,
	dbCfg *config.Db,
	prod *producer.Producer,
) (*Consumer, error) {
	readers := make([]*kafka.Reader, 0, kafkaCfg.Partitions)
	for range kafkaCfg.Partitions {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaCfg.Broker},
			Topic:    kafkaCfg.URLTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		}))
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix, "")
	if err != nil {
		return nil, fmt.Errorf("create object storage %v", err)
	}

	dbClient, err := shareddb.New(ctx, dbCfg.Uri)
	if err != nil {
		return nil, fmt.Errorf("connect to db %v", err)
	}

	idxCtx, cancelIdxCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelIdxCtx()
	if err := dbClient.CreateTTLIndex(idxCtx, dbCfg.Name, dbCfg.HostCollection, "createdAt", createdAtTTL); err != nil {
		return nil, fmt.Errorf("create createdAt TTL (host) index: %v", err)
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
		db: &db{
			client:         dbClient,
			urlCollection:  dbClient.Collection(dbCfg.Name, dbCfg.URLCollection),
			hostCollection: dbClient.Collection(dbCfg.Name, dbCfg.HostCollection),
		},
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
	parsedURL, parseErr := url.Parse(string(msg.Value))
	if parseErr != nil {
		return parseErr
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("unsupported scheme: %q", parsedURL.Scheme)
	}

	pageURL := parsedURL.String()

	crawled, crawlCheckErr := c.alreadyCrawled(ctx, pageURL)
	if crawlCheckErr != nil {
		return crawlCheckErr
	}
	if crawled {
		return nil
	}

	host := string(msg.Key)

	isAllowed, rateLimitErr := c.handleRateLimit(ctx, pageURL, parsedURL, host)
	if rateLimitErr != nil {
		return rateLimitErr
	}
	if !isAllowed {
		return nil
	}

	res, skipped, fetchErr := c.fetchHTMLWithLimit(ctx, pageURL)
	if fetchErr != nil {
		return fetchErr
	}
	if skipped {
		return nil
	}

	storageURL, objStoreErr := c.objStore.StoreRawHTML(ctx, pageURL, res)
	if objStoreErr != nil {
		return objStoreErr
	}

	if err := c.updateURLMetadata(ctx, pageURL, storageURL, host); err != nil {
		return err
	}

	return c.producer.ProduceParser(ctx, pageURL, storageURL, host)
}

func (c *Consumer) Close() {
	c.Consumer.Close()

	closeCtx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	if err := c.db.client.Close(closeCtx); err != nil {
		slog.Error("close db connection", slog.Any("error", err))
	}
}
