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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/producer"
	sharedDb "github.com/marie20767/web-crawler/shared/db"
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

	dbTimeout = 5 * time.Second
)

type Consumer struct {
	*sharedconsumer.Consumer
	httpClient *http.Client
	objStore   *objstorage.Store
	producer   *producer.Producer
	db         *db
}

type db struct {
	client           *sharedDb.Client
	urlCollection    *mongo.Collection
	domainCollection *mongo.Collection
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
			Topic:    kafkaCfg.InitTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		}))
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix, "")
	if err != nil {
		return nil, fmt.Errorf("create object storage %v", err)
	}

	dbClient, err := sharedDb.New(ctx, dbCfg.Uri)
	if err != nil {
		return nil, fmt.Errorf("connect to db %v", err)
	}
	defer dbClient.Close(ctx)
	urlCollection := dbClient.Collection("", dbCfg.URLCollection)
	domainCollection := dbClient.Collection("", dbCfg.DomainCollection)

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
			client:           dbClient,
			urlCollection:    urlCollection,
			domainCollection: domainCollection,
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

	seedURL := parsedURL.String()

	dbCtx, cancelDbCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelDbCtx()

	shouldFetch, shouldFetchErr := c.shouldFetchHTML(dbCtx, seedURL)
	if shouldFetchErr != nil {
		return shouldFetchErr
	}
	if !shouldFetch {
		return nil
	}

	res, skipped, fetchErr := c.fetchHTMLWithLimit(ctx, seedURL)
	if fetchErr != nil {
		return fetchErr
	}
	if skipped {
		return nil
	}

	storageURL, objStoreErr := c.objStore.StoreRawHTML(ctx, string(msg.Key), res)
	if objStoreErr != nil {
		return objStoreErr
	}

	filter := bson.M{"url": seedURL}
	update := bson.M{
		"$set": bson.M{
			"lastCrawlTime": time.Now(),
			"storageUrl":    storageURL,
		},
	}
	_, dbErr := c.db.urlCollection.UpdateOne(dbCtx, filter, update, options.Update().SetUpsert(true))
	if dbErr != nil {
		return fmt.Errorf("update URL %v", dbErr)
	}

	return c.producer.ProduceParser(ctx, string(msg.Key), seedURL, storageURL)
}

func (c *Consumer) shouldFetchHTML(ctx context.Context, url string) (bool, error) {
	var doc struct {
		LastCrawlTime time.Time `bson:"lastCrawlTime"`
	}

	err := c.db.urlCollection.FindOne(ctx, bson.M{
		"url": url,
	}).Decode(&doc)
	if err != nil && err != mongo.ErrNoDocuments {
		return false, fmt.Errorf("fetch url from db: %v", err)
	}

	cutoff := time.Now().AddDate(0, 0, -14)

	if err == mongo.ErrNoDocuments {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("fetch url from db: %v", err)
	} else if doc.LastCrawlTime.IsZero() || doc.LastCrawlTime.Before(cutoff) {
		return true, nil
	}

	slog.Info("skipped url: data not stale")
	return false, nil
}

func (c *Consumer) fetchHTMLWithLimit(ctx context.Context, seedURL string) (data []byte, skipped bool, err error) {
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
