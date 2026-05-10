package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/producer"
	shareddb "github.com/marie20767/web-crawler/shared/db"
	"github.com/marie20767/web-crawler/shared/httperr"
	sharedconsumer "github.com/marie20767/web-crawler/shared/kafka/consumer"
	"github.com/marie20767/web-crawler/shared/kafka/message"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	dbTimeout = 5 * time.Second

	mongoDuplicateKeyErr = 11000

	urlTTL = 14 * 24 * time.Hour
)

type Consumer struct {
	*sharedconsumer.Consumer
	objStore *objstorage.Store
	producer *producer.Producer
	db       *db
}

type db struct {
	client     *shareddb.Client
	collection *mongo.Collection
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, dbCfg *config.Db, prod *producer.Producer) (*Consumer, error) {
	readers := make([]*kafka.Reader, 0, kafkaCfg.Partitions)
	for range kafkaCfg.Partitions {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaCfg.Broker},
			Topic:    kafkaCfg.ParserTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		}))
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.HTMLBucketPrefix, awsCfg.TextBucketPrefix)
	if err != nil {
		return nil, err
	}

	dbClient, err := shareddb.New(ctx, dbCfg.Uri)
	if err != nil {
		return nil, fmt.Errorf("connect to db %v", err)
	}

	idxCtx, cancelIdxCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelIdxCtx()
	if err := dbClient.CreateTTLIndex(idxCtx, dbCfg.Name, dbCfg.Collection, "queuedAt", urlTTL); err != nil {
		return nil, fmt.Errorf("create queuedAt TTL (URL) index: %v", err)
	}

	return &Consumer{
		Consumer: sharedconsumer.New(readers),
		objStore: objStore,
		producer: prod,
		db: &db{
			client:     dbClient,
			collection: dbClient.Collection(dbCfg.Name, dbCfg.Collection),
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
	var parserMsg message.ParserMessage
	if err := json.Unmarshal(msg.Value, &parserMsg); err != nil {
		return fmt.Errorf("unmarshal parser message: %v", err)
	}

	baseURL, err := url.Parse(parserMsg.PageURL)
	if err != nil {
		return fmt.Errorf("parse page URL: %v", err)
	}

	rawHTML, err := c.objStore.FetchRawHTML(ctx, parserMsg.StorageURL)
	if err != nil {
		return err
	}

	parsedRes, err := c.parseRawHTML(rawHTML, baseURL)
	if err != nil {
		return err
	}

	if err := c.objStore.StoreParsedText(ctx, string(msg.Key), parsedRes.text); err != nil {
		return err
	}

	uniqueURLs, err := c.getUniqueURLs(ctx, parsedRes.urls)
	if err != nil {
		return err
	}

	return c.producer.ProduceSeedURLs(ctx, uniqueURLs)
}

func (c *Consumer) Close() {
	c.Consumer.Close()

	closeCtx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	if err := c.db.client.Close(closeCtx); err != nil {
		slog.Error("close db connection", slog.Any("error", err))
	}
}
