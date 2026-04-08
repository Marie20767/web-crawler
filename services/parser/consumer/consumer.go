package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/shared/objstorage"
	"github.com/segmentio/kafka-go"
)

const (
	workerCount = 10

	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB
)

type Consumer struct {
	reader   *kafka.Reader
	ctx      context.Context
	objStore *objstorage.Store
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaCfg.Broker},
		Topic:   kafkaCfg.ParserTopic,

		GroupID:  "parser",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		reader:   reader,
		ctx:      ctx,
		objStore: objStore,
	}, nil
}

func (c *Consumer) Consume() error {
	return nil
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		slog.Error("close consumer", slog.Any("error", err))
	}
}
