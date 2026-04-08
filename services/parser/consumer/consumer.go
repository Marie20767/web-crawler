package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/marie20767/web-crawler/services/parser/config"
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
	reader      *kafka.Reader
	ctx         context.Context
	objectStore *objstorage.Store
}

func New(ctx context.Context, cfg *config.Kafka) (*kafka.Reader, error) {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Broker},
		Topic:   cfg.ParserTopic,

		GroupID:  "parser",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	}), nil
}

func (c *Consumer) Consume() error {
	return nil
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		slog.Error("close consumer", slog.Any("error", err))
	}
}
