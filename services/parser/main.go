package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/parser/config"
)

const (
	workerCount = 10

	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB
)

func main() {
	if err := run(); err != nil {
		slog.Error("parser run", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down parser...")
}

func run() error {
	ctx := context.Background()

	cfg, err := config.ParseEnv()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	reader := newReader(cfg.Kafka.Broker, cfg.Kafka.ParserTopic)

	return nil
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

func processMessages() {
	// TODO: check url exists in DB, if yes commit offset, if no run logic below

	
	// TODO: loop through workers
	// add to jobs channel
	// read from jobs channel
	// fetch raw HTML from s3
	// extract text & new urls
	// save text to s3
	// produce to init topic
}
