package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/consumer"
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

	cons, err := consumer.New(ctx, cfg.Kafka)
	if err != nil {
		return err
	}
	defer cons.Close()

	return cons.Consume()
}

func newReader(broker, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,

		GroupID:  "parser",
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
