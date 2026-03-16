package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/marie20767/web-crawler/config"
	"github.com/segmentio/kafka-go"
)

var seedUrls = []string{
	"https://www.bookbrowse.com/read-alikes/",
	"https://www.goodreads.com/list/tag/read-alikes",
	"https://www.whatshouldireadnext.com/",
}

func main() {
	if err := run(); err != nil {
		slog.Error("run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down...")
}

func run() error {
	ctx := context.Background()

	cfg, err := config.ParseEnv()
	if err != nil {
		slog.Error("failed to parse env vars", slog.Any("error", err))
		return fmt.Errorf("failed to parse env vars: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	writer := newWriter(cfg.Kafka.Broker, cfg.Kafka.Topic)
	defer writer.Close()

	for _, url := range seedUrls {
		msgId := uuid.New()
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%s", msgId)),
			Value: []byte(url),
		}

		err := writer.WriteMessages(ctx, msg)
		if err != nil {
			slog.Error("Failed to write message", slog.Any("error", err))
			continue
		}

		slog.Info("Produced message", slog.String("id", msgId.String()), slog.String("url", url))
	}

	return nil
}

func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  5,
	}
}
