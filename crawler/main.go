package crawler

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("crawler run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down crawler...")
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

	reader := newReader(cfg.Kafka.Broker, cfg.Kafka.Topic)
	defer reader.Close() //nolint:errcheck

	// TODO: read from topic

	return nil
}

func newReader(broker, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,

		// TODO: what to set this to?
		GroupID:  "my-consumer-group", // enables consumer group / offset tracking
		MinBytes: 10e3,                // 10KB
		MaxBytes: 10e6,                // 10MB
	})
}
