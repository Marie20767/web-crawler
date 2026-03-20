package crawler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/config"
)

const kafkaTimeout = 5 * time.Second

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

	for {
		ctx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			slog.Error("error consuming from topic", slog.Any("error", err))
			return err
		}

		var url string
		if err := json.Unmarshal(msg.Value, &url); err != nil {
			slog.Error("failed to unmarshal url", slog.Any("error", err))
			continue
		}

		resp, err := http.Get(url)
		if err != nil {
			slog.Error("failed to fetch web page", slog.Any("error", err))
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			slog.Error("failed to parse response body", slog.Any("error", err))
		}

		fmt.Println(">>> body: ", string(body))
	}

	slog.Info("consuming from topic complete")

	return nil
}

func newReader(broker, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,

		GroupID:  "crawler",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}
