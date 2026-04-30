package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/marie20767/web-crawler/services/initialiser/config"
	"github.com/marie20767/web-crawler/services/initialiser/producer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("initialiser run", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down initialiser...")
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

	prod, err := producer.New(cfg.Kafka.Broker, cfg.Kafka.UrlTopic)
	if err != nil {
		return err
	}
	defer prod.Close()
	return prod.ProduceSeedURLs(ctx)
}
