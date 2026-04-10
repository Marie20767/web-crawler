package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/consumer"
	"github.com/marie20767/web-crawler/services/parser/producer"
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

	prod, err := producer.New(ctx, cfg.Kafka)
	if err != nil {
		return err
	}
	defer prod.Close()

	cons, err := consumer.New(ctx, cfg.Kafka, cfg.AWS, prod)
	if err != nil {
		return err
	}
	defer cons.Close()

	return cons.Consume()
}
