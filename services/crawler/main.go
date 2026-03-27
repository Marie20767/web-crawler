package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/consumer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("crawler run", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down crawler gracefully...")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.ParseEnv()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	consmr, err := consumer.New(ctx, cfg.Kafka.Broker, cfg.Kafka.Topic, cfg.AWS.BucketName, cfg.AWS.ObjectStorePrefix)
	if err != nil {
		return err
	}
	defer consmr.Close()

	return consmr.Consume()
}
