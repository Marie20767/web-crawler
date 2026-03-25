package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/marie20767/web-crawler/config"
	"github.com/marie20767/web-crawler/consumer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("crawler run failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down crawler gracefully...")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.ParseEnv()
	if err != nil {
		slog.Error("failed to parse env vars", slog.Any("error", err))
		return fmt.Errorf("failed to parse env vars: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	reader := consumer.New(ctx, cfg.Kafka)
	defer reader.Close()

	err = reader.Consume()
	return err
}
