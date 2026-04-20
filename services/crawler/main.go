package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/consumer"
	"github.com/marie20767/web-crawler/services/crawler/producer"
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

	prod, err := producer.New(cfg.Kafka)
	if err != nil {
		return err
	}
	defer prod.Close()

	cons, err := consumer.New(ctx, cfg.Kafka, cfg.AWS, prod)
	if err != nil {
		return err
	}
	defer cons.Close()

	return cons.Consume(ctx)
}
