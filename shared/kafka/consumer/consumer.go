package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	workerCount       = 10
	bufferSize        = workerCount * 2
)

type messageHandler func(ctx context.Context, msg *kafka.Message) error

type Consumer struct {
	reader *kafka.Reader
}

func New(reader *kafka.Reader) *Consumer {
	return &Consumer{reader: reader}
}

func (c *Consumer) Consume(ctx context.Context, handler messageHandler) error {
	jobs := make(chan kafka.Message, bufferSize)
	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			c.processMessages(ctx, jobs, handler)
		})
	}

	defer wg.Wait()
	defer close(jobs)

	return c.fetchMessages(ctx, jobs)
}

func (c *Consumer) fetchMessages(ctx context.Context, jobs chan<- kafka.Message) error {
	for {
		readCtx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		msg, err := c.reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(kafkaPollInterval)

				slog.Info("no messages available, waiting to poll again", slog.Int64("interval_seconds", int64(kafkaPollInterval.Seconds())))
				select {
				case <-ctx.Done():
					timer.Stop()
					slog.Info("context cancelled")
					return nil
				case <-timer.C:
					continue
				}
			}

			if errors.Is(err, context.Canceled) {
				slog.Info("context cancelled")
				return nil
			}

			slog.Error("consume from topic", slog.Any("error", err))
			return err
		}

		select {
		case jobs <- msg:
		case <-ctx.Done():
			slog.Warn("context cancelled, dropping unqueued message", slog.String("url", string(msg.Key)))
			return nil
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, jobs <-chan kafka.Message, handler messageHandler) {
	ctxNoCancel := context.WithoutCancel(ctx)

	for j := range jobs {
		slog.Info("processing message", slog.String("id", string(j.Key)))
		if err := handler(ctxNoCancel, &j); err != nil {
			slog.Error("process message", slog.Any("error", err))
		}

		if err := c.reader.CommitMessages(ctxNoCancel, j); err != nil {
			slog.Error("commit message offset", slog.Any("error", err))
		}
	}
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		slog.Error("close consumer", slog.Any("error", err))
	}
}
