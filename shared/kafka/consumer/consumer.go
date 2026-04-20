package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

const (
	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	workerCount       = 10
	bufferSize        = workerCount * 2
)

type messageHandler func(ctx context.Context, msg *kafka.Message) error

type Consumer struct {
	readers []*kafka.Reader
}

type job struct {
	reader *kafka.Reader
	msg    kafka.Message
}

func New(readers []*kafka.Reader) *Consumer {
	return &Consumer{readers: readers}
}

func (c *Consumer) Consume(ctx context.Context, handler messageHandler) error {
	jobs := make(chan job, bufferSize)
	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			c.processMessages(ctx, jobs, handler)
		})
	}

	defer wg.Wait()
	defer close(jobs)

	errGrp, errGrpCtx := errgroup.WithContext(ctx)
	for _, reader := range c.readers {
		errGrp.Go(func() error {
			return c.fetchMessages(errGrpCtx, reader, jobs)
		})
	}

	return errGrp.Wait()
}

func (c *Consumer) fetchMessages(ctx context.Context, reader *kafka.Reader, jobs chan<- job) error {
	for {
		readCtx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		msg, err := reader.FetchMessage(readCtx)
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
		case jobs <- job{reader: reader, msg: msg}:
		case <-ctx.Done():
			slog.Warn("context cancelled, dropping unqueued message", slog.String("url", string(msg.Key)))
			return nil
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, jobs <-chan job, handler messageHandler) {
	ctxNoCancel := context.WithoutCancel(ctx)

	for j := range jobs {
		slog.Info("processing message", slog.String("id", string(j.msg.Key)))
		if err := handler(ctxNoCancel, &j.msg); err != nil {
			slog.Error("process message", slog.Any("error", err))
		}

		if err := j.reader.CommitMessages(ctxNoCancel, j.msg); err != nil {
			slog.Error("commit message offset", slog.Any("error", err))
		}
	}
}

func (c *Consumer) Close() {
	for _, reader := range c.readers {
		if err := reader.Close(); err != nil {
			slog.Error("close consumer", slog.Any("error", err))
		}
	}
}
