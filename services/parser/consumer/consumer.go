package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	workerCount = 10

	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB
)

type Consumer struct {
	reader       *kafka.Reader
	ctx          context.Context
	objStore     *objstorage.Store
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaCfg.Broker},
		Topic:   kafkaCfg.ParserTopic,

		GroupID:  "parser",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		reader:   reader,
		ctx:      ctx,
		objStore: objStore,
	}, nil
}

func (c *Consumer) Consume() error {
	jobs := make(chan kafka.Message, workerCount)
	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			for job := range jobs {
				slog.Info("processing message", slog.String("id", string(job.Key)))
				if err := c.processMessage(&job); err != nil {
					slog.Error("process message", slog.Any("error", err))
					// TODO: publish to DLQ
				}

				if err := c.reader.CommitMessages(context.WithoutCancel(c.ctx), job); err != nil {
					slog.Error("commit message offset", slog.Any("error", err))
				}
			}
		})
	}

	defer wg.Wait()
	defer close(jobs)

	for {
		readCtx, cancel := context.WithTimeout(c.ctx, kafkaTimeout)
		msg, err := c.reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(kafkaPollInterval)

				slog.Info("no messages available, waiting to poll again", slog.Int64("interval_seconds", int64(kafkaPollInterval.Seconds())))
				select {
				case <-c.ctx.Done():
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
		case <-c.ctx.Done():
			slog.Warn("context cancelled, dropping unqueued message", slog.String("url", string(msg.Key)))
			return nil
		}
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) error {
	// TODO: check url exists in DB, if yes commit offset, if no run logic below
	ctx := context.WithoutCancel(c.ctx)

	_, err := c.objStore.FetchRawHTML(ctx, string(msg.Value))
	if err != nil {
		return err
	}

	slog.Info("successfully fetched HTML from object store")

	// TODO:
	// extract text & new urls
	// save text to s3
	// produce to init topic

	return nil
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		slog.Error("close consumer", slog.Any("error", err))
	}
}
