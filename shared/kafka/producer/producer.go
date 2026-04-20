package producer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaTimeout     = 10 * time.Second
	kafkaMaxAttempts = 5
)

type Producer struct {
	writer *kafka.Writer
}

func New(broker string) (*Producer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  kafkaMaxAttempts,
	}

	return &Producer{
		writer: writer,
	}, nil
}

func (p *Producer) Produce(ctx context.Context, key, value []byte, topic string) error {
	ctxNoCancel := context.WithoutCancel(ctx)
	writeCtx, cancelCtx := context.WithTimeout(ctxNoCancel, kafkaTimeout)
	defer cancelCtx()

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	if err := p.writer.WriteMessages(writeCtx, msg); err != nil {
		slog.Error("produce message", slog.String("topic", topic), slog.Any("error", err))
		return fmt.Errorf("produce message to topic %s %v", topic, err)
	}

	slog.Info("producing message complete", slog.String("topic", topic))
	return nil
}

func (p *Producer) ProduceBatch(ctx context.Context, msgs []kafka.Message, topic string) error {
	ctxNoCancel := context.WithoutCancel(ctx)
	writeCtx, cancelCtx := context.WithTimeout(ctxNoCancel, kafkaTimeout)
	defer cancelCtx()

	if err := p.writer.WriteMessages(writeCtx, msgs...); err != nil {
		slog.Error("produce messages", slog.String("topic", topic), slog.Any("error", err))
		return fmt.Errorf("produce messages to topic %s %v", topic, err)
	}

	slog.Info("producing messages complete", slog.String("topic", topic))
	return nil
}

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		slog.Error("close producer", slog.Any("error", err))
	}
}
