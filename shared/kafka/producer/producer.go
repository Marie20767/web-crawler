package producer

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaTimeout     = 10 * time.Second
	kafkaMaxAttempts = 5
)

type Producer struct {
	writer *kafka.Writer
	ctx    context.Context
}

func New(ctx context.Context, broker string, topics ...string) (*Producer, error) {
	if err := ensureTopics(broker, topics...); err != nil {
		return nil, fmt.Errorf("ensure kafka topics: %v", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  kafkaMaxAttempts,
	}

	return &Producer{
		writer: writer,
		ctx:    ctx,
	}, nil
}

func ensureTopics(broker string, topics ...string) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close() //nolint:errcheck

	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	return controllerConn.CreateTopics(configs...)
}

func (p *Producer) Publish(key, value []byte, topic string) {
	ctx := context.WithoutCancel(p.ctx)
	writeCtx, cancelCtx := context.WithTimeout(ctx, kafkaTimeout)
	defer cancelCtx()

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	if err := p.writer.WriteMessages(writeCtx, msg); err != nil {
		slog.Error("write message", slog.String("topic", topic), slog.Any("error", err))
	} else {
		slog.Info("producing to topic complete", slog.String("topic", topic))
	}
}

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		slog.Error("close producer", slog.Any("error", err))
	}
}
