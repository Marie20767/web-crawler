package producer

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/crawler/config"
)

const (
	kafkaTimeout = 10 * time.Second
)

var permanentErrCodes = []int{
	400, // Bad Request
	401, // Unauthorised
	403, // Forbidden
	404, // Not Found
	405, // Method Not Allowed
	406, // Not Acceptable
	410, // Gone
	451, // Unavailable For Legal Reasons
}

type Producer struct {
	writer   *kafka.Writer // topic agnostic
	kafkaCfg *config.Kafka
	ctx      context.Context
}

func New(ctx context.Context, kafkaCfg *config.Kafka) (*Producer, error) {
	if err := ensureTopics(kafkaCfg.Broker, kafkaCfg.DLQTopic, kafkaCfg.ParserTopic); err != nil {
		return nil, fmt.Errorf("ensure kafka topics: %v", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaCfg.Broker),
		Balancer: &kafka.LeastBytes{},
	}

	return &Producer{
		writer:   writer,
		ctx:      ctx,
		kafkaCfg: kafkaCfg,
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

// non-HTTP error -> errCode = 0 -> always dlq
func (p *Producer) PublishDLQ(msg *kafka.Message, errCode int) {
	if slices.Contains(permanentErrCodes, errCode) {
		// only publish transient errors
		slog.Info("skipped producing", slog.Int("error code", errCode))
		return
	}

	p.publish(msg.Key, msg.Value, p.kafkaCfg.DLQTopic)
}

func (p *Producer) PublishParser(url, storageLink string) {
	p.publish([]byte(url), []byte(storageLink), p.kafkaCfg.ParserTopic)
}

func (p *Producer) publish(key, value []byte, topic string) {
	ctx := context.WithoutCancel(p.ctx)
	writeCtx, cancelCtx := context.WithTimeout(ctx, kafkaTimeout)
	defer cancelCtx()

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	if err := p.writer.WriteMessages(writeCtx, msg); err != nil {
		// in prod env you would send an alert here
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
