package producer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer // topic agnostic
	ctx    context.Context
}

func New(ctx context.Context, kafkaCfg *config.Kafka) (*Producer, error) {
	if err := ensureTopics(kafkaCfg.DLQTopic); err != nil {
		return nil, fmt.Errorf("ensure kafka topics: %v", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaCfg.Broker),
		Balancer: &kafka.LeastBytes{},
	}

	return &Producer{
		writer: writer,
		ctx:    ctx,
	}, nil
}

// TODO: implement
func PublishDLQ(msg *kafka.Message) error {
	// TODO: manually add dlq topic to writer
	return nil
}

// TODO: read
func ensureTopics(broker string, topics ...string) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	// Dial needs to reach the controller to create topics
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

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

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		slog.Error("close producer", slog.Any("error", err))
	}
}
