package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/shared/httperr"
	"github.com/marie20767/web-crawler/shared/kafka/message"
	sharedproducer "github.com/marie20767/web-crawler/shared/kafka/producer"
)

type Producer struct {
	*sharedproducer.Producer
	cfg *config.Kafka
}

func New(kafkaCfg *config.Kafka) (*Producer, error) {
	prod, err := sharedproducer.New(kafkaCfg.Broker)
	if err != nil {
		return nil, err
	}

	return &Producer{Producer: prod, cfg: kafkaCfg}, nil
}

// non-HTTP error -> errCode = 0 -> always dlq
func (p *Producer) ProduceDLQ(ctx context.Context, msg *kafka.Message, errCode int) {
	if slices.Contains(httperr.PermanentErrCodes, errCode) {
		slog.Info("skipped producing", slog.Int("error code", errCode))
		return
	}

	_ = p.Produce(ctx, msg.Key, msg.Value, p.cfg.DLQTopic)
}

func (p *Producer) ProduceParser(ctx context.Context, pageURL, storageURL, host string) error {
	payload, err := json.Marshal(message.ParserMessage{
		PageURL:    pageURL,
		StorageURL: storageURL,
	})
	if err != nil {
		return fmt.Errorf("marshal parser message %v", err)
	}

	return p.Produce(ctx, []byte(host), payload, p.cfg.ParserTopic)
}

func (p *Producer) ReproduceURL(ctx context.Context, pageURL, host string) error {
	return p.Produce(ctx, []byte(host), []byte(pageURL), p.cfg.URLTopic)
}
