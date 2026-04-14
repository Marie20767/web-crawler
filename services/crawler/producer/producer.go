package producer

import (
	"context"
	"encoding/json"
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

func New(ctx context.Context, kafkaCfg *config.Kafka) (*Producer, error) {
	prod, err := sharedproducer.New(ctx, kafkaCfg.Broker, kafkaCfg.DLQTopic, kafkaCfg.ParserTopic)
	if err != nil {
		return nil, err
	}

	return &Producer{Producer: prod, cfg: kafkaCfg}, nil
}

// non-HTTP error -> errCode = 0 -> always dlq
func (p *Producer) PublishDLQ(msg *kafka.Message, errCode int) {
	if slices.Contains(httperr.PermanentErrCodes, errCode) {
		slog.Info("skipped producing", slog.Int("error code", errCode))
		return
	}

	p.Publish(msg.Key, msg.Value, p.cfg.DLQTopic)
}

func (p *Producer) PublishParser(messageID, pageURL, storageURL string) {
	payload, err := json.Marshal(message.ParserMessage{
		PageURL:    pageURL,
		StorageURL: storageURL,
	})
	if err != nil {
		slog.Error("marshal parser message", slog.Any("error", err))
		return
	}

	p.Publish([]byte(messageID), payload, p.cfg.ParserTopic)
}
