package producer

import (
	"context"
	"log/slog"
	"slices"

	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/shared/httperr"
	sharedproducer "github.com/marie20767/web-crawler/shared/kafka/producer"
)

type Producer struct {
	*sharedproducer.Producer
	cfg *config.Kafka
}

func New(ctx context.Context, kafkaCfg *config.Kafka) (*Producer, error) {
	prod, err := sharedproducer.New(ctx, kafkaCfg.Broker, kafkaCfg.DLQTopic, kafkaCfg.InitTopic)
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

func (p *Producer) PublishInit(msgID, url string) {
	p.Publish([]byte(msgID), []byte(url), p.cfg.InitTopic)
}
