package producer

import (
	"context"
	"log/slog"
	"slices"

	"github.com/google/uuid"
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
	prod, err := sharedproducer.New(ctx, kafkaCfg.Broker)
	if err != nil {
		return nil, err
	}

	return &Producer{Producer: prod, cfg: kafkaCfg}, nil
}

// non-HTTP error -> errCode = 0 -> always dlq
func (p *Producer) ProduceDLQ(msg *kafka.Message, errCode int) {
	if slices.Contains(httperr.PermanentErrCodes, errCode) {
		slog.Info("skipped producing", slog.Int("error code", errCode))
		return
	}

	_ = p.Produce(msg.Key, msg.Value, p.cfg.DLQTopic)
}

const seedURLsBatchSize = 100

func (p *Producer) ProduceSeedURLs(urls []string) error {
	for i := 0; i < len(urls); i += seedURLsBatchSize {
		chunk := urls[i:min(i+seedURLsBatchSize, len(urls))]

		msgs := make([]kafka.Message, 0, len(chunk))
		for _, url := range chunk {
			msgs = append(msgs, kafka.Message{
				Topic: p.cfg.InitTopic,
				Key:   []byte(uuid.New().String()),
				Value: []byte(url),
			})
		}

		err := p.Producer.ProduceBatch(msgs, p.cfg.InitTopic)
		if err != nil {
			return err
		}
	}

	return nil
}
