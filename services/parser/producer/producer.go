package producer

import (
	"context"
	"log/slog"
	"net/url"
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

const seedURLsBatchSize = 100

func (p *Producer) ProduceSeedURLs(ctx context.Context, urls []string) error {
	for i := 0; i < len(urls); i += seedURLsBatchSize {
		chunk := urls[i:min(i+seedURLsBatchSize, len(urls))]

		msgs := make([]kafka.Message, 0, len(chunk))
		for _, u := range chunk {
			parsed, err := url.Parse(u)
			if err != nil {
				slog.Error("parse URL", slog.String("URL", u))
				continue
			}

			msgs = append(msgs, kafka.Message{
				Topic: p.cfg.UrlTopic,
				Key:   []byte(parsed.Hostname()),
				Value: []byte(u),
			})
		}

		err := p.ProduceBatch(ctx, msgs, p.cfg.UrlTopic)
		if err != nil {
			return err
		}
	}

	return nil
}
