package consumer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/html"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/producer"
	"github.com/marie20767/web-crawler/shared/httperr"
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
	reader   *kafka.Reader
	ctx      context.Context
	objStore *objstorage.Store
	producer *producer.Producer
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, prod *producer.Producer) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaCfg.Broker},
		Topic:   kafkaCfg.ParserTopic,

		GroupID:  "parser",
		MinBytes: kafkaMinBatchSize,
		MaxBytes: kafkaMaxBatchSize,
	})

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.HTMLBucketPrefix, awsCfg.TextBucketPrefix)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		reader:   reader,
		ctx:      ctx,
		objStore: objStore,
		producer: prod,
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
					var hErr *httperr.Err
					errStatusCode := 0
					if errors.As(err, &hErr) {
						errStatusCode = hErr.StatusCode
					}

					c.producer.PublishDLQ(&job, errStatusCode)
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

type Parsed struct {
	text string
	urls []string
}

func (c *Consumer) processMessage(msg *kafka.Message) error {
	ctx := context.WithoutCancel(c.ctx)

	raw, err := c.objStore.FetchRawHTML(ctx, string(msg.Value))
	if err != nil {
		return err
	}

	parsed, _ := c.parseRawHTML(raw)

	err = c.objStore.StoreParsedText(ctx, string(msg.Key), parsed.text)
	if err != nil {
		return err
	}

	for _, url := range parsed.urls {
		c.producer.PublishInit(uuid.New().String(), url)
	}

	return nil
}

func (c *Consumer) parseRawHTML(raw []byte) (parsed *Parsed, err error) {
	doc, err := html.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}

	var sb strings.Builder
	urls := []string{}

	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		if n.Parent != nil {
			switch n.Type {
			case html.TextNode:
				switch n.Parent.Data {
				case "script", "style":
				// skip
				default:
					sb.WriteString(strings.TrimSpace(n.Data))
				}

			case html.ElementNode:
				if n.Data == "a" {
					for _, attr := range n.Attr {
						if attr.Key == "href" && !isResourceURL(attr.Val) {
							urls = append(urls, attr.Val)
						}
					}
				}
			}
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}

	walk(doc)

	return &Parsed{
		text: sb.String(),
		urls: urls,
	}, nil
}

func (c *Consumer) Close() {
	if err := c.reader.Close(); err != nil {
		slog.Error("close consumer", slog.Any("error", err))
	}
}

func isResourceURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		slog.Error("isResourceURL: parse URL", slog.Any("error", err))
		return true
	}

	switch strings.ToLower(path.Ext(parsed.Path)) {
	case ".css", ".woff", ".woff2", ".ttf", ".eot", ".otf",
		".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp":
		return true
	}

	return false
}
