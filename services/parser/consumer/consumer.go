package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/net/html"
	"golang.org/x/sync/errgroup"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/producer"
	"github.com/marie20767/web-crawler/shared/httperr"
	"github.com/marie20767/web-crawler/shared/kafka/message"
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
	readers  []*kafka.Reader
	objStore *objstorage.Store
	producer *producer.Producer
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, prod *producer.Producer) (*Consumer, error) {
	readers := make([]*kafka.Reader, 0, kafkaCfg.Partitions)
	for range kafkaCfg.Partitions {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaCfg.Broker},
			Topic:    kafkaCfg.ParserTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		})

		readers = append(readers, reader)
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.HTMLBucketPrefix, awsCfg.TextBucketPrefix)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		readers:  readers,
		objStore: objStore,
		producer: prod,
	}, nil
}

type job struct {
	reader *kafka.Reader
	msg    kafka.Message
}

func (c *Consumer) Consume(ctx context.Context) error {
	bufferSize := workerCount * 2
	jobs := make(chan job, bufferSize)
	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			c.processMessages(ctx, jobs)
		})
	}

	defer wg.Wait()
	defer close(jobs)

	errGrp, errGrpCtx := errgroup.WithContext(ctx)
	for _, reader := range c.readers {
		errGrp.Go(func() error {
			return c.fetchMessages(errGrpCtx, reader, jobs)
		})
	}

	return errGrp.Wait()
}

func (c *Consumer) fetchMessages(ctx context.Context, reader *kafka.Reader, jobs chan<- job) error {
	for {
		readCtx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		msg, err := reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(kafkaPollInterval)

				slog.Info("no messages available, waiting to poll again", slog.Int64("interval_seconds", int64(kafkaPollInterval.Seconds())))
				select {
				case <-ctx.Done():
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
		case jobs <- job{reader: reader, msg: msg}:
		case <-ctx.Done():
			slog.Warn("context cancelled, dropping unqueued message", slog.String("url", string(msg.Key)))
			return nil
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, jobs <-chan job) {
	ctxNoCancel := context.WithoutCancel(ctx)

	for job := range jobs {
		slog.Info("processing message", slog.String("id", string(job.msg.Key)))
		if err := c.processMessage(ctxNoCancel, &job.msg); err != nil {
			slog.Error("process message", slog.Any("error", err))
			var hErr *httperr.Err
			errStatusCode := 0
			if errors.As(err, &hErr) {
				errStatusCode = hErr.StatusCode
			}

			c.producer.ProduceDLQ(ctxNoCancel, &job.msg, errStatusCode)
		}

		if err := job.reader.CommitMessages(ctxNoCancel, job.msg); err != nil {
			slog.Error("commit message offset", slog.Any("error", err))
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) error {
	var parserMsg message.ParserMessage
	if err := json.Unmarshal(msg.Value, &parserMsg); err != nil {
		return fmt.Errorf("unmarshal parser message: %v", err)
	}

	baseURL, err := url.Parse(parserMsg.PageURL)
	if err != nil {
		return fmt.Errorf("parse page URL: %v", err)
	}

	rawHTML, err := c.objStore.FetchRawHTML(ctx, parserMsg.StorageURL)
	if err != nil {
		return err
	}

	parsedRes, err := c.parseRawHTML(rawHTML, baseURL)
	if err != nil {
		return err
	}

	err = c.objStore.StoreParsedText(ctx, string(msg.Key), parsedRes.text)
	if err != nil {
		return err
	}

	return c.producer.ProduceSeedURLs(ctx, parsedRes.urls)
}

type parsed struct {
	text string
	urls []string
}

func (c *Consumer) parseRawHTML(raw []byte, baseURL *url.URL) (*parsed, error) {
	doc, err := html.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse raw HTML %v", err)
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
					text := strings.TrimSpace(n.Data)
					if text != "" {
						if sb.Len() > 0 {
							sb.WriteByte(' ')
						}
						sb.WriteString(text)
					}
				}

			case html.ElementNode:
				if n.Data == "a" {
					for _, attr := range n.Attr {
						parsedHref, err := url.Parse(attr.Val)
						if err != nil {
							continue
						}

						if attr.Key != "href" || isResourceURL(parsedHref) {
							continue
						}

						switch parsedHref.Scheme {
						case "", "http", "https":
						default:
							continue
						}

						resolved := baseURL.ResolveReference(parsedHref)
						urls = append(urls, resolved.String())
					}
				}
			}
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}

	walk(doc)

	return &parsed{
		text: sb.String(),
		urls: urls,
	}, nil
}

func (c *Consumer) Close() {
	for _, reader := range c.readers {
		if err := reader.Close(); err != nil {
			slog.Error("close consumer", slog.Any("error", err))
		}
	}
}

func isResourceURL(u *url.URL) bool {
	switch strings.ToLower(path.Ext(u.Path)) {
	case ".css", ".woff", ".woff2", ".ttf", ".eot", ".otf",
		".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp":
		return true
	}

	return false
}
