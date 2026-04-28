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
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/html"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/producer"
	sharedDb "github.com/marie20767/web-crawler/shared/db"
	"github.com/marie20767/web-crawler/shared/httperr"
	sharedconsumer "github.com/marie20767/web-crawler/shared/kafka/consumer"
	"github.com/marie20767/web-crawler/shared/kafka/message"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	dbTimeout = 5 * time.Second
)

type Consumer struct {
	*sharedconsumer.Consumer
	objStore *objstorage.Store
	producer *producer.Producer
	db       *db
}

type db struct {
	client     *sharedDb.Client
	collection *mongo.Collection
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, dbCfg *config.Db, prod *producer.Producer) (*Consumer, error) {
	readers := make([]*kafka.Reader, 0, kafkaCfg.Partitions)
	for range kafkaCfg.Partitions {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaCfg.Broker},
			Topic:    kafkaCfg.ParserTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		}))
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.HTMLBucketPrefix, awsCfg.TextBucketPrefix)
	if err != nil {
		return nil, err
	}

	dbClient, err := sharedDb.New(ctx, dbCfg.Uri)
	if err != nil {
		return nil, fmt.Errorf("connect to db %v", err)
	}

	return &Consumer{
		Consumer: sharedconsumer.New(readers),
		objStore: objStore,
		producer: prod,
		db: &db{
			client:     dbClient,
			collection: dbClient.Collection(dbCfg.Name, dbCfg.Collection),
		},
	}, nil
}

func (c *Consumer) Consume(ctx context.Context) error {
	return c.Consumer.Consume(ctx, c.handle)
}

func (c *Consumer) handle(ctx context.Context, msg *kafka.Message) error {
	err := c.processMessage(ctx, msg)
	if err != nil {
		var hErr *httperr.Err
		errStatusCode := 0
		if errors.As(err, &hErr) {
			errStatusCode = hErr.StatusCode
		}
		c.producer.ProduceDLQ(ctx, msg, errStatusCode)
	}
	return err
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

	uniqueURLs, err := c.getUniqueURLs(ctx, parsedRes.urls)
	if err != nil {
		return err
	}

	return c.producer.ProduceSeedURLs(ctx, uniqueURLs)
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
	var urls []string
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
						parsedHref, hrefErr := url.Parse(attr.Val)
						if hrefErr != nil {
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

func isResourceURL(u *url.URL) bool {
	switch strings.ToLower(path.Ext(u.Path)) {
	case ".js", ".css", ".woff", ".woff2", ".ttf", ".eot", ".otf",
		".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp":
		return true
	}

	return false
}

func (c *Consumer) getUniqueURLs(ctx context.Context, parsedURLs []string) (unique []string, err error) {
	duplicates := []string{}

	filter := bson.M{
		"_id": bson.M{"$in": parsedURLs},
	}
	cursor, err := c.db.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("fetch duplicate URLs from db %v", err)
	}

	for cursor.Next(ctx) {
		var duplicate struct {
			url string `bson:"_id"`
		}
		err := cursor.Decode(&duplicate)
		if err != nil {
			return nil, fmt.Errorf("decode duplicate URL %v", err)
		}

		duplicates = append(duplicates, duplicate.url)
	}

	return duplicates, nil
}

func (c *Consumer) Close() {
	c.Consumer.Close()

	closeCtx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	err := c.db.client.Close(closeCtx)
	if err != nil {
		slog.Error("close db connection", slog.Any("error", err))
	}
}
