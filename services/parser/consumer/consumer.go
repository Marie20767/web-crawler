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
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/html"

	"github.com/marie20767/web-crawler/services/parser/config"
	"github.com/marie20767/web-crawler/services/parser/producer"
	shareddb "github.com/marie20767/web-crawler/shared/db"
	"github.com/marie20767/web-crawler/shared/httperr"
	sharedconsumer "github.com/marie20767/web-crawler/shared/kafka/consumer"
	"github.com/marie20767/web-crawler/shared/kafka/message"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	dbTimeout = 5 * time.Second

	mongoDuplicateKeyErr = 11000

	urlTTL = 14 * 24 * time.Hour
)

type Consumer struct {
	*sharedconsumer.Consumer
	objStore *objstorage.Store
	producer *producer.Producer
	db       *db
}

type db struct {
	client     *shareddb.Client
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

	dbClient, err := shareddb.New(ctx, dbCfg.Uri)
	if err != nil {
		return nil, fmt.Errorf("connect to db %v", err)
	}

	indexCtx, cancelIndexCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelIndexCtx()
	if err := dbClient.CreateTTLIndex(indexCtx, dbCfg.Name, dbCfg.Collection, "queuedAt", urlTTL); err != nil {
		return nil, fmt.Errorf("create TTL index: %v", err)
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
						if attr.Key != "href" {
							continue
						}

						parsedHref, hrefErr := url.Parse(attr.Val)

						if hrefErr != nil || isResourceURL(parsedHref) {
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

func (c *Consumer) getUniqueURLs(ctx context.Context, parsedURLs []string) ([]string, error) {
	if len(parsedURLs) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(parsedURLs))
	deduped := make([]string, 0, len(parsedURLs))
	for _, u := range parsedURLs {
		if _, ok := seen[u]; !ok {
			seen[u] = struct{}{}
			deduped = append(deduped, u)
		}
	}

	dbCtx, cancelDbCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelDbCtx()

	models := make([]mongo.WriteModel, len(deduped))
	now := time.Now()
	for i, u := range deduped {
		models[i] = mongo.NewInsertOneModel().SetDocument(bson.M{"_id": u, "queuedAt": now})
	}

	_, err := c.db.collection.BulkWrite(dbCtx, models, options.BulkWrite().SetOrdered(false))
	if err == nil {
		return deduped, nil
	}

	var bulkErr mongo.BulkWriteException
	if !errors.As(err, &bulkErr) {
		return nil, fmt.Errorf("bulk insert URLs: %v", err)
	}

	alreadySeen := make(map[string]struct{}, len(bulkErr.WriteErrors))
	for _, we := range bulkErr.WriteErrors {
		if we.Code != mongoDuplicateKeyErr {
			return nil, fmt.Errorf("insert URL into db: %v", we.Message)
		}
		alreadySeen[deduped[we.Index]] = struct{}{}
	}

	unique := make([]string, 0, len(deduped)-len(alreadySeen))
	for _, u := range deduped {
		if _, isDuplicate := alreadySeen[u]; !isDuplicate {
			unique = append(unique, u)
		}
	}

	return unique, nil
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
