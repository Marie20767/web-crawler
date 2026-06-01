package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const defaultCrawlDelay = 1 * time.Second

type hostRecord struct {
	Robots        Robots    `bson:"robots"`
	LastCrawlTime time.Time `bson:"lastCrawlTime"`
}

func (c *Consumer) handleRateLimit(ctx context.Context, pageURL string, parsedURL *url.URL, host string) (isAllowed bool, err error) {
	var hostRecord hostRecord

	readCtx, cancelReadCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelReadCtx()
	if err = c.db.hostCollection.FindOne(readCtx, bson.M{"_id": host}).Decode(&hostRecord); err != nil { //nolint:goconst
		if errors.Is(err, mongo.ErrNoDocuments) {
			if err := c.handleNewRobots(ctx, &hostRecord, parsedURL.Scheme, host); err != nil {
				return false, err
			}
		} else {
			return false, fmt.Errorf("find host in db %v", err)
		}
	}

	if !isPathAllowed(parsedURL.Path, hostRecord.Robots.AllowedPaths, hostRecord.Robots.DisallowedPaths) {
		slog.Warn("path not allowed", slog.String("URL", pageURL))
		return false, nil
	}

	delayDur, err := time.ParseDuration(hostRecord.Robots.CrawlDelay)
	if err != nil {
		return false, fmt.Errorf("convert crawl delay %v", err)
	}

	if hostRecord.LastCrawlTime.IsZero() || time.Since(hostRecord.LastCrawlTime) >= delayDur {
		return true, nil
	}

	slog.Info("rate limited", slog.Time("last crawled", hostRecord.LastCrawlTime), slog.Duration("delay", delayDur))

	const maxCrawlDelay = 30 * time.Second
	time.Sleep(min(delayDur, maxCrawlDelay))

	if time.Since(hostRecord.LastCrawlTime) >= delayDur {
		return true, nil
	}

	slog.Info("rate limited", slog.Time("last crawled", hostRecord.LastCrawlTime), slog.Duration("delay", delayDur))

	err = c.producer.ReproduceURL(ctx, pageURL, host)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (c *Consumer) handleNewRobots(ctx context.Context, record *hostRecord, scheme, host string) error {
	robotsData, err := c.fetchRobots(ctx, scheme, host)
	switch {
	case err != nil:
		return err
	case robotsData == nil:
		record.Robots = Robots{
			CrawlDelay:      defaultCrawlDelay.String(),
			AllowedPaths:    nil,
			DisallowedPaths: nil,
		}
	default:
		record.Robots = parseRobots(robotsData)
	}

	filter := bson.M{"_id": host}
	update := bson.M{
		// no-op if other worker already inserted
		"$setOnInsert": bson.M{
			"_id":           host,
			"robots":        record.Robots,
			"createdAt":     time.Now(),
			"lastCrawlTime": time.Time{},
		},
	}
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	writeCtx, cancelWriteCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelWriteCtx()
	if err = c.db.hostCollection.FindOneAndUpdate(writeCtx, filter, update, opts).Decode(record); err != nil {
		return fmt.Errorf("add new host to db %v", err)
	}

	return nil
}
