package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const defaultCrawlDelay = 1 * time.Second

type hostRecord struct {
	robots
	LastCrawlTime time.Time `json:"lastCrawlTime"`
}

func (c *Consumer) handleRateLimit(ctx context.Context, pageURL, path, scheme, host string) (isAllowed bool, err error) {
	var hostRecord hostRecord

	readCtx, cancelReadCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelReadCtx()
	if err = c.db.hostCollection.FindOne(readCtx, bson.M{"_id": host}).Decode(&hostRecord); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			if err := c.handleNewRobots(ctx, &hostRecord, scheme, host); err != nil {
				return false, err
			}
		} else {
			return false, fmt.Errorf("find host in db %v", err)
		}
	}

	if !isPathAllowed(path, hostRecord.AllowedPaths, hostRecord.DisallowedPaths) {
		slog.Warn("path not allowed", slog.String("URL", pageURL))
		return false, nil
	}

	if hostRecord.LastCrawlTime.IsZero() || time.Since(hostRecord.LastCrawlTime) >= hostRecord.CrawlDelay {
		return true, nil
	}

	slog.Info("rate limited", slog.Time("last crawled", hostRecord.LastCrawlTime), slog.Duration("delay", hostRecord.CrawlDelay))

	const maxCrawlDelay = 30 * time.Second
	time.Sleep(min(hostRecord.CrawlDelay, maxCrawlDelay))

	if time.Since(hostRecord.LastCrawlTime) >= hostRecord.CrawlDelay {
		return true, nil
	}

	slog.Info("rate limited", slog.Time("last crawled", hostRecord.LastCrawlTime), slog.Duration("delay", hostRecord.CrawlDelay))

	err = c.producer.ReproduceURL(ctx, pageURL, host)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (c *Consumer) handleNewRobots(ctx context.Context, hostRecord *hostRecord, scheme, host string) error {
	robotsData, err := c.fetchRobots(ctx, scheme, host)
	switch {
	case err != nil:
		return err
	case robotsData == nil:
		hostRecord.robots = robots{
			CrawlDelay:      defaultCrawlDelay,
			AllowedPaths:    []string{"*"},
			DisallowedPaths: nil,
		}
	default:
		hostRecord.robots = parseRobots(robotsData)
	}

	writeCtx, cancelWriteCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelWriteCtx()
	_, err = c.db.hostCollection.InsertOne(writeCtx, hostRecord.robots)
	if err != nil {
		return fmt.Errorf("add new host to db %v", err)
	}

	return nil
}
