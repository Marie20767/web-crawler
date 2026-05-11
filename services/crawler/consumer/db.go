package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	shareddb "github.com/marie20767/web-crawler/shared/db"
)

type db struct {
	client         *shareddb.Client
	urlCollection  *mongo.Collection
	hostCollection *mongo.Collection
}

func (c *Consumer) alreadyCrawled(ctx context.Context, pageURL string) (bool, error) {
	dbCtx, cancelDbCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelDbCtx()

	var doc struct {
		StorageURL string `bson:"storageUrl"`
	}

	err := c.db.urlCollection.FindOne(dbCtx, bson.M{
		"_id": pageURL,
	}).Decode(&doc)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return false, fmt.Errorf("fetch url from db: %v", err)
	}

	if errors.Is(err, mongo.ErrNoDocuments) || doc.StorageURL == "" {
		return false, nil
	}

	slog.Info("skipped url: already crawled", slog.String("url", pageURL))
	return true, nil
}

func (c *Consumer) updateURLMetadata(ctx context.Context, pageURL, storageURL, host string) error {
	writeURLCtx, writeURLCancelCtx := context.WithTimeout(ctx, dbTimeout)
	defer writeURLCancelCtx()
	if _, err := c.db.urlCollection.UpdateOne(
		writeURLCtx,
		bson.M{"_id": pageURL},
		bson.M{"$set": bson.M{"storageUrl": storageURL}},
		options.Update().SetUpsert(true)); err != nil {
		return fmt.Errorf("update URL %v", err)
	}

	writeHostCtx, cancelWriteHostCtx := context.WithTimeout(ctx, dbTimeout)
	defer cancelWriteHostCtx()
	if _, err := c.db.hostCollection.UpdateOne(
		writeHostCtx,
		bson.M{"_id": host},
		bson.M{"$set": bson.M{"lastCrawlTime": time.Now()}},
	); err != nil {
		return fmt.Errorf("update host %v", err)
	}

	return nil
}
