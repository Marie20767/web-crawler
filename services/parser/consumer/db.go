package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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
