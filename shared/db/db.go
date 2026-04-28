package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	timeout = 10 * time.Second
)

type Client struct {
	client *mongo.Client
}

func New(ctx context.Context, uri string) (*Client, error) {
	dbCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client, err := mongo.Connect(dbCtx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect to db %v", err)
	}

	if err := client.Ping(dbCtx, nil); err != nil {
		return nil, fmt.Errorf("ping db %v", err)
	}

	slog.Info("successfully connected to db")

	return &Client{client: client}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *Client) Collection(db, collection string) *mongo.Collection {
	return c.client.Database(db).Collection(collection)
}
