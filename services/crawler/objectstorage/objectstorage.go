package objectstorage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	client     *s3.Client
	prefix     string
	bucketName string
	ctx        context.Context
}

func New(ctx context.Context, bucketName, prefix string) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	return &Store{
		client:     client,
		prefix:     prefix,
		bucketName: bucketName,
		ctx:        ctx,
	}, nil
}

func (s *Store) StoreHTML(messageID string, html []byte) error {
	contentType := "text/html"
	key := s.prefix + "/" + messageID

	if _, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:      &s.bucketName,
		Key:         &key,
		Body:        bytes.NewReader(html),
		ContentType: &contentType,
	}); err != nil {
		return fmt.Errorf("upload HTML to object store %v", err)
	}

	slog.Info("successfully uploaded HTML to object store")
	return nil
}
