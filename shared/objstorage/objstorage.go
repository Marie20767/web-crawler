package objstorage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	client       *s3.Client
	bucketPrefix string
	bucketName   string
}

func New(ctx context.Context, bucketName, bucketPrefix string) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	return &Store{
		client:       client,
		bucketPrefix: bucketPrefix,
		bucketName:   bucketName,
	}, nil
}

func (s *Store) StoreRawHTML(ctx context.Context, messageID string, html []byte) (string, error) {
	contentType := "text/html"
	key := s.bucketPrefix + "/" + messageID

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucketName,
		Key:         &key,
		Body:        bytes.NewReader(html),
		ContentType: &contentType,
	}); err != nil {
		return "", fmt.Errorf("upload HTML to object store %v", err)
	}

	slog.Info("successfully uploaded HTML to object store")
	return fmt.Sprintf("s3://%s/%s", s.bucketName, key), nil
}
