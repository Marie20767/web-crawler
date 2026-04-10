package objstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const objStoreURLPrefix = "s3://"

type Store struct {
	client     *s3.Client
	htmlPrefix string
	textPrefix string
	bucketName string
}

func New(ctx context.Context, bucketName, htmlPrefix, textPrefix string) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	return &Store{
		client:     client,
		bucketName: bucketName,
		htmlPrefix: htmlPrefix,
		textPrefix: textPrefix,
	}, nil
}

func (s *Store) StoreRawHTML(ctx context.Context, messageID string, html []byte) (string, error) {
	contentType := "text/html"
	key := s.htmlPrefix + "/" + messageID

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucketName,
		Key:         &key,
		Body:        bytes.NewReader(html),
		ContentType: &contentType,
	}); err != nil {
		return "", fmt.Errorf("upload raw HTML to object store %v", err)
	}

	slog.Info("successfully uploaded raw HTML to object store")
	return fmt.Sprintf("%s%s/%s", objStoreURLPrefix, s.bucketName, key), nil
}

func (s *Store) FetchRawHTML(ctx context.Context, url string) ([]byte, error) {
	bucket, key := s.getBucketAndKey(url)

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		return nil, fmt.Errorf("fetch raw HTML from object store %v", err)
	}
	defer out.Body.Close()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read response %v", err)
	}

	slog.Info("successfully fetched HTML from object store")
	return raw, nil
}

func (s *Store) getBucketAndKey(objStoreURL string) (bucket, key string) {
	path := strings.TrimPrefix(objStoreURL, objStoreURLPrefix)
	firstSlashI := strings.Index(path, "/")

	bucket = path[:firstSlashI]
	key = path[firstSlashI+1:]

	return bucket, key
}

func (s *Store) StoreParsedText(ctx context.Context, text, messageID string) error {
	contentType := "text/plain"
	key := s.textPrefix + "/" + messageID

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucketName,
		Key:         &key,
		Body:        strings.NewReader(text),
		ContentType: &contentType,
	}); err != nil {
		return fmt.Errorf("upload parsed text to object store %v", err)
	}

	slog.Info("successfully uploaded parsed text to object store")
	return nil
}
