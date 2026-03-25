package objectstorage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Store struct {
	client     *s3.Client
	bucketName string
}

func New(ctx context.Context, bucketName string) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)

	return &Store{
		client:     client,
		bucketName: bucketName,
	}, nil
}
