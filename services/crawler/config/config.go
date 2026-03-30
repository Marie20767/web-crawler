package config

import (
	"log/slog"

	sharedconfig "github.com/marie20767/web-crawler/shared/config"
)

type App struct {
	LogLevel slog.Level
	Kafka    *sharedconfig.Kafka
	AWS      *AWS
}

type AWS struct {
	ObjectStorePrefix string
	BucketName        string
}

func ParseEnv() (*App, error) {
	envVars, err := sharedconfig.LoadEnvVars([]string{
		"LOG_LEVEL",
		"KAFKA_BROKER",
		"KAFKA_TOPIC",
		"BUCKET_NAME",
		"OBJECT_STORE_PREFIX",
	})
	if err != nil {
		return nil, err
	}

	logLevel, err := sharedconfig.ParseLogLevel(envVars["LOG_LEVEL"])
	if err != nil {
		return nil, err
	}

	return &App{
		LogLevel: logLevel,
		Kafka: &sharedconfig.Kafka{
			Broker: envVars["KAFKA_BROKER"],
			Topic:  envVars["KAFKA_TOPIC"],
		},
		AWS: &AWS{
			BucketName:        envVars["BUCKET_NAME"],
			ObjectStorePrefix: envVars["OBJECT_STORE_PREFIX"],
		},
	}, nil
}
