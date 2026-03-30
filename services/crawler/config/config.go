package config

import (
	"log/slog"

	sharedconfig "github.com/marie20767/web-crawler/shared/config"
)

type Kafka struct {
	Broker   string
	URLTopic string
	DLQTopic string
}

type AWS struct {
	ObjectStorePrefix string
	BucketName        string
}

type App struct {
	LogLevel slog.Level
	Kafka    *Kafka
	AWS      *AWS
}

func ParseEnv() (*App, error) {
	envVars, err := sharedconfig.LoadEnvVars([]string{
		"LOG_LEVEL",
		"KAFKA_BROKER",
		"KAFKA_URL_TOPIC",
		"KAFKA_DLQ_TOPIC",
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
		Kafka: &Kafka{
			Broker:   envVars["KAFKA_BROKER"],
			URLTopic: envVars["KAFKA_URL_TOPIC"],
			DLQTopic: envVars["KAFKA_DLQ_TOPIC"],
		},
		AWS: &AWS{
			BucketName:        envVars["BUCKET_NAME"],
			ObjectStorePrefix: envVars["OBJECT_STORE_PREFIX"],
		},
	}, nil
}
