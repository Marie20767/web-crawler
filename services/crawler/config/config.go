package config

import (
	"log/slog"

	sharedconfig "github.com/marie20767/web-crawler/shared/config"
)

type Kafka struct {
	Broker      string
	InitTopic   string
	DLQTopic    string
	ParserTopic string
}

type AWS struct {
	BucketPrefix string
	BucketName   string
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
		"KAFKA_INIT_TOPIC",
		"KAFKA_DLQ_TOPIC",
		"KAFKA_PARSER_TOPIC",
		"BUCKET_NAME",
		"BUCKET_PREFIX",
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
			Broker:      envVars["KAFKA_BROKER"],
			InitTopic:   envVars["KAFKA_INIT_TOPIC"],
			DLQTopic:    envVars["KAFKA_DLQ_TOPIC"],
			ParserTopic: envVars["KAFKA_PARSER_TOPIC"],
		},
		AWS: &AWS{
			BucketName:   envVars["BUCKET_NAME"],
			BucketPrefix: envVars["BUCKET_PREFIX"],
		},
	}, nil
}
