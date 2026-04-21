package config

import (
	"fmt"
	"log/slog"
	"strconv"

	sharedconfig "github.com/marie20767/web-crawler/shared/config"
)

type Kafka struct {
	Broker      string
	InitTopic   string
	DLQTopic    string
	ParserTopic string
	Partitions  int
	GroupID     string
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
		"KAFKA_URL_TOPIC",
		"KAFKA_DLQ_TOPIC",
		"KAFKA_PARSER_TOPIC",
		"KAFKA_PARTITIONS",
		"KAFKA_GROUP_ID",
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

	partitions, err := strconv.Atoi(envVars["KAFKA_PARTITIONS"])
	if err != nil {
		return nil, fmt.Errorf("converting kafka partitions %v", err)
	}

	return &App{
		LogLevel: logLevel,
		Kafka: &Kafka{
			Broker:      envVars["KAFKA_BROKER"],
			InitTopic:   envVars["KAFKA_URL_TOPIC"],
			DLQTopic:    envVars["KAFKA_DLQ_TOPIC"],
			ParserTopic: envVars["KAFKA_PARSER_TOPIC"],
			Partitions:  partitions,
			GroupID:     envVars["KAFKA_GROUP_ID"],
		},
		AWS: &AWS{
			BucketName:   envVars["BUCKET_NAME"],
			BucketPrefix: envVars["BUCKET_PREFIX"],
		},
	}, nil
}
