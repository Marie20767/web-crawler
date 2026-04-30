package config

import (
	"fmt"
	"log/slog"
	"strconv"

	sharedconfig "github.com/marie20767/web-crawler/shared/config"
)

type App struct {
	LogLevel slog.Level
	Kafka    *Kafka
	AWS      *AWS
	Db       *Db
}

type Kafka struct {
	Broker      string
	ParserTopic string
	DLQTopic    string
	InitTopic   string
	Partitions  int
	GroupID     string
}

type AWS struct {
	HTMLBucketPrefix string
	TextBucketPrefix string
	BucketName       string
}

type Db struct {
	Uri        string
	Name       string
	Collection string
}

func ParseEnv() (*App, error) {
	envVars, err := sharedconfig.LoadEnvVars([]string{
		"LOG_LEVEL",
		"KAFKA_BROKER",
		"KAFKA_PARSER_TOPIC",
		"KAFKA_DLQ_TOPIC",
		"KAFKA_URL_TOPIC",
		"KAFKA_PARTITIONS",
		"KAFKA_GROUP_ID",
		"BUCKET_NAME",
		"HTML_BUCKET_PREFIX",
		"TEXT_BUCKET_PREFIX",
		"DB_URI",
		"DB_NAME",
		"DB_COLLECTION",
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
			ParserTopic: envVars["KAFKA_PARSER_TOPIC"],
			DLQTopic:    envVars["KAFKA_DLQ_TOPIC"],
			InitTopic:   envVars["KAFKA_URL_TOPIC"],
			Partitions:  partitions,
			GroupID:     envVars["KAFKA_GROUP_ID"],
		},
		AWS: &AWS{
			BucketName:       envVars["BUCKET_NAME"],
			HTMLBucketPrefix: envVars["HTML_BUCKET_PREFIX"],
			TextBucketPrefix: envVars["TEXT_BUCKET_PREFIX"],
		},
		Db: &Db{
			Uri:        envVars["DB_URI"],
			Name:       envVars["DB_NAME"],
			Collection: envVars["DB_COLLECTION"],
		},
	}, nil
}
