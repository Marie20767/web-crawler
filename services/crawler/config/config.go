package config

import (
	"log/slog"

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
	URLTopic    string
	DLQTopic    string
	ParserTopic string
	GroupID     string
}

type AWS struct {
	BucketPrefix string
	BucketName   string
}

type Db struct {
	Uri            string
	Name           string
	URLCollection  string
	HostCollection string
}

func ParseEnv() (*App, error) {
	envVars, err := sharedconfig.LoadEnvVars([]string{
		"LOG_LEVEL",
		"KAFKA_BROKER",
		"KAFKA_URL_TOPIC",
		"KAFKA_DLQ_TOPIC",
		"KAFKA_PARSER_TOPIC",
		"KAFKA_GROUP_ID",
		"BUCKET_NAME",
		"BUCKET_PREFIX",
		"DB_URI",
		"DB_NAME",
		"DB_URL_COLLECTION",
		"DB_HOST_COLLECTION",
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
			URLTopic:    envVars["KAFKA_URL_TOPIC"],
			DLQTopic:    envVars["KAFKA_DLQ_TOPIC"],
			ParserTopic: envVars["KAFKA_PARSER_TOPIC"],
			GroupID:     envVars["KAFKA_GROUP_ID"],
		},
		AWS: &AWS{
			BucketName:   envVars["BUCKET_NAME"],
			BucketPrefix: envVars["BUCKET_PREFIX"],
		},
		Db: &Db{
			Uri:            envVars["DB_URI"],
			Name:           envVars["DB_NAME"],
			URLCollection:  envVars["DB_URL_COLLECTION"],
			HostCollection: envVars["DB_HOST_COLLECTION"],
		},
	}, nil
}
