package config

import (
	"log/slog"

	sharedconfig "github.com/marie20767/web-crawler/shared/config"
)

type App struct {
	LogLevel slog.Level
	Kafka    *sharedconfig.Kafka
}

func ParseEnv() (*App, error) {
	envVars, err := sharedconfig.LoadEnvVars([]string{
		"LOG_LEVEL",
		"KAFKA_BROKER",
		"KAFKA_URL_TOPIC",
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
			Broker:   envVars["KAFKA_BROKER"],
			URLTopic: envVars["KAFKA_URL_TOPIC"],
		},
	}, nil
}
