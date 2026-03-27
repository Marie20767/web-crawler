package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

type Role string

type App struct {
	LogLevel slog.Level
	Kafka    *Kafka
}

var logLevelMap = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

type Kafka struct {
	Broker string
	Topic  string
}

func ParseEnv() (*App, error) {
	// Ignore error because in production there will be no .env file, env vars will be passed
	// in at runtime via docker run command/docker-compose
	_ = godotenv.Load()

	envVars := map[string]string{
		"LOG_LEVEL":    "",
		"KAFKA_BROKER": "",
		"KAFKA_TOPIC":  "",
	}

	for key := range envVars {
		value := os.Getenv(key)
		if value == "" {
			return nil, fmt.Errorf("%s environment variable is not set", key)
		}
		envVars[key] = value
	}

	logLevel, ok := logLevelMap[envVars["LOG_LEVEL"]]
	if !ok {
		return nil, errors.New("LOG_LEVEL should be one of debug|info|warning|error")
	}

	return &App{
		LogLevel: logLevel,
		Kafka: &Kafka{
			Broker: envVars["KAFKA_BROKER"],
			Topic:  envVars["KAFKA_TOPIC"],
		},
	}, nil
}
