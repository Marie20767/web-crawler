package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"

	"github.com/joho/godotenv"
)

type Role string

type App struct {
	Port        string
	LogLevel    slog.Level
	Environment Environment
	Kafka       *Kafka
	AWS         *AWS
}

type Environment string

const (
	EnvironmentDevelopment Environment = "development"
	EnvironmentProduction  Environment = "production"
	EnvironmentTest        Environment = "test"
)

var validEnvironments = []Environment{
	EnvironmentDevelopment,
	EnvironmentProduction,
	EnvironmentTest,
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

type AWS struct {
	bucketName string
}

func ParseEnv() (*App, error) {
	// Ignore error because in production there will be no .env file, env vars will be passed
	// in at runtime via docker run command/docker-compose
	_ = godotenv.Load()

	envVars := map[string]string{
		"SERVER_PORT":  "",
		"LOG_LEVEL":    "",
		"ENVIRONMENT":  "",
		"KAFKA_BROKER": "",
		"KAFKA_TOPIC":  "",
		"BUCKET_NAME":  "",
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

	environment := Environment(envVars["ENVIRONMENT"])
	if !slices.Contains(validEnvironments, environment) {
		return nil, errors.New("ENVIRONMENT should be one of development|production|test")
	}

	return &App{
		Port:        envVars["SERVER_PORT"],
		LogLevel:    logLevel,
		Environment: environment,
		Kafka: &Kafka{
			Broker: envVars["KAFKA_BROKER"],
			Topic:  envVars["KAFKA_TOPIC"],
		},
		AWS: &AWS{
			bucketName: envVars["BUCKET_NAME"],
		},
	}, nil
}
