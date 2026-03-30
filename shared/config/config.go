package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

var logLevelMap = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func ParseLogLevel(value string) (slog.Level, error) {
	level, ok := logLevelMap[value]
	if !ok {
		return 0, errors.New("LOG_LEVEL should be one of debug|info|warn|error")
	}
	return level, nil
}

func LoadEnvVars(keys []string) (map[string]string, error) {
	// Ignore error because in production there will be no .env file, env vars will be passed
	// in at runtime via docker run command/docker-compose
	_ = godotenv.Load()

	result := make(map[string]string, len(keys))
	for _, key := range keys {
		value := os.Getenv(key)
		if value == "" {
			return nil, fmt.Errorf("%s environment variable is not set", key)
		}
		result[key] = value
	}
	return result, nil
}
