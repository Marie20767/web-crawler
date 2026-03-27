package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/marie20767/web-crawler/services/initialiser/config"
)

const kafkaMaxAttempts = 5

var seedURLs = []string{
	"https://www.bookbrowse.com/read-alikes/",
	"https://www.goodreads.com/list/tag/read-alikes",
	"https://www.whatshouldireadnext.com/",
	"https://www.novelist.ep.la/NoveList/servlet/NoveList",
	"https://www.libraryreads.org/",
	"https://www.pearsonvuereads.com/",
	"https://www.overbooked.org/",
	"https://www.whichbook.net/",
	"https://www.gnooks.com/",
	"https://www.yalsa.ala.org/thehub/category/read-alikes/",
	"https://bookriot.com/?s=read+alike",
	"https://www.thereadinglist.co.uk/",
	"https://www.goodreads.com/list/tag/similar-books",
	"https://www.goodreads.com/list/tag/if-you-liked",
	"https://www.panmacmillan.com/readers-resources/read-alikes",
	"https://www.penguinrandomhouse.com/the-read-down/",
	"https://www.reddit.com/r/suggestmeabook/",
	"https://www.reddit.com/r/booksuggestions/",
	"https://www.reddit.com/r/Fantasy/comments/readalikes",
	"https://www.reddit.com/r/scifi/search/?q=if+you+like",
	"https://www.mysterysequels.com/",
	"https://www.fantasticfiction.com/similar/",
	"https://www.sfsite.com/",
	"https://crimereads.com/?s=if+you+like",
	"https://www.thrillerwriters.org/resources/read-alikes/",
	"https://www.nytimes.com/search?query=if+you+liked",
	"https://www.theguardian.com/books/series/if-you-liked",
}

func main() {
	if err := run(); err != nil {
		slog.Error("initialiser run", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("shutting down initialiser...")
}

func run() error {
	ctx := context.Background()

	cfg, err := config.ParseEnv()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	writer := newWriter(cfg.Kafka.Broker, cfg.Kafka.Topic)
	defer writer.Close() //nolint:errcheck

	failedCount := 0
	for _, url := range seedURLs {
		msgID := uuid.New()
		msg := kafka.Message{
			Key:   msgID[:],
			Value: []byte(url),
		}

		err := writer.WriteMessages(ctx, msg)
		if err != nil {
			failedCount++
			slog.Error("write message", slog.Any("error", err))
			continue
		}

		slog.Info("produced message", slog.String("id", msgID.String()), slog.String("url", url))
	}

	slog.Info("producing to topic complete", slog.Int("total", len(seedURLs)), slog.Int("failed", failedCount))

	return nil
}

func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  kafkaMaxAttempts,
	}
}
