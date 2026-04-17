package producer

import (
	"context"

	"github.com/google/uuid"
	sharedproducer "github.com/marie20767/web-crawler/shared/kafka/producer"
)

var seedURLs = []string{
	"https://www.bookbrowse.com/read-alikes/",
	"https://www.goodreads.com/list/tag/read-alikes",
	"https://www.whatshouldireadnext.com/",
	"https://www.libraryreads.org/",
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
	"https://crimereads.com/?s=if+you+like",
	"https://www.thrillerwriters.org/resources/read-alikes/",
	"https://www.nytimes.com/search?query=if+you+liked",
	"https://www.theguardian.com/books/series/if-you-liked",
}

type Producer struct {
	*sharedproducer.Producer
	topic  string
	broker string
}

func New(ctx context.Context, broker, topic string) (*Producer, error) {
	prod, err := sharedproducer.New(ctx, broker)
	if err != nil {
		return nil, err
	}

	return &Producer{
		Producer: prod,
		broker:   broker,
		topic:    topic,
	}, nil
}

func (p *Producer) ProduceSeedURLs() error {
	for _, url := range seedURLs {
		msgID := uuid.New().String()
		err := p.Producer.Produce([]byte(msgID), []byte(url), p.topic)

		if err != nil {
			return err
		}
	}

	return nil
}
