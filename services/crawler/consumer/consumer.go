package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

	"github.com/marie20767/web-crawler/services/crawler/config"
	"github.com/marie20767/web-crawler/services/crawler/producer"
	"github.com/marie20767/web-crawler/shared/httperr"
	"github.com/marie20767/web-crawler/shared/objstorage"
)

const (
	workerCount = 10

	kafkaTimeout      = 10 * time.Second
	kafkaPollInterval = 30 * time.Second
	kafkaMinBatchSize = 10e3 // 10KB
	kafkaMaxBatchSize = 10e6 // 10MB

	maxContentSize = 2 * 1024 * 1024 // 2 MB

	httpTimeout         = 30 * time.Second
	minErrStatusCode    = 400
	maxIdleConns        = 200
	maxIdleConnsPerHost = 20
	idleConnTimeout     = 90 * time.Second
)

type Consumer struct {
	httpClient *http.Client
	readers    []*kafka.Reader
	objStore   *objstorage.Store
	producer   *producer.Producer
}

func New(ctx context.Context, kafkaCfg *config.Kafka, awsCfg *config.AWS, prod *producer.Producer) (*Consumer, error) {
	readers := make([]*kafka.Reader, 0, kafkaCfg.Partitions)
	for range kafkaCfg.Partitions {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaCfg.Broker},
			Topic:    kafkaCfg.InitTopic,
			GroupID:  kafkaCfg.GroupID,
			MinBytes: kafkaMinBatchSize,
			MaxBytes: kafkaMaxBatchSize,
		})

		readers = append(readers, reader)
	}

	objStore, err := objstorage.New(ctx, awsCfg.BucketName, awsCfg.BucketPrefix, "")
	if err != nil {
		return nil, err
	}

	return &Consumer{
		readers: readers,
		httpClient: &http.Client{
			Timeout: httpTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleConns,
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     idleConnTimeout,
			},
		},
		objStore: objStore,
		producer: prod,
	}, nil
}

type job struct {
	msg    kafka.Message
	reader *kafka.Reader
}

func (c *Consumer) Consume(ctx context.Context) error {
	jobs := make(chan job, workerCount)

	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			c.processMessages(ctx, jobs)
		})
	}

	defer wg.Wait()
	defer close(jobs)

	errGrp, errGrpCtx := errgroup.WithContext(ctx)
	for _, reader := range c.readers {
		errGrp.Go(func() error {
			return c.fetchMessages(errGrpCtx, reader, jobs)
		})
	}

	return errGrp.Wait()
}

func (c *Consumer) fetchMessages(ctx context.Context, reader *kafka.Reader, jobs chan<- job) error {
	for {
		readCtx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		msg, err := reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(kafkaPollInterval)

				slog.Info("no messages available, waiting to poll again", slog.Int64("interval_seconds", int64(kafkaPollInterval.Seconds())))
				select {
				case <-ctx.Done():
					timer.Stop()
					slog.Info("context cancelled")
					return nil
				case <-timer.C:
					continue
				}
			}

			if errors.Is(err, context.Canceled) {
				slog.Info("context cancelled")
				return nil
			}

			slog.Error("consume from topic", slog.Any("error", err))
			return err
		}

		select {
		case jobs <- job{reader: reader, msg: msg}:
		case <-ctx.Done():
			slog.Warn("context cancelled, dropping unqueued message", slog.String("id", string(msg.Key)))
			return nil
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, jobs <-chan job) {
	for job := range jobs {
		slog.Info("processing message", slog.String("id", string(job.msg.Key)))
		if err := c.processMessage(ctx, &job.msg); err != nil {
			slog.Error("process message", slog.Any("error", err))
			var hErr *httperr.Err
			errStatusCode := 0
			if errors.As(err, &hErr) {
				errStatusCode = hErr.StatusCode
			}

			c.producer.ProduceDLQ(&job.msg, errStatusCode)
		}

		if err := job.reader.CommitMessages(context.WithoutCancel(ctx), job.msg); err != nil {
			slog.Error("commit message offset", slog.Any("error", err))
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) error {
	parsedURL, err := url.Parse(string(msg.Value))
	if err != nil {
		return err
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("invalid URL: %q", string(msg.Value))
	}

	ctxNoCancel := context.WithoutCancel(ctx)

	// prevent SSRF attacks (malicious page embeds links to internal network addresses)
	if private, err := isPrivateHost(ctxNoCancel, parsedURL.Hostname()); err != nil {
		return fmt.Errorf("resolve host %q: %v", parsedURL.Hostname(), err)
	} else if private {
		return fmt.Errorf("blocked request to private host: %q", parsedURL.Hostname())
	}

	res, skipped, err := c.fetchWithLimit(ctxNoCancel, parsedURL.String())
	if err != nil {
		return err
	}
	if skipped {
		return nil
	}

	storageLink, err := c.objStore.StoreRawHTML(ctxNoCancel, string(msg.Key), res)
	if err != nil {
		return err
	}

	return c.producer.ProduceParser(string(msg.Key), parsedURL.String(), storageLink)
}

func (c *Consumer) fetchWithLimit(ctx context.Context, seedURL string) (data []byte, skipped bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, seedURL, http.NoBody)
	if err != nil {
		return nil, false, fmt.Errorf("create web page request %v", err)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("make web page request %v", err)
	}
	defer res.Body.Close() //nolint:errcheck
	if res.StatusCode >= minErrStatusCode {
		return nil, false, &httperr.Err{StatusCode: res.StatusCode}
	}

	if res.ContentLength > maxContentSize {
		slog.Info("skipped large web page based on content-length header", slog.Int64("content-length", res.ContentLength))
		return nil, true, nil
	}

	// fallback if content-length header is absent/untrustworthy
	limited := io.LimitReader(res.Body, maxContentSize+1)
	data, err = io.ReadAll(limited)

	if err != nil {
		return nil, false, fmt.Errorf("read response: %v", err)
	}

	if int64(len(data)) > maxContentSize {
		slog.Info("skipped large web page request", slog.Int64("content-length", int64(len(data))))
		return nil, true, nil
	}

	return data, false, nil
}

func (c *Consumer) Close() {
	for _, reader := range c.readers {
		if err := reader.Close(); err != nil {
			slog.Error("close consumer", slog.Any("error", err))
		}
	}
}

// privateIPRanges covers loopback, link-local (incl. AWS metadata at 169.254.169.254),
// and RFC-1918 private ranges for both IPv4 and IPv6.
var privateIPRanges = func() []*net.IPNet {
	blocks := []string{
		"127.0.0.0/8",    // IPv4 loopback
		"169.254.0.0/16", // IPv4 link-local
		"10.0.0.0/8",     // RFC-1918
		"172.16.0.0/12",  // RFC-1918
		"192.168.0.0/16", // RFC-1918
		"::1/128",        // IPv6 loopback
		"fc00::/7",       // IPv6 unique local
		"fe80::/10",      // IPv6 link-local
	}

	ranges := make([]*net.IPNet, len(blocks))
	for i, block := range blocks {
		_, ipNet, _ := net.ParseCIDR(block)
		ranges[i] = ipNet
	}

	return ranges
}()

func isPrivateHost(ctx context.Context, hostname string) (bool, error) {
	addrs, err := net.DefaultResolver.LookupHost(ctx, hostname)
	if err != nil {
		return false, err
	}

	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip == nil {
			slog.Warn("unparsable DNS address", slog.String("ip", ip.String()))
			continue
		}

		for _, block := range privateIPRanges {
			if block.Contains(ip) {
				return true, nil
			}
		}
	}

	return false, nil
}
