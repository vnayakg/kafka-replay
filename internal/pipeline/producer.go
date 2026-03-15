package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerConfig holds configuration for the target producer.
type ProducerConfig struct {
	Brokers     []string
	Topic       string
	BatchSize   int
	ExactlyOnce bool
	SessionID   string

	// Tuning options (zero values use defaults).
	BatchMaxBytes    int32         // default: 1MB (1<<20)
	Linger           time.Duration // default: 5ms
	Retries          int           // default: 3
	RequireAllAcks   *bool         // default: true (AllISRAcks)
	MaxBufferedScale int           // multiplier for BatchSize to set MaxBufferedRecords; default: 2
}

// Producer wraps a franz-go client for batched, idempotent production.
type Producer struct {
	client *kgo.Client
	topic  string
	mu     sync.Mutex
	errors []error
}

// NewProducer creates a Producer configured for high-throughput idempotent production.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	// Apply defaults for zero-value tuning options.
	batchMaxBytes := cfg.BatchMaxBytes
	if batchMaxBytes == 0 {
		batchMaxBytes = 1 << 20 // 1MB
	}
	linger := cfg.Linger
	if linger == 0 {
		linger = 5 * time.Millisecond
	}
	retries := cfg.Retries
	if retries == 0 {
		retries = 3
	}
	bufferedScale := cfg.MaxBufferedScale
	if bufferedScale == 0 {
		bufferedScale = 2
	}
	acks := kgo.AllISRAcks()
	if cfg.RequireAllAcks != nil && !*cfg.RequireAllAcks {
		acks = kgo.LeaderAck()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchMaxBytes(batchMaxBytes),
		kgo.ProducerLinger(linger),
		kgo.RecordRetries(retries),
		kgo.RequiredAcks(acks),
		kgo.MaxBufferedRecords(cfg.BatchSize * bufferedScale),
	}

	if cfg.ExactlyOnce {
		txnID := fmt.Sprintf("kafka-replay-%s", cfg.SessionID)
		opts = append(opts, kgo.TransactionalID(txnID))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}

	return &Producer{
		client: cl,
		topic:  cfg.Topic,
	}, nil
}

// ProduceAsync sends a record asynchronously and calls onComplete when done.
// The record's topic is overridden to the target topic.
func (p *Producer) ProduceAsync(ctx context.Context, record *kgo.Record, onComplete func(err error)) {
	// Override the topic to the target
	rec := &kgo.Record{
		Key:       record.Key,
		Value:     record.Value,
		Headers:   record.Headers,
		Topic:     p.topic,
		Timestamp: record.Timestamp,
	}

	p.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			p.mu.Lock()
			p.errors = append(p.errors, err)
			p.mu.Unlock()
		}
		if onComplete != nil {
			onComplete(err)
		}
	})
}

// Flush blocks until all buffered records have been produced or the context is cancelled.
func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// BeginTransaction starts a new transaction (only valid if ExactlyOnce is enabled).
func (p *Producer) BeginTransaction() error {
	return p.client.BeginTransaction()
}

// EndTransaction commits or aborts the current transaction.
func (p *Producer) EndTransaction(ctx context.Context, commit bool) error {
	return p.client.EndTransaction(ctx, kgo.TransactionEndTry(commit))
}

// Errors returns all production errors accumulated so far.
func (p *Producer) Errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	errs := make([]error, len(p.errors))
	copy(errs, p.errors)
	return errs
}

// Close flushes and closes the producer.
func (p *Producer) Close(ctx context.Context) error {
	if err := p.Flush(ctx); err != nil {
		return err
	}
	p.client.Close()
	return nil
}
