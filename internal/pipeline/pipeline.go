package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vnayakg/kafka-replay/internal/checkpoint"
	"github.com/vnayakg/kafka-replay/internal/config"
	"github.com/vnayakg/kafka-replay/internal/filter"
	"github.com/vnayakg/kafka-replay/internal/ratelimit"
)

// Stats holds per-partition and aggregate replay statistics.
type Stats struct {
	Consumed atomic.Int64
	Produced atomic.Int64
	Filtered atomic.Int64
	Errors   atomic.Int64
}

// Pipeline orchestrates the partition-parallel replay.
type Pipeline struct {
	cfg      *config.Config
	filter   filter.Filter
	producer *Producer
	store    checkpoint.Store
	limiter  *ratelimit.Limiter
	stats    *Stats
}

// New creates a new Pipeline from the given configuration.
func New(cfg *config.Config, f filter.Filter, store checkpoint.Store, limiter *ratelimit.Limiter) (*Pipeline, error) {
	return &Pipeline{
		cfg:     cfg,
		filter:  f,
		store:   store,
		limiter: limiter,
		stats:   &Stats{},
	}, nil
}

// Run executes the replay pipeline. It blocks until all partitions are
// consumed or the context is cancelled.
func (p *Pipeline) Run(ctx context.Context) (*Stats, error) {
	// Resolve partition bounds
	consumerCfg := ConsumerConfig{
		Brokers:    p.cfg.SourceBrokers,
		Topic:      p.cfg.SourceTopic,
		FromOffset: p.cfg.FromOffset,
		ToOffset:   p.cfg.ToOffset,
		FromTime:   p.cfg.FromTime,
		ToTime:     p.cfg.ToTime,
	}

	bounds, err := ResolvePartitionBounds(ctx, consumerCfg)
	if err != nil {
		return p.stats, fmt.Errorf("resolve bounds: %w", err)
	}

	// Apply checkpoint overrides
	if p.store != nil {
		cp, loadErr := p.store.Load()
		if loadErr == nil && cp != nil {
			for i, b := range bounds {
				if pcp, ok := cp.Partitions[b.Partition]; ok {
					resumeOffset := pcp.LastProducedOffset + 1
					if resumeOffset > b.StartOffset {
						slog.Info("resuming from checkpoint",
							"partition", b.Partition,
							"checkpoint_offset", pcp.LastProducedOffset,
							"resume_offset", resumeOffset,
						)
						bounds[i].StartOffset = resumeOffset
					}
				}
			}
		}
	}

	slog.Info("starting replay",
		"source_topic", p.cfg.SourceTopic,
		"target_topic", p.cfg.TargetTopic,
		"partitions", len(bounds),
		"dry_run", p.cfg.DryRun,
	)

	// Create producer (unless dry-run)
	if !p.cfg.DryRun {
		producerCfg := ProducerConfig{
			Brokers:     p.cfg.TargetBrokers,
			Topic:       p.cfg.TargetTopic,
			BatchSize:   p.cfg.BatchSize,
			ExactlyOnce: p.cfg.ExactlyOnce,
			SessionID:   fmt.Sprintf("%s-%d", p.cfg.SourceTopic, time.Now().UnixNano()),
		}
		p.producer, err = NewProducer(producerCfg)
		if err != nil {
			return p.stats, fmt.Errorf("create producer: %w", err)
		}
		defer func() { _ = p.producer.Close(ctx) }()
	}

	// Launch partition goroutines
	var wg sync.WaitGroup
	errCh := make(chan error, len(bounds))

	for _, b := range bounds {
		wg.Add(1)
		go func(b PartitionBounds) {
			defer wg.Done()
			if err := p.replayPartition(ctx, b); err != nil {
				errCh <- fmt.Errorf("partition %d: %w", b.Partition, err)
			}
		}(b)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		return p.stats, fmt.Errorf("replay errors: %v", errs)
	}

	return p.stats, nil
}

func (p *Pipeline) replayPartition(ctx context.Context, bounds PartitionBounds) error {
	cl, err := NewPartitionConsumer(p.cfg.SourceBrokers, p.cfg.SourceTopic, bounds.Partition, bounds.StartOffset)
	if err != nil {
		return err
	}
	defer cl.Close()

	slog.Info("replaying partition",
		"partition", bounds.Partition,
		"start", bounds.StartOffset,
		"end", bounds.EndOffset,
	)

	// Nothing to consume if start >= end
	if bounds.EndOffset > 0 && bounds.StartOffset >= bounds.EndOffset {
		slog.Info("partition already fully consumed", "partition", bounds.Partition)
		return nil
	}

	lastSeenOffset := bounds.StartOffset - 1

	for {
		select {
		case <-ctx.Done():
			return nil // graceful shutdown, not an error
		default:
		}

		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled || e.Err == context.DeadlineExceeded {
					return nil
				}
				slog.Error("fetch error", "partition", bounds.Partition, "err", e.Err)
			}
		}

		done := false
		fetches.EachRecord(func(record *kgo.Record) {
			if done {
				return
			}

			lastSeenOffset = record.Offset

			// Check if we've reached the end offset
			if bounds.EndOffset > 0 && record.Offset >= bounds.EndOffset {
				done = true
				return
			}

			p.stats.Consumed.Add(1)

			// Apply filters
			if p.filter != nil && !p.filter.Match(record) {
				p.stats.Filtered.Add(1)
				return
			}

			// Rate limit
			if p.limiter != nil {
				if err := p.limiter.Wait(ctx); err != nil {
					return
				}
			}

			// Produce or dry-run
			if p.cfg.DryRun {
				slog.Info("dry-run",
					"partition", record.Partition,
					"offset", record.Offset,
					"key", string(record.Key),
					"value_size", len(record.Value),
					"timestamp", record.Timestamp,
				)
				p.stats.Produced.Add(1)
				return
			}

			var wg sync.WaitGroup
			wg.Add(1)
			p.producer.ProduceAsync(ctx, record, func(err error) {
				defer wg.Done()
				if err != nil {
					p.stats.Errors.Add(1)
					slog.Error("produce error",
						"partition", record.Partition,
						"offset", record.Offset,
						"err", err,
					)
				} else {
					p.stats.Produced.Add(1)
					// Update checkpoint
					if p.store != nil {
						p.store.Update(bounds.Partition, record.Offset)
					}
				}
			})
			wg.Wait()
		})

		if done {
			break
		}

		// If last seen offset reached (or passed) the end, we're done
		if bounds.EndOffset > 0 && lastSeenOffset >= bounds.EndOffset-1 {
			break
		}
	}

	slog.Info("partition replay complete",
		"partition", bounds.Partition,
	)
	return nil
}

// GetStats returns the current stats.
func (p *Pipeline) GetStats() *Stats {
	return p.stats
}
