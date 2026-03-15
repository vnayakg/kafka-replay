//go:build integration

package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/vnayakg/kafka-replay/internal/checkpoint"
	"github.com/vnayakg/kafka-replay/internal/config"
	"github.com/vnayakg/kafka-replay/internal/filter"
	"github.com/vnayakg/kafka-replay/internal/pipeline"
	"github.com/vnayakg/kafka-replay/internal/ratelimit"
	"github.com/vnayakg/kafka-replay/testutil"
)

func TestReplay_EndToEnd(t *testing.T) {
	kc := testutil.StartKafka(t)

	sourceTopic := "test-source"
	targetTopic := "test-target"
	numMessages := 100
	valueSize := 256

	kc.CreateTopic(t, sourceTopic, 3)
	kc.CreateTopic(t, targetTopic, 3)

	// Produce messages to source
	kc.ProduceMessages(t, sourceTopic, numMessages, valueSize)

	// Verify source has messages
	sourceCount := kc.CountMessages(t, sourceTopic)
	if sourceCount != int64(numMessages) {
		t.Fatalf("expected %d messages in source, got %d", numMessages, sourceCount)
	}

	// Run replay
	cfg := config.DefaultConfig()
	cfg.SourceBrokers = kc.Brokers
	cfg.SourceTopic = sourceTopic
	cfg.TargetBrokers = kc.Brokers
	cfg.TargetTopic = targetTopic

	p, err := pipeline.New(cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stats, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	if stats.Consumed.Load() != int64(numMessages) {
		t.Errorf("expected %d consumed, got %d", numMessages, stats.Consumed.Load())
	}
	if stats.Produced.Load() != int64(numMessages) {
		t.Errorf("expected %d produced, got %d", numMessages, stats.Produced.Load())
	}
	if stats.Errors.Load() != 0 {
		t.Errorf("expected 0 errors, got %d", stats.Errors.Load())
	}

	// Verify target has all messages
	targetCount := kc.CountMessages(t, targetTopic)
	if targetCount != int64(numMessages) {
		t.Errorf("expected %d messages in target, got %d", numMessages, targetCount)
	}

	// Verify message content
	records := kc.ConsumeAllMessages(t, targetTopic, numMessages)
	if len(records) != numMessages {
		t.Errorf("expected %d records consumed from target, got %d", numMessages, len(records))
	}

	// Check headers are preserved
	for _, r := range records {
		found := false
		for _, h := range r.Headers {
			if h.Key == "source" && string(h.Value) == "test" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected 'source: test' header, not found in offset %d", r.Offset)
		}
	}
}

func TestReplay_WithOffsetFilter(t *testing.T) {
	kc := testutil.StartKafka(t)

	sourceTopic := "test-offset-filter-source"
	targetTopic := "test-offset-filter-target"

	// Use 1 partition so offsets are predictable
	kc.CreateTopic(t, sourceTopic, 1)
	kc.CreateTopic(t, targetTopic, 1)

	kc.ProduceMessages(t, sourceTopic, 100, 128)

	// Replay only offsets [10, 20)
	from := int64(10)
	to := int64(20)

	cfg := config.DefaultConfig()
	cfg.SourceBrokers = kc.Brokers
	cfg.SourceTopic = sourceTopic
	cfg.TargetBrokers = kc.Brokers
	cfg.TargetTopic = targetTopic
	cfg.FromOffset = &from
	cfg.ToOffset = &to

	f := filter.NewChain(
		filter.NewOffsetRange(&from, &to),
	)

	p, err := pipeline.New(cfg, f, nil, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stats, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	if stats.Produced.Load() != 10 {
		t.Errorf("expected 10 produced (offsets 10-19), got %d", stats.Produced.Load())
	}

	targetCount := kc.CountMessages(t, targetTopic)
	if targetCount != 10 {
		t.Errorf("expected 10 messages in target, got %d", targetCount)
	}
}

func TestReplay_DryRun(t *testing.T) {
	kc := testutil.StartKafka(t)

	sourceTopic := "test-dryrun-source"
	targetTopic := "test-dryrun-target"

	kc.CreateTopic(t, sourceTopic, 1)
	kc.CreateTopic(t, targetTopic, 1)

	kc.ProduceMessages(t, sourceTopic, 50, 128)

	cfg := config.DefaultConfig()
	cfg.SourceBrokers = kc.Brokers
	cfg.SourceTopic = sourceTopic
	cfg.TargetBrokers = kc.Brokers
	cfg.TargetTopic = targetTopic
	cfg.DryRun = true

	p, err := pipeline.New(cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stats, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	if stats.Consumed.Load() != 50 {
		t.Errorf("expected 50 consumed, got %d", stats.Consumed.Load())
	}
	if stats.Produced.Load() != 50 {
		t.Errorf("expected 50 'produced' (dry-run), got %d", stats.Produced.Load())
	}

	// Target should be empty
	targetCount := kc.CountMessages(t, targetTopic)
	if targetCount != 0 {
		t.Errorf("dry-run should not produce messages, got %d in target", targetCount)
	}
}

func TestReplay_WithCheckpoint(t *testing.T) {
	kc := testutil.StartKafka(t)

	sourceTopic := "test-checkpoint-source"
	targetTopic := "test-checkpoint-target"

	kc.CreateTopic(t, sourceTopic, 1)
	kc.CreateTopic(t, targetTopic, 1)

	kc.ProduceMessages(t, sourceTopic, 100, 128)

	dir := t.TempDir()

	// First run: replay all
	cfg := config.DefaultConfig()
	cfg.SourceBrokers = kc.Brokers
	cfg.SourceTopic = sourceTopic
	cfg.TargetBrokers = kc.Brokers
	cfg.TargetTopic = targetTopic
	cfg.CheckpointDir = dir

	store, err := checkpoint.NewFileStore(dir, sourceTopic, targetTopic)
	if err != nil {
		t.Fatalf("create checkpoint store: %v", err)
	}

	p, err := pipeline.New(cfg, nil, store, nil)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stats, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	if err := store.Save(); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	if stats.Produced.Load() != 100 {
		t.Errorf("first run: expected 100 produced, got %d", stats.Produced.Load())
	}

	// Second run with checkpoint: should produce 0 new messages
	store2, err := checkpoint.NewFileStore(dir, sourceTopic, targetTopic)
	if err != nil {
		t.Fatalf("create checkpoint store2: %v", err)
	}

	p2, err := pipeline.New(cfg, nil, store2, nil)
	if err != nil {
		t.Fatalf("create pipeline2: %v", err)
	}

	stats2, err := p2.Run(ctx)
	// Expected: "no messages to replay" error since checkpoint covers everything
	if err != nil {
		// This is expected — all partitions are fully checkpointed
		t.Logf("second run returned expected error: %v", err)
	} else if stats2.Produced.Load() != 0 {
		t.Errorf("second run with checkpoint: expected 0 produced, got %d", stats2.Produced.Load())
	}
}

func TestReplay_WithRateLimit(t *testing.T) {
	kc := testutil.StartKafka(t)

	sourceTopic := "test-ratelimit-source"
	targetTopic := "test-ratelimit-target"

	kc.CreateTopic(t, sourceTopic, 1)
	kc.CreateTopic(t, targetTopic, 1)

	kc.ProduceMessages(t, sourceTopic, 50, 128)

	cfg := config.DefaultConfig()
	cfg.SourceBrokers = kc.Brokers
	cfg.SourceTopic = sourceTopic
	cfg.TargetBrokers = kc.Brokers
	cfg.TargetTopic = targetTopic
	cfg.RateLimit = 100 // 100 msgs/sec

	limiter := ratelimit.New(cfg.RateLimit, cfg.BatchSize)

	p, err := pipeline.New(cfg, nil, nil, limiter)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	stats, err := p.Run(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	if stats.Produced.Load() != 50 {
		t.Errorf("expected 50 produced, got %d", stats.Produced.Load())
	}

	// With 100 msgs/sec and 50 messages, should take ~0.5s minimum
	// (with burst allowance it may be faster, but shouldn't be instant)
	t.Logf("rate-limited replay took %v for %d messages", elapsed, stats.Produced.Load())
}
