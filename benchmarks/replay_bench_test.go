//go:build integration

package benchmarks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vnayakg/kafka-replay/internal/config"
	"github.com/vnayakg/kafka-replay/internal/pipeline"
	"github.com/vnayakg/kafka-replay/internal/ratelimit"
	"github.com/vnayakg/kafka-replay/testutil"
)

var benchKafka *testutil.KafkaContainer

func setupBenchKafka(b *testing.B) *testutil.KafkaContainer {
	b.Helper()
	if benchKafka == nil {
		// Use testing.T adapter since StartKafka expects *testing.T
		t := &testing.T{}
		benchKafka = testutil.StartKafka(t)
	}
	return benchKafka
}

// BenchmarkReplay_MessageVolume measures throughput for varying message counts.
func BenchmarkReplay_10K(b *testing.B) {
	benchmarkReplayVolume(b, 10_000, 256, 3)
}

func BenchmarkReplay_100K(b *testing.B) {
	benchmarkReplayVolume(b, 100_000, 256, 3)
}

func BenchmarkReplay_1M(b *testing.B) {
	benchmarkReplayVolume(b, 1_000_000, 256, 3)
}

// BenchmarkReplay_PartitionScaling measures throughput with different partition counts.
func BenchmarkReplay_Partitions1(b *testing.B) {
	benchmarkReplayVolume(b, 50_000, 256, 1)
}

func BenchmarkReplay_Partitions4(b *testing.B) {
	benchmarkReplayVolume(b, 50_000, 256, 4)
}

func BenchmarkReplay_Partitions16(b *testing.B) {
	benchmarkReplayVolume(b, 50_000, 256, 16)
}

func BenchmarkReplay_Partitions32(b *testing.B) {
	benchmarkReplayVolume(b, 50_000, 256, 32)
}

// BenchmarkReplay_MessageSizes measures throughput for different message sizes.
func BenchmarkReplay_100B(b *testing.B) {
	benchmarkReplayVolume(b, 50_000, 100, 3)
}

func BenchmarkReplay_1KB(b *testing.B) {
	benchmarkReplayVolume(b, 50_000, 1024, 3)
}

func BenchmarkReplay_10KB(b *testing.B) {
	benchmarkReplayVolume(b, 10_000, 10*1024, 3)
}

func BenchmarkReplay_100KB(b *testing.B) {
	benchmarkReplayVolume(b, 5_000, 100*1024, 3)
}

// BenchmarkReplay_WithRateLimiter verifies rate limiter accuracy under load.
func BenchmarkReplay_WithRateLimiter(b *testing.B) {
	kc := setupBenchKafka(b)

	sourceTopic := fmt.Sprintf("bench-ratelimit-src-%d", time.Now().UnixNano())
	targetTopic := fmt.Sprintf("bench-ratelimit-tgt-%d", time.Now().UnixNano())
	numMessages := 5000
	ratePerSec := 2000

	t := &testing.T{}
	kc.CreateTopic(t, sourceTopic, 1)
	kc.CreateTopic(t, targetTopic, 1)
	kc.ProduceMessages(t, sourceTopic, numMessages, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tgt := fmt.Sprintf("%s-%d", targetTopic, i)
		kc.CreateTopic(t, tgt, 1)

		cfg := config.DefaultConfig()
		cfg.SourceBrokers = kc.Brokers
		cfg.SourceTopic = sourceTopic
		cfg.TargetBrokers = kc.Brokers
		cfg.TargetTopic = tgt
		cfg.RateLimit = ratePerSec

		limiter := ratelimit.New(ratePerSec, cfg.BatchSize)

		p, err := pipeline.New(cfg, nil, nil, limiter)
		if err != nil {
			b.Fatalf("create pipeline: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		stats, err := p.Run(ctx)
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			b.Fatalf("run pipeline: %v", err)
		}

		produced := stats.Produced.Load()
		actualRate := float64(produced) / elapsed.Seconds()

		// Rate should be within 50% of target (allowing for burst and startup overhead)
		if actualRate > float64(ratePerSec)*1.5 {
			b.Errorf("rate limiter not effective: target=%d/s, actual=%.0f/s", ratePerSec, actualRate)
		}

		b.ReportMetric(actualRate, "msgs/sec")
		b.ReportMetric(float64(produced), "total_msgs")
	}
}

func benchmarkReplayVolume(b *testing.B, numMessages int, valueSize int, partitions int32) {
	kc := setupBenchKafka(b)

	sourceTopic := fmt.Sprintf("bench-src-%d-%d-%d-%d", numMessages, valueSize, partitions, time.Now().UnixNano())
	t := &testing.T{}
	kc.CreateTopic(t, sourceTopic, partitions)

	b.Logf("Producing %d messages (%d bytes each) to %d partitions...", numMessages, valueSize, partitions)
	kc.ProduceMessages(t, sourceTopic, numMessages, valueSize)

	totalBytes := int64(numMessages) * int64(valueSize)

	b.ResetTimer()
	b.SetBytes(totalBytes)

	for i := 0; i < b.N; i++ {
		targetTopic := fmt.Sprintf("bench-tgt-%d-%d-%d-%d-%d", numMessages, valueSize, partitions, time.Now().UnixNano(), i)
		kc.CreateTopic(t, targetTopic, partitions)

		cfg := config.DefaultConfig()
		cfg.SourceBrokers = kc.Brokers
		cfg.SourceTopic = sourceTopic
		cfg.TargetBrokers = kc.Brokers
		cfg.TargetTopic = targetTopic

		p, err := pipeline.New(cfg, nil, nil, nil)
		if err != nil {
			b.Fatalf("create pipeline: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		start := time.Now()
		stats, err := p.Run(ctx)
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			b.Fatalf("run pipeline: %v", err)
		}

		produced := stats.Produced.Load()
		if produced != int64(numMessages) {
			b.Errorf("expected %d produced, got %d", numMessages, produced)
		}

		msgsPerSec := float64(produced) / elapsed.Seconds()
		mbPerSec := float64(totalBytes) / elapsed.Seconds() / (1024 * 1024)

		b.ReportMetric(msgsPerSec, "msgs/sec")
		b.ReportMetric(mbPerSec, "MB/sec")
		b.ReportMetric(float64(elapsed.Milliseconds())/float64(produced), "ms/msg")
	}
}
