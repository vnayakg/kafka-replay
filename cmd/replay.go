package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/vnayakg/kafka-replay/internal/checkpoint"
	"github.com/vnayakg/kafka-replay/internal/config"
	"github.com/vnayakg/kafka-replay/internal/filter"
	"github.com/vnayakg/kafka-replay/internal/pipeline"
	"github.com/vnayakg/kafka-replay/internal/ratelimit"
)

var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "Replay messages from a source topic to a target topic",
	Long: `Replay messages from a source Kafka topic (e.g., a Dead Letter Topic) to a target topic.

Supports partition-parallel consumption, batched idempotent production,
filtering by offset/timestamp ranges, checkpointing, and rate limiting.`,
	RunE: runReplay,
}

func init() {
	f := replayCmd.Flags()

	// Source
	f.StringSlice("source-brokers", []string{"localhost:9092"}, "Source Kafka broker addresses")
	f.String("source-topic", "", "Source topic to replay from (required)")

	// Target
	f.StringSlice("target-brokers", nil, "Target Kafka broker addresses (defaults to source-brokers)")
	f.String("target-topic", "", "Target topic to replay to (required unless --dry-run)")

	// Filters
	f.Int64("from-offset", -1, "Start replay from this offset (inclusive, -1 = beginning)")
	f.Int64("to-offset", -1, "Stop replay at this offset (exclusive, -1 = end)")
	f.String("from-timestamp", "", "Start replay from this timestamp (RFC3339, e.g., 2024-01-01T00:00:00Z)")
	f.String("to-timestamp", "", "Stop replay at this timestamp (RFC3339)")

	// Performance
	f.Int("batch-size", 500, "Producer batch size")
	f.Int("buffer-size", 1000, "Per-partition buffer size for backpressure")
	f.Int("rate-limit", 0, "Max messages per second (0 = unlimited)")

	// Behavior
	f.Bool("dry-run", false, "Preview replay without producing messages")
	f.Bool("exactly-once", false, "Enable exactly-once semantics via Kafka transactions")
	f.Bool("verbose", false, "Enable verbose logging")

	// Checkpoint
	f.String("checkpoint-dir", "", "Directory for checkpoint files (empty = no checkpointing)")

	_ = replayCmd.MarkFlagRequired("source-topic")

	rootCmd.AddCommand(replayCmd)
}

func runReplay(cmd *cobra.Command, args []string) error {
	cfg, err := buildReplayConfig(cmd)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Setup logging
	logLevel := slog.LevelInfo
	if cfg.Verbose {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	// Build filter chain
	f := buildFilters(cfg)

	// Build checkpoint store
	store, err := checkpoint.NewFileStore(cfg.CheckpointDir, cfg.SourceTopic, cfg.TargetTopic)
	if err != nil {
		return fmt.Errorf("create checkpoint store: %w", err)
	}

	// Build rate limiter
	limiter := ratelimit.New(cfg.RateLimit, cfg.BatchSize)

	// Create pipeline
	p, err := pipeline.New(cfg, f, store, limiter)
	if err != nil {
		return fmt.Errorf("create pipeline: %w", err)
	}

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down gracefully...", "signal", sig)
		cancel()
	}()

	// Run
	start := time.Now()
	stats, err := p.Run(ctx)
	elapsed := time.Since(start)

	// Save final checkpoint
	if store != nil {
		if saveErr := store.Save(); saveErr != nil {
			slog.Error("failed to save final checkpoint", "err", saveErr)
		}
	}

	// Print summary
	fmt.Fprintf(os.Stderr, "\n--- Replay Summary ---\n")
	fmt.Fprintf(os.Stderr, "Duration:  %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "Consumed:  %d\n", stats.Consumed.Load())
	fmt.Fprintf(os.Stderr, "Produced:  %d\n", stats.Produced.Load())
	fmt.Fprintf(os.Stderr, "Filtered:  %d\n", stats.Filtered.Load())
	fmt.Fprintf(os.Stderr, "Errors:    %d\n", stats.Errors.Load())
	if elapsed.Seconds() > 0 {
		throughput := float64(stats.Produced.Load()) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "Throughput: %.0f msgs/sec\n", throughput)
	}

	return err
}

func buildReplayConfig(cmd *cobra.Command) (*config.Config, error) {
	cfg := config.DefaultConfig()

	var err error
	cfg.SourceBrokers, err = cmd.Flags().GetStringSlice("source-brokers")
	if err != nil {
		return nil, err
	}
	cfg.SourceTopic, err = cmd.Flags().GetString("source-topic")
	if err != nil {
		return nil, err
	}

	cfg.TargetBrokers, err = cmd.Flags().GetStringSlice("target-brokers")
	if err != nil {
		return nil, err
	}
	if len(cfg.TargetBrokers) == 0 {
		cfg.TargetBrokers = cfg.SourceBrokers
	}
	cfg.TargetTopic, err = cmd.Flags().GetString("target-topic")
	if err != nil {
		return nil, err
	}

	fromOffset, _ := cmd.Flags().GetInt64("from-offset")
	if fromOffset >= 0 {
		cfg.FromOffset = &fromOffset
	}
	toOffset, _ := cmd.Flags().GetInt64("to-offset")
	if toOffset >= 0 {
		cfg.ToOffset = &toOffset
	}

	fromTS, _ := cmd.Flags().GetString("from-timestamp")
	if fromTS != "" {
		t, err := parseTimestamp(fromTS)
		if err != nil {
			return nil, fmt.Errorf("invalid from-timestamp: %w", err)
		}
		cfg.FromTime = &t
	}
	toTS, _ := cmd.Flags().GetString("to-timestamp")
	if toTS != "" {
		t, err := parseTimestamp(toTS)
		if err != nil {
			return nil, fmt.Errorf("invalid to-timestamp: %w", err)
		}
		cfg.ToTime = &t
	}

	cfg.BatchSize, _ = cmd.Flags().GetInt("batch-size")
	cfg.BufferSize, _ = cmd.Flags().GetInt("buffer-size")
	cfg.RateLimit, _ = cmd.Flags().GetInt("rate-limit")
	cfg.DryRun, _ = cmd.Flags().GetBool("dry-run")
	cfg.ExactlyOnce, _ = cmd.Flags().GetBool("exactly-once")
	cfg.Verbose, _ = cmd.Flags().GetBool("verbose")
	cfg.CheckpointDir, _ = cmd.Flags().GetString("checkpoint-dir")

	return cfg, nil
}

func buildFilters(cfg *config.Config) filter.Filter {
	return filter.NewChain(
		filter.NewOffsetRange(cfg.FromOffset, cfg.ToOffset),
		filter.NewTimestampRange(cfg.FromTime, cfg.ToTime),
	)
}

func parseTimestamp(s string) (time.Time, error) {
	// Try RFC3339 first
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return t, nil
	}

	// Try common formats
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		t, err = time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse %q; expected RFC3339 format (e.g., %s)", s, strings.Replace(time.RFC3339, "Z07:00", "Z", 1))
}
