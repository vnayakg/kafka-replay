package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vnayakg/kafka-replay/internal/config"
	"github.com/vnayakg/kafka-replay/internal/filter"
	"github.com/vnayakg/kafka-replay/internal/pipeline"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Preview messages in a topic without replaying",
	Long: `Inspect messages in a Kafka topic. Useful for previewing what would be replayed
before actually executing a replay.`,
	RunE: runInspect,
}

func init() {
	f := inspectCmd.Flags()

	f.StringSlice("brokers", []string{"localhost:9092"}, "Kafka broker addresses")
	f.String("topic", "", "Topic to inspect (required)")
	f.String("from-timestamp", "", "Start from this timestamp (RFC3339)")
	f.String("to-timestamp", "", "Stop at this timestamp (RFC3339)")
	f.Int("limit", 10, "Maximum number of messages to display")
	f.Bool("verbose", false, "Show full message values")

	_ = inspectCmd.MarkFlagRequired("topic")

	rootCmd.AddCommand(inspectCmd)
}

func runInspect(cmd *cobra.Command, args []string) error {
	cfg, err := buildInspectConfig(cmd)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	logLevel := slog.LevelWarn
	if cfg.Verbose {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Build timestamp filter
	f := filter.NewTimestampRange(cfg.FromTime, cfg.ToTime)

	// Resolve partition bounds
	consumerCfg := pipeline.ConsumerConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		FromTime: cfg.FromTime,
		ToTime:   cfg.ToTime,
	}

	bounds, err := pipeline.ResolvePartitionBounds(ctx, consumerCfg)
	if err != nil {
		return fmt.Errorf("resolve bounds: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Inspecting topic %q (%d partitions with data)\n\n", cfg.Topic, len(bounds))

	count := 0
	for _, b := range bounds {
		if count >= cfg.Limit {
			break
		}

		cl, err := pipeline.NewPartitionConsumer(cfg.Brokers, cfg.Topic, b.Partition, b.StartOffset)
		if err != nil {
			return fmt.Errorf("create consumer for partition %d: %w", b.Partition, err)
		}

		for count < cfg.Limit {
			select {
			case <-ctx.Done():
				cl.Close()
				return nil
			default:
			}

			fetches := cl.PollFetches(ctx)
			done := false
			fetches.EachRecord(func(record *kgo.Record) {
				if done || count >= cfg.Limit {
					done = true
					return
				}
				if b.EndOffset > 0 && record.Offset >= b.EndOffset {
					done = true
					return
				}
				if f != nil && !f.Match(record) {
					return
				}

				count++
				printRecord(record, cfg.Verbose)
			})

			if done {
				break
			}
		}
		cl.Close()
	}

	fmt.Fprintf(os.Stderr, "\n--- Displayed %d message(s) ---\n", count)
	return nil
}

func buildInspectConfig(cmd *cobra.Command) (*config.InspectConfig, error) {
	cfg := &config.InspectConfig{}

	var err error
	cfg.Brokers, err = cmd.Flags().GetStringSlice("brokers")
	if err != nil {
		return nil, err
	}
	cfg.Topic, err = cmd.Flags().GetString("topic")
	if err != nil {
		return nil, err
	}
	cfg.Limit, _ = cmd.Flags().GetInt("limit")
	cfg.Verbose, _ = cmd.Flags().GetBool("verbose")

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

	return cfg, nil
}

func printRecord(record *kgo.Record, verbose bool) {
	fmt.Printf("--- Partition: %d | Offset: %d | Timestamp: %s ---\n",
		record.Partition, record.Offset, record.Timestamp.Format(time.RFC3339))
	fmt.Printf("  Key:   %s\n", truncate(string(record.Key), 100))

	if verbose {
		fmt.Printf("  Value: %s\n", string(record.Value))
	} else {
		fmt.Printf("  Value: %s (%d bytes)\n", truncate(string(record.Value), 80), len(record.Value))
	}

	if len(record.Headers) > 0 {
		fmt.Printf("  Headers:\n")
		for _, h := range record.Headers {
			fmt.Printf("    %s: %s\n", h.Key, truncate(string(h.Value), 80))
		}
	}
	fmt.Println()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
