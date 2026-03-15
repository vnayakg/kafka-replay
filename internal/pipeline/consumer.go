package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerConfig holds configuration for creating a source consumer.
type ConsumerConfig struct {
	Brokers    []string
	Topic      string
	FromOffset *int64
	ToOffset   *int64
	FromTime   *time.Time
	ToTime     *time.Time
}

// PartitionBounds holds the start and end offsets for a partition.
type PartitionBounds struct {
	Partition   int32
	StartOffset int64
	EndOffset   int64 // exclusive; -1 means consume to high watermark at start time
}

// ResolvePartitionBounds queries the broker for topic metadata and computes
// the start/end offsets for each partition based on the consumer config.
func ResolvePartitionBounds(ctx context.Context, cfg ConsumerConfig) ([]PartitionBounds, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return nil, fmt.Errorf("create admin client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	// Get partition list
	topics, err := adm.ListTopics(ctx, cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	topicDetail, ok := topics[cfg.Topic]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", cfg.Topic)
	}

	partitions := make([]int32, 0, len(topicDetail.Partitions))
	for _, p := range topicDetail.Partitions {
		partitions = append(partitions, p.Partition)
	}

	// Resolve start offsets
	startOffsets, err := resolveStartOffsets(ctx, adm, cfg, partitions)
	if err != nil {
		return nil, fmt.Errorf("resolve start offsets: %w", err)
	}

	// Resolve end offsets
	endOffsets, err := resolveEndOffsets(ctx, adm, cfg, partitions)
	if err != nil {
		return nil, fmt.Errorf("resolve end offsets: %w", err)
	}

	bounds := make([]PartitionBounds, 0, len(partitions))
	for _, p := range partitions {
		start := startOffsets[p]
		end := endOffsets[p]
		if start >= end && end != -1 {
			slog.Debug("skipping empty partition", "partition", p, "start", start, "end", end)
			continue
		}
		bounds = append(bounds, PartitionBounds{
			Partition:   p,
			StartOffset: start,
			EndOffset:   end,
		})
	}

	if len(bounds) == 0 {
		return nil, fmt.Errorf("no messages to replay in topic %q with the given filters", cfg.Topic)
	}

	return bounds, nil
}

func resolveStartOffsets(ctx context.Context, adm *kadm.Client, cfg ConsumerConfig, partitions []int32) (map[int32]int64, error) {
	result := make(map[int32]int64, len(partitions))

	if cfg.FromTime != nil {
		offsets, err := adm.ListOffsetsAfterMilli(ctx, cfg.FromTime.UnixMilli(), cfg.Topic)
		if err != nil {
			return nil, fmt.Errorf("list offsets after timestamp: %w", err)
		}
		offsets.Each(func(o kadm.ListedOffset) {
			if o.Err != nil {
				slog.Warn("error resolving timestamp offset", "partition", o.Partition, "err", o.Err)
				return
			}
			result[o.Partition] = o.Offset
		})
		// Apply FromOffset as a further constraint if both are set
		if cfg.FromOffset != nil {
			for _, p := range partitions {
				if result[p] < *cfg.FromOffset {
					result[p] = *cfg.FromOffset
				}
			}
		}
		return result, nil
	}

	if cfg.FromOffset != nil {
		for _, p := range partitions {
			result[p] = *cfg.FromOffset
		}
		return result, nil
	}

	// Default: start from beginning
	offsets, err := adm.ListStartOffsets(ctx, cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			result[o.Partition] = o.Offset
		}
	})
	return result, nil
}

func resolveEndOffsets(ctx context.Context, adm *kadm.Client, cfg ConsumerConfig, partitions []int32) (map[int32]int64, error) {
	result := make(map[int32]int64, len(partitions))

	if cfg.ToTime != nil {
		offsets, err := adm.ListOffsetsAfterMilli(ctx, cfg.ToTime.UnixMilli(), cfg.Topic)
		if err != nil {
			return nil, fmt.Errorf("list offsets after timestamp: %w", err)
		}
		offsets.Each(func(o kadm.ListedOffset) {
			if o.Err != nil {
				slog.Warn("error resolving timestamp offset", "partition", o.Partition, "err", o.Err)
				return
			}
			result[o.Partition] = o.Offset
		})
		// Apply ToOffset as a further constraint if both are set
		if cfg.ToOffset != nil {
			for _, p := range partitions {
				if v, ok := result[p]; ok && v > *cfg.ToOffset {
					result[p] = *cfg.ToOffset
				}
			}
		}
		return result, nil
	}

	if cfg.ToOffset != nil {
		for _, p := range partitions {
			result[p] = *cfg.ToOffset
		}
		return result, nil
	}

	// Default: consume up to current high watermark
	offsets, err := adm.ListEndOffsets(ctx, cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			result[o.Partition] = o.Offset
		}
	})
	return result, nil
}

// NewPartitionConsumer creates a kgo.Client configured to consume a single
// partition of the given topic starting at startOffset.
func NewPartitionConsumer(brokers []string, topic string, partition int32, startOffset int64) (*kgo.Client, error) {
	offsets := make(map[string]map[int32]kgo.Offset)
	offsets[topic] = map[int32]kgo.Offset{
		partition: kgo.NewOffset().At(startOffset),
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(offsets),
		kgo.FetchMaxBytes(5<<20), // 5MB per fetch for throughput
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create partition consumer for partition %d: %w", partition, err)
	}
	return cl, nil
}
