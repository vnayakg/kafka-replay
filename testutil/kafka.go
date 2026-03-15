//go:build integration

package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaContainer holds a running Kafka testcontainer.
type KafkaContainer struct {
	Container *kafka.KafkaContainer
	Brokers   []string
}

// StartKafka starts a Kafka container for testing.
func StartKafka(t *testing.T) *KafkaContainer {
	t.Helper()

	ctx := context.Background()
	container, err := kafka.Run(ctx, "confluentinc/confluent-local:7.6.0")
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("get brokers: %v", err)
	}

	t.Cleanup(func() {
		container.Terminate(context.Background())
	})

	return &KafkaContainer{
		Container: container,
		Brokers:   brokers,
	}
}

// CreateTopic creates a topic with the given number of partitions.
// Retries on transient connection errors (Kafka may not be fully ready).
func (kc *KafkaContainer) CreateTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		cl, err := kgo.NewClient(kgo.SeedBrokers(kc.Brokers...))
		if err != nil {
			lastErr = err
			continue
		}

		adm := kadm.NewClient(cl)
		resp, err := adm.CreateTopics(context.Background(), partitions, 1, nil, topic)
		cl.Close()
		if err != nil {
			lastErr = err
			continue
		}

		success := true
		for _, r := range resp.Sorted() {
			if r.Err != nil {
				lastErr = fmt.Errorf("create topic %s: %v", r.Topic, r.Err)
				success = false
				break
			}
		}
		if success {
			time.Sleep(500 * time.Millisecond)
			return
		}
	}
	t.Fatalf("create topic %s after retries: %v", topic, lastErr)
}

// ProduceMessages produces n messages to the given topic.
// Messages have keys like "key-0", "key-1", etc. and values of the given size.
func (kc *KafkaContainer) ProduceMessages(t *testing.T, topic string, n int, valueSize int) {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kc.Brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer cl.Close()

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte('A' + (i % 26))
	}

	ctx := context.Background()
	for i := 0; i < n; i++ {
		record := &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "source", Value: []byte("test")},
			},
		}
		cl.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce error: %v", err)
			}
		})
	}

	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// CountMessages counts all messages in a topic.
func (kc *KafkaContainer) CountMessages(t *testing.T, topic string) int64 {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(kc.Brokers...))
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	startOffsets, err := adm.ListStartOffsets(context.Background(), topic)
	if err != nil {
		t.Fatalf("list start offsets: %v", err)
	}
	endOffsets, err := adm.ListEndOffsets(context.Background(), topic)
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}

	var count int64
	endOffsets.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			start := int64(0)
			startOffsets.Each(func(so kadm.ListedOffset) {
				if so.Partition == o.Partition && so.Err == nil {
					start = so.Offset
				}
			})
			count += o.Offset - start
		}
	})

	return count
}

// ConsumeAllMessages reads all messages from a topic and returns them.
func (kc *KafkaContainer) ConsumeAllMessages(t *testing.T, topic string, expected int) []*kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kc.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxWait(time.Second),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) < expected {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
		if ctx.Err() != nil {
			break
		}
	}

	return records
}
