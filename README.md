# kafka-replay

A high-performance CLI tool for replaying Kafka messages from one topic to another. Built primarily for replaying Dead Letter Topic (DLT) messages back to their original topic after a bug fix, but works for any topic-to-topic replay scenario.

## Why

When a Kafka consumer fails to process messages, they typically end up in a Dead Letter Topic. Once the root cause is fixed, you need to replay those messages back. Existing approaches (Kafka CLI tools, custom scripts) don't scale — they're single-threaded, lack resumability, and provide no filtering or rate limiting.

`kafka-replay` solves this with partition-parallel consumption, batched production, checkpoint-based resumability, and configurable rate limiting — handling millions of messages efficiently.

## Quick Start

```bash
# Build
make build

# Inspect messages in a DLT before replaying
./bin/kafka-replay inspect \
  --brokers localhost:9092 \
  --topic orders.DLT \
  --limit 10

# Replay all DLT messages to the original topic
./bin/kafka-replay replay \
  --source-brokers localhost:9092 \
  --source-topic orders.DLT \
  --target-brokers localhost:9092 \
  --target-topic orders

# Replay with filters, rate limiting, and checkpointing
./bin/kafka-replay replay \
  --source-topic orders.DLT \
  --target-topic orders \
  --from-timestamp "2025-03-14T00:00:00Z" \
  --to-timestamp "2025-03-15T00:00:00Z" \
  --rate-limit 5000 \
  --checkpoint-dir /tmp/replay-checkpoints

# Preview what would be replayed (no messages produced)
./bin/kafka-replay replay \
  --source-topic orders.DLT \
  --target-topic orders \
  --from-offset 100 \
  --to-offset 5000 \
  --dry-run
```

## Architecture

### High-Level Pipeline

```
Source Topic (e.g., orders.DLT)
┌──────────────┐
│ Partition 0   │──┐
│ Partition 1   │──┤
│ Partition 2   │──┤
│ ...           │──┤
│ Partition N   │──┘
└──────────────┘
        │
        │  One goroutine per partition (partition-parallel)
        │
        ▼
┌────────────────────────────────────────────────────────┐
│              Per-Partition Pipeline                    │
│                                                        │
│  ┌─────────┐   ┌────────┐   ┌──────────┐               │
│  │ Consumer│──▶│ Filter │──▶│ Rate     │──▶ Producer (async, batched)
│  │ (seek)  │   │ Chain  │   │ Limiter  │               │
│  └─────────┘   └────────┘   └──────────┘               │
│       │                                    │           │
│       │         Checkpoint ◀──── on success │          │
│       │                                                │
└────────────────────────────────────────────────────────┘
        │
        ▼
Target Topic (e.g., orders)
┌──────────────┐
│ Partition ... │
└──────────────┘
```

### Design Decisions

**Partition-parallel, not message-parallel.** Each source partition gets its own goroutine running the full consume-filter-produce chain. This preserves per-partition ordering (Kafka's ordering guarantee) while scaling linearly with partition count. Message-level parallelism would break ordering and is intentionally avoided.

**Messages are opaque bytes.** The tool never deserializes keys, values, or headers. This means it works with any serialization format (Avro, Protobuf, JSON, binary) and preserves messages exactly as they were produced, including all headers (retry counts, error reasons, trace IDs).

**Finite operation, not streaming.** The consumer resolves start and end offsets before consuming and exits when it reaches the end. This is a batch replay tool, not a streaming bridge.

**At-least-once by default, exactly-once optional.** The default mode uses idempotent production with file-based checkpoints. On crash recovery, duplicates are limited to the last un-checkpointed batch. For stricter guarantees, `--exactly-once` wraps production in Kafka transactions at ~10-20% throughput cost.

## How It Works — Detailed

### 1. Offset Resolution

Before any messages are consumed, the pipeline resolves the exact start and end offsets for every partition of the source topic.

```
ResolvePartitionBounds(ctx, ConsumerConfig)
    │
    ├── ListTopics() → get partition IDs
    │
    ├── resolveStartOffsets()
    │     ├── If --from-timestamp: ListOffsetsAfterMilli(timestamp) → offset
    │     ├── If --from-offset: use as lower bound
    │     ├── If both: max(timestamp_offset, from_offset) per partition
    │     └── If neither: ListStartOffsets() → beginning of topic
    │
    ├── resolveEndOffsets()
    │     ├── If --to-timestamp: ListOffsetsAfterMilli(timestamp) → offset
    │     ├── If --to-offset: use as upper bound
    │     ├── If both: min(timestamp_offset, to_offset) per partition
    │     └── If neither: ListEndOffsets() → current high watermark
    │
    └── Filter out empty partitions (start >= end)
```

This ensures the consumer knows exactly how many messages to process per partition and can exit cleanly without polling forever.

### 2. Checkpoint Resume

If a checkpoint directory is configured and a prior checkpoint exists:

```
For each partition:
    checkpoint_offset = checkpoint.Partitions[partition].LastProducedOffset
    resume_offset = checkpoint_offset + 1
    if resume_offset > resolved_start_offset:
        start_offset = resume_offset  // skip already-replayed messages
```

If `start_offset >= end_offset` after checkpoint adjustment, the partition is skipped entirely — instant completion for fully-replayed partitions.

### 3. Per-Partition Consumption Loop

Each partition goroutine runs this loop:

```go
for {
    // 1. Check for cancellation (SIGINT/SIGTERM)
    if ctx.Done(): return nil

    // 2. Fetch batch of records from source
    fetches := consumer.PollFetches(ctx)

    // 3. Process each record
    for record in fetches:
        // Track last seen offset for termination detection
        lastSeenOffset = record.Offset

        // Stop if past end offset
        if record.Offset >= endOffset: break

        stats.Consumed++

        // Apply filter chain (AND logic)
        if !filter.Match(record):
            stats.Filtered++
            continue

        // Rate limit (blocks until token available)
        if limiter != nil: limiter.Wait(ctx)

        // Produce to target (or log if dry-run)
        if dryRun:
            log record details
            stats.Produced++
        else:
            producer.ProduceAsync(record, callback)
            // callback increments stats.Produced or stats.Errors
            // callback updates checkpoint on success

    // 4. Exit if all records consumed
    if lastSeenOffset >= endOffset - 1: break
}
```

### 4. Producer Internals

The producer wraps a franz-go `kgo.Client` configured for high throughput:

| Setting | Value | Why |
|---|---|---|
| `ProducerBatchMaxBytes` | 1 MB | Amortize per-produce overhead |
| `ProducerLinger` | 5 ms | Allow time for batching without adding significant latency |
| `RequiredAcks` | AllISRAcks | Wait for all in-sync replicas for durability |
| `RecordRetries` | 3 | Automatic retry on transient broker errors |
| `MaxBufferedRecords` | BatchSize * 2 | Backpressure when target is slow |

When producing, the original record's **key, value, headers, and timestamp** are preserved. Only the **topic** is overridden to the target topic. This means:
- Key-based partition assignment works correctly in the target
- Headers (retry counts, error metadata, trace IDs) are preserved
- Timestamps reflect when the original message was produced

### 5. Filter Chain

Filters are composable with AND logic. A record must pass **all** filters to be replayed:

```
Chain(OffsetRange AND TimestampRange)
```

**OffsetRange** `[From, To)`:
- `From` (inclusive): skip records before this offset
- `To` (exclusive): skip records at or after this offset
- Either bound can be nil (unbounded)

**TimestampRange** `[From, To)`:
- `From` (inclusive): skip records with timestamp before this
- `To` (exclusive): skip records with timestamp at or after this
- Uses the record's Kafka timestamp (usually producer timestamp)

Filters are stateless and zero-allocation on the hot path.

### 6. Checkpointing

Checkpoints are JSON files stored atomically (write to `.tmp`, then rename):

```json
{
  "source_topic": "orders.DLT",
  "target_topic": "orders",
  "session_id": "",
  "partitions": {
    "0": {
      "last_produced_offset": 4999,
      "records_produced": 5000,
      "records_filtered": 0
    },
    "1": {
      "last_produced_offset": 3200,
      "records_produced": 3201,
      "records_filtered": 42
    }
  },
  "updated_at": "2025-03-15T10:30:00Z"
}
```

The file path is `{checkpoint-dir}/checkpoint-{source-topic}-{target-topic}.json`. The atomic write strategy (temp file + rename) ensures the checkpoint is never corrupted, even if the process crashes mid-write.

### 7. Rate Limiting

Uses a token bucket algorithm (`golang.org/x/time/rate`):
- Fills at `--rate-limit` tokens per second
- Burst size equals `--batch-size` (allows efficient batch production while maintaining average rate)
- Each partition goroutine calls `limiter.Wait(ctx)` before producing
- The limiter is shared across all partition goroutines, so the total replay rate is bounded

### 8. Graceful Shutdown

On SIGINT or SIGTERM:
1. Context is cancelled
2. Each partition goroutine detects cancellation and returns nil (not an error)
3. Producer flushes all in-flight records
4. Final checkpoint is saved
5. Summary is printed (consumed, produced, filtered, errors, throughput)

## Performance Characteristics

| Optimization | Implementation | Impact |
|---|---|---|
| Partition parallelism | One goroutine per source partition | Linear throughput scaling with partition count |
| Batch producing | franz-go `ProducerBatchMaxBytes` (1MB) + `ProducerLinger` (5ms) | 5-10x throughput vs single-record produce |
| Batch fetching | `FetchMaxBytes` (5MB) per fetch | Fewer network round trips on consume side |
| Zero-copy forwarding | `*kgo.Record` pointers, no byte slice copying | No allocation overhead for large messages |
| Async production | `client.Produce()` with callback | Consumer not blocked waiting for broker acks |
| Idempotent producer | Built into franz-go | Safe retries without duplicates |

## CLI Reference

### `kafka-replay replay`

Replay messages from a source topic to a target topic.

```
Flags:
  Source:
    --source-brokers strings   Source Kafka broker addresses (default [localhost:9092])
    --source-topic string      Source topic to replay from (required)

  Target:
    --target-brokers strings   Target Kafka broker addresses (defaults to source-brokers)
    --target-topic string      Target topic to replay to (required unless --dry-run)

  Filters:
    --from-offset int          Start from this offset, inclusive (-1 = beginning, default -1)
    --to-offset int            Stop at this offset, exclusive (-1 = end, default -1)
    --from-timestamp string    Start from this timestamp (RFC3339, e.g., 2025-01-01T00:00:00Z)
    --to-timestamp string      Stop at this timestamp (RFC3339)

  Performance:
    --batch-size int           Producer batch size (default 500)
    --buffer-size int          Per-partition buffer size for backpressure (default 1000)
    --rate-limit int           Max messages per second, 0 = unlimited (default 0)

  Behavior:
    --dry-run                  Preview replay without producing messages
    --exactly-once             Enable exactly-once semantics via Kafka transactions
    --verbose                  Enable debug logging

  Checkpoint:
    --checkpoint-dir string    Directory for checkpoint files (empty = no checkpointing)
```

### `kafka-replay inspect`

Preview messages in a topic without replaying.

```
Flags:
    --brokers strings          Kafka broker addresses (default [localhost:9092])
    --topic string             Topic to inspect (required)
    --from-timestamp string    Start from this timestamp (RFC3339)
    --to-timestamp string      Stop at this timestamp (RFC3339)
    --limit int                Maximum number of messages to display (default 10)
    --verbose                  Show full message values
```

### `kafka-replay version`

Print version, git commit, and build date.

## Project Structure

```
kafka-replay/
├── main.go                              # Entry point
├── go.mod / go.sum                      # Go module and dependencies
├── Makefile                             # Build, test, bench, lint targets
├── cmd/
│   ├── root.go                          # Root Cobra command
│   ├── replay.go                        # Replay command (flag parsing, signal handling, summary)
│   ├── inspect.go                       # Inspect command (preview messages)
│   └── version.go                       # Version command
├── internal/
│   ├── config/
│   │   ├── config.go                    # Config and InspectConfig structs, validation
│   │   └── config_test.go               # 12 unit tests
│   ├── pipeline/
│   │   ├── pipeline.go                  # Pipeline orchestrator (partition goroutines, stats)
│   │   ├── consumer.go                  # Partition consumer, offset/timestamp resolution
│   │   ├── producer.go                  # Batched idempotent producer with async callbacks
│   │   └── pipeline_integration_test.go # 5 integration tests (EndToEnd, OffsetFilter, DryRun, Checkpoint, RateLimit)
│   ├── filter/
│   │   ├── filter.go                    # Filter interface and AND-composed Chain
│   │   ├── offset.go                    # Offset range filter [From, To)
│   │   ├── timestamp.go                 # Timestamp range filter [From, To)
│   │   └── filter_test.go              # 8 unit tests
│   ├── checkpoint/
│   │   ├── store.go                     # Store interface (Load, Update, Save)
│   │   ├── file.go                      # Atomic JSON file store (temp + rename)
│   │   └── store_test.go               # 4 unit tests
│   ├── ratelimit/
│   │   ├── limiter.go                   # Token bucket rate limiter
│   │   └── limiter_test.go             # 4 unit tests
│   ├── progress/
│   │   └── tracker.go                   # Per-partition progress tracking with atomic counters
│   └── metrics/
│       ├── metrics.go                   # Prometheus counter/histogram definitions
│       └── server.go                    # Optional /metrics HTTP endpoint
├── testutil/
│   ├── kafka.go                         # testcontainers-go Kafka helpers (StartKafka, CreateTopic, ProduceMessages, etc.)
│   └── records.go                       # Test record factories
└── benchmarks/
    └── replay_bench_test.go             # Performance benchmarks (10K/100K/1M, partition scaling, message sizes)
```

## Dependencies

| Package | Purpose |
|---|---|
| [franz-go](https://github.com/twmb/franz-go) | Kafka client — pure Go, no CGO, 2x faster than confluent-kafka-go |
| [franz-go/pkg/kadm](https://github.com/twmb/franz-go) | Kafka admin operations (topic metadata, offset resolution) |
| [cobra](https://github.com/spf13/cobra) | CLI framework |
| [golang.org/x/time/rate](https://pkg.go.dev/golang.org/x/time/rate) | Token bucket rate limiting |
| [prometheus/client_golang](https://github.com/prometheus/client_golang) | Prometheus metrics |
| [testcontainers-go](https://github.com/testcontainers/testcontainers-go) | Integration tests with real Kafka |

## Testing

```bash
# Unit tests (no Docker required)
make test

# Integration tests (requires Docker)
make test-integration

# Performance benchmarks (requires Docker)
make bench
```

### Integration Tests

| Test | What it verifies |
|---|---|
| `TestReplay_EndToEnd` | 100 messages across 3 partitions: all consumed, produced, headers preserved |
| `TestReplay_WithOffsetFilter` | Only offsets [10, 20) replayed from 100 messages → exactly 10 produced |
| `TestReplay_DryRun` | All 50 messages "replayed" in dry-run mode, target topic remains empty |
| `TestReplay_WithCheckpoint` | First run produces 100 messages, second run skips all (instant) |
| `TestReplay_WithRateLimit` | 50 messages at 100 msgs/sec, verifies rate-limited throughput |

### Benchmarks

| Benchmark | Measures |
|---|---|
| `BenchmarkReplay_10K/100K/1M` | Throughput (msgs/sec, MB/sec) at different volumes |
| `BenchmarkReplay_Partitions1/4/16/32` | Throughput scaling with partition count |
| `BenchmarkReplay_100B/1KB/10KB/100KB` | Throughput for different message sizes |
| `BenchmarkReplay_WithRateLimiter` | Rate limiter accuracy under load |

## Building

```bash
# Build binary
make build

# Build with version info
VERSION=v1.0.0 make build

# Cross-compile
GOOS=linux GOARCH=amd64 make build
```
