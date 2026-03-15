package progress

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// PartitionProgress tracks the progress of a single partition's replay.
type PartitionProgress struct {
	Partition int32
	Consumed  atomic.Int64
	Produced  atomic.Int64
	Filtered  atomic.Int64
	Errors    atomic.Int64
	StartOffset int64
	EndOffset   int64
}

// Tracker tracks overall replay progress across partitions.
type Tracker struct {
	mu         sync.RWMutex
	partitions map[int32]*PartitionProgress
	startTime  time.Time
}

// NewTracker creates a new progress tracker.
func NewTracker() *Tracker {
	return &Tracker{
		partitions: make(map[int32]*PartitionProgress),
		startTime:  time.Now(),
	}
}

// AddPartition registers a partition for tracking.
func (t *Tracker) AddPartition(partition int32, startOffset, endOffset int64) *PartitionProgress {
	t.mu.Lock()
	defer t.mu.Unlock()

	pp := &PartitionProgress{
		Partition:   partition,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	t.partitions[partition] = pp
	return pp
}

// GetPartition returns the progress for a partition.
func (t *Tracker) GetPartition(partition int32) *PartitionProgress {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.partitions[partition]
}

// TotalConsumed returns the total number of consumed records across all partitions.
func (t *Tracker) TotalConsumed() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var total int64
	for _, pp := range t.partitions {
		total += pp.Consumed.Load()
	}
	return total
}

// TotalProduced returns the total number of produced records across all partitions.
func (t *Tracker) TotalProduced() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var total int64
	for _, pp := range t.partitions {
		total += pp.Produced.Load()
	}
	return total
}

// PrintSummary writes a summary of the replay progress.
func (t *Tracker) PrintSummary(w io.Writer) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	elapsed := time.Since(t.startTime)
	var totalConsumed, totalProduced, totalFiltered, totalErrors int64

	for _, pp := range t.partitions {
		totalConsumed += pp.Consumed.Load()
		totalProduced += pp.Produced.Load()
		totalFiltered += pp.Filtered.Load()
		totalErrors += pp.Errors.Load()
	}

	_, _ = fmt.Fprintf(w, "\n--- Replay Summary ---\n")
	_, _ = fmt.Fprintf(w, "Duration:   %s\n", elapsed.Round(time.Millisecond))
	_, _ = fmt.Fprintf(w, "Consumed:   %d\n", totalConsumed)
	_, _ = fmt.Fprintf(w, "Produced:   %d\n", totalProduced)
	_, _ = fmt.Fprintf(w, "Filtered:   %d\n", totalFiltered)
	_, _ = fmt.Fprintf(w, "Errors:     %d\n", totalErrors)
	_, _ = fmt.Fprintf(w, "Partitions: %d\n", len(t.partitions))
	if elapsed.Seconds() > 0 {
		throughput := float64(totalProduced) / elapsed.Seconds()
		_, _ = fmt.Fprintf(w, "Throughput: %.0f msgs/sec\n", throughput)
	}
}
