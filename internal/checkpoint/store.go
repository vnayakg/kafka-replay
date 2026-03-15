package checkpoint

import (
	"time"
)

// Checkpoint holds the state of a replay session.
type Checkpoint struct {
	SourceTopic string                        `json:"source_topic"`
	TargetTopic string                        `json:"target_topic"`
	SessionID   string                        `json:"session_id"`
	Partitions  map[int32]PartitionCheckpoint `json:"partitions"`
	UpdatedAt   time.Time                     `json:"updated_at"`
}

// PartitionCheckpoint holds the state of a single partition's replay.
type PartitionCheckpoint struct {
	LastProducedOffset int64 `json:"last_produced_offset"`
	RecordsProduced    int64 `json:"records_produced"`
	RecordsFiltered    int64 `json:"records_filtered"`
}

// Store is the interface for checkpoint persistence.
type Store interface {
	// Load returns the most recent checkpoint, or nil if none exists.
	Load() (*Checkpoint, error)

	// Update records a successfully produced offset for a partition.
	Update(partition int32, offset int64)

	// Save persists the current checkpoint state to storage.
	Save() error
}
