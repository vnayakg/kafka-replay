package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStore persists checkpoint state to a JSON file with atomic writes.
type FileStore struct {
	dir         string
	sourceTopic string
	targetTopic string
	mu          sync.Mutex
	checkpoint  *Checkpoint
}

// NewFileStore creates a FileStore. If dir is empty, checkpointing is disabled.
func NewFileStore(dir, sourceTopic, targetTopic string) (*FileStore, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	return &FileStore{
		dir:         dir,
		sourceTopic: sourceTopic,
		targetTopic: targetTopic,
		checkpoint: &Checkpoint{
			SourceTopic: sourceTopic,
			TargetTopic: targetTopic,
			Partitions:  make(map[int32]PartitionCheckpoint),
		},
	}, nil
}

func (s *FileStore) filename() string {
	return filepath.Join(s.dir, fmt.Sprintf("checkpoint-%s-%s.json", s.sourceTopic, s.targetTopic))
}

// Load reads the checkpoint from disk.
func (s *FileStore) Load() (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.filename())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	s.checkpoint = &cp
	return &cp, nil
}

// Update records a successfully produced offset for a partition.
func (s *FileStore) Update(partition int32, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pcp := s.checkpoint.Partitions[partition]
	if offset > pcp.LastProducedOffset {
		pcp.LastProducedOffset = offset
	}
	pcp.RecordsProduced++
	s.checkpoint.Partitions[partition] = pcp
	s.checkpoint.UpdatedAt = time.Now()
}

// Save persists the checkpoint atomically (write temp file, then rename).
func (s *FileStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(s.checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	tmpFile := s.filename() + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o644); err != nil {
		return fmt.Errorf("write temp checkpoint: %w", err)
	}

	if err := os.Rename(tmpFile, s.filename()); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}
