package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStore_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()

	store, err := NewFileStore(dir, "source-topic", "target-topic")
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	// Update some offsets
	store.Update(0, 100)
	store.Update(0, 200)
	store.Update(1, 50)

	// Save
	if err := store.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify file exists
	filename := filepath.Join(dir, "checkpoint-source-topic-target-topic.json")
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatal("checkpoint file should exist after save")
	}

	// Load in a new store
	store2, err := NewFileStore(dir, "source-topic", "target-topic")
	if err != nil {
		t.Fatalf("NewFileStore2: %v", err)
	}

	cp, err := store2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint, got nil")
	}

	// Verify partition 0
	p0, ok := cp.Partitions[0]
	if !ok {
		t.Fatal("expected partition 0 in checkpoint")
	}
	if p0.LastProducedOffset != 200 {
		t.Errorf("expected LastProducedOffset=200, got %d", p0.LastProducedOffset)
	}
	if p0.RecordsProduced != 2 {
		t.Errorf("expected RecordsProduced=2, got %d", p0.RecordsProduced)
	}

	// Verify partition 1
	p1, ok := cp.Partitions[1]
	if !ok {
		t.Fatal("expected partition 1 in checkpoint")
	}
	if p1.LastProducedOffset != 50 {
		t.Errorf("expected LastProducedOffset=50, got %d", p1.LastProducedOffset)
	}
}

func TestFileStore_LoadNonExistent(t *testing.T) {
	dir := t.TempDir()

	store, err := NewFileStore(dir, "source", "target")
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	cp, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp != nil {
		t.Error("expected nil checkpoint for non-existent file")
	}
}

func TestNewFileStore_EmptyDir(t *testing.T) {
	store, err := NewFileStore("", "source", "target")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store != nil {
		t.Error("expected nil store for empty dir")
	}
}

func TestFileStore_AtomicWrite(t *testing.T) {
	dir := t.TempDir()

	store, err := NewFileStore(dir, "source", "target")
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	store.Update(0, 42)
	if err := store.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify no temp file left behind
	tmpFile := filepath.Join(dir, "checkpoint-source-target.json.tmp")
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error("temp file should not exist after save")
	}
}
