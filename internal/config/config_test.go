package config

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BatchSize != 500 {
		t.Errorf("expected BatchSize=500, got %d", cfg.BatchSize)
	}
	if cfg.BufferSize != 1000 {
		t.Errorf("expected BufferSize=1000, got %d", cfg.BufferSize)
	}
	if cfg.CheckpointInterval != 5*time.Second {
		t.Errorf("expected CheckpointInterval=5s, got %v", cfg.CheckpointInterval)
	}
}

func TestValidate_MissingSourceTopic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "target"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing source topic")
	}
}

func TestValidate_MissingSourceBrokers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceTopic = "source"
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "target"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing source brokers")
	}
}

func TestValidate_MissingTargetTopic(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "source"
	cfg.TargetBrokers = []string{"localhost:9092"}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing target topic")
	}
}

func TestValidate_DryRunNoTarget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "source"
	cfg.DryRun = true

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("dry-run should not require target topic, got: %v", err)
	}
}

func TestValidate_SameTopicSameBrokers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "topic"
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "topic"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for same source and target topic on same brokers")
	}
}

func TestValidate_SameTopicDifferentBrokers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"broker1:9092"}
	cfg.SourceTopic = "topic"
	cfg.TargetBrokers = []string{"broker2:9092"}
	cfg.TargetTopic = "topic"

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("same topic on different brokers should be valid, got: %v", err)
	}
}

func TestValidate_InvalidOffsetRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "source"
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "target"

	from := int64(100)
	to := int64(50)
	cfg.FromOffset = &from
	cfg.ToOffset = &to

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for from-offset > to-offset")
	}
}

func TestValidate_InvalidTimestampRange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "source"
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "target"

	from := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg.FromTime = &from
	cfg.ToTime = &to

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for from-timestamp after to-timestamp")
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "source"
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "target"

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
}

func TestValidate_InvalidBatchSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceBrokers = []string{"localhost:9092"}
	cfg.SourceTopic = "source"
	cfg.TargetBrokers = []string{"localhost:9092"}
	cfg.TargetTopic = "target"
	cfg.BatchSize = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for zero batch size")
	}
}

func TestInspectConfig_Validate(t *testing.T) {
	cfg := &InspectConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
		Limit:   10,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}

	cfg.Topic = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing topic")
	}
}
