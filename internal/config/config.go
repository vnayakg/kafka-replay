package config

import (
	"fmt"
	"time"
)

// Config holds the complete configuration for a replay operation.
type Config struct {
	// Source configuration
	SourceBrokers []string
	SourceTopic   string

	// Target configuration
	TargetBrokers []string
	TargetTopic   string

	// Filter configuration
	FromOffset *int64
	ToOffset   *int64
	FromTime   *time.Time
	ToTime     *time.Time

	// Performance configuration
	BatchSize  int
	BufferSize int
	RateLimit  int // messages per second, 0 = unlimited

	// Behavior configuration
	DryRun      bool
	ExactlyOnce bool
	Verbose     bool

	// Checkpoint configuration
	CheckpointDir      string
	CheckpointInterval time.Duration

	// Metrics configuration
	MetricsPort int // 0 = disabled

	// Producer tuning (zero values use defaults in producer)
	ProducerBatchMaxBytes    int32
	ProducerLinger           time.Duration
	ProducerRetries          int
	ProducerRequireAllAcks   *bool
	ProducerMaxBufferedScale int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		BatchSize:          500,
		BufferSize:         1000,
		CheckpointInterval: 5 * time.Second,
	}
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.SourceTopic == "" {
		return fmt.Errorf("source topic is required")
	}
	if len(c.SourceBrokers) == 0 {
		return fmt.Errorf("source brokers are required")
	}
	if c.TargetTopic == "" && !c.DryRun {
		return fmt.Errorf("target topic is required (unless --dry-run)")
	}
	if !c.DryRun && len(c.TargetBrokers) == 0 {
		return fmt.Errorf("target brokers are required (unless --dry-run)")
	}
	if c.SourceTopic == c.TargetTopic && sameBrokers(c.SourceBrokers, c.TargetBrokers) {
		return fmt.Errorf("source and target topic must differ when using the same brokers")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if c.BufferSize <= 0 {
		return fmt.Errorf("buffer size must be positive")
	}
	if c.RateLimit < 0 {
		return fmt.Errorf("rate limit must be non-negative")
	}
	if c.FromOffset != nil && c.ToOffset != nil && *c.FromOffset > *c.ToOffset {
		return fmt.Errorf("from-offset must be <= to-offset")
	}
	if c.FromTime != nil && c.ToTime != nil && c.FromTime.After(*c.ToTime) {
		return fmt.Errorf("from-timestamp must be before to-timestamp")
	}
	return nil
}

// InspectConfig holds configuration for the inspect command.
type InspectConfig struct {
	Brokers  []string
	Topic    string
	FromTime *time.Time
	ToTime   *time.Time
	Limit    int
	Verbose  bool
}

// ValidateInspect checks the inspect configuration for errors.
func (c *InspectConfig) Validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers are required")
	}
	if c.Limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}
	return nil
}

func sameBrokers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, v := range a {
		set[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}
