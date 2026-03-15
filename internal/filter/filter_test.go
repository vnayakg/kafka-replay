package filter

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestOffsetRange_Match(t *testing.T) {
	from := int64(10)
	to := int64(20)

	tests := []struct {
		name   string
		from   *int64
		to     *int64
		offset int64
		want   bool
	}{
		{"in range", &from, &to, 15, true},
		{"at start", &from, &to, 10, true},
		{"at end (exclusive)", &from, &to, 20, false},
		{"before range", &from, &to, 5, false},
		{"after range", &from, &to, 25, false},
		{"no lower bound", nil, &to, 5, true},
		{"no upper bound", &from, nil, 100, true},
		{"nil filter", nil, nil, 100, true}, // NewOffsetRange returns nil
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewOffsetRange(tt.from, tt.to)
			if f == nil {
				// nil filter means match all
				if !tt.want {
					t.Error("expected nil filter to match")
				}
				return
			}
			record := &kgo.Record{Offset: tt.offset}
			got := f.Match(record)
			if got != tt.want {
				t.Errorf("OffsetRange.Match(offset=%d) = %v, want %v", tt.offset, got, tt.want)
			}
		})
	}
}

func TestTimestampRange_Match(t *testing.T) {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		from *time.Time
		to   *time.Time
		ts   time.Time
		want bool
	}{
		{"in range", &from, &to, time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), true},
		{"at start", &from, &to, from, true},
		{"at end (exclusive)", &from, &to, to, false},
		{"before range", &from, &to, time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), false},
		{"after range", &from, &to, time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC), false},
		{"no lower bound", nil, &to, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), true},
		{"no upper bound", &from, nil, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTimestampRange(tt.from, tt.to)
			if f == nil {
				if !tt.want {
					t.Error("expected nil filter to match")
				}
				return
			}
			record := &kgo.Record{Timestamp: tt.ts}
			got := f.Match(record)
			if got != tt.want {
				t.Errorf("TimestampRange.Match(ts=%v) = %v, want %v", tt.ts, got, tt.want)
			}
		})
	}
}

func TestChain_Match(t *testing.T) {
	from := int64(10)
	to := int64(100)
	fromTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	toTime := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	chain := NewChain(
		NewOffsetRange(&from, &to),
		NewTimestampRange(&fromTime, &toTime),
	)

	tests := []struct {
		name   string
		offset int64
		ts     time.Time
		want   bool
	}{
		{"both match", 50, time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), true},
		{"offset out of range", 5, time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), false},
		{"timestamp out of range", 50, time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"both out of range", 5, time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &kgo.Record{Offset: tt.offset, Timestamp: tt.ts}
			got := chain.Match(record)
			if got != tt.want {
				t.Errorf("Chain.Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewChain_NilFilters(t *testing.T) {
	// All nil filters should return nil chain
	chain := NewChain(nil, nil)
	if chain != nil {
		t.Error("expected nil chain for all nil filters")
	}
}

func TestNewChain_SingleFilter(t *testing.T) {
	from := int64(10)
	chain := NewChain(NewOffsetRange(&from, nil), nil)
	// Single filter should be returned directly (not wrapped in Chain)
	if _, ok := chain.(*Chain); ok {
		t.Error("single filter should not be wrapped in Chain")
	}
}

func TestOffsetRange_String(t *testing.T) {
	from := int64(10)
	to := int64(20)
	f := NewOffsetRange(&from, &to)
	s := f.String()
	if s != "Offset[10, 20)" {
		t.Errorf("unexpected String(): %s", s)
	}
}

func TestTimestampRange_String(t *testing.T) {
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewTimestampRange(&from, nil)
	s := f.String()
	expected := "Timestamp[2025-01-01T00:00:00Z, end)"
	if s != expected {
		t.Errorf("unexpected String(): %s, expected: %s", s, expected)
	}
}
