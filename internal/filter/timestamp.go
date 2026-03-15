package filter

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TimestampRange filters records by their timestamp within a [From, To) range.
type TimestampRange struct {
	From *time.Time // inclusive, nil means no lower bound
	To   *time.Time // exclusive, nil means no upper bound
}

// NewTimestampRange creates a timestamp range filter.
func NewTimestampRange(from, to *time.Time) Filter {
	if from == nil && to == nil {
		return nil
	}
	return &TimestampRange{From: from, To: to}
}

func (f *TimestampRange) Match(record *kgo.Record) bool {
	if f.From != nil && record.Timestamp.Before(*f.From) {
		return false
	}
	if f.To != nil && !record.Timestamp.Before(*f.To) {
		return false
	}
	return true
}

func (f *TimestampRange) String() string {
	from := "start"
	to := "end"
	if f.From != nil {
		from = f.From.Format(time.RFC3339)
	}
	if f.To != nil {
		to = f.To.Format(time.RFC3339)
	}
	return fmt.Sprintf("Timestamp[%s, %s)", from, to)
}
