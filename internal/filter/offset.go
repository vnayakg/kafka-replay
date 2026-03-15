package filter

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// OffsetRange filters records by their offset within a [From, To) range.
type OffsetRange struct {
	From *int64 // inclusive, nil means no lower bound
	To   *int64 // exclusive, nil means no upper bound
}

// NewOffsetRange creates an offset range filter.
func NewOffsetRange(from, to *int64) Filter {
	if from == nil && to == nil {
		return nil
	}
	return &OffsetRange{From: from, To: to}
}

func (f *OffsetRange) Match(record *kgo.Record) bool {
	if f.From != nil && record.Offset < *f.From {
		return false
	}
	if f.To != nil && record.Offset >= *f.To {
		return false
	}
	return true
}

func (f *OffsetRange) String() string {
	from := "start"
	to := "end"
	if f.From != nil {
		from = fmt.Sprintf("%d", *f.From)
	}
	if f.To != nil {
		to = fmt.Sprintf("%d", *f.To)
	}
	return fmt.Sprintf("Offset[%s, %s)", from, to)
}
