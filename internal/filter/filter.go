package filter

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// Filter determines whether a record should be included in the replay.
type Filter interface {
	Match(record *kgo.Record) bool
	String() string
}

// Chain applies multiple filters with AND logic.
// A record must pass all filters to be included.
type Chain struct {
	filters []Filter
}

// NewChain creates a filter chain from the given filters.
// Nil filters are ignored. If no filters remain, returns nil (match all).
func NewChain(filters ...Filter) Filter {
	var valid []Filter
	for _, f := range filters {
		if f != nil {
			valid = append(valid, f)
		}
	}
	if len(valid) == 0 {
		return nil
	}
	if len(valid) == 1 {
		return valid[0]
	}
	return &Chain{filters: valid}
}

// Match returns true if the record passes all filters in the chain.
func (c *Chain) Match(record *kgo.Record) bool {
	for _, f := range c.filters {
		if !f.Match(record) {
			return false
		}
	}
	return true
}

func (c *Chain) String() string {
	s := "Chain("
	for i, f := range c.filters {
		if i > 0 {
			s += " AND "
		}
		s += f.String()
	}
	return s + ")"
}
