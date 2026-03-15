//go:build integration

package testutil

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MakeRecords creates n test records with keys "key-0" through "key-{n-1}".
func MakeRecords(n int, valueSize int) []*kgo.Record {
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte('A' + (i % 26))
	}

	records := make([]*kgo.Record, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		records[i] = &kgo.Record{
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     value,
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Headers: []kgo.RecordHeader{
				{Key: "source", Value: []byte("test")},
				{Key: "index", Value: []byte(fmt.Sprintf("%d", i))},
			},
		}
	}
	return records
}
