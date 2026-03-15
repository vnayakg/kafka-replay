package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RecordsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_replay_records_consumed_total",
		Help: "Total number of records consumed from the source topic",
	})

	RecordsProduced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_replay_records_produced_total",
		Help: "Total number of records produced to the target topic",
	})

	RecordsFiltered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_replay_records_filtered_total",
		Help: "Total number of records filtered out",
	})

	ProduceErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_replay_produce_errors_total",
		Help: "Total number of produce errors",
	})

	ProduceLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_replay_produce_latency_seconds",
		Help:    "Histogram of produce latencies",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
)
