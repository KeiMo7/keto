// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Batch write observability. Labels contain only stable outcome/kind words;
// never namespace, object, subject or idempotency key values.
var (
	batchWriteSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "keto_relationtuple_batch_write_items",
		Help:    "Distribution of accepted batch write sizes (number of request items).",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000},
	})

	batchWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "keto_relationtuple_batch_write_duration_seconds",
		Help:    "Wall-clock duration of batch write processing, including the transaction.",
		Buckets: prometheus.DefBuckets,
	})

	batchWriteOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keto_relationtuple_batch_write_outcomes_total",
		Help: "Per-item batch outcomes, labelled by stable outcome.",
	}, []string{"outcome"})

	batchWriteConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keto_relationtuple_batch_write_conflicts_total",
		Help: "Batch writes rejected with a distinguishable conflict, labelled by kind.",
	}, []string{"kind"})

	batchWriteReplays = promauto.NewCounter(prometheus.CounterOpts{
		Name: "keto_relationtuple_batch_write_idempotent_replays_total",
		Help: "Batch writes served from a stored idempotency record without writing.",
	})
)

// RecordBatchWriteMetrics records size, outcome counts and duration for one
// processed request. It must be called exactly once per inbound batch.
func RecordBatchWriteMetrics(size int, results []ItemResult, replayed bool, conflictKind BatchErrorKind, duration time.Duration) {
	if size >= 0 {
		batchWriteSize.Observe(float64(size))
	}
	batchWriteDuration.Observe(duration.Seconds())

	if conflictKind != "" {
		batchWriteConflicts.WithLabelValues(string(conflictKind)).Inc()
	}
	for i := range results {
		batchWriteOutcomes.WithLabelValues(string(results[i].Outcome)).Inc()
	}
	if replayed {
		batchWriteReplays.Inc()
	}
}

// BatchOutcomeCounter exposes the outcome counter for tests/assertions.
func BatchOutcomeCounter() *prometheus.CounterVec { return batchWriteOutcomes }

// BatchConflictCounter exposes the conflict counter for tests/assertions.
func BatchConflictCounter() *prometheus.CounterVec { return batchWriteConflicts }
