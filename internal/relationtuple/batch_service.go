// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ory/x/logrusx"
	"go.opentelemetry.io/otel/trace"

	"github.com/ory/keto/internal/driver/config"
	"github.com/ory/keto/ketoapi"
	"github.com/ory/keto/x/events"
)

// IdempotencyKeyHeader is the standard HTTP header carrying a request-level
// idempotency key. X-Idempotency-Key is accepted as an alias.
const (
	IdempotencyKeyHeader       = "Idempotency-Key"
	IdempotencyKeyHeaderAlt    = "X-Idempotency-Key"
	DetailedResponseQuery      = "detailed"
	DetailedResponsePreference = "return=representation"
)

// BatchServiceDeps are the dependencies required by BatchWriteService.
type BatchServiceDeps interface {
	ManagerProvider
	MapperProvider
	config.Provider
	logrusx.Provider
}

// BatchWriteRequest is the entry-neutral batch request.
type BatchWriteRequest struct {
	Inputs         []BatchInput
	IdempotencyKey string
}

// BatchWriteResponse is the entry-neutral batch outcome.
type BatchWriteResponse struct {
	Results  []ItemResult
	Replayed bool
}

// BatchWriteService is the single source of truth for batch writes. Both the
// HTTP PATCH handler and the connect/gRPC TransactRelationTuples handler call
// Execute, so outcomes and error classifications cannot drift.
type BatchWriteService struct {
	d BatchServiceDeps
}

// NewBatchWriteService constructs the shared batch write service.
func NewBatchWriteService(d BatchServiceDeps) *BatchWriteService {
	return &BatchWriteService{d: d}
}

// Execute validates, bounds, de-duplicates through ordered planning and
// atomically applies the batch (or replays a stored result).
func (s *BatchWriteService) Execute(ctx context.Context, req *BatchWriteRequest) (resp *BatchWriteResponse, err error) {
	start := time.Now()
	size := 0
	if req != nil {
		size = len(req.Inputs)
	}
	var (
		results      []ItemResult
		replayed     bool
		conflictKind BatchErrorKind
	)
	defer func() {
		if conflictKind == "" && err != nil {
			if be, ok := AsBatchError(err); ok {
				conflictKind = be.Kind
			}
		}
		RecordBatchWriteMetrics(size, results, replayed, conflictKind, time.Since(start))
	}()

	if req == nil || size == 0 {
		be := newBatchError(BatchErrEmpty, -1)
		conflictKind = be.Kind
		s.safeLog(ctx, size, nil, false, start, be)
		return nil, be
	}

	// Shape validation first, before any size/database work. Messages preserve
	// the historical HTTP validation contract.
	for i := range req.Inputs {
		in := &req.Inputs[i]
		if in.Tuple == nil {
			be := NewBatchInvalidItemReason(i, "relation_tuple is missing")
			conflictKind = be.Kind
			return nil, be
		}
		if verr := in.Tuple.Validate(); verr != nil {
			return nil, verr
		}
		switch in.Action {
		case ketoapi.ActionInsert, ketoapi.ActionDelete:
		default:
			be := NewBatchInvalidItemReason(i, "unknown action "+string(in.Action))
			conflictKind = be.Kind
			return nil, be
		}
		if in.Condition == "" {
			in.Condition = ConditionAny
		}
	}

	// Configurable size cap; rejected before touching the database.
	limit := 0
	if cfg := s.d.Config(ctx); cfg != nil {
		limit = cfg.MaxWriteBatchSize()
	}
	if limit > 0 && size > limit {
		be := newBatchError(BatchErrTooLarge, -1)
		be.Limit = limit
		conflictKind = be.Kind
		s.safeLog(ctx, size, nil, false, start, be)
		return nil, be
	}

	hash, canonical, ferr := Fingerprint(req.Inputs)
	if ferr != nil {
		return nil, NewBatchInternalError("could not canonicalize batch request")
	}
	requestPayload := CanonicalPayload(canonical)

	key := req.IdempotencyKey
	ttl := time.Duration(s.d.Config(ctx).WriteIdempotencyTTLSeconds()) * time.Second
	codec := NewBatchTokenCodec(s.d.Config(ctx).BatchTokenSecret())
	mgr := s.d.RelationTupleManager()

	// Fast replay path before UUID mapping, so a replay performs no mapping
	// writes at all.
	if key != "" {
		rec, ierr := mgr.FindIdempotencyRecord(ctx, key)
		if ierr != nil {
			return nil, ierr
		}
		if rec != nil {
			now := time.Now().UTC()
			switch {
			case rec.Expired(now):
				// Fall through and atomically replace the expired record.
			case rec.RequestHash != hash:
				idx := FirstDifferentIndexPayload(rec.RequestPayload, requestPayload)
				be := NewBatchIdempotencyConflict(key, idx)
				conflictKind = be.Kind
				s.safeLog(ctx, size, nil, false, start, be)
				return nil, be
			case rec.State == IdemStateCompleted && rec.ResponsePayload != "":
				var stored BatchResult
				if jerr := json.Unmarshal([]byte(rec.ResponsePayload), &stored); jerr != nil {
					return nil, NewBatchInternalError("corrupt idempotency response")
				}
				results = stored.Results
				replayed = true
				s.safeLog(ctx, size, results, true, start, nil)
				return &BatchWriteResponse{Results: results, Replayed: true}, nil
			default:
				be := NewBatchInFlightError()
				conflictKind = be.Kind
				return nil, be
			}
		}
	}

	// Map API tuples to internal UUID tuples and run the whole batch in one
	// transaction (mapping writes included).
	txErr := mgr.Transaction(ctx, func(txCtx context.Context) error {
		apiTuples := make([]*ketoapi.RelationTuple, size)
		for i := range req.Inputs {
			apiTuples[i] = req.Inputs[i].Tuple
		}

		mapped, merr := s.d.Mapper().FromTuple(txCtx, apiTuples...)
		if merr != nil {
			return merr
		}
		if len(mapped) != size {
			return NewBatchInternalError("tuple mapping count mismatch")
		}

		items := make([]BatchItem, size)
		for i := range req.Inputs {
			items[i] = BatchItem{
				Action:          BatchAction(req.Inputs[i].Action),
				Tuple:           mapped[i],
				Condition:       req.Inputs[i].Condition,
				ExpectedVersion: req.Inputs[i].ExpectedVersion,
			}
		}

		exec := &BatchExecution{Items: items, Codec: codec}
		batchResults, didReplay, berr := mgr.BatchWrite(txCtx, exec, key, hash, requestPayload, ttl)
		if berr != nil {
			return berr
		}
		results = batchResults
		replayed = didReplay
		return nil
	})
	if txErr != nil {
		if be, ok := AsBatchError(txErr); ok {
			conflictKind = be.Kind
		}
		s.safeLog(ctx, size, results, false, start, txErr)
		return nil, txErr
	}

	// Preserve the relation-changed notification exactly once for fresh
	// writes; replays performed no write and therefore emit no event.
	if !replayed {
		trace.SpanFromContext(ctx).AddEvent(events.NewRelationtuplesChanged(ctx))
	}

	s.safeLog(ctx, size, results, replayed, start, nil)
	return &BatchWriteResponse{Results: results, Replayed: replayed}, nil
}

// safeLog emits only aggregate, relation-free fields. It never logs tuple
// identities, subjects or idempotency keys.
func (s *BatchWriteService) safeLog(ctx context.Context, size int, results []ItemResult, replayed bool, start time.Time, err error) {
	if s == nil || s.d == nil {
		return
	}
	created, unchanged, deleted, notFound := countOutcomes(results)
	fields := map[string]any{
		"batch_size":  size,
		"created":     created,
		"unchanged":   unchanged,
		"deleted":     deleted,
		"not_found":   notFound,
		"replayed":    replayed,
		"duration_ms": time.Since(start).Milliseconds(),
	}
	logger := s.d.Logger().WithFields(fields)
	if err != nil {
		logger = logger.WithField("error_kind", batchErrorKind(err))
		logger.Debug("batch write rejected or failed")
		return
	}
	logger.Debug("batch write completed")
}

func countOutcomes(results []ItemResult) (created, unchanged, deleted, notFound int) {
	for i := range results {
		switch results[i].Outcome {
		case OutcomeCreated:
			created++
		case OutcomeUnchanged:
			unchanged++
		case OutcomeDeleted:
			deleted++
		case OutcomeNotFound:
			notFound++
		}
	}
	return
}

func batchErrorKind(err error) string {
	if be, ok := AsBatchError(err); ok {
		return string(be.Kind)
	}
	return "dependency_error"
}
