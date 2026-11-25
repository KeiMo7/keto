// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ory/pop/v6"
	"github.com/ory/x/otelx"
	"github.com/ory/x/sqlcon"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ory/keto/internal/relationtuple"
)

// batchIdempotencyTable is the persistent request-idempotency store.
const batchIdempotencyTable = "keto_batch_idempotency"

type batchIdempotencyRow struct {
	NID             uuid.UUID    `db:"nid"`
	Key             string       `db:"idem_key"`
	RequestHash     string       `db:"request_hash"`
	RequestPayload  string       `db:"request_payload"`
	State           string       `db:"state"`
	ResponsePayload string       `db:"response_payload"`
	CreatedAt       time.Time    `db:"created_at"`
	ExpiresAt       sql.NullTime `db:"expires_at"`
}

func (batchIdempotencyRow) TableName() string { return batchIdempotencyTable }

func (r batchIdempotencyRow) toDomain() *relationtuple.IdempotencyRecord {
	rec := &relationtuple.IdempotencyRecord{
		NetworkID:       r.NID,
		Key:             r.Key,
		RequestHash:     r.RequestHash,
		RequestPayload:  r.RequestPayload,
		State:           r.State,
		ResponsePayload: r.ResponsePayload,
		CreatedAt:       r.CreatedAt,
	}
	if r.ExpiresAt.Valid {
		t := r.ExpiresAt.Time.UTC()
		rec.ExpiresAt = &t
	}
	return rec
}

// FindIdempotencyRecord loads the idempotency envelope for the request network.
// A missing record is (nil, nil).
func (p *Persister) FindIdempotencyRecord(ctx context.Context, key string) (_ *relationtuple.IdempotencyRecord, err error) {
	if key == "" {
		return nil, nil
	}
	ctx, span := p.d.Tracer(ctx).Tracer().Start(ctx, "persistence.sql.FindIdempotencyRecord")
	defer otelx.End(span, &err)

	var row batchIdempotencyRow
	q := p.queryWithNetwork(ctx).Where("idem_key = ?", key)
	if err := q.First(&row); err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, sqlcon.HandleError(err)
	}
	return row.toDomain(), nil
}

// BatchWrite implements the ordered batch contract atomically. See
// relationtuple.Manager.BatchWrite.
func (p *Persister) BatchWrite(
	ctx context.Context,
	exec *relationtuple.BatchExecution,
	idempotencyKey, requestHash, requestPayload string,
	ttl time.Duration,
) (results []relationtuple.ItemResult, replayed bool, err error) {
	ctx, span := p.d.Tracer(ctx).Tracer().Start(ctx, "persistence.sql.BatchWrite",
		trace.WithAttributes(attribute.Int("items", len(exec.Items))))
	defer otelx.End(span, &err)

	if exec == nil {
		return nil, false, errors.WithStack(relationtuple.NewBatchInternalError("nil batch execution"))
	}

	err = p.Transaction(ctx, func(ctx context.Context) error {
		nid := p.NetworkID(ctx)
		now := time.Now().UTC()
		conn := p.Connection(ctx)

		// 1. Persistent idempotency claim / replay.
		if idempotencyKey != "" {
			replay, berr := p.beginIdempotency(ctx, conn, nid, idempotencyKey, requestHash, requestPayload, now, ttl)
			if berr != nil {
				return berr
			}
			if replay != "" {
				var br relationtuple.BatchResult
				if jerr := json.Unmarshal([]byte(replay), &br); jerr != nil {
					return relationtuple.NewBatchInternalError("corrupt idempotency response payload")
				}
				results = br.Results
				replayed = true
				return nil
			}
		}

		// 2. Load current state of every touched logical relation.
		keys := make([]relationtuple.TupleKey, 0, len(exec.Items))
		seen := make(map[relationtuple.TupleKey]struct{}, len(exec.Items))
		for i := range exec.Items {
			if exec.Items[i].Tuple == nil {
				return relationtuple.NewBatchInvalidItemError(i)
			}
			k := relationtuple.NewTupleKey(exec.Items[i].Tuple)
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}

		stored, lerr := p.loadStoredRows(ctx, keys)
		if lerr != nil {
			return lerr
		}

		shardFn := exec.NewShardID
		if shardFn == nil {
			shardFn = p.d.NewShardID
		}

		// 3. Pure ordered planning (conditions + version tokens + outcomes).
		plan, berr := relationtuple.PlanBatch(exec.Items, stored, nid, exec.Codec, shardFn)
		if berr != nil {
			return berr
		}

		hook := relationtuple.BatchChunkHookFromContext(ctx)

		// 4. Collapsed writes, chunked. Every chunk shares this transaction,
		//    so a later failure rolls back earlier chunks as well.
		if derr := p.execBatchDeletes(ctx, plan.Deletes, hook); derr != nil {
			return derr
		}
		if ierr := p.execBatchInserts(ctx, nid, plan.Inserts, hook); ierr != nil {
			return ierr
		}

		results = plan.Results

		// 5. Persist the first response for replays.
		if idempotencyKey != "" {
			payload, merr := json.Marshal(relationtuple.BatchResult{Results: results})
			if merr != nil {
				return relationtuple.NewBatchInternalError("could not encode batch result")
			}
			if uerr := conn.RawQuery(
				fmt.Sprintf("UPDATE %s SET state = ?, response_payload = ? WHERE nid = ? AND idem_key = ?", batchIdempotencyTable),
				relationtuple.IdemStateCompleted, string(payload), nid, idempotencyKey,
			).Exec(); uerr != nil {
				p.d.Logger().WithError(uerr).Error("could not complete idempotency record")
				return storageBatchError(uerr, 0, len(exec.Items))
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return results, replayed, nil
}

// beginIdempotency returns a non-empty replay payload when the request must be
// served from the stored first response. A nil payload means "proceed".
func (p *Persister) beginIdempotency(
	ctx context.Context,
	conn *pop.Connection,
	nid uuid.UUID,
	key, requestHash, requestPayload string,
	now time.Time,
	ttl time.Duration,
) (string, error) {
	existing, ferr := p.FindIdempotencyRecord(ctx, key)
	if ferr != nil {
		return "", ferr
	}

	var expires any
	if ttl > 0 {
		expires = now.Add(ttl)
	}

	if existing != nil {
		switch {
		case existing.Expired(now):
			// Retention elapsed: treat the replay as a fresh request and
			// atomically replace the expired envelope.
			uerr := conn.RawQuery(fmt.Sprintf(
				"UPDATE %s SET request_hash = ?, request_payload = ?, state = ?, response_payload = '', created_at = ?, expires_at = ? WHERE nid = ? AND idem_key = ?",
				batchIdempotencyTable),
				requestHash, requestPayload, relationtuple.IdemStatePending, now, expires, nid, key,
			).Exec()
			if uerr != nil {
				return "", storageBatchError(uerr, -1, -1)
			}
			return "", nil

		case existing.RequestHash != requestHash:
			idx := relationtuple.FirstDifferentIndexPayload(existing.RequestPayload, requestPayload)
			return "", relationtuple.NewBatchIdempotencyConflict(key, idx)

		case existing.State == relationtuple.IdemStateCompleted && existing.ResponsePayload != "":
			return existing.ResponsePayload, nil

		default:
			// A same-key batch is in flight (or its completion was interrupted).
			return "", relationtuple.NewBatchInFlightError()
		}
	}

	// No record: claim the key. The primary key (nid, idem_key) is the hard
	// guard against concurrent same-key submissions.
	insErr := conn.RawQuery(fmt.Sprintf(
		"INSERT INTO %s (nid, idem_key, request_hash, request_payload, state, response_payload, created_at, expires_at) "+
			"VALUES (?, ?, ?, ?, ?, '', ?, ?)", batchIdempotencyTable),
		nid, key, requestHash, requestPayload, relationtuple.IdemStatePending, now, expires,
	).Exec()
	if insErr != nil {
		if isUniqueViolation(insErr) || isBusyOrLocked(insErr) {
			// Concurrent claim won. Ask the client to retry so the winner's
			// stored result can be replayed.
			return "", relationtuple.NewBatchInFlightError()
		}
		p.d.Logger().WithError(insErr).Error("could not persist idempotency record")
		return "", storageBatchError(insErr, -1, -1)
	}
	return "", nil
}

// loadStoredRows selects the current rows matching the touched keys, chunked
// to keep the OR expression tree parseable.
func (p *Persister) loadStoredRows(ctx context.Context, keys []relationtuple.TupleKey) ([]relationtuple.StoredRow, error) {
	stored := make([]relationtuple.StoredRow, 0, len(keys))
	nid := p.NetworkID(ctx)

	chunkIdx := 0
	for chunk := range slices.Chunk(keys, chunkSizeDeleteTuple) {
		chunkIdx++
		ors := make([]string, 0, len(chunk))
		args := make([]any, 0, 5*len(chunk)+1)
		for i := range chunk {
			pred, pargs, err := identityKeyPredicate(chunk[i])
			if err != nil {
				return nil, err
			}
			ors = append(ors, pred)
			args = append(args, pargs...)
		}

		query := "SELECT shard_id, nid, namespace, object, relation, subject_id, " +
			"subject_set_namespace, subject_set_object, subject_set_relation, commit_time " +
			"FROM " + (&RelationTuple{}).TableName() +
			" WHERE nid = ? AND (" + strings.Join(ors, " OR ") + ")"
		args = append([]any{nid}, args...)

		var rows relationTuples
		if err := p.Connection(ctx).RawQuery(query, args...).All(&rows); err != nil {
			p.d.Logger().WithError(sqlcon.HandleError(err)).WithField("chunk", chunkIdx).
				Error("batch write: failed loading current tuples")
			return nil, storageBatchError(err, -1, -1)
		}
		for i := range rows {
			internal := rows[i].ToInternal()
			stored = append(stored, relationtuple.StoredRow{
				Key:     relationtuple.NewTupleKey(internal),
				ShardID: rows[i].ID,
			})
		}
	}
	return stored, nil
}

func (p *Persister) execBatchDeletes(ctx context.Context, muts []relationtuple.PlannedMutation, hook relationtuple.BatchChunkHook) error {
	chunkIdx := 0
	for chunk := range slices.Chunk(muts, chunkSizeDeleteTuple) {
		chunkIdx++
		from, to := mutationInterval(chunk)
		if hook != nil {
			if herr := hook("delete", chunkIdx, from, to); herr != nil {
				return storageBatchError(herr, from, to)
			}
		}

		keys := make([]relationtuple.TupleKey, len(chunk))
		for i := range chunk {
			keys[i] = chunk[i].Key
		}
		query, args, err := buildDeleteByKeys(p.NetworkID(ctx), keys)
		if err != nil {
			return err
		}
		if err := p.Connection(ctx).RawQuery(query, args...).Exec(); err != nil {
			if isBusyOrLocked(err) || isSerializationConflict(err) {
				return relationtuple.NewBatchConcurrentError()
			}
			p.d.Logger().WithError(sqlcon.HandleError(err)).
				WithField("from_index", from).WithField("to_index", to).
				Error("batch write: delete chunk failed, rolling back")
			return storageBatchError(err, from, to)
		}
	}
	return nil
}

func (p *Persister) execBatchInserts(ctx context.Context, nid uuid.UUID, muts []relationtuple.PlannedMutation, hook relationtuple.BatchChunkHook) error {
	chunkIdx := 0
	for chunk := range slices.Chunk(muts, chunkSizeInsertTuple) {
		chunkIdx++
		from, to := mutationInterval(chunk)
		if hook != nil {
			if herr := hook("insert", chunkIdx, from, to); herr != nil {
				return storageBatchError(herr, from, to)
			}
		}

		query, args, err := buildPlannedInsert(nid, chunk)
		if err != nil {
			return err
		}
		if err := p.Connection(ctx).RawQuery(query, args...).Exec(); err != nil {
			switch {
			case isUniqueViolation(err) || isBusyOrLocked(err) || isSerializationConflict(err):
				// Overlapping concurrent batch won the unique identity race.
				return relationtuple.NewBatchConcurrentError()
			default:
				p.d.Logger().WithError(sqlcon.HandleError(err)).
					WithField("from_index", from).WithField("to_index", to).
					Error("batch write: insert chunk failed, rolling back")
				return storageBatchError(err, from, to)
			}
		}
	}
	return nil
}

func mutationInterval(chunk []relationtuple.PlannedMutation) (from, to int) {
	from = -1
	to = -1
	for i := range chunk {
		idx := chunk[i].Index
		if idx < 0 {
			continue
		}
		if from == -1 || idx < from {
			from = idx
		}
		if idx+1 > to {
			to = idx + 1
		}
	}
	return from, to
}

// identityKeyPredicate renders the same exactly-one-subject predicates as
// buildDelete, but from a comparable TupleKey.
func identityKeyPredicate(k relationtuple.TupleKey) (string, []any, error) {
	switch k.SubjectKind {
	case relationtuple.SubjectKindID:
		return "(namespace = ? AND object = ? AND relation = ? AND subject_id = ? AND " +
				"subject_set_namespace IS NULL AND subject_set_object IS NULL AND subject_set_relation IS NULL)",
			[]any{k.Namespace, k.Object, k.Relation, k.SubjectID}, nil
	case relationtuple.SubjectKindSet:
		return "(namespace = ? AND object = ? AND relation = ? AND subject_id IS NULL AND " +
				"subject_set_namespace = ? AND subject_set_object = ? AND subject_set_relation = ?)",
			[]any{k.Namespace, k.Object, k.Relation, k.SetNamespace, k.SetObject, k.SetRelation}, nil
	default:
		return "", nil, errors.WithStack(relationtuple.ErrBatchMalformedSubject())
	}
}

func buildDeleteByKeys(nid uuid.UUID, keys []relationtuple.TupleKey) (string, []any, error) {
	if len(keys) == 0 {
		return "", nil, errors.WithStack(relationtuple.ErrBatchMalformedInput())
	}
	ors := make([]string, 0, len(keys))
	args := make([]any, 0, 6*len(keys)+1)
	for i := range keys {
		pred, pargs, err := identityKeyPredicate(keys[i])
		if err != nil {
			return "", nil, err
		}
		ors = append(ors, pred)
		args = append(args, pargs...)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE (%s) AND nid = ?", (&RelationTuple{}).TableName(), strings.Join(ors, " OR "))
	args = append(args, nid)
	return query, args, nil
}

// buildPlannedInsert inserts rows with planner-minted shard ids so returned
// version tokens stay valid.
func buildPlannedInsert(nid uuid.UUID, muts []relationtuple.PlannedMutation) (string, []any, error) {
	if len(muts) == 0 {
		return "", nil, errors.WithStack(relationtuple.ErrBatchMalformedInput())
	}

	var q strings.Builder
	fmt.Fprintf(&q, "INSERT INTO %s (shard_id, nid, namespace, object, relation, subject_id, subject_set_namespace, subject_set_object, subject_set_relation, commit_time) VALUES ", (&RelationTuple{}).TableName())
	const placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	q.Grow(len(muts) * (len(placeholders) + 2))
	args := make([]any, 0, 10*len(muts))
	commitTime := time.Now().UTC()

	for i := range muts {
		if muts[i].Tuple == nil {
			return "", nil, errors.WithStack(relationtuple.ErrBatchMalformedSubject())
		}
		row := &RelationTuple{
			ID:         muts[i].ShardID,
			NetworkID:  nid,
			CommitTime: commitTime,
		}
		if err := row.FromInternal(muts[i].Tuple); err != nil {
			return "", nil, err
		}
		if i > 0 {
			q.WriteString(", ")
		}
		q.WriteString(placeholders)
		args = append(args, row.ID, row.NetworkID, row.Namespace, row.Object, row.Relation,
			row.SubjectID, row.SubjectSetNamespace, row.SubjectSetObject, row.SubjectSetRelation, row.CommitTime)
	}
	return q.String(), args, nil
}

func storageBatchError(raw error, from, to int) *relationtuple.BatchError {
	return relationtuple.NewBatchStorageError(from, to).WithInner(raw)
}

// --- driver error classification using message tokens (classification only;
// messages are never returned to external clients) ---------------------------

func errText(err error) string {
	if err == nil {
		return ""
	}
	return strings.ToLower(err.Error())
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	txt := errText(sqlcon.HandleError(err))
	// sqlcon maps unique violations to HTTP 409 / AlreadyExists.
	if strings.Contains(txt, "already exists") || strings.Contains(txt, "unique") ||
		strings.Contains(txt, "duplicate") || strings.Contains(txt, "constraint failed: unique") {
		return true
	}
	raw := errText(err)
	return strings.Contains(raw, "unique constraint") || strings.Contains(raw, "duplicate entry") ||
		strings.Contains(raw, "duplicate key value") || strings.Contains(raw, "unique index") ||
		strings.Contains(raw, "constraint failed") && strings.Contains(raw, "unique")
}

func isBusyOrLocked(err error) bool {
	txt := errText(err)
	return strings.Contains(txt, "database is locked") || strings.Contains(txt, "sqlite_busy") ||
		strings.Contains(txt, "sqlstate 55p03") || strings.Contains(txt, "lock wait timeout") ||
		strings.Contains(txt, "try restarting transaction")
}

func isSerializationConflict(err error) bool {
	txt := errText(err)
	return strings.Contains(txt, "could not serialize access") || strings.Contains(txt, "40001") ||
		strings.Contains(txt, "serialization failure") || strings.Contains(txt, "deadlock") ||
		strings.Contains(txt, "cr40001") || strings.Contains(txt, "retry transaction")
}
