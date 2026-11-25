// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ory/herodot"
	"google.golang.org/grpc/codes"

	"github.com/ory/keto/ketoapi"
)

// This file contains the storage-independent contract for the robust batch
// write endpoint: ordered mixed insert/delete semantics, per-item outcomes,
// conditional writes, opaque version tokens, request idempotency and safe,
// classified errors. Both the HTTP and the connect/gRPC entry points route
// through BatchWriteService so their outcomes and error classifications can
// never drift apart.

type (
	// BatchAction is the requested operation for one batch item.
	BatchAction string

	// BatchCondition is an optional existence precondition evaluated against
	// the state immediately before the item in request order.
	BatchCondition string

	// ItemOutcome is the concrete, per-item result of a batch item.
	ItemOutcome string
)

const (
	BatchActionInsert BatchAction = BatchAction(ketoapi.ActionInsert)
	BatchActionDelete BatchAction = BatchAction(ketoapi.ActionDelete)

	// ConditionAny means no existence precondition.
	ConditionAny BatchCondition = ""
	// ConditionAbsent requires that the relation does not exist at evaluation time.
	ConditionAbsent BatchCondition = "if_absent"
	// ConditionPresent requires that the relation exists at evaluation time.
	ConditionPresent BatchCondition = "if_present"

	// OutcomeCreated: the relation was not present and is now stored.
	OutcomeCreated ItemOutcome = "created"
	// OutcomeUnchanged: an insert targeted an already present relation; no row changed.
	OutcomeUnchanged ItemOutcome = "unchanged"
	// OutcomeDeleted: a present relation was removed.
	OutcomeDeleted ItemOutcome = "deleted"
	// OutcomeNotFound: a delete targeted an absent relation; nothing changed.
	OutcomeNotFound ItemOutcome = "not_found"
)

// Outcome aliases accepted by/returned on the wire so different API surfaces
// can use their preferred vocabulary without changing semantics.
var outcomeWireAliases = map[ItemOutcome][]string{
	OutcomeCreated:   {"created", "create", "inserted", "insert"},
	OutcomeUnchanged: {"unchanged", "already_exists", "already_present", "exists", "noop"},
	OutcomeDeleted:   {"deleted", "removed", "delete"},
	OutcomeNotFound:  {"not_found", "absent", "did_not_exist", "no_change_delete"},
}

// ParseCondition accepts the canonical condition values and a few common
// aliases. Unknown values result in an invalid-item error.
func ParseCondition(raw string) (BatchCondition, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "any", "none":
		return ConditionAny, true
	case "if_absent", "absent", "must_not_exist", "require_absent", "insert_if_absent":
		return ConditionAbsent, true
	case "if_present", "present", "must_exist", "require_present", "delete_if_present":
		return ConditionPresent, true
	default:
		return ConditionAny, false
	}
}

// TupleKey is the comparable identity of one logical relation tuple. It
// includes the network id nowhere: callers always scope by nid separately and
// every persistence query/index is nid-prefixed.
type TupleKey struct {
	Namespace string
	Object    uuid.UUID
	Relation  string

	SubjectKind int // 1 = SubjectID, 2 = SubjectSet

	SubjectID uuid.UUID

	SetNamespace string
	SetObject    uuid.UUID
	SetRelation  string
}

const (
	SubjectKindUnknown = 0
	SubjectKindID      = 1
	SubjectKindSet     = 2
)

// NewTupleKey builds the identity key for a mapped internal tuple.
func NewTupleKey(rt *RelationTuple) TupleKey {
	k := TupleKey{
		Namespace: rt.Namespace,
		Object:    rt.Object,
		Relation:  rt.Relation,
	}
	switch s := rt.Subject.(type) {
	case *SubjectID:
		k.SubjectKind = SubjectKindID
		k.SubjectID = s.ID
	case *SubjectSet:
		k.SubjectKind = SubjectKindSet
		k.SetNamespace = s.Namespace
		k.SetObject = s.Object
		k.SetRelation = s.Relation
	}
	return k
}

// KeyHash returns a short, non-reversible hash of the tuple identity. It is
// only used to bind opaque version tokens to one relation.
func (k TupleKey) KeyHash() string {
	sum := sha256.Sum256([]byte(k.canonical()))
	return hex.EncodeToString(sum[:8])
}

func (k TupleKey) canonical() string {
	var b strings.Builder
	b.WriteString(k.Namespace)
	b.WriteByte(0)
	b.WriteString(k.Object.String())
	b.WriteByte(0)
	b.WriteString(k.Relation)
	b.WriteByte(0)
	if k.SubjectKind == SubjectKindID {
		b.WriteString("id:")
		b.WriteString(k.SubjectID.String())
	} else {
		b.WriteString("ss:")
		b.WriteString(k.SetNamespace)
		b.WriteByte(0)
		b.WriteString(k.SetObject.String())
		b.WriteByte(0)
		b.WriteString(k.SetRelation)
	}
	return b.String()
}

// BatchInput is one wire-level batch item before UUID mapping.
type BatchInput struct {
	Action          ketoapi.PatchAction
	Tuple           *ketoapi.RelationTuple
	Condition       BatchCondition
	ExpectedVersion string
}

// BatchItem is one ordered batch item with a mapped internal tuple.
type BatchItem struct {
	Action          BatchAction
	Tuple           *RelationTuple
	Condition       BatchCondition
	ExpectedVersion string
}

// ItemResult is the per-item outcome in request order.
type ItemResult struct {
	Index        int         `json:"index"`
	Outcome      ItemOutcome `json:"outcome"`
	VersionToken string      `json:"version_token"`
}

// BatchResult is the ordered, length-preserving result of a batch.
type BatchResult struct {
	Results []ItemResult `json:"results"`
}

// Snaptokens returns the per-item version tokens in request order. The length
// always equals the request item count.
func (r *BatchResult) Snaptokens() []string {
	out := make([]string, len(r.Results))
	for i := range r.Results {
		out[i] = r.Results[i].VersionToken
	}
	return out
}

// Counts groups outcomes for metrics and assertions.
func (r *BatchResult) Counts() (created, unchanged, deleted, notFound int) {
	for _, it := range r.Results {
		switch it.Outcome {
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

// IdempotencyRecord is the persisted first-response envelope.
type IdempotencyRecord struct {
	NetworkID       uuid.UUID
	Key             string
	RequestHash     string
	RequestPayload  string
	State           string // pending | completed
	ResponsePayload string
	CreatedAt       time.Time
	ExpiresAt       *time.Time
}

const (
	IdemStatePending   = "pending"
	IdemStateCompleted = "completed"
)

// Expired reports whether the record is past its retention deadline.
func (r *IdempotencyRecord) Expired(now time.Time) bool {
	return r != nil && r.ExpiresAt != nil && now.After(*r.ExpiresAt)
}

// BatchExecution is passed to the persistence layer inside one transaction.
type BatchExecution struct {
	Items      []BatchItem
	Codec      *BatchTokenCodec
	NewShardID func() uuid.UUID
}

// BatchErrorKind enumerates the stable, distinguishable batch error classes.
type BatchErrorKind string

const (
	BatchErrEmpty       BatchErrorKind = "empty_batch"
	BatchErrTooLarge    BatchErrorKind = "batch_too_large"
	BatchErrInvalidItem BatchErrorKind = "invalid_item"
	BatchErrCondition   BatchErrorKind = "condition_failed"
	BatchErrVersion     BatchErrorKind = "version_conflict"
	BatchErrIdempotency BatchErrorKind = "idempotency_key_conflict"
	BatchErrInFlight    BatchErrorKind = "idempotency_in_flight"
	BatchErrConcurrent  BatchErrorKind = "concurrent_conflict"
	BatchErrStorage     BatchErrorKind = "storage_failure"
)

// BatchError is the only error type crossing the batch contract boundary for
// batch-specific failures. Its Error() string and herodot details contain
// only stable codes, numeric indices/counts and the configured limit - never
// relation content, raw driver errors or internal mapping identifiers.
type BatchError struct {
	Kind      BatchErrorKind `json:"kind"`
	Index     int            `json:"item_index,omitempty"`
	Limit     int            `json:"max_items,omitempty"`
	FromIndex int            `json:"failed_from_index,omitempty"`
	ToIndex   int            `json:"failed_to_index,omitempty"`
	Expected  string         `json:"expected,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	Retryable bool           `json:"retryable,omitempty"`
	IdemKey   string         `json:"idempotency_key,omitempty"`
	inner     error
}

func (e *BatchError) Error() string {
	switch e.Kind {
	case BatchErrTooLarge:
		return fmt.Sprintf("batch write rejected: batch size exceeds the configured maximum of %d items", e.Limit)
	case BatchErrEmpty:
		return "batch write rejected: batch must contain at least one item"
	case BatchErrInvalidItem:
		if e.Reason != "" {
			return e.Reason
		}
		return fmt.Sprintf("batch write rejected: item %d is invalid", e.Index)
	case BatchErrCondition:
		return fmt.Sprintf("batch write conflict at item %d: precondition %q not met", e.Index, e.Expected)
	case BatchErrVersion:
		return fmt.Sprintf("batch write conflict at item %d: version token does not match the current relation version", e.Index)
	case BatchErrIdempotency:
		return fmt.Sprintf("idempotency key %q was already used with a different request content; first differing item: %d", e.IdemKey, e.Index)
	case BatchErrInFlight:
		return "a batch with the same idempotency key is currently being processed; retry shortly"
	case BatchErrConcurrent:
		return "batch write conflict due to a concurrent modification of overlapping relations; retry the batch"
	case BatchErrStorage:
		return fmt.Sprintf("batch write failed in storage; the whole batch was rolled back; failed item interval: [%d,%d)", e.FromIndex, e.ToIndex)
	default:
		return "batch write failed"
	}
}

// Unwrap exposes an inner error only for errors.Is/As flows. Callers must
// never render it to external clients.
func (e *BatchError) Unwrap() error { return e.inner }

// WithInner attaches the raw storage error for server-side inspection.
func (e *BatchError) WithInner(err error) *BatchError {
	e.inner = err
	return e
}

// Is enables errors.Is(target) comparisons by kind.
func (e *BatchError) Is(target error) bool {
	t, ok := target.(*BatchError)
	return ok && t.Kind == e.Kind
}

func newBatchError(kind BatchErrorKind, index int) *BatchError {
	return &BatchError{Kind: kind, Index: index, FromIndex: -1, ToIndex: -1}
}

// NewBatchInvalidItemError builds an invalid-item rejection for an index.
func NewBatchInvalidItemError(index int) *BatchError {
	return newBatchError(BatchErrInvalidItem, index)
}

// NewBatchInvalidItemReason builds an invalid-item rejection carrying a safe,
// relation-free validation message.
func NewBatchInvalidItemReason(index int, reason string) *BatchError {
	e := newBatchError(BatchErrInvalidItem, index)
	e.Reason = reason
	return e
}

// NewBatchIdempotencyConflict builds an idempotency-content conflict. A
// negative index means "no item-level diff available".
func NewBatchIdempotencyConflict(key string, index int) *BatchError {
	e := newBatchError(BatchErrIdempotency, index)
	e.IdemKey = key
	return e
}

// NewBatchInFlightError builds a retryable same-key-in-flight conflict.
func NewBatchInFlightError() *BatchError {
	e := newBatchError(BatchErrInFlight, -1)
	e.Retryable = true
	return e
}

// NewBatchConcurrentError builds a retryable overlapping-batch conflict.
func NewBatchConcurrentError() *BatchError {
	e := newBatchError(BatchErrConcurrent, -1)
	e.Retryable = true
	return e
}

// NewBatchStorageError builds a safe storage failure carrying only an item
// interval. The raw driver error is attached internally but never rendered.
func NewBatchStorageError(from, to int) *BatchError {
	return &BatchError{Kind: BatchErrStorage, Index: -1, FromIndex: from, ToIndex: to}
}

// NewBatchInternalError builds a safe internal batch error. The message must
// be a static, relation-free description.
func NewBatchInternalError(safeMessage string) *BatchError {
	return &BatchError{
		Kind:      BatchErrStorage,
		Index:     -1,
		FromIndex: -1,
		ToIndex:   -1,
		inner:     stderrors.New(safeMessage),
	}
}

// ErrBatchMalformedInput is returned when an internal write builder receives
// no rows.
func ErrBatchMalformedInput() error { return stderrors.New("batch: malformed input") }

// ErrBatchMalformedSubject is returned when a mapped tuple has no subject.
func ErrBatchMalformedSubject() error { return stderrors.New("batch: malformed subject") }

// AsBatchError extracts a *BatchError from an error chain.
func AsBatchError(err error) (*BatchError, bool) {
	var be *BatchError
	if stderrors.As(err, &be) {
		return be, true
	}
	return nil, false
}

// Herodot maps the batch error to the stable Ory HTTP/gRPC error family. Both
// entry points use this exact mapping, which guarantees identical error
// classification.
func (e *BatchError) Herodot() *herodot.DefaultError {
	var h *herodot.DefaultError
	switch e.Kind {
	case BatchErrEmpty, BatchErrInvalidItem:
		h = herodot.ErrBadRequest().WithID("err-batch-write-invalid").WithError(e.Error())
	case BatchErrTooLarge:
		h = herodot.ErrBadRequest().WithID("err-batch-write-too-large").WithError(e.Error())
	case BatchErrCondition:
		h = herodot.ErrConflict().WithID("err-batch-condition-failed").WithError(e.Error())
		h.GRPCCodeField = codes.FailedPrecondition
	case BatchErrVersion:
		h = herodot.ErrConflict().WithID("err-batch-version-conflict").WithError(e.Error())
		h.GRPCCodeField = codes.Aborted
	case BatchErrIdempotency:
		h = herodot.ErrConflict().WithID("err-idempotency-key-conflict").WithError(e.Error())
		h.GRPCCodeField = codes.AlreadyExists
	case BatchErrInFlight:
		h = herodot.ErrConflict().WithID("err-idempotency-request-in-flight").WithError(e.Error())
		h.GRPCCodeField = codes.Aborted
	case BatchErrConcurrent:
		h = herodot.ErrConflict().WithID("err-batch-concurrent-conflict").WithError(e.Error())
		h.GRPCCodeField = codes.Aborted
	case BatchErrStorage:
		h = herodot.ErrInternalServerError().WithID("err-batch-write-storage").
			WithError("the batch write failed and was rolled back; no item was applied")
	default:
		h = herodot.ErrInternalServerError()
	}

	h = h.WithDetail("error_kind", string(e.Kind)).
		WithDetail("retryable", e.Retryable || e.Kind == BatchErrConcurrent || e.Kind == BatchErrInFlight)

	// Structured, non-sensitive details.
	switch e.Kind {
	case BatchErrTooLarge:
		h = h.WithDetail("max_items", e.Limit)
	case BatchErrCondition, BatchErrVersion, BatchErrInvalidItem:
		h = h.WithDetail("item_index", e.Index)
		if e.Expected != "" {
			h = h.WithDetail("expected_state", e.Expected)
		}
	case BatchErrIdempotency:
		h = h.WithDetail("item_index", e.Index).WithDetail("idempotency_key", e.IdemKey)
	case BatchErrStorage:
		h = h.WithDetail("failed_from_index", e.FromIndex).
			WithDetail("failed_to_index", e.ToIndex)
	}
	return h
}

// HTTPStatusCode is the stable status classification for the batch error.
func (e *BatchError) HTTPStatusCode() int {
	return e.Herodot().CodeField
}

// Fingerprint canonicalizes batch inputs for idempotency conflict detection.
// It deliberately includes every field that changes the logical request,
// including conditions and expected version tokens.
func Fingerprint(inputs []BatchInput) (hash string, canonical [][]byte, err error) {
	canonical = make([][]byte, len(inputs))
	h := sha256.New()
	for i := range inputs {
		// Normalize through a stable struct instead of the API struct tags, so
		// reordering of unrelated fields cannot cause spurious conflicts.
		norm, mErr := json.Marshal(struct {
			Action          string          `json:"action"`
			Tuple           json.RawMessage `json:"tuple"`
			Condition       string          `json:"condition,omitempty"`
			ExpectedVersion string          `json:"expected_version,omitempty"`
		}{
			Action:          string(inputs[i].Action),
			Tuple:           canonicalTuple(inputs[i].Tuple),
			Condition:       string(inputs[i].Condition),
			ExpectedVersion: inputs[i].ExpectedVersion,
		})
		if mErr != nil {
			return "", nil, mErr
		}
		canonical[i] = norm
		h.Write(norm)
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), canonical, nil
}

func canonicalTuple(rt *ketoapi.RelationTuple) json.RawMessage {
	if rt == nil {
		return json.RawMessage("null")
	}
	// Re-marshal to drop zero/empty optional fields in a deterministic way.
	b, err := json.Marshal(rt)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}

// FirstDifferentIndex compares two canonical item sets and returns the index
// of the first difference. When lengths differ it returns the first index at
// the shorter boundary. Returns -1 when they are equal.
func FirstDifferentIndex(a, b [][]byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if string(a[i]) != string(b[i]) {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

// canonicalItemSep separates per-item canonical JSON records in the persisted
// request payload. Canonical item JSON never contains a raw newline.
const canonicalItemSep = "\n"

// CanonicalPayload joins canonical per-item JSON into the persisted payload.
func CanonicalPayload(items [][]byte) string {
	parts := make([]string, len(items))
	for i := range items {
		parts[i] = string(items[i])
	}
	return strings.Join(parts, canonicalItemSep)
}

// FirstDifferentIndexPayload compares two persisted canonical payloads and
// returns the first differing item index (or -1 when equal).
func FirstDifferentIndexPayload(storedPayload, currentPayload string) int {
	a := strings.Split(storedPayload, canonicalItemSep)
	b := strings.Split(currentPayload, canonicalItemSep)
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

// Ensure net/http is referenced for status constants in follow-up files.
var _ = http.StatusBadRequest
