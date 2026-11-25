package relationtuple_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/relationtuple"
)

// The explicit rich endpoint always returns 200 + ordered outcomes.
func TestBatchExplicitEndpoint(t *testing.T) {
	h := newBatchHarness(t)
	rt := h.sidTuple("ep-1", "s")

	body := map[string]any{
		"idempotency_key": "ep-key",
		"items": []map[string]any{
			{"action": "insert", "relation_tuple": rt},
			{"action": "insert", "relation_tuple": rt},
		},
	}
	resp, decoded := h.postBatch(body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	results, ok := decoded["results"].([]any)
	require.True(t, ok)
	require.Len(t, results, 2)
	assert.Equal(t, "created", results[0].(map[string]any)["outcome"])
	assert.Equal(t, "unchanged", results[1].(map[string]any)["outcome"])
	assert.Equal(t, "ep-key", decoded["idempotency_key"])

	// Replay through the explicit endpoint returns the stored result.
	resp2, decoded2 := h.postBatch(body)
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, true, decoded2["replayed"])
	require.Len(t, decoded2["results"].([]any), 2)
}

// Hard compatibility: duplicate single PUT keeps returning 201 and produces
// exactly one row (storage-level idempotent insert).
func TestSinglePutDuplicateStill201(t *testing.T) {
	h := newBatchHarness(t)
	rt := h.sidTuple("put-dup", "s")
	raw, err := json.Marshal(rt)
	require.NoError(t, err)

	for range 2 {
		req, err := http.NewRequest(http.MethodPut, h.ts.URL+relationtuple.WriteRouteBase, bytes.NewReader(raw))
		require.NoError(t, err)
		resp, err := h.ts.Client().Do(req) //nolint:gosec
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		_ = resp.Body.Close()
	}
	assert.Equal(t, 1, h.tupleCountInNamespace())

	// Query-based delete still returns 204.
	req, err := http.NewRequest(http.MethodDelete,
		h.ts.URL+relationtuple.WriteRouteBase+"?namespace="+h.ns+"&object=put-dup&relation=member", nil)
	require.NoError(t, err)
	resp, err := h.ts.Client().Do(req) //nolint:gosec
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	_ = resp.Body.Close()
	assert.Equal(t, 0, h.tupleCountInNamespace())
}

// Expired idempotency records are treated as a fresh request.
func TestBatchIdempotencyExpiry(t *testing.T) {
	h := newBatchHarness(t)
	in := []relationtuple.BatchInput{{Action: "insert", Tuple: h.sidTuple("exp-1", "s")}}
	svc := relationtuple.NewBatchWriteService(h.reg)
	key := "exp-key"

	first, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in, IdempotencyKey: key})
	require.NoError(t, err)
	require.False(t, first.Replayed)

	// Expire the persisted envelope.
	nid := h.reg.Persister().NetworkID(context.Background())
	require.NoError(t, h.reg.Persister().Connection(context.Background()).RawQuery(
		"UPDATE keto_batch_idempotency SET expires_at = datetime('now','-1 hour') WHERE nid = ? AND idem_key = ?",
		nid, key,
	).Exec())

	third, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in, IdempotencyKey: key})
	require.NoError(t, err)
	assert.False(t, third.Replayed, "expired record must not replay")
	require.Len(t, third.Results, 1)
	assert.Equal(t, relationtuple.OutcomeUnchanged, third.Results[0].Outcome)
}
