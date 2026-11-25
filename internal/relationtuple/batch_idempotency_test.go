package relationtuple_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/ory/x/dbal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/namespace"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/internal/x/dbx"
	"github.com/ory/keto/ketoapi"
)

func namespaces(names ...string) []*namespace.Namespace {
	out := make([]*namespace.Namespace, 0, len(names))
	for _, n := range names {
		out = append(out, &namespace.Namespace{Name: n})
	}
	return out
}

func apiTuple(ns, obj, subj string) *ketoapi.RelationTuple {
	return &ketoapi.RelationTuple{Namespace: ns, Object: obj, Relation: "member", SubjectID: &subj}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func decodeMap(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))
	return m
}

// Form 5: same idempotency key replays the identical first outcomes without
// writing again; same key with different content is a conflict.
func TestBatchIdempotencyReplayAndConflict(t *testing.T) {
	h := newBatchHarness(t)
	deltas := []*ketoapi.PatchDelta{
		insertDelta(h.sidTuple("idem-1", "s1")),
		insertDelta(h.sidTuple("idem-1", "s1")),
	}

	resp1, body1 := h.patch(deltas, "key-abc")
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	require.False(t, body1.Replayed)
	require.Len(t, body1.Results, 2)
	countAfterFirst := h.tupleCountInNamespace()
	require.Equal(t, 1, countAfterFirst)

	// Replay: identical outcomes (including tokens), flagged replayed, no writes.
	resp2, body2 := h.patch(deltas, "key-abc")
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.True(t, body2.Replayed)
	assert.Equal(t, body1.Results, body2.Results)
	assert.Equal(t, body1.Snaptokens, body2.Snaptokens)
	assert.Equal(t, countAfterFirst, h.tupleCountInNamespace())

	// Same key, different content at index 1 -> conflict with item index.
	changed := []*ketoapi.PatchDelta{
		insertDelta(h.sidTuple("idem-1", "s1")),
		insertDelta(h.sidTuple("idem-DIFFERENT", "s2")),
	}
	raw := mustJSON(t, changed)
	resp3 := h.patchRaw(raw, "key-abc", true)
	assert.Equal(t, http.StatusConflict, resp3.StatusCode)
	errBody := decodeMap(t, resp3)
	errObj, _ := errBody["error"].(map[string]any)
	require.NotNil(t, errObj)
	assert.Contains(t, errObj["id"], "idempotency-key-conflict")
	details, _ := errObj["details"].(map[string]any)
	assert.EqualValues(t, "idempotency_key_conflict", details["error_kind"])
	assert.EqualValues(t, 1, details["item_index"])
	assert.Equal(t, countAfterFirst, h.tupleCountInNamespace(), "conflicting reuse must not write")
}

// Persistence across restart: a second registry on the same database file
// still holds uniqueness and idempotency records and replays identically.
func TestBatchIdempotencySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dsn := dbal.NewSQLiteDatabase(dir)
	dsnT := &dbx.DsnT{Conn: dsn, Name: "sqlite", MigrateUp: true}

	ns := "restart-ns"
	reg1 := driver.NewTestRegistry(t, dsnT, driver.WithNamespaces(namespaces(ns)))

	svc1 := relationtuple.NewBatchWriteService(reg1)
	in := []relationtuple.BatchInput{{
		Action: "insert",
		Tuple:  apiTuple(ns, "o", "s"),
	}}
	first, err := svc1.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in, IdempotencyKey: "persist-key"})
	require.NoError(t, err)
	require.Len(t, first.Results, 1)
	require.Equal(t, relationtuple.OutcomeCreated, first.Results[0].Outcome)

	// "Restart": a brand new registry pointing at the same database file.
	reg2 := driver.NewTestRegistry(t, dsnT, driver.WithNamespaces(namespaces(ns)))
	svc2 := relationtuple.NewBatchWriteService(reg2)

	second, err := svc2.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in, IdempotencyKey: "persist-key"})
	require.NoError(t, err)
	assert.True(t, second.Replayed)
	require.Len(t, second.Results, 1)
	assert.Equal(t, first.Results[0], second.Results[0], "replay after restart returns the stored outcome/token")

	// Retrying the raw batch without the key after restart converges to unchanged.
	third, err := svc2.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in})
	require.NoError(t, err)
	require.Len(t, third.Results, 1)
	assert.Equal(t, relationtuple.OutcomeUnchanged, third.Results[0].Outcome)
}
