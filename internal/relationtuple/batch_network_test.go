package relationtuple_test

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/relationtuple"
)

// Form 9: uniqueness, idempotency and conditions are network scoped.
func TestBatchNetworkIsolation(t *testing.T) {
	h := newBatchHarness(t)
	nidA := uuid.Must(uuid.NewV4())
	nidB := uuid.Must(uuid.NewV4())
	p := h.reg.Persister()

	// Register both networks so the nid foreign key is satisfied.
	for _, nid := range []uuid.UUID{nidA, nidB} {
		require.NoError(t, p.Connection(context.Background()).RawQuery(
			"INSERT INTO networks (id, created_at, updated_at) VALUES (?, datetime('now'), datetime('now'))", nid,
		).Exec())
	}

	svc := relationtuple.NewBatchWriteService(h.reg)

	in := []relationtuple.BatchInput{{Action: "insert", Tuple: h.sidTuple("net-obj", "net-subj")}}

	p.SetNetwork(nidA)
	r1, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in})
	require.NoError(t, err)
	require.Equal(t, relationtuple.OutcomeCreated, r1.Results[0].Outcome)

	r2, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in})
	require.NoError(t, err)
	assert.Equal(t, relationtuple.OutcomeUnchanged, r2.Results[0].Outcome, "same network dedupes")
	assert.Equal(t, 1, h.tupleCountInNamespace())

	// Same logical relation in a different network is independent.
	p.SetNetwork(nidB)
	r3, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in})
	require.NoError(t, err)
	assert.Equal(t, relationtuple.OutcomeCreated, r3.Results[0].Outcome, "different network must not see the A row")
	assert.Equal(t, 1, h.tupleCountInNamespace())

	// Idempotency keys are network scoped: use the same key for different
	// content in A vs B and prove the networks do not collide.
	key := "shared-key"
	p.SetNetwork(nidA)
	_, err = svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in, IdempotencyKey: key})
	require.NoError(t, err)

	p.SetNetwork(nidB)
	other := []relationtuple.BatchInput{{Action: "insert", Tuple: h.sidTuple("net-obj-2", "s2")}}
	rBKey, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: other, IdempotencyKey: key})
	require.NoError(t, err, "same key with different content must not collide across networks")
	assert.False(t, rBKey.Replayed)

	// Replaying the B request in B returns its stored result.
	rBReplay, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: other, IdempotencyKey: key})
	require.NoError(t, err)
	assert.True(t, rBReplay.Replayed)

	// Back in A the key still holds its own record and replays.
	p.SetNetwork(nidA)
	rAKey, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: in, IdempotencyKey: key})
	require.NoError(t, err)
	assert.True(t, rAKey.Replayed)
	assert.Equal(t, 1, h.tupleCountInNamespace(), "network A still has exactly its one row")

	p.SetNetwork(nidB)
	assert.Equal(t, 2, h.tupleCountInNamespace(), "network B has its two rows")
}
