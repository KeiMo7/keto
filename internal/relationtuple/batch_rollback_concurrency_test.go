package relationtuple_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ory/x/dbal"
	keysetpagination "github.com/ory/x/pagination/keysetpagination_v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/internal/x/dbx"
	"github.com/ory/keto/ketoapi"
)

// Form 8 / acceptance rollback: a fault injected in a later chunk rolls back
// earlier chunks too; row count equals before and the error carries only an
// item interval and no relation content/driver error.
func TestBatchRollbackOnMidStorageFailure(t *testing.T) {
	h := newBatchHarness(t)
	const n = 150 // > chunkSizeDeleteTuple(100) => multiple internal segments

	tuples := make([]*ketoapi.RelationTuple, n)
	deltas := make([]*ketoapi.PatchDelta, n)
	for i := range n {
		tuples[i] = h.sidTuple(fmt.Sprintf("rb-%03d", i), "s")
		deltas[i] = deleteDelta(tuples[i])
	}
	testhelpers.MapAndInsertTuples(t, h.reg, tuples...)
	require.Equal(t, n, h.tupleCountInNamespace())

	hook := func(phase string, chunkIndex, fromIndex, toIndex int) error {
		// Fail the second delete segment after the first segment executed.
		if phase == "delete" && chunkIndex == 2 {
			return errors.New("injected storage failure DRIVER_SECRET_DETAIL")
		}
		return nil
	}
	ctx := relationtuple.WithBatchChunkHook(context.Background(), hook)
	svc := relationtuple.NewBatchWriteService(h.reg)
	_, err := svc.Execute(ctx, &relationtuple.BatchWriteRequest{
		Inputs: mustInputs(t, deltas),
	})
	require.Error(t, err)

	be, ok := relationtuple.AsBatchError(err)
	require.True(t, ok, "expected BatchError, got %T: %v", err, err)
	assert.Equal(t, relationtuple.BatchErrStorage, be.Kind)
	assert.GreaterOrEqual(t, be.FromIndex, 0)
	assert.Greater(t, be.ToIndex, be.FromIndex)
	assert.LessOrEqual(t, be.ToIndex, n)

	// Falsifiable rollback: row count is exactly as before.
	assert.Equal(t, n, h.tupleCountInNamespace(), "mid-batch failure must roll back all segments")

	// Outer, classified error and its herodot rendering leak nothing sensitive.
	safe := be.Error()
	assert.NotContains(t, safe, "rb-001")
	assert.NotContains(t, safe, "DRIVER_SECRET_DETAIL")
	herodotJSON := fmt.Sprintf("%+v", be.Herodot())
	assert.NotContains(t, herodotJSON, "DRIVER_SECRET_DETAIL")
	assert.NotContains(t, herodotJSON, "rb-0")
}

func mustInputs(t *testing.T, deltas []*ketoapi.PatchDelta) []relationtuple.BatchInput {
	t.Helper()
	out := make([]relationtuple.BatchInput, len(deltas))
	for i, d := range deltas {
		cond, ok := relationtuple.ParseCondition(d.Condition)
		require.True(t, ok)
		out[i] = relationtuple.BatchInput{Action: d.Action, Tuple: d.RelationTuple, Condition: cond, ExpectedVersion: d.ExpectedVersion}
	}
	return out
}

// Failure then immediate retry converges: after an injected mid-batch failure
// rolls everything back, retrying the same batch without the fault succeeds,
// produces no duplicates and never reported rolled-back items as success.
func TestBatchRetryAfterFailureConverges(t *testing.T) {
	h := newBatchHarness(t)
	const n = 130
	deltas := make([]*ketoapi.PatchDelta, n)
	for i := range n {
		deltas[i] = insertDelta(h.sidTuple(fmt.Sprintf("rc-%03d", i), "s"))
	}
	inputs := mustInputs(t, deltas)
	svc := relationtuple.NewBatchWriteService(h.reg)

	failHook := func(phase string, chunkIndex, from, to int) error {
		if phase == "insert" && chunkIndex == 1 {
			return errors.New("injected transient storage failure")
		}
		return nil
	}
	failCtx := relationtuple.WithBatchChunkHook(context.Background(), failHook)
	_, err := svc.Execute(failCtx, &relationtuple.BatchWriteRequest{Inputs: inputs})
	require.Error(t, err)
	be, ok := relationtuple.AsBatchError(err)
	require.True(t, ok)
	assert.Equal(t, relationtuple.BatchErrStorage, be.Kind)
	assert.Equal(t, 0, h.tupleCountInNamespace(), "rolled-back batch leaves zero rows")

	// Immediate retry of the SAME batch without the fault converges.
	resp, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: inputs})
	require.NoError(t, err)
	require.Len(t, resp.Results, n)
	assert.Equal(t, relationtuple.OutcomeCreated, resp.Results[0].Outcome)
	assert.Equal(t, n, h.tupleCountInNamespace())

	// A third run is fully idempotent at the data level (all unchanged).
	resp3, err := svc.Execute(context.Background(), &relationtuple.BatchWriteRequest{Inputs: inputs})
	require.NoError(t, err)
	for _, r := range resp3.Results {
		assert.Equal(t, relationtuple.OutcomeUnchanged, r.Outcome)
	}
	assert.Equal(t, n, h.tupleCountInNamespace(), "retry never duplicates data")
}

// Hard constraint concurrency: two batches from different connection pools
// (the multi-instance deployment shape) overlap on the same logical relation.
// Exactly one wins and the other gets a retryable classified conflict; the
// unique identity index leaves one row. A start barrier makes the overlap
// deterministic. Run with -race.
func TestBatchConcurrentOverlap(t *testing.T) {
	dir := t.TempDir()
	dsn := dbal.NewSQLiteDatabase(dir)
	dsnT := &dbx.DsnT{Conn: dsn, Name: "sqlite", MigrateUp: true}
	ns := "race-ns"
	reg1 := driver.NewTestRegistry(t, dsnT, driver.WithNamespaces(namespaces(ns)))
	reg2 := driver.NewTestRegistry(t, dsnT, driver.WithNamespaces(namespaces(ns)))

	obj := uuid.Must(uuid.NewV4())
	sub := uuid.Must(uuid.NewV4())
	newExec := func() *relationtuple.BatchExecution {
		return &relationtuple.BatchExecution{Items: []relationtuple.BatchItem{{
			Action: relationtuple.BatchActionInsert,
			Tuple: &relationtuple.RelationTuple{
				Namespace: ns, Object: obj, Relation: "member",
				Subject: &relationtuple.SubjectID{ID: sub},
			},
		}}}
	}

	var entered atomic.Int32
	bothIn := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	hook := func(phase string, chunkIndex, from, to int) error {
		if phase != "insert" {
			return nil
		}
		if entered.Add(1) == 2 {
			once.Do(func() { close(bothIn) })
		}
		select {
		case <-release:
		case <-time.After(15 * time.Second):
			t.Error("barrier timed out")
		}
		return nil
	}
	ctx := relationtuple.WithBatchChunkHook(context.Background(), hook)

	type result struct{ err error }
	resCh := make(chan result, 2)
	go func() {
		_, _, e := reg1.RelationTupleManager().BatchWrite(ctx, newExec(), "", "", "", 0)
		resCh <- result{e}
	}()
	go func() {
		_, _, e := reg2.RelationTupleManager().BatchWrite(ctx, newExec(), "", "", "", 0)
		resCh <- result{e}
	}()

	select {
	case <-bothIn:
	case <-time.After(20 * time.Second):
		close(release)
		t.Fatal("both batches did not reach the flush barrier")
	}
	close(release)

	var success, conflict int
	for range 2 {
		select {
		case r := <-resCh:
			if r.err == nil {
				success++
				continue
			}
			be, ok := relationtuple.AsBatchError(r.err)
			require.True(t, ok, "expected classified conflict, got %T %v", r.err, r.err)
			assert.Equal(t, relationtuple.BatchErrConcurrent, be.Kind)
			assert.True(t, be.Retryable)
			conflict++
		case <-time.After(30 * time.Second):
			t.Fatal("batch did not return after release")
		}
	}
	assert.Equal(t, 1, success, "exactly one batch succeeds")
	assert.Equal(t, 1, conflict, "exactly one batch gets a retryable conflict")

	rows, _, err := reg1.RelationTupleManager().GetRelationTuples(context.Background(),
		&relationtuple.RelationQuery{Namespace: &ns}, keysetpagination.WithSize(100))
	require.NoError(t, err)
	assert.Len(t, rows, 1, "unique identity leaves exactly one row")
}
