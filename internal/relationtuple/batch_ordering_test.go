package relationtuple_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/ketoapi"
)

func outcomes(resp httpBatchResponse) []string {
	out := make([]string, len(resp.Results))
	for i := range resp.Results {
		out[i] = resp.Results[i].Outcome
	}
	return out
}

type batchCase struct {
	name        string
	obj         string
	preExisting bool
	actions     []string // "i" insert or "d" delete, all on the same tuple
	want        []string
	wantRows    int
}

func runSingleKeyCase(t *testing.T, c batchCase) {
	t.Helper()
	h := newBatchHarness(t)
	rt := h.sidTuple(c.obj, "s")
	if c.preExisting {
		testhelpers.MapAndInsertTuples(t, h.reg, rt)
	}
	deltas := make([]*ketoapi.PatchDelta, 0, len(c.actions))
	for _, a := range c.actions {
		if a == "i" {
			deltas = append(deltas, insertDelta(rt))
		} else {
			deltas = append(deltas, deleteDelta(rt))
		}
	}

	resp, body := h.patch(deltas, "")
	require.Equal(t, 200, resp.StatusCode)
	require.Len(t, body.Results, len(c.actions))
	for i, r := range body.Results {
		assert.Equal(t, i, r.Index)
		assert.NotEmpty(t, r.VersionToken, "every item returns an opaque token")
	}
	assert.Equal(t, c.want, outcomes(body))
	assert.Len(t, body.Snaptokens, len(c.actions))
	assert.Equal(t, c.wantRows, h.tupleCountInNamespace())
}

// Form 1 + 2 + 3: ordered mixed actions, last-action-wins, duplicates and
// length-preserving per-item outcomes.
func TestBatchOrderingDuplicatesAndOutcomes(t *testing.T) {
	cases := []batchCase{
		{name: "add then delete on absent", obj: "o-ad", actions: []string{"i", "d"}, want: []string{"created", "deleted"}, wantRows: 0},
		{name: "delete then add on absent", obj: "o-da", actions: []string{"d", "i"}, want: []string{"not_found", "created"}, wantRows: 1},
		{name: "add existing unchanged then delete", obj: "o-ed", preExisting: true, actions: []string{"i", "d"}, want: []string{"unchanged", "deleted"}, wantRows: 0},
		{name: "delete then add on existing", obj: "o-dea", preExisting: true, actions: []string{"d", "i"}, want: []string{"deleted", "created"}, wantRows: 1},
		{name: "exact duplicate inserts", obj: "o-dup", actions: []string{"i", "i"}, want: []string{"created", "unchanged"}, wantRows: 1},
		{name: "duplicate deletes", obj: "o-dupd", preExisting: true, actions: []string{"d", "d"}, want: []string{"deleted", "not_found"}, wantRows: 0},
		{name: "three repeats end delete", obj: "o-three", actions: []string{"i", "i", "d"}, want: []string{"created", "unchanged", "deleted"}, wantRows: 0},
		{name: "delete add delete add last wins present", obj: "o-dada", actions: []string{"d", "i", "d", "i"}, want: []string{"not_found", "created", "deleted", "created"}, wantRows: 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { runSingleKeyCase(t, c) })
	}
}

// Grouping invariance: an interleaved request must not become insert-all then
// delete-all.
func TestBatchMixedOrderNotGrouped(t *testing.T) {
	h := newBatchHarness(t)

	rA := h.sidTuple("A", "s")
	rB := h.sidTuple("B", "s")
	rC := h.sidTuple("C", "s")

	deltas := []*ketoapi.PatchDelta{
		insertDelta(rA), // created
		deleteDelta(rB), // not_found
		insertDelta(rB), // created
		deleteDelta(rA), // deleted
		insertDelta(rC), // created
	}
	resp, body := h.patch(deltas, "")
	require.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, []string{"created", "not_found", "created", "deleted", "created"}, outcomes(body))

	stored := h.storedAPITuples()
	assert.ElementsMatch(t, []*ketoapi.RelationTuple{rB, rC}, stored)
}
