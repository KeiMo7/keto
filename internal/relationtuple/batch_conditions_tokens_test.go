package relationtuple_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/ketoapi"
)

func conditionalDelta(d *ketoapi.PatchDelta, condition, version string) *ketoapi.PatchDelta {
	d.Condition = condition
	d.ExpectedVersion = version
	return d
}

// Form 6: existence preconditions abort the whole batch with the item index.
func TestBatchConditions(t *testing.T) {
	t.Run("insert if_absent rejects existing at index", func(t *testing.T) {
		h := newBatchHarness(t)
		rt := h.sidTuple("cond-1", "s")
		_, body0 := h.patch([]*ketoapi.PatchDelta{insertDelta(rt)}, "")
		require.Equal(t, "created", body0.Results[0].Outcome)

		deltas := []*ketoapi.PatchDelta{
			insertDelta(h.sidTuple("cond-other", "s")),
			conditionalDelta(insertDelta(rt), "if_absent", ""),
		}
		status, errBody := h.patchError(deltas, "")
		assert.Equal(t, http.StatusConflict, status)
		errObj, _ := errBody["error"].(map[string]any)
		details, _ := errObj["details"].(map[string]any)
		assert.EqualValues(t, "condition_failed", details["error_kind"])
		assert.EqualValues(t, 1, details["item_index"])
		// Whole batch rejected: the first item must not be present either.
		assert.Equal(t, 1, h.tupleCountInNamespace(), "only the pre-existing row remains")
	})

	t.Run("delete if_present rejects absent and rolls back", func(t *testing.T) {
		h := newBatchHarness(t)
		present := h.sidTuple("cond-present", "s")
		_, b0 := h.patch([]*ketoapi.PatchDelta{insertDelta(present)}, "")
		require.Equal(t, "created", b0.Results[0].Outcome)

		absent := h.sidTuple("cond-absent", "s")
		deltas := []*ketoapi.PatchDelta{
			deleteDelta(present),
			conditionalDelta(deleteDelta(absent), "if_present", ""),
		}
		status, errBody := h.patchError(deltas, "")
		assert.Equal(t, http.StatusConflict, status)
		details, _ := errBody["error"].(map[string]any)["details"].(map[string]any)
		assert.EqualValues(t, 1, details["item_index"])
		// Rollback: present tuple still there.
		assert.Equal(t, 1, h.tupleCountInNamespace())
	})

	t.Run("valid preconditions pass", func(t *testing.T) {
		h := newBatchHarness(t)
		rt := h.sidTuple("cond-ok", "s")
		resp, body := h.patch([]*ketoapi.PatchDelta{
			conditionalDelta(insertDelta(rt), "if_absent", ""),
		}, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "created", body.Results[0].Outcome)

		resp2, body2 := h.patch([]*ketoapi.PatchDelta{
			conditionalDelta(deleteDelta(rt), "if_present", ""),
		}, "")
		require.Equal(t, http.StatusOK, resp2.StatusCode)
		assert.Equal(t, "deleted", body2.Results[0].Outcome)
		assert.Equal(t, 0, h.tupleCountInNamespace())
	})
}

// Form 7: opaque version tokens round-trip and mismatch conflicts.
func TestBatchVersionTokens(t *testing.T) {
	h := newBatchHarness(t)
	rt := h.sidTuple("ver-1", "s")

	_, created := h.patch([]*ketoapi.PatchDelta{insertDelta(rt)}, "")
	token := created.Results[0].VersionToken
	require.NotEmpty(t, token)
	assert.Contains(t, token, "kbt1.")
	assert.NotContains(t, token, "ver-1", "token must not embed relation content")
	assert.NotContains(t, token, h.ns)

	// Same token reused for an unconditional repeat still matches current row.
	resp, body := h.patch([]*ketoapi.PatchDelta{
		conditionalDelta(insertDelta(rt), "", token),
	}, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "unchanged", body.Results[0].Outcome)

	// Delete returns an absent-state token.
	_, deleted := h.patch([]*ketoapi.PatchDelta{deleteDelta(rt)}, "")
	require.Equal(t, "deleted", deleted.Results[0].Outcome)
	absentToken := deleted.Results[0].VersionToken
	require.NotEqual(t, token, absentToken)

	// The stale present token no longer matches the absent state -> conflict at index.
	statusConflict, errConflict := h.patchError([]*ketoapi.PatchDelta{
		conditionalDelta(insertDelta(rt), "", token),
	}, "")
	assert.Equal(t, http.StatusConflict, statusConflict)
	details, _ := errConflict["error"].(map[string]any)["details"].(map[string]any)
	assert.EqualValues(t, "version_conflict", details["error_kind"])
	assert.EqualValues(t, 0, details["item_index"])
	assert.Equal(t, 0, h.tupleCountInNamespace(), "version conflict writes nothing")

	// A forged/garbage token is also a version conflict, not a 500.
	statusForged, _ := h.patchError([]*ketoapi.PatchDelta{
		conditionalDelta(insertDelta(h.sidTuple("ver-2", "s")), "", "kbt1.not-a-valid-token"),
	}, "")
	assert.Equal(t, http.StatusConflict, statusForged)
}
