package relationtuple_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/driver/config"
)

func nDeltas(h *batchHarness, n int) []map[string]any {
	out := make([]map[string]any, n)
	for i := range n {
		out[i] = map[string]any{
			"action": "insert",
			"relation_tuple": map[string]any{
				"namespace": h.ns, "object": "o", "relation": "member",
				"subject_id": "s-" + itoa(i),
			},
		}
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// Form 4: configurable cap; exactly-at-limit allowed, over-limit rejected
// before any DB write and the limit is reported.
func TestBatchSizeLimitBoundary(t *testing.T) {
	const capN = 3
	h := newBatchHarness(t, driver.WithConfig(config.KeyMaxWriteBatchSize, capN))

	// Exactly the limit passes.
	resp, body := h.patchRawMustJSON(t, nDeltas(h, capN))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, body.Results, capN)
	assert.Equal(t, capN, h.tupleCountInNamespace())

	// One over is rejected with the cap stated and writes nothing.
	before := h.tupleCountInNamespace()
	raw, err := json.Marshal(nDeltas(h, capN+1))
	require.NoError(t, err)
	r, err := http.NewRequest(http.MethodPatch, h.ts.URL+"/admin/relation-tuples?detailed=true", bytes.NewReader(raw))
	require.NoError(t, err)
	resp2, err := h.ts.Client().Do(r) //nolint:gosec
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	var errBody map[string]any
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&errBody))
	_ = resp2.Body.Close()
	errObj, _ := errBody["error"].(map[string]any)
	require.NotNil(t, errObj)
	details, _ := errObj["details"].(map[string]any)
	assert.EqualValues(t, capN, details["max_items"])
	assert.Contains(t, errObj["message"], "3")
	assert.Equal(t, before, h.tupleCountInNamespace(), "over-limit batch must not write")
}

func (h *batchHarness) patchRawMustJSON(t *testing.T, v any) (*http.Response, httpBatchResponse) {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	resp := h.patchRaw(string(raw), "", true)
	var out httpBatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	_ = resp.Body.Close()
	return resp, out
}

// Form 4 validation precedence: empty batch, unknown action, missing subject
// are rejected before any database effect.
func TestBatchValidationRejectsFirst(t *testing.T) {
	h := newBatchHarness(t)

	t.Run("empty batch", func(t *testing.T) {
		resp := h.patchRaw("[]", "", true)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		_ = resp.Body.Close()
		assert.Equal(t, 0, h.tupleCountInNamespace())
	})

	t.Run("unknown action", func(t *testing.T) {
		raw := `[{"action":"unknown_action_foo","relation_tuple":{"namespace":"` + h.ns + `","object":"o","relation":"member","subject_id":"s"}}]`
		resp := h.patchRaw(raw, "", true)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = resp.Body.Close()
		assert.Contains(t, buf.String(), "unknown_action_foo")
		assert.Equal(t, 0, h.tupleCountInNamespace())
	})

	t.Run("missing relation_tuple", func(t *testing.T) {
		raw := `[{"action":"insert"}]`
		resp := h.patchRaw(raw, "", true)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		_ = resp.Body.Close()
		assert.Contains(t, buf.String(), "relation_tuple is missing")
		assert.Equal(t, 0, h.tupleCountInNamespace())
	})

	t.Run("missing subject rejected", func(t *testing.T) {
		raw := `[{"action":"insert","relation_tuple":{"namespace":"` + h.ns + `","object":"o","relation":"member"}}]`
		resp := h.patchRaw(raw, "", true)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		_ = resp.Body.Close()
		assert.Equal(t, 0, h.tupleCountInNamespace())
	})
}

// Default configuration keeps accepting thousand-item batches.
func TestBatchDefaultAllowsThousand(t *testing.T) {
	if testing.Short() {
		t.Skip("thousand-item smoke is fine but keep -short fast")
	}
	h := newBatchHarness(t)
	resp, body := h.patchRawMustJSON(t, nDeltas(h, 1000))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, body.Results, 1000)
	assert.Equal(t, 1000, h.tupleCountInNamespace())
}
