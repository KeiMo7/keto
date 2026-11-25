package relationtuple_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ory/x/httprouterx"
	keysetpagination "github.com/ory/x/pagination/keysetpagination_v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/driver"
	"github.com/ory/keto/internal/driver/config"
	"github.com/ory/keto/internal/namespace"
	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/ketoapi"
)

func gatherBatchMetricNames() ([]string, error) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	return names, nil
}

func bptr(s string) *string { return &s }

type batchHarness struct {
	t   *testing.T
	ts  *httptest.Server
	reg *driver.RegistryDefault
	ns  string
}

func newBatchHarness(t *testing.T, opts ...driver.TestRegistryOption) *batchHarness {
	t.Helper()
	r := httprouterx.NewRouterAdmin()
	reg := driver.NewSqliteTestRegistry(t, opts...)
	ns := t.Name() + "-ns"
	require.NoError(t, reg.Config(t.Context()).Set(config.KeyNamespaces, []*namespace.Namespace{{Name: ns}}))
	relationtuple.NewWriteHandler(reg).RegisterWriteRoutes(r)
	relationtuple.NewReadHandler(reg).RegisterReadRoutes(r.ToPublic())
	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)
	return &batchHarness{t: t, ts: ts, reg: reg, ns: ns}
}

func (h *batchHarness) sidTuple(obj, subj string) *ketoapi.RelationTuple {
	return &ketoapi.RelationTuple{Namespace: h.ns, Object: obj, Relation: "member", SubjectID: bptr(subj)}
}

func insertDelta(rt *ketoapi.RelationTuple) *ketoapi.PatchDelta {
	return &ketoapi.PatchDelta{Action: ketoapi.ActionInsert, RelationTuple: rt}
}

func deleteDelta(rt *ketoapi.RelationTuple) *ketoapi.PatchDelta {
	return &ketoapi.PatchDelta{Action: ketoapi.ActionDelete, RelationTuple: rt}
}

type httpItemResult struct {
	Index        int    `json:"index"`
	Outcome      string `json:"outcome"`
	Status       string `json:"status"`
	VersionToken string `json:"version_token"`
}

type httpBatchResponse struct {
	Replayed   bool             `json:"replayed"`
	Results    []httpItemResult `json:"results"`
	Snaptokens []string         `json:"snaptokens"`
}

func (h *batchHarness) patchRaw(raw string, key string, detailed bool) *http.Response {
	h.t.Helper()
	url := relationtuple.WriteRouteBase
	if detailed {
		url += "?detailed=true"
	}
	req, err := http.NewRequest(http.MethodPatch, h.ts.URL+url, bytes.NewBufferString(raw))
	require.NoError(h.t, err)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := h.ts.Client().Do(req) //nolint:gosec
	require.NoError(h.t, err)
	return resp
}

func (h *batchHarness) patch(deltas []*ketoapi.PatchDelta, key string) (*http.Response, httpBatchResponse) {
	h.t.Helper()
	raw, err := json.Marshal(deltas)
	require.NoError(h.t, err)
	resp := h.patchRaw(string(raw), key, true)
	var out httpBatchResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	_ = resp.Body.Close()
	return resp, out
}

// patchError performs a detailed PATCH and returns status + decoded body.
func (h *batchHarness) patchError(deltas []*ketoapi.PatchDelta, key string) (int, map[string]any) {
	h.t.Helper()
	raw, err := json.Marshal(deltas)
	require.NoError(h.t, err)
	resp := h.patchRaw(string(raw), key, true)
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func (h *batchHarness) postBatch(body any) (*http.Response, map[string]any) {
	h.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(h.t, err)
	req, err := http.NewRequest(http.MethodPost, h.ts.URL+relationtuple.WriteRouteBase+"/batch", bytes.NewReader(raw))
	require.NoError(h.t, err)
	resp, err := h.ts.Client().Do(req) //nolint:gosec
	require.NoError(h.t, err)
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	_ = resp.Body.Close()
	return resp, decoded
}

// tupleCountInNamespace returns the number of stored internal rows, using a
// raw COUNT so pagination caps cannot truncate the total.
func (h *batchHarness) tupleCountInNamespace() int {
	h.t.Helper()
	var rows []struct {
		C int64 `db:"c"`
	}
	nid := h.reg.Persister().NetworkID(context.Background())
	err := h.reg.Persister().Connection(context.Background()).RawQuery(
		"SELECT COUNT(*) AS c FROM keto_relation_tuples WHERE namespace = ? AND nid = ?", h.ns, nid,
	).All(&rows)
	require.NoError(h.t, err)
	require.Len(h.t, rows, 1)
	return int(rows[0].C)
}

// storedAPITuples maps all stored rows back to API tuples for comparison.
func (h *batchHarness) storedAPITuples() []*ketoapi.RelationTuple {
	h.t.Helper()
	rows, _, err := h.reg.RelationTupleManager().GetRelationTuples(context.Background(),
		&relationtuple.RelationQuery{Namespace: &h.ns}, keysetpagination.WithSize(10000))
	require.NoError(h.t, err)
	out, err := h.reg.Mapper().ToTuple(context.Background(), rows...)
	require.NoError(h.t, err)
	return out
}
