package relationtuple_test

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rts "github.com/ory/keto/gen/go/ory/keto/relation_tuples/v1alpha2"
	"github.com/ory/keto/gen/go/ory/keto/relation_tuples/v1alpha2/relationtuplesconnect"
	"github.com/ory/keto/internal/testhelpers"
	"github.com/ory/keto/ketoapi"
)

func protoDeltas(deltas []*ketoapi.PatchDelta) []*rts.RelationTupleDelta {
	out := make([]*rts.RelationTupleDelta, len(deltas))
	for i, d := range deltas {
		pd := &rts.RelationTupleDelta{RelationTuple: d.RelationTuple.ToProto()}
		if d.Action == ketoapi.ActionDelete {
			pd.Action = rts.RelationTupleDelta_ACTION_DELETE
		} else {
			pd.Action = rts.RelationTupleDelta_ACTION_INSERT
		}
		switch d.Condition {
		case "if_absent":
			pd.Condition = rts.RelationTupleDeltaCondition_RELATION_TUPLE_DELTA_CONDITION_IF_ABSENT
		case "if_present":
			pd.Condition = rts.RelationTupleDeltaCondition_RELATION_TUPLE_DELTA_CONDITION_IF_PRESENT
		}
		pd.ExpectedVersion = d.ExpectedVersion
		out[i] = pd
	}
	return out
}

// Hard constraint single-source: the same ordered batch through HTTP and the
// connect/gRPC entry yields identical outcomes and matching error classes.
func TestBatchTwoEntryParity(t *testing.T) {
	hHTTP := newBatchHarness(t)
	hGRPC := newBatchHarness(t)

	mk := func(h *batchHarness) []*ketoapi.PatchDelta {
		t1 := h.sidTuple("p1", "s")
		t2 := h.sidTuple("p2", "s")
		return []*ketoapi.PatchDelta{
			insertDelta(t1),
			deleteDelta(t2),
			insertDelta(t2),
			insertDelta(t1),
		}
	}

	_, httpBody := hHTTP.patch(mk(hHTTP), "parity-key")
	want := []string{"created", "not_found", "created", "unchanged"}
	assert.Equal(t, want, outcomes(httpBody))

	client := relationtuplesconnect.NewWriteServiceClient(http.DefaultClient, hGRPC.ts.URL)
	deltas := mk(hGRPC)
	gresp, err := client.TransactRelationTuples(context.Background(), connect.NewRequest(
		&rts.TransactRelationTuplesRequest{RelationTupleDeltas: protoDeltas(deltas)},
	))
	require.NoError(t, err)
	require.Len(t, gresp.Msg.Results, len(deltas))
	grpcOutcomes := make([]string, len(deltas))
	for i, r := range gresp.Msg.Results {
		assert.Equal(t, int32(i), r.Index)
		assert.NotEmpty(t, r.VersionToken)
		grpcOutcomes[i] = r.Outcome
	}
	assert.Equal(t, want, grpcOutcomes)
	assert.Len(t, gresp.Msg.Snaptokens, len(deltas))
	assert.False(t, gresp.Msg.Replayed)

	// Same idempotency key over gRPC replays identical results.
	gresp2, err := client.TransactRelationTuples(context.Background(), connect.NewRequest(
		&rts.TransactRelationTuplesRequest{
			RelationTupleDeltas: protoDeltas(deltas),
			IdempotencyKey:      "grpc-key",
		},
	))
	require.NoError(t, err)
	assert.False(t, gresp2.Msg.Replayed)

	gresp3, err := client.TransactRelationTuples(context.Background(), connect.NewRequest(
		&rts.TransactRelationTuplesRequest{
			RelationTupleDeltas: protoDeltas(deltas),
			IdempotencyKey:      "grpc-key",
		},
	))
	require.NoError(t, err)
	assert.True(t, gresp3.Msg.Replayed)
	assert.Equal(t, gresp2.Msg.Snaptokens, gresp3.Msg.Snaptokens)
}

// Error classification parity: a failed condition is HTTP 409 family and
// connect FailedPrecondition.
func TestBatchTwoEntryErrorParity(t *testing.T) {
	hHTTP := newBatchHarness(t)
	hGRPC := newBatchHarness(t)

	rtH := hHTTP.sidTuple("ec", "s")
	testhelpers.MapAndInsertTuples(t, hHTTP.reg, rtH)
	status, errBody := hHTTP.patchError([]*ketoapi.PatchDelta{
		conditionalDelta(insertDelta(rtH), "if_absent", ""),
	}, "")
	assert.Equal(t, http.StatusConflict, status)
	details, _ := errBody["error"].(map[string]any)["details"].(map[string]any)
	assert.EqualValues(t, "condition_failed", details["error_kind"])
	assert.EqualValues(t, 0, details["item_index"])

	rtG := hGRPC.sidTuple("ec", "s")
	testhelpers.MapAndInsertTuples(t, hGRPC.reg, rtG)
	client := relationtuplesconnect.NewWriteServiceClient(http.DefaultClient, hGRPC.ts.URL)
	_, err := client.TransactRelationTuples(context.Background(), connect.NewRequest(
		&rts.TransactRelationTuplesRequest{RelationTupleDeltas: protoDeltas([]*ketoapi.PatchDelta{
			conditionalDelta(insertDelta(rtG), "if_absent", ""),
		})},
	))
	require.Error(t, err)
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeFailedPrecondition, connectErr.Code())
}
