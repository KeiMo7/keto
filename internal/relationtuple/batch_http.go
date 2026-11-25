// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"encoding/json"
	"net/http"

	"github.com/ory/keto/ketoapi"
)

// HTTP JSON shapes for the rich batch contract. They are additive: the legacy
// PATCH array request and its default 204 response remain unchanged.

type batchItemResultJSON struct {
	Index        int    `json:"index"`
	Outcome      string `json:"outcome"`
	Status       string `json:"status"`
	VersionToken string `json:"version_token"`
	Snaptoken    string `json:"snaptoken"`
}

type batchWriteResponseJSON struct {
	IdempotencyKey string                `json:"idempotency_key,omitempty"`
	Replayed       bool                  `json:"replayed"`
	Results        []batchItemResultJSON `json:"results"`
	Snaptokens     []string              `json:"snaptokens"`
}

type batchWriteBodyJSON struct {
	IdempotencyKey string `json:"idempotency_key"`

	Items               []*ketoapi.PatchDelta `json:"items"`
	Deltas              []*ketoapi.PatchDelta `json:"deltas"`
	RelationTupleDeltas []*ketoapi.PatchDelta `json:"relation_tuple_deltas"`
}

func toJSONResponse(key string, resp *BatchWriteResponse) batchWriteResponseJSON {
	out := batchWriteResponseJSON{
		IdempotencyKey: key,
		Results:        make([]batchItemResultJSON, 0),
		Snaptokens:     []string{},
	}
	if resp == nil {
		return out
	}
	out.Replayed = resp.Replayed
	out.Results = make([]batchItemResultJSON, len(resp.Results))
	out.Snaptokens = make([]string, len(resp.Results))
	for i, r := range resp.Results {
		out.Results[i] = batchItemResultJSON{
			Index:        r.Index,
			Outcome:      string(r.Outcome),
			Status:       string(r.Outcome),
			VersionToken: r.VersionToken,
			Snaptoken:    r.VersionToken,
		}
		out.Snaptokens[i] = r.VersionToken
	}
	return out
}

// writeBatchError renders batch errors through the stable herodot family and
// leaves every other error on the default path. Raw driver errors are never
// exposed because BatchError.Herodot renders only safe, classified messages.
func (h *WriteHandler) writeBatchError(w http.ResponseWriter, r *http.Request, err error) {
	if be, ok := AsBatchError(err); ok {
		h.d.Writer().WriteError(w, r, be.Herodot())
		return
	}
	h.d.Writer().WriteError(w, r, err)
}

// deltasToInputs converts HTTP patch deltas into entry-neutral inputs.
func deltasToInputs(deltas []*ketoapi.PatchDelta) ([]BatchInput, error) {
	inputs := make([]BatchInput, len(deltas))
	for i, d := range deltas {
		if d == nil {
			return nil, NewBatchInvalidItemReason(i, "relation_tuple is missing")
		}
		cond, ok := ParseCondition(d.Condition)
		if !ok {
			return nil, NewBatchInvalidItemReason(i, "unknown condition "+d.Condition)
		}
		inputs[i] = BatchInput{
			Action:          d.Action,
			Tuple:           d.RelationTuple,
			Condition:       cond,
			ExpectedVersion: d.ExpectedVersion,
		}
	}
	return inputs, nil
}

func idempotencyKeyFromRequest(r *http.Request, bodyKey string) string {
	if bodyKey != "" {
		return bodyKey
	}
	if k := r.Header.Get(IdempotencyKeyHeader); k != "" {
		return k
	}
	return r.Header.Get(IdempotencyKeyHeaderAlt)
}

// wantsDetailedResponse reports whether the client opted into the rich JSON
// outcome body. New-capability requests (idempotency key or conditional
// fields) also receive it automatically.
func wantsDetailedResponse(r *http.Request, deltas []*ketoapi.PatchDelta, key string) bool {
	q := r.URL.Query()
	if v := q.Get(DetailedResponseQuery); v == "1" || v == "true" || v == "detailed" {
		return true
	}
	if r.Header.Get("X-Batch-Detailed") != "" {
		return true
	}
	if pref := r.Header.Get("Prefer"); pref != "" && containsToken(pref, DetailedResponsePreference) {
		return true
	}
	if key != "" {
		return true
	}
	for _, d := range deltas {
		if d != nil && (d.Condition != "" || d.ExpectedVersion != "") {
			return true
		}
	}
	return false
}

func containsToken(header, token string) bool {
	for i := 0; i+len(token) <= len(header); i++ {
		if header[i:i+len(token)] == token {
			return true
		}
	}
	return false
}

// writeBatch is the explicit rich batch endpoint:
//
//	POST /admin/relation-tuples/batch
//
// It always returns 200 with request-order per-item outcomes.
func (h *WriteHandler) writeBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var body batchWriteBodyJSON
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.d.Writer().WriteError(w, r, newBatchError(BatchErrInvalidItem, -1).
			withReasonMessage("request body must be a valid batch JSON object"))
		return
	}

	deltas := body.Items
	if len(deltas) == 0 {
		deltas = body.Deltas
	}
	if len(deltas) == 0 {
		deltas = body.RelationTupleDeltas
	}

	key := idempotencyKeyFromRequest(r, body.IdempotencyKey)
	inputs, err := deltasToInputs(deltas)
	if err != nil {
		h.writeBatchError(w, r, err)
		return
	}

	resp, err := NewBatchWriteService(h.d).Execute(ctx, &BatchWriteRequest{
		Inputs:         inputs,
		IdempotencyKey: key,
	})
	if err != nil {
		h.writeBatchError(w, r, err)
		return
	}

	h.d.Writer().WriteCode(w, r, http.StatusOK, toJSONResponse(key, resp))
}

// withReasonMessage is a small helper for malformed-envelope errors.
func (e *BatchError) withReasonMessage(reason string) *BatchError {
	e.Reason = reason
	return e
}
