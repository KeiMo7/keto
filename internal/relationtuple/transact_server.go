// Copyright © 2023 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"context"
	"encoding/json"
	"net/http"

	"connectrpc.com/connect"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"

	"github.com/ory/herodot"

	rts "github.com/ory/keto/gen/go/ory/keto/relation_tuples/v1alpha2"
	"github.com/ory/keto/gen/go/ory/keto/relation_tuples/v1alpha2/relationtuplesconnect"
	"github.com/ory/keto/internal/x/validate"
	"github.com/ory/keto/ketoapi"
	"github.com/ory/keto/x/events"
)

var _ relationtuplesconnect.WriteServiceHandler = (*WriteHandler)(nil)

func protoDeltaToBatchInputs(deltas []*rts.RelationTupleDelta) ([]BatchInput, error) {
	inputs := make([]BatchInput, len(deltas))
	for i, d := range deltas {
		if d == nil {
			return nil, NewBatchInvalidItemReason(i, "relation_tuple_delta is missing").Herodot()
		}

		var action ketoapi.PatchAction
		switch d.Action {
		case rts.RelationTupleDelta_ACTION_INSERT:
			action = ketoapi.ActionInsert
		case rts.RelationTupleDelta_ACTION_DELETE:
			action = ketoapi.ActionDelete
		default:
			return nil, NewBatchInvalidItemReason(i, "unknown or unspecified action").Herodot()
		}

		it, err := (&ketoapi.RelationTuple{}).FromDataProvider(d.RelationTuple)
		if err != nil {
			return nil, err
		}

		var condition BatchCondition
		switch d.Condition {
		case rts.RelationTupleDeltaCondition_RELATION_TUPLE_DELTA_CONDITION_IF_ABSENT:
			condition = ConditionAbsent
		case rts.RelationTupleDeltaCondition_RELATION_TUPLE_DELTA_CONDITION_IF_PRESENT:
			condition = ConditionPresent
		default:
			condition = ConditionAny
		}

		inputs[i] = BatchInput{
			Action:          action,
			Tuple:           it,
			Condition:       condition,
			ExpectedVersion: d.ExpectedVersion,
		}
	}
	return inputs, nil
}

func (h *WriteHandler) TransactRelationTuples(ctx context.Context, req *connect.Request[rts.TransactRelationTuplesRequest]) (*connect.Response[rts.TransactRelationTuplesResponse], error) {
	inputs, err := protoDeltaToBatchInputs(req.Msg.RelationTupleDeltas)
	if err != nil {
		return nil, batchErrorToHerodot(err)
	}

	key := req.Msg.IdempotencyKey
	if key == "" {
		key = req.Header().Get(IdempotencyKeyHeader)
		if key == "" {
			key = req.Header().Get(IdempotencyKeyHeaderAlt)
		}
	}

	resp, err := NewBatchWriteService(h.d).Execute(ctx, &BatchWriteRequest{
		Inputs:         inputs,
		IdempotencyKey: key,
	})
	if err != nil {
		return nil, batchErrorToHerodot(err)
	}

	results := make([]*rts.BatchItemResult, len(resp.Results))
	snaptokens := make([]string, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = &rts.BatchItemResult{
			Index:        int32(r.Index),
			Outcome:      string(r.Outcome),
			VersionToken: r.VersionToken,
		}
		snaptokens[i] = r.VersionToken
	}

	return connect.NewResponse(&rts.TransactRelationTuplesResponse{
		Snaptokens: snaptokens,
		Results:    results,
		Replayed:   resp.Replayed,
	}), nil
}

// batchErrorToHerodot keeps the connect error classification identical to the
// HTTP entry point.
func batchErrorToHerodot(err error) error {
	if be, ok := AsBatchError(err); ok {
		return be.Herodot()
	}
	return err
}

func (h *WriteHandler) DeleteRelationTuples(ctx context.Context, req *connect.Request[rts.DeleteRelationTuplesRequest]) (*connect.Response[rts.DeleteRelationTuplesResponse], error) {
	var q ketoapi.RelationQuery

	switch {
	case req.Msg.RelationQuery != nil:
		q.FromDataProvider(&queryWrapper{req.Msg.RelationQuery})
		//lint:ignore SA1019 required for compatibility
	case req.Msg.Query != nil: //nolint:staticcheck
		//lint:ignore SA1019 backwards compatibility
		q.FromDataProvider(&deprecatedQueryWrapper{(*rts.ListRelationTuplesRequest_Query)(req.Msg.Query)}) //nolint:staticcheck
	default:
		return nil, errors.WithStack(herodot.ErrBadRequest().WithReason("invalid request"))
	}

	iq, err := h.d.ReadOnlyMapper().FromQuery(ctx, &q)
	if err != nil {
		return nil, err
	}
	if err := h.d.RelationTupleManager().DeleteAllRelationTuples(ctx, iq); err != nil {
		return nil, errors.WithStack(herodot.ErrInternalServerError().WithError(err.Error()))
	}

	trace.SpanFromContext(ctx).AddEvent(events.NewRelationtuplesDeleted(ctx))

	return connect.NewResponse(&rts.DeleteRelationTuplesResponse{}), nil
}

// Create Relationship Request Parameters
//
// swagger:parameters createRelationship
//
//lint:ignore U1000 required for OpenAPI
type createRelationship struct {
	// in: body
	Body createRelationshipBody
}

// Create Relationship Request Body
//
// swagger:model createRelationshipBody
type createRelationshipBody struct {
	ketoapi.RelationQuery
}

// swagger:route PUT /admin/relation-tuples relationship createRelationship
//
// # Create a Relationship
//
// Use this endpoint to create a relationship.
//
//	Consumes:
//	-  application/json
//
//	Produces:
//	- application/json
//
//	Schemes: http, https
//
//	Responses:
//	  201: relationship
//	  400: errorGeneric
//	  default: errorGeneric
//
//	Extensions:
//	  x-ory-ratelimit-bucket: keto-admin-low
func (h *WriteHandler) createRelation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var rt ketoapi.RelationTuple
	if err := json.NewDecoder(r.Body).Decode(&rt); err != nil {
		h.d.Writer().WriteError(w, r, errors.WithStack(herodot.ErrBadRequest().WithError(err.Error())))
		return
	}

	if err := rt.Validate(); err != nil {
		h.d.Writer().WriteError(w, r, err)
		return
	}

	h.d.Logger().WithFields(rt.ToLoggerFields()).Debug("creating relation tuple")

	err := h.d.RelationTupleManager().Transaction(ctx, func(ctx context.Context) error {
		it, err := h.d.Mapper().FromTuple(ctx, &rt)
		if err != nil {
			h.d.Logger().WithError(err).WithFields(rt.ToLoggerFields()).Errorf("could not map relation tuple to UUIDs")
			return err
		}
		if err := h.d.RelationTupleManager().WriteRelationTuples(ctx, it...); err != nil {
			h.d.Logger().WithError(err).WithFields(rt.ToLoggerFields()).Errorf("got an error while creating the relation tuple")
			return err
		}
		return nil
	})

	if err != nil {
		h.d.Writer().WriteError(w, r, err)
		return
	}

	trace.SpanFromContext(ctx).AddEvent(events.NewRelationtuplesCreated(ctx))

	h.d.Writer().WriteCreated(w, r,
		ReadRouteBase+"?"+rt.ToURLQuery().Encode(),
		&rt,
	)
}

// swagger:route DELETE /admin/relation-tuples relationship deleteRelationships
//
// # Delete Relationships
//
// Use this endpoint to delete relationships
//
//	Consumes:
//	-  application/x-www-form-urlencoded
//
//	Produces:
//	- application/json
//
//	Schemes: http, https
//
//	Responses:
//	  204: emptyResponse
//	  400: errorGeneric
//	  default: errorGeneric
//
//	Extensions:
//	  x-ory-ratelimit-bucket: keto-admin-low
func (h *WriteHandler) deleteRelations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if err := validate.All(r,
		validate.NoExtraQueryParams(ketoapi.RelationQueryKeys...),
		validate.QueryParamsContainsOneOf(ketoapi.NamespaceKey),
		validate.HasEmptyBody(),
	); err != nil {
		h.d.Writer().WriteError(w, r, err)
		return
	}

	q := r.URL.Query()
	query, err := (&ketoapi.RelationQuery{}).FromURLQuery(q)
	if err != nil {
		h.d.Writer().WriteError(w, r, herodot.ErrBadRequest().WithError(err.Error()))
		return
	}

	l := h.d.Logger()
	for k := range q {
		l = l.WithField(k, q.Get(k))
	}
	l.Debug("deleting relationships")

	iq, err := h.d.ReadOnlyMapper().FromQuery(ctx, query)
	if err != nil {
		h.d.Logger().WithError(err).Errorf("could not map fields to UUIDs")
		h.d.Writer().WriteError(w, r, err)
		return
	}
	if err := h.d.RelationTupleManager().DeleteAllRelationTuples(ctx, iq); err != nil {
		l.WithError(err).Errorf("got an error while deleting relationships")
		h.d.Writer().WriteError(w, r, herodot.ErrInternalServerError().WithError(err.Error()))
		return
	}

	trace.SpanFromContext(ctx).AddEvent(events.NewRelationtuplesDeleted(ctx))

	w.WriteHeader(http.StatusNoContent)
}

// swagger:route PATCH /admin/relation-tuples relationship patchRelationships
//
// # Patch Multiple Relationships
//
// Use this endpoint to patch one or more relationships.
//
//	Consumes:
//	- application/json
//
//	Produces:
//	- application/json
//
//	Schemes: http, https
//
//	Responses:
//	  204: emptyResponse
//	  400: errorGeneric
//	  404: errorGeneric
//	  default: errorGeneric
//
//	Extensions:
//	  x-ory-ratelimit-bucket: keto-admin-low
func (h *WriteHandler) patchRelationTuples(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var deltas []*ketoapi.PatchDelta
	if err := json.NewDecoder(r.Body).Decode(&deltas); err != nil {
		h.d.Writer().WriteError(w, r, herodot.ErrBadRequest().WithError(err.Error()))
		return
	}

	// Preserve the historical nil-tuple validation message and early reject.
	for i, d := range deltas {
		if d == nil || d.RelationTuple == nil {
			h.d.Writer().WriteError(w, r, NewBatchInvalidItemReason(i, "relation_tuple is missing").Herodot())
			return
		}
	}

	key := idempotencyKeyFromRequest(r, "")
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

	// Default wire behavior is unchanged: 204 No Content. Clients opting into
	// the rich contract (detail parameter, Prefer header, idempotency key or
	// conditional fields) receive 200 with request-order outcomes.
	if !wantsDetailedResponse(r, deltas, key) {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.d.Writer().WriteCode(w, r, http.StatusOK, toJSONResponse(key, resp))
}
