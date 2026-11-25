// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"github.com/gofrs/uuid"
)

// StoredRow is one currently persisted relation version used to seed the
// planner. The unique identity index guarantees at most one row per TupleKey.
type StoredRow struct {
	Key     TupleKey
	ShardID uuid.UUID
}

// PlannedMutation is one net persistence operation. Index records the request
// item that decided it, used to report safe failure intervals.
type PlannedMutation struct {
	Index   int
	Key     TupleKey
	Tuple   *RelationTuple
	ShardID uuid.UUID
}

// BatchPlan holds per-item outcomes and the collapsed net mutations.
type BatchPlan struct {
	Results []ItemResult
	Inserts []PlannedMutation
	Deletes []PlannedMutation
}

type planKeyState struct {
	key TupleKey

	origShard *uuid.UUID // nil when absent before the batch
	exists    bool
	shard     uuid.UUID // current version when exists == true
	isNew     bool      // current shard was minted inside this batch
	row       *RelationTuple

	createdIndex int
	deletedIndex int
}

// PlanBatch resolves an ordered mixed insert/delete batch against the rows
// currently stored. It is a pure, deterministic function (shard generation is
// injected) so every ordering/dedup/condition/token rule can be unit tested.
//
// Semantics:
//   - items are evaluated strictly in request order against read-your-writes
//     state accumulated from earlier items;
//   - the last action for a relation decides its final state;
//   - exact duplicate items naturally collapse to one stored row;
//   - any failed precondition/token aborts with the item index before the
//     caller issues any write.
func PlanBatch(items []BatchItem, stored []StoredRow, nid uuid.UUID, codec *BatchTokenCodec, newShard func() uuid.UUID) (*BatchPlan, *BatchError) {
	states := make(map[TupleKey]*planKeyState, len(items))
	order := make([]TupleKey, 0, len(items))
	getState := func(k TupleKey) *planKeyState {
		st, ok := states[k]
		if !ok {
			st = &planKeyState{key: k}
			states[k] = st
			order = append(order, k)
		}
		return st
	}

	for i := range stored {
		r := stored[i]
		shard := r.ShardID
		st := getState(r.Key)
		st.origShard = &shard
		st.exists = true
		st.shard = shard
		st.isNew = false
	}

	plan := &BatchPlan{Results: make([]ItemResult, 0, len(items))}
	absentToken := func(k TupleKey) string {
		if codec == nil {
			return ""
		}
		return codec.MintAbsent(nid, k)
	}
	presentToken := func(k TupleKey, shard uuid.UUID) string {
		if codec == nil {
			return ""
		}
		return codec.MintPresent(nid, k, shard)
	}

	for i := range items {
		it := items[i]
		if it.Tuple == nil {
			return nil, newBatchError(BatchErrInvalidItem, i)
		}
		k := NewTupleKey(it.Tuple)
		st := getState(k)

		// Optimistic version token is checked first and is the strongest
		// precondition. It is evaluated against in-batch state so ordering is
		// observable.
		if !codec.verifyExpectedToken(it.ExpectedVersion, nid, k, st) {
			return nil, newBatchError(BatchErrVersion, i)
		}

		// Explicit existence precondition.
		switch it.Condition {
		case ConditionAbsent:
			if st.exists {
				e := newBatchError(BatchErrCondition, i)
				e.Expected = string(ConditionAbsent)
				return nil, e
			}
		case ConditionPresent:
			if !st.exists {
				e := newBatchError(BatchErrCondition, i)
				e.Expected = string(ConditionPresent)
				return nil, e
			}
		case ConditionAny:
		default:
			e := newBatchError(BatchErrInvalidItem, i)
			e.Expected = "condition"
			return nil, e
		}

		switch it.Action {
		case BatchActionInsert:
			if st.exists {
				plan.Results = append(plan.Results, ItemResult{
					Index:        i,
					Outcome:      OutcomeUnchanged,
					VersionToken: presentToken(k, st.shard),
				})
				continue
			}

			shard := newShard()
			st.exists = true
			st.shard = shard
			st.isNew = true
			st.row = it.Tuple
			st.createdIndex = i

			plan.Results = append(plan.Results, ItemResult{
				Index:        i,
				Outcome:      OutcomeCreated,
				VersionToken: presentToken(k, shard),
			})

		case BatchActionDelete:
			if !st.exists {
				plan.Results = append(plan.Results, ItemResult{
					Index:        i,
					Outcome:      OutcomeNotFound,
					VersionToken: absentToken(k),
				})
				continue
			}

			st.exists = false
			st.deletedIndex = i

			plan.Results = append(plan.Results, ItemResult{
				Index:        i,
				Outcome:      OutcomeDeleted,
				VersionToken: absentToken(k),
			})

		default:
			return nil, newBatchError(BatchErrInvalidItem, i)
		}
	}

	// Collapse per-key state into net mutations.
	for _, k := range order {
		st := states[k]
		switch {
		case st.exists && st.isNew:
			// If an original row existed earlier and was deleted in-batch,
			// remove it before inserting the replacement version.
			if st.origShard != nil {
				plan.Deletes = append(plan.Deletes, PlannedMutation{
					Index:   st.deletedIndex,
					Key:     k,
					Tuple:   nil,
					ShardID: *st.origShard,
				})
			}
			plan.Inserts = append(plan.Inserts, PlannedMutation{
				Index:   st.createdIndex,
				Key:     k,
				Tuple:   st.row,
				ShardID: st.shard,
			})
		case !st.exists && st.origShard != nil:
			plan.Deletes = append(plan.Deletes, PlannedMutation{
				Index:   st.deletedIndex,
				Key:     k,
				ShardID: *st.origShard,
			})
		}
	}

	return plan, nil
}
