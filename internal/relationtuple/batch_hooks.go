// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import "context"

// BatchChunkHook is an optional test seam invoked before every persistence
// chunk of a batch flush. Returning an error aborts the batch and rolls the
// whole transaction back. It lets rollback/interval assertions inject a
// mid-batch storage failure without driver-specific fault mechanics.
//
// Production requests never set this hook; the context lookup is a no-op.
type BatchChunkHook func(phase string, chunkIndex, fromIndex, toIndex int) error

type batchChunkHookCtxKey struct{}

// WithBatchChunkHook attaches a chunk fault/observation hook to the context.
// Intended for tests only.
func WithBatchChunkHook(ctx context.Context, hook BatchChunkHook) context.Context {
	return context.WithValue(ctx, batchChunkHookCtxKey{}, hook)
}

// BatchChunkHookFromContext returns the attached hook, if any.
func BatchChunkHookFromContext(ctx context.Context) BatchChunkHook {
	h, _ := ctx.Value(batchChunkHookCtxKey{}).(BatchChunkHook)
	return h
}
