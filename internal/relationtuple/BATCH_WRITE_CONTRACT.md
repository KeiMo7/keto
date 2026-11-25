# Batch write contract

The relationship write entry (`PATCH /admin/relation-tuples`, the explicit
`POST /admin/relation-tuples/batch`, and the connect/gRPC
`WriteService.TransactRelationTuples`) shares one implementation:
`relationtuple.BatchWriteService`. Both transports therefore return identical
per-item outcomes and identical classified errors.

The legacy wire format and default status codes are unchanged: a plain JSON
array `PATCH` without new opt-in fields still returns `204 No Content`, and
single `PUT` / query `DELETE` behavior is unchanged.

## Ordered semantics

Items are evaluated strictly in request order against read-your-writes state
inside one transaction. Items are never re-grouped by action. For one logical
relation the last action wins:

- add then delete  => absent
- delete then add  => present

Exact duplicate items collapse to one stored row; the duplicate item still
receives its own outcome so the response length always equals the request
length and preserves indices.

## Outcomes

Each item returns one of:

| `outcome`     | meaning                                              |
|---------------|------------------------------------------------------|
| `created`     | relation was absent and is now stored                |
| `unchanged`   | insert targeted an already present relation          |
| `deleted`     | a present relation was removed                       |
| `not_found`   | delete targeted an absent relation                   |

Every item also returns an opaque, authenticated/encrypted
`version_token` (`kbt1.…`). Tokens never contain namespace/object/subject
values, are not reversible, and are only optimistic-concurrency cursors -
they are never credentials; authorization is re-enforced on every request.

Optional per-item request fields:

- `condition`: `if_absent` (insert requires absence) or `if_present`
  (delete requires existence).
- `expected_version`: a previously returned `version_token`.

A failed condition/version aborts and rolls back the whole batch and the
classified error carries `details.item_index`.

## Idempotency

Send `Idempotency-Key` header (HTTP) or `idempotency_key` (JSON body / proto).
Records and the stored first response are persisted in
`keto_batch_idempotency`, primary key `(nid, idem_key)`, so they survive
restarts and are network scoped.

- same key + same content => identical outcomes/tokens, `replayed=true`, no writes
- same key + different content => `409 idempotency_key_conflict` with the first differing `item_index`
- concurrent same key => retryable `409 idempotency_in_flight`
- expired record => handled as a fresh request

TTL: `limit.write_idempotency_ttl_seconds` (default `86400`; `0` = never expire).

## Size limit

`limit.max_write_batch_size` (default `0` = unlimited, preserving historical
thousand-item batches). A positive value accepts exactly the limit and rejects
larger requests before any database access with
`400 batch_too_large` and `details.max_items`.

Empty batches, unknown actions and missing `relation_tuple` are rejected up front.

## Persistence guarantees

- Migration `20261125120000000000_batchwrite` de-duplicates legacy rows and
  adds network-prefixed partial unique indexes (SQLite/Postgres/Cockroach) or
  generated-column unique indexes (MySQL), plus the idempotency table.
- Uniqueness, idempotency claims and conditional/version checks all live in
  the storage transaction; there is no check-then-write uniqueness.
- Overlapping concurrent batches hit the unique identity and receive a
  retryable `409 concurrent_conflict`; exactly one commits.
- All chunks (including the internal delete/insert segments) run in one
  transaction; a storage failure rolls every segment back. The outer error
  carries only `details.failed_from_index` / `failed_to_index` and a stable
  `error_kind`; raw driver errors and internal identifiers are logged
  server-side only and never returned.

## Observability

Prometheus metrics (default registerer):

- `keto_relationtuple_batch_write_items` (size histogram)
- `keto_relationtuple_batch_write_duration_seconds`
- `keto_relationtuple_batch_write_outcomes_total{outcome=...}`
- `keto_relationtuple_batch_write_conflicts_total{kind=...}`
- `keto_relationtuple_batch_write_idempotent_replays_total`

Batch logs contain only aggregate fields (size and created/unchanged/deleted/
not-found counts, duration, error kind/index interval) - never relation
namespace/object/subject or idempotency key content.

## HTTP shapes

`PATCH /admin/relation-tuples?detailed=true` (or `Prefer: return=representation`,
`X-Batch-Detailed: true`, or any idempotency/conditional field) returns:

```json
{
  "replayed": false,
  "results": [
    {"index": 0, "outcome": "created", "status": "created",
     "version_token": "kbt1.…", "snaptoken": "kbt1.…"}
  ],
  "snaptokens": ["kbt1.…"]
}
```

`POST /admin/relation-tuples/batch` accepts
`{"idempotency_key":"…","items":[ …PatchDelta ]}` and always returns this body.
