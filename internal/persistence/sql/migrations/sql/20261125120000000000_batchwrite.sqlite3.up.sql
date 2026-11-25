-- Batch write contract:
--  1. collapse pre-existing duplicate logical relation tuples (upgrade-safe),
--  2. enforce one row per logical relation per network at the storage layer,
--  3. persist request-level idempotency records.
--
-- The dedupe statement is idempotent itself: on an already-unique table it
-- deletes zero rows. It must never fail startup/migration, even when legacy
-- deployments contain duplicate rows.
--
-- shard_id/nid are NOT NULL, so the row-value NOT IN sub-query can never
-- degrade to an all-NULL result.
DELETE FROM keto_relation_tuples
WHERE (shard_id, nid) NOT IN (
    SELECT min_shard_id, nid
    FROM (
        SELECT MIN(shard_id) AS min_shard_id, nid
        FROM keto_relation_tuples
        GROUP BY nid,
                 namespace,
                 object,
                 relation,
                 subject_id,
                 subject_set_namespace,
                 subject_set_object,
                 subject_set_relation
    ) AS keto_batch_keepers
);

-- Partial unique indexes match the exactly-one-subject CHECK constraint and
-- make NULL columns irrelevant for uniqueness.
CREATE UNIQUE INDEX keto_rt_identity_subject_id_uq
    ON keto_relation_tuples (nid, namespace, object, relation, subject_id)
    WHERE subject_id IS NOT NULL;

CREATE UNIQUE INDEX keto_rt_identity_subject_set_uq
    ON keto_relation_tuples (nid, namespace, object, relation,
                             subject_set_namespace, subject_set_object,
                             subject_set_relation)
    WHERE subject_id IS NULL;

CREATE TABLE keto_batch_idempotency
(
    nid              TEXT         NOT NULL,
    idem_key         VARCHAR(255) NOT NULL,
    request_hash     VARCHAR(128) NOT NULL,
    request_payload  TEXT         NOT NULL DEFAULT '',
    -- pending or completed state, where pending rows are in-flight claims used to make
    -- concurrent same-key submissions observable instead of duplicated.
    state            VARCHAR(16)  NOT NULL DEFAULT 'completed',
    -- Opaque, JSON-encoded first response (item outcomes + version tokens).
    response_payload TEXT         NOT NULL DEFAULT '',
    created_at       DATETIME     NOT NULL,
    expires_at       DATETIME     NULL,
    PRIMARY KEY (nid, idem_key)
);

CREATE INDEX keto_batch_idem_expires_idx
    ON keto_batch_idempotency (expires_at);
