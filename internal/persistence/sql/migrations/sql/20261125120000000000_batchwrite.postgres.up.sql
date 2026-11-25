-- Batch write contract: collapse legacy duplicate rows, enforce logical
-- uniqueness per network, persist idempotency records.
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
    nid              UUID         NOT NULL,
    idem_key         VARCHAR(255) NOT NULL,
    request_hash     VARCHAR(128) NOT NULL,
    request_payload  TEXT         NOT NULL DEFAULT '',
    state            VARCHAR(16)  NOT NULL DEFAULT 'completed',
    response_payload TEXT         NOT NULL DEFAULT '',
    created_at       timestamp    NOT NULL,
    expires_at       timestamp    NULL,
    PRIMARY KEY (nid, idem_key)
);

CREATE INDEX keto_batch_idem_expires_idx
    ON keto_batch_idempotency (expires_at);
