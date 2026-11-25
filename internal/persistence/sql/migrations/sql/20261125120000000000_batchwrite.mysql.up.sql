-- Batch write contract (MySQL 5.7+).
-- MySQL has no partial indexes and its UNIQUE indexes treat NULLs as distinct,
-- so we normalize the nullable subject columns through STORED generated
-- columns and enforce uniqueness on those.
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

ALTER TABLE keto_relation_tuples
    ADD COLUMN keto_identity_branch TINYINT GENERATED ALWAYS AS
        (CASE WHEN subject_id IS NULL THEN 0 ELSE 1 END) STORED,
    ADD COLUMN keto_identity_sid CHAR(36) GENERATED ALWAYS AS
        (COALESCE(subject_id, '00000000-0000-0000-0000-000000000000')) STORED,
    ADD COLUMN keto_identity_ss_ns VARCHAR(200) GENERATED ALWAYS AS
        (COALESCE(subject_set_namespace, '')) STORED,
    ADD COLUMN keto_identity_ss_obj CHAR(36) GENERATED ALWAYS AS
        (COALESCE(subject_set_object, '00000000-0000-0000-0000-000000000000')) STORED,
    ADD COLUMN keto_identity_ss_rel VARCHAR(64) GENERATED ALWAYS AS
        (COALESCE(subject_set_relation, '')) STORED,
    ADD UNIQUE INDEX keto_rt_identity_uq
        (nid, namespace, object, relation,
         keto_identity_branch, keto_identity_sid,
         keto_identity_ss_ns, keto_identity_ss_obj, keto_identity_ss_rel);

CREATE TABLE keto_batch_idempotency
(
    nid              CHAR(36)     NOT NULL,
    idem_key         VARCHAR(255)  NOT NULL,
    request_hash     VARCHAR(128)  NOT NULL,
    request_payload  LONGTEXT      NOT NULL,
    state            VARCHAR(16)   NOT NULL DEFAULT 'completed',
    response_payload LONGTEXT      NOT NULL,
    created_at       DATETIME      NOT NULL,
    expires_at       DATETIME      NULL,
    PRIMARY KEY (nid, idem_key),
    INDEX keto_batch_idem_expires_idx (expires_at)
) ENGINE=InnoDB;
