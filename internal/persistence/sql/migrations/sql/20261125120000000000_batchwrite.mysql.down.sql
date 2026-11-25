DROP TABLE IF EXISTS keto_batch_idempotency;
ALTER TABLE keto_relation_tuples DROP INDEX keto_rt_identity_uq;
ALTER TABLE keto_relation_tuples
    DROP COLUMN keto_identity_ss_rel,
    DROP COLUMN keto_identity_ss_obj,
    DROP COLUMN keto_identity_ss_ns,
    DROP COLUMN keto_identity_sid,
    DROP COLUMN keto_identity_branch;
