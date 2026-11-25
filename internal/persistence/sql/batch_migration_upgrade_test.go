package sql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/driver"
	ketsql "github.com/ory/keto/internal/persistence/sql"
	"github.com/ory/keto/internal/relationtuple"
)

type countRow struct {
	C int64 `db:"c"`
}

func rawCount(t *testing.T, reg *driver.RegistryDefault, ns string) int64 {
	t.Helper()
	var rows []countRow
	err := reg.Persister().Connection(context.Background()).RawQuery(
		"SELECT COUNT(*) AS c FROM keto_relation_tuples WHERE namespace = ?", ns,
	).All(&rows)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0].C
}

// Acceptance upgrade case: duplicate legacy rows are collapsed by the
// migration, startup/migration does not fail, and reads no longer duplicate.
func TestBatchMigrationDeduplicatesLegacyRows(t *testing.T) {
	reg := driver.NewSqliteTestRegistry(t)
	ctx := context.Background()
	conn := reg.Persister().Connection(ctx)
	const ns = "migration-ns"

	// Simulate a legacy deployment that lacks the identity unique indexes and
	// the idempotency table.
	require.NoError(t, conn.RawQuery("DROP INDEX IF EXISTS keto_rt_identity_subject_id_uq").Exec())
	require.NoError(t, conn.RawQuery("DROP INDEX IF EXISTS keto_rt_identity_subject_set_uq").Exec())
	require.NoError(t, conn.RawQuery("DROP TABLE IF EXISTS keto_batch_idempotency").Exec())

	obj1, sub1 := uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4())
	obj2, sub2 := uuid.Must(uuid.NewV4()), uuid.Must(uuid.NewV4())
	rt1 := &relationtuple.RelationTuple{Namespace: ns, Object: obj1, Relation: "member", Subject: &relationtuple.SubjectID{ID: sub1}}
	rt2 := &relationtuple.RelationTuple{Namespace: ns, Object: obj2, Relation: "member", Subject: &relationtuple.SubjectID{ID: sub2}}

	// Two identical logical tuples for rt1, three for rt2.
	require.NoError(t, reg.RelationTupleManager().WriteRelationTuples(ctx, rt1, rt1))
	require.NoError(t, reg.RelationTupleManager().WriteRelationTuples(ctx, rt2, rt2, rt2))
	require.EqualValues(t, 5, rawCount(t, reg, ns), "legacy duplicates exist before migration")

	// Re-run the real sqlite up migration verbatim against this legacy state.
	migration, err := ketsql.Migrations.ReadFile("migrations/sql/20261125120000000000_batchwrite.sqlite3.up.sql")
	require.NoError(t, err)
	for _, stmt := range strings.Split(string(migration), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || onlyComments(stmt) {
			continue
		}
		require.NoError(t, conn.RawQuery(stmt).Exec(), "migration statement must apply on duplicate data: %.80s", stmt)
	}

	assert.EqualValues(t, 2, rawCount(t, reg, ns), "duplicate rows collapsed to one per logical relation")

	// The unique index now rejects a second logical row at the storage layer
	// when using a plain INSERT (the compatibility write builder adds
	// ON CONFLICT DO NOTHING and is covered by handler tests).
	nid := reg.Persister().NetworkID(ctx)
	rawInsert := `INSERT INTO keto_relation_tuples
		(shard_id, nid, namespace, object, relation, subject_id,
		 subject_set_namespace, subject_set_object, subject_set_relation, commit_time)
		VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, datetime('now'))`
	err = conn.RawQuery(rawInsert,
		uuid.Must(uuid.NewV4()), nid, ns, obj1, "member", sub1,
	).Exec()
	require.Error(t, err, "unique identity index must reject duplicate raw inserts")

	// Read API returns exactly one tuple per relation.
	rows, _, err := reg.RelationTupleManager().GetRelationTuples(ctx, &relationtuple.RelationQuery{Namespace: strPtr(ns)})
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func onlyComments(stmt string) bool {
	for _, line := range strings.Split(stmt, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return false
		}
	}
	return true
}

func strPtr(s string) *string { return &s }
