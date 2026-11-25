package relationtuple_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ory/keto/internal/relationtuple"
	"github.com/ory/keto/ketoapi"
)

type captureHook struct {
	mu      sync.Mutex
	entries []string
}

func (c *captureHook) Levels() []logrus.Level { return logrus.AllLevels }
func (c *captureHook) Fire(e *logrus.Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := []string{e.Message}
	for k, v := range e.Data {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	c.entries = append(c.entries, strings.Join(parts, " "))
	return nil
}
func (c *captureHook) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.entries, "\n")
}

// Form 10: metrics track outcomes/conflicts and logs never contain relation
// content (user/object identifiers).
func TestBatchMetricsAndLogPrivacy(t *testing.T) {
	h := newBatchHarness(t)
	hook := &captureHook{}
	h.reg.Logger().Logrus().AddHook(hook)

	const secretObj = "zz-secret-object-77331"
	secretSubj := "zz-secret-alice-88442"
	secretTuple := &ketoapi.RelationTuple{Namespace: h.ns, Object: secretObj, Relation: "member", SubjectID: &secretSubj}

	createdBefore := testutil.ToFloat64(relationtuple.BatchOutcomeCounter().WithLabelValues("created"))
	unchangedBefore := testutil.ToFloat64(relationtuple.BatchOutcomeCounter().WithLabelValues("unchanged"))
	conflictBefore := testutil.ToFloat64(relationtuple.BatchConflictCounter().WithLabelValues("condition_failed"))

	// created + unchanged.
	d := []*ketoapi.PatchDelta{
		insertDelta(secretTuple),
		insertDelta(secretTuple),
	}
	resp, body := h.patch(d, "")
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, []string{"created", "unchanged"}, outcomes(body))

	// A condition conflict.
	_, errBody := h.patchError([]*ketoapi.PatchDelta{
		conditionalDelta(insertDelta(secretTuple), "if_absent", ""),
	}, "")
	require.Contains(t, errBody, "error")

	createdAfter := testutil.ToFloat64(relationtuple.BatchOutcomeCounter().WithLabelValues("created"))
	unchangedAfter := testutil.ToFloat64(relationtuple.BatchOutcomeCounter().WithLabelValues("unchanged"))
	conflictAfter := testutil.ToFloat64(relationtuple.BatchConflictCounter().WithLabelValues("condition_failed"))

	assert.Equal(t, float64(1), createdAfter-createdBefore, "created metric matches outcomes")
	assert.Equal(t, float64(1), unchangedAfter-unchangedBefore, "unchanged metric matches outcomes")
	assert.Equal(t, float64(1), conflictAfter-conflictBefore, "conflict metric matches rejections")

	// Histograms/series are exposed on the default registerer.
	gathered, err := gatherBatchMetricNames()
	require.NoError(t, err)
	for _, name := range []string{
		"keto_relationtuple_batch_write_items",
		"keto_relationtuple_batch_write_duration_seconds",
		"keto_relationtuple_batch_write_outcomes_total",
		"keto_relationtuple_batch_write_conflicts_total",
	} {
		assert.Contains(t, gathered, name)
	}

	// Give any asynchronous request logging a moment, then scan ALL captured
	// log entries for relation content.
	text := hook.text()
	assert.NotEmpty(t, text, "capture hook should observe request logs")
	assert.NotContains(t, text, secretObj, "logs must not contain the object id")
	assert.NotContains(t, text, secretSubj, "logs must not contain the subject id")

	// The safe-log fields must only carry aggregates (structural guarantee):
	// exercise the service logger once more directly.
	_, _ = relationtuple.NewBatchWriteService(h.reg).Execute(context.Background(),
		&relationtuple.BatchWriteRequest{Inputs: []relationtuple.BatchInput{{Action: "insert", Tuple: h.sidTuple("another-secret-9", "u9")}}})
	text2 := hook.text()
	assert.NotContains(t, text2, "another-secret-9")
}
