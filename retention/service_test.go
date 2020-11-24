package retention_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/catalog"
	catalogfactory "github.com/treeverse/lakefs/catalog/factory"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
	"github.com/treeverse/lakefs/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		logging.Default().Fatalf("Could not connect to Docker: %s", err)
	}
	databaseURI, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func setupService(t *testing.T, opts ...testutil.GetDBOption) *retention.DBRetentionService {
	t.Helper()
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, opts...)
	cataloger := catalogfactory.BuildCataloger(cdb, nil)
	_, err := cataloger.CreateRepository(ctx, "repo", "s3://repo", "master")
	testutil.MustDo(t, "create repository", err)
	return retention.NewDBRetentionService(cdb)
}

func TestDBRetentionService_Config(t *testing.T) {
	s := setupService(t)

	period := catalog.TimePeriodHours(48)

	policy := catalog.Policy{
		Description: "Retention policy for testing",
		Rules: []catalog.Rule{
			{
				Enabled:      true,
				FilterPrefix: "/foo/path",
				Expiration:   catalog.Expiration{All: &period},
			},
		},
	}

	before := time.Now()
	err := s.SetPolicy("repo", &policy, time.Now())
	after := time.Now()
	if err != nil {
		t.Fatalf("failed to set policy %v: %s", policy, err)
	}

	returnedPolicy, err := s.GetPolicy("repo")
	if err != nil {
		t.Fatalf("failed to get policy: %s", err)
	}

	diff := deep.Equal(policy, returnedPolicy.Policy)
	if diff != nil {
		t.Errorf("policy round-trip: %s.  Wrote %v, read %v", diff, policy, returnedPolicy)
	}
	// Fuzz "before" and "after" times, Postgres rounds times to
	// microsecond precision (and even less accuracy).
	if !(before.Before(returnedPolicy.CreatedAt.Add(time.Millisecond)) && after.After(returnedPolicy.CreatedAt.Add(-1*time.Millisecond))) {
		t.Errorf("expected policy creation date between %v and %v, got %v", before, after, returnedPolicy.CreatedAt)
	}
}
