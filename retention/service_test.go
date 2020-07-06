package retention_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
	"github.com/treeverse/lakefs/testutil"
)

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		logging.Default().Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

func setupService(t *testing.T, opts ...testutil.GetDBOption) *retention.DBRetentionService {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseUri, config.SchemaCatalog, opts...)
	cataloger := catalog.NewCataloger(cdb)

	cataloger.CreateRepository(ctx, "repo", "s3://repo", "master")

	return retention.NewDBRetentionService(cdb)
}

func TestDBRetentionService_Config(t *testing.T) {
	s := setupService(t)

	period := retention.TimePeriodHours(48)

	policy := retention.Policy{
		Rules: []retention.Rule{
			retention.Rule{
				Enabled:      true,
				FilterPrefix: "/foo/path",
				Expiration:   retention.Expiration{All: &period},
			},
		},
	}

	err := s.SetPolicy("repo", &policy, "comment", time.Now())
	if err != nil {
		t.Fatalf("failed to set policy %v: %s", policy, err)
	}

	returnedPolicy, err := s.GetPolicy("repo")
	if err != nil {
		t.Fatalf("failed to get policy: %s", err)
	}

	diff := deep.Equal(policy, *returnedPolicy)
	if diff != nil {
		t.Fatalf("policy round-trip: %s.  Wrote %v, read %v", diff, policy, returnedPolicy)
	}
}
