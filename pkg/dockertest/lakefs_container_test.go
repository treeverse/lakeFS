package dockertest_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	lakefs_ci "github.com/treeverse/lakefs/pkg/dockertest"

	"github.com/ory/dockertest/v3"
)

func TestLakeFSContainer(t *testing.T) {
	const version = "1.9.1"

	ctx := context.TODO()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatal(err)
	}

	container := lakefs_ci.New().WithTag(version).New(ctx, t, pool)
	defer container.Close()

	res, err := container.Client.ListRepositoriesWithResponse(ctx, &apigen.ListRepositoriesParams{})
	if err != nil {
		t.Fatal(err)
	}
	if err = helpers.ResponseAsError(res); err != nil {
		t.Fatalf("Received error: %s", err)
	}
	if len(res.JSON200.Results) != 0 {
		t.Errorf("Got repositories %+v", res.JSON200.Results)
	}
}
