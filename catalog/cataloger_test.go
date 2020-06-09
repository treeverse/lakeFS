package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/treeverse/lakefs/testutil"
)

func testCataloger(t *testing.T) Cataloger {
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	return NewCataloger(cdb)
}

func testCatalogerUniqueID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func testCatalogerRepo(t *testing.T, ctx context.Context, c Cataloger, prefix string, branch string) string {
	name := prefix + testCatalogerUniqueID()
	if err := c.CreateRepository(ctx, name, "bucket", branch); err != nil {
		t.Fatalf("create repository %s, branch %s, failed: %s", name, branch, err)
	}
	return name
}

func testCatalogerBranch(t *testing.T, ctx context.Context, c Cataloger, repository, name, source string) int {
	id, err := c.CreateBranch(ctx, repository, name, source)
	if err != nil {
		t.Fatalf("failed to create branch %s (%s) on %s: %s", name, source, repository, err)
	}
	return id
}
