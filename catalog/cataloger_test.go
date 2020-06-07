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
	if err := c.CreateRepo(ctx, name, "bucket", branch); err != nil {
		t.Fatalf("create repo %s, branch %s, failed: %s", name, branch, err)
	}
	return name
}

func testCatalogerBranch(t *testing.T, ctx context.Context, c Cataloger, repo, name, source string) *Branch {
	b, err := c.CreateBranch(ctx, repo, name, source)
	if err != nil {
		t.Fatalf("failed to create branch %s (%s) on %s: %s", name, source, repo, err)
	}
	return b
}
