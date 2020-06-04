package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/treeverse/lakefs/testutil"
)

func setupCatalogerForTesting(t *testing.T) Cataloger {
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	return NewCataloger(cdb)
}

func setupCatalogerRepo(t *testing.T, ctx context.Context, c Cataloger, prefix string, branch string) string {
	uid := uuid.New().String()
	name := prefix + strings.ReplaceAll(uid, "-", "")
	if err := c.CreateRepo(ctx, name, "bucket", branch); err != nil {
		t.Fatalf("create repo %s, branch %s, failed: %s", name, branch, err)
	}
	return name
}
