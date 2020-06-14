package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	return strings.ReplaceAll(uuid.New().String(), "-", "")[0:7]
}

func testCatalogerRepo(t *testing.T, ctx context.Context, c Cataloger, prefix string, branch string) string {
	name := prefix + "-" + testCatalogerUniqueID()
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

func testCatalogerCreateEntry(t *testing.T, ctx context.Context, c Cataloger, repository, branch, key string, metadata Metadata, seed string) {
	h := sha256.New()
	h.Write([]byte(seed))
	h.Write([]byte(key))
	checksum := hex.EncodeToString(h.Sum(nil))
	size := 0
	for i := range checksum {
		size += int(checksum[i])
	}
	err := c.CreateEntry(ctx, repository, branch, key, checksum, checksum, size, metadata)
	if err != nil {
		t.Fatalf("Failed to create entry %s on branch %s, repository %s: %s", key, branch, repository, err)
	}
}
