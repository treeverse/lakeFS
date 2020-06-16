package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

type testEntryInfo struct {
	Path    string
	Seed    string
	Deleted bool
}

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
	checksum := testCreateEntryCalcChecksum(key, seed)
	size := 0
	for i := range checksum {
		size += int(checksum[i])
	}
	err := c.CreateEntry(ctx, repository, branch, key, checksum, checksum, size, metadata)
	if err != nil {
		t.Fatalf("Failed to create entry %s on branch %s, repository %s: %s", key, branch, repository, err)
	}
}

func testCreateEntryCalcChecksum(key string, seed string) string {
	h := sha256.New()
	h.Write([]byte(seed))
	h.Write([]byte(key))
	checksum := hex.EncodeToString(h.Sum(nil))
	return checksum
}

func testVerifyEntries(t *testing.T, ctx context.Context, c Cataloger, repository string, branch string, commitID CommitID, entries []testEntryInfo) {
	for _, entry := range entries {
		ent, err := c.GetEntry(ctx, repository, branch, commitID, entry.Path)
		if entry.Deleted {
			if !errors.As(err, &db.ErrNotFound) {
				t.Fatalf("Get entry '%s', err = %s, expected not found", entry.Path, err)
			}
		} else {
			testutil.MustDo(t, fmt.Sprintf("Get entry=%s, repository=%s, branch=%s", entry.Path, repository, branch), err)
			expectedAddr := testCreateEntryCalcChecksum(entry.Path, entry.Seed)
			if ent.PhysicalAddress != expectedAddr {
				t.Fatalf("Get entry %s, addr = %s, expected %s", entry.Path, ent.PhysicalAddress, expectedAddr)
			}
		}
	}
}
