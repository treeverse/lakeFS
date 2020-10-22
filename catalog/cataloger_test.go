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

type TestCataloger struct {
	Cataloger
	DbConnURI string
}

func testCataloger(t testing.TB, options ...CatalogerOption) TestCataloger {
	t.Helper()
	conn, uri := testutil.GetDB(t, databaseURI)
	return TestCataloger{Cataloger: NewCataloger(conn, options...), DbConnURI: uri}
}

func testCatalogerUniqueID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[0:7]
}

func testCatalogerRepo(t testing.TB, ctx context.Context, c Cataloger, prefix string, branch string) string {
	t.Helper()
	name := prefix + "-" + testCatalogerUniqueID()
	_, err := c.CreateRepository(ctx, name, "s3://bucket", branch)
	if err != nil {
		t.Fatalf("create repository %s, branch %s, failed: %s", name, branch, err)
	}
	return name
}

func testCatalogerBranch(t testing.TB, ctx context.Context, c Cataloger, repository, name, source string) {
	t.Helper()
	_, err := c.CreateBranch(ctx, repository, name, source)
	if err != nil {
		t.Fatalf("failed to create branch %s (%s) on %s: %s", name, source, repository, err)
	}
}

// testCatalogerCreateEntry creates a test entry on cataloger, returning a (fake) checksum based on the path, the test name, and a seed.
func testCatalogerCreateEntry(t testing.TB, ctx context.Context, c Cataloger, repository, branch, path string, metadata Metadata, seed string) string {
	t.Helper()
	checksum := testCreateEntryCalcChecksum(path, t.Name()+seed)
	var size int64
	for i := range checksum {
		size += int64(checksum[i])
	}
	err := c.CreateEntry(ctx, repository, branch, Entry{
		Path:            path,
		Checksum:        checksum,
		PhysicalAddress: checksum,
		Size:            size,
		Metadata:        metadata,
	}, CreateEntryParams{})
	if err != nil {
		t.Fatalf("Failed to create entry %s on branch %s, repository %s: %s", path, branch, repository, err)
	}
	return checksum
}

func testCatalogerGetEntry(t testing.TB, ctx context.Context, c Cataloger, repository, reference, path string, expect bool) {
	t.Helper()
	entry, err := c.GetEntry(ctx, repository, reference, path, GetEntryParams{ReturnExpired: true})
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("get entry from repository: %s, reference: %s, path: %s - %s", repository, path, reference, err)
	}
	if expect != (entry != nil) {
		t.Fatalf("get entry from repository: %s, reference: %s, path: %s - expected %t", repository, path, reference, expect)
	}
}

func testCreateEntryCalcChecksum(key string, seed string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(seed))
	_, _ = h.Write([]byte(key))
	checksum := hex.EncodeToString(h.Sum(nil))
	return checksum
}

func testVerifyEntries(t testing.TB, ctx context.Context, c Cataloger, repository string, reference string, entries []testEntryInfo) {
	t.Helper()
	for _, entry := range entries {
		ent, err := c.GetEntry(ctx, repository, reference, entry.Path, GetEntryParams{})
		if entry.Deleted {
			if !errors.As(err, &db.ErrNotFound) {
				t.Fatalf("Get entry '%s', err = %s, expected not found", entry.Path, err)
			}
		} else {
			testutil.MustDo(t, fmt.Sprintf("Get entry=%s, repository=%s, reference=%s", entry.Path, repository, reference), err)
			expectedAddr := testCreateEntryCalcChecksum(entry.Path, entry.Seed)
			if ent.PhysicalAddress != expectedAddr {
				t.Fatalf("Get entry %s, addr = %s, expected %s", entry.Path, ent.PhysicalAddress, expectedAddr)
			}
		}
	}
}
