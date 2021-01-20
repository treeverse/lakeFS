package migrate

import (
	"context"
	"crypto"
	"fmt"
	"hash/maphash"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/treeverse/lakefs/ident"

	"github.com/go-test/deep"

	"github.com/cockroachdb/pebble"
	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/block/mem"
	lru "github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/sstable"
	"github.com/treeverse/lakefs/graveler/staging"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/pyramid"
	"github.com/treeverse/lakefs/pyramid/params"
	"github.com/treeverse/lakefs/testutil"
)

func TestMigrate(t *testing.T) {
	conn, mvccCataloger, entryCatalog := testSetupServices(t)

	// setup data on mvcc
	ctx := context.Background()
	_, err := mvccCataloger.CreateRepository(ctx, "repo1", "mem://", "main")
	testutil.Must(t, err)
	testCreateEntry(t, ctx, mvccCataloger, "repo1", "main", "file1")
	testCreateEntry(t, ctx, mvccCataloger, "repo1", "main", "file2")
	testCommit(t, ctx, mvccCataloger, "repo1", "main", "first on main")
	testCreateEntry(t, ctx, mvccCataloger, "repo1", "main", "file0")
	testCreateEntry(t, ctx, mvccCataloger, "repo1", "main", "file3")
	testCommit(t, ctx, mvccCataloger, "repo1", "main", "second on main")

	// migrate information
	migrateTool, err := NewMigrate(conn, entryCatalog, mvccCataloger)
	if err != nil {
		t.Fatal("Failed to create migrate:", err)
	}

	err = migrateTool.Run()
	if err != nil {
		t.Fatal("Failed to migrate", err)
	}

	// verify migrated repository
	repo, err := entryCatalog.GetRepository(ctx, "repo1")
	testutil.Must(t, err)
	if repo.DefaultBranchID != "main" {
		t.Fatalf("Migrated repository default branch %s, expected main", repo.DefaultBranchID)
	}
	if repo.StorageNamespace != "mem://" {
		t.Fatalf("Migrated repository storage namespace %s, expected mem", repo.StorageNamespace)
	}

	// verify repository commit log
	branch, err := entryCatalog.GetBranch(ctx, "repo1", "main")
	testutil.Must(t, err)
	logIt, err := entryCatalog.Log(ctx, "repo1", branch.CommitID)
	testutil.Must(t, err)
	var commitMessages []string
	for logIt.Next() {
		commit := logIt.Value()
		commitMessages = append(commitMessages, commit.Message)
	}
	testutil.Must(t, logIt.Err())

	expectedCommits := []string{
		"second on main",
		"first on main",
		"Repository created",
		"Repository import branch created",
		"Create empty new branch for migrate",
	}
	if diff := deep.Equal(commitMessages, expectedCommits); diff != nil {
		t.Fatal("Log diff found:", diff)
	}
}

func newDefaultInstanceParams(name string) *params.InstanceParams {
	const totalAllocatedBytes = 15 * 1024 * 1024
	const pebbleSSTableCacheSizeBytes = 8 * 1024 * 1024
	baseDir := os.TempDir()
	return &params.InstanceParams{
		SharedParams: params.SharedParams{
			Logger: logging.Default(),
			Local: params.LocalDiskParams{
				TotalAllocatedBytes: totalAllocatedBytes,
				BaseDir:             baseDir,
			},
			Adapter:                     mem.New(),
			BlockStoragePrefix:          "",
			Eviction:                    nil,
			PebbleSSTableCacheSizeBytes: pebbleSSTableCacheSizeBytes,
		},
		FSName:              name,
		DiskAllocProportion: 1.0,
	}
}

func newEntryCatalogInMem(conn db.Database) (*rocks.EntryCatalog, error) {
	metaRangeFS, err := pyramid.NewFS(newDefaultInstanceParams("meta-range"))
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed meta-range: %w", err)
	}

	rangeFS, err := pyramid.NewFS(newDefaultInstanceParams("rage"))
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed range: %w", err)
	}

	const cacheSize = 1024 * 1024 * 8
	pebbleSSTableCache := pebble.NewCache(cacheSize)
	defer pebbleSSTableCache.Unref()

	metaRangeCache := sstable.NewCache(lru.ParamsWithDisposal{},
		metaRangeFS,
		pebblesst.ReaderOptions{Cache: pebbleSSTableCache})

	rangeCache := sstable.NewCache(lru.ParamsWithDisposal{},
		rangeFS,
		pebblesst.ReaderOptions{Cache: pebbleSSTableCache})

	sstableManager := sstable.NewPebbleSSTableRangeManager(rangeCache, rangeFS, crypto.SHA256)
	sstableMetaManager := sstable.NewPebbleSSTableRangeManager(metaRangeCache, metaRangeFS, crypto.SHA256)
	sstableMetaRangeManager := committed.NewMetaRangeManager(committed.Params{
		MinRangeSizeBytes:          10240,
		MaxRangeSizeBytes:          10240,
		RangeSizeEntriesRaggedness: 1024,
	},
		sstableMetaManager,
		sstableManager,
	)
	committedManager := committed.NewCommittedManager(sstableMetaRangeManager)

	stagingManager := staging.NewManager(conn)
	refManager := ref.NewPGRefManager(conn, ident.NewHexAddressProvider())
	branchLocker := ref.NewBranchLocker(conn)
	return &rocks.EntryCatalog{
		Store: graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager),
	}, nil
}

func testSetupServices(t testing.TB) (db.Database, catalog.Cataloger, *rocks.EntryCatalog) {
	conn, _ := testutil.GetDB(t, databaseURI)
	mvccCataloger := mvcc.NewCataloger(conn)
	entryCataloger, err := newEntryCatalogInMem(conn)
	testutil.MustDo(t, "new entry catalog", err)
	return conn, mvccCataloger, entryCataloger
}

func testCommit(t *testing.T, ctx context.Context, cataloger catalog.Cataloger, repo string, branch string, msg string) catalog.CommitLog {
	commit, err := cataloger.Commit(ctx, repo, branch, msg, "tester", nil)
	testutil.MustDo(t, "commit", err)
	return *commit
}

func testCreateEntry(t testing.TB, ctx context.Context, cataloger catalog.Cataloger, repo, branch, path string) {
	var h maphash.Hash
	_, _ = h.Write([]byte(repo))
	_, _ = h.Write([]byte(branch))
	_, _ = h.Write([]byte(path))
	sum := h.Sum64()
	pathHash := strconv.FormatUint(sum, 16)

	err := cataloger.CreateEntry(ctx, repo, branch, catalog.Entry{
		Path:            path,
		PhysicalAddress: pathHash,
		CreationDate:    time.Now(),
		Size:            int64(sum),
		Checksum:        pathHash,
	}, catalog.CreateEntryParams{})
	testutil.MustDo(t, "create entry", err)
}
