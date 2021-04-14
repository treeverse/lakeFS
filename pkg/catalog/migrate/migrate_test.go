package migrate

import (
	"context"
	"github.com/stretchr/testify/require"
	"hash/fnv"
	"strconv"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestMigrate(t *testing.T) {
	conn, catalog := testSetupServices(t)

	// setup data on mvcc
	ctx := context.Background()
	_, err := catalog.CreateRepository(ctx, "repo1", "mem://", "main")
	testutil.Must(t, err)

	//               root
	//  				|
	//  		t1 <- commit1
	//  				|
	//  		      commit2
	//  				|	  \
	//  		t2 <- commit3  \
	//  			  /		|   \
	//  	b2 <- commit4 	|    \
	//  			  \		|     \
	//  b1,t3,t4 <- mergeCommit1   \
	//  					|	   /
	//         t5 <-  mergeCommit2
	//  				    |
	//  	main,t6 <-   commit5

	testCreateEntry(t, ctx, catalog, "repo1", "main", "file1")
	testCreateEntry(t, ctx, catalog, "repo1", "main", "file2")
	commit1 := testCommit(t, ctx, catalog, "repo1", "main", "first on main")
	testCreateEntry(t, ctx, catalog, "repo1", "main", "file0")
	testCreateEntry(t, ctx, catalog, "repo1", "main", "file3")
	_ = testCommit(t, ctx, catalog, "repo1", "main", "second on main")

	testCreateBranch(t, ctx, catalog, "repo1", "b1", "main")
	testCreateEntry(t, ctx, catalog, "repo1", "b1", "file11")
	testCreateEntry(t, ctx, catalog, "repo1", "b1", "file22")
	commit3 := testCommit(t, ctx, catalog, "repo1", "b1", "first on b1")
	testCreateTag(t, ctx, catalog, "repo1", "t1", commit1.Reference)
	testCreateTag(t, ctx, catalog, "repo1", "t2", commit3.Reference)

	testCreateBranch(t, ctx, catalog, "repo1", "b2", "b1")
	testCreateEntry(t, ctx, catalog, "repo1", "b2", "file111")
	testCreateEntry(t, ctx, catalog, "repo1", "b2", "file222")
	commit4 := testCommit(t, ctx, catalog, "repo1", "b2", "first on b2")

	mergeCommit1 := testMerge(t, ctx, catalog, "repo1", "b1", "b2", "merging b2 to b1")
	mergeCommit2 := testMerge(t, ctx, catalog, "repo1", "main", "b1", "merging b1 to main")
	testCreateTag(t, ctx, catalog, "repo1", "t3", mergeCommit1)
	testCreateTag(t, ctx, catalog, "repo1", "t4", mergeCommit1)
	testCreateTag(t, ctx, catalog, "repo1", "t5", mergeCommit2)

	testCreateEntry(t, ctx, catalog, "repo1", "main", "file2223245")
	commit5 := testCommit(t, ctx, catalog, "repo1", "main", "commit after merge")
	testCreateTag(t, ctx, catalog, "repo1", "t6", commit5.Reference)

	// migrate information
	migrateTool, err := NewMigrate(conn, ident.NewHexAddressProvider())
	if err != nil {
		t.Fatal("Failed to create migrate:", err)
	}

	require.True(t, CheckMigrationRequired(ctx, conn))
	err = migrateTool.Run(ctx)
	if err != nil {
		t.Fatal("Failed to migrate", err)
	}

	require.False(t, CheckMigrationRequired(ctx, conn))

	// verify migrated repository
	repo, err := catalog.GetRepository(ctx, "repo1")
	testutil.Must(t, err)
	require.Equal(t, repo.DefaultBranch, "main", "Migrated repository default branch %s, expected main", repo.DefaultBranch)
	require.Equal(t, repo.StorageNamespace, "mem://", "Migrated repository storage namespace %s, expected mem", repo.StorageNamespace)

	// verify repository commit
	log, err := catalog.GetCommit(ctx, "repo1", "main")
	testutil.Must(t, err)
	require.NotEqual(t, log.Reference, mergeCommit2, "Commit reference should change, got: %s", log.Reference)

	log, err = catalog.GetCommit(ctx, "repo1", "b1")
	testutil.Must(t, err)
	require.NotEqual(t, log.Reference, mergeCommit1, "Commit reference should change, got: %s", log.Reference)

	t1Commit, err := catalog.GetTag(ctx, "repo1", "t3")
	testutil.Must(t, err)
	require.Equal(t, log.Reference, t1Commit, "expected tag %s, got: %s", t1Commit, log.Reference)

	log, err = catalog.GetCommit(ctx, "repo1", "b2")
	testutil.Must(t, err)
	require.Equal(t, log.Reference, commit4.Reference, "Commit reference shouldn't change, got: %s", log.Reference)
}

func testMerge(t *testing.T, ctx context.Context, cataloger catalog.Catalog, repo, targetBranch, sourceBranch, msg string) string {
	t.Helper()
	mr, err := cataloger.Merge(ctx, repo, targetBranch, sourceBranch, "tester", msg, nil)
	testutil.MustDo(t, "merge", err)
	return mr.Reference
}

func testCreateBranch(t testing.TB, ctx context.Context, cataloger catalog.Catalog, repo string, branch string, parent string) {
	t.Helper()
	_, err := cataloger.CreateBranch(ctx, repo, branch, parent)
	testutil.MustDo(t, "create branch", err)
}

func newEntryCatalogForTesting(t testing.TB, conn db.Database) (*catalog.Catalog, error) {
	t.Helper()
	cfg := config.NewConfig()
	cfg.Override(func(configurator config.Configurator) {
		configurator.SetDefault(config.BlockstoreTypeKey, mem.BlockstoreType)
	})
	//block := mem.New()
	return catalog.New(context.Background(), catalog.Config{
		Config: cfg,
		DB:     conn,
	})
}

func testSetupServices(t testing.TB) (db.Database, catalog.Catalog) {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	cataloger, err := newEntryCatalogForTesting(t, conn)

	testutil.MustDo(t, "new entry catalog", err)
	return conn, *cataloger
}

func testCommit(t *testing.T, ctx context.Context, cataloger catalog.Catalog, repo string, branch string, msg string) catalog.CommitLog {
	t.Helper()
	commit, err := cataloger.Commit(ctx, repo, branch, msg, "tester", nil)
	testutil.MustDo(t, "commit", err)
	return *commit
}

func testCreateEntry(t testing.TB, ctx context.Context, cataloger catalog.Catalog, repo, branch, path string) {
	t.Helper()
	sum := calcPathSum(repo, branch, path)
	pathHash := strconv.FormatUint(sum, 16)

	err := cataloger.CreateEntry(ctx, repo, branch, catalog.DBEntry{
		Path:            path,
		PhysicalAddress: pathHash,
		CreationDate:    time.Now(),
		Size:            int64(sum),
		Checksum:        pathHash,
	})
	testutil.MustDo(t, "create entry", err)
}

func testCreateTag(t testing.TB, ctx context.Context, cataloger catalog.Catalog, repo, tag, commit string) {
	t.Helper()
	_, err := cataloger.CreateTag(ctx, repo, tag, commit)
	testutil.MustDo(t, "create tag", err)
}

func calcPathSum(repo string, branch string, path string) uint64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(repo))
	_, _ = h.Write([]byte(branch))
	_, _ = h.Write([]byte(path))
	sum := h.Sum64()
	return sum
}
func calcPathHash(repo string, branch string, path string) string {
	sum := calcPathSum(repo, branch, path)
	return strconv.FormatUint(sum, 16)
}

func newDefaultInstanceParams(t testing.TB, name string) *params.InstanceParams {
	t.Helper()
	const totalAllocatedBytes = 15 * 1024 * 1024
	const pebbleSSTableCacheSizeBytes = 8 * 1024 * 1024
	baseDir := t.TempDir()
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
