package retention_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/retention"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

var cases = []struct {
	Name      string
	StorageID graveler.StorageID
}{
	{
		Name:      "Default StorageID",
		StorageID: config.SingleBlockstoreID,
	},
	{
		Name:      "Another StorageID",
		StorageID: "something-else",
	},
}

func TestGarbageCollectionManager_GetUncommittedLocation(t *testing.T) {
	blockAdapter := mem.New(context.Background())
	refMgr := &testutil.RefsFake{}
	const prefix = "test_prefix"
	const runID = "my_test_runID"
	ns := graveler.StorageNamespace("mem://test-namespace/my-repo")
	path := fmt.Sprintf("%s/%s/retention/gc/uncommitted/%s/uncommitted/", ns, prefix, runID)

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			gc := retention.NewGarbageCollectionManager(blockAdapter, refMgr, prefix)
			location, err := gc.GetUncommittedLocation(runID, tc.StorageID, ns)
			require.NoError(t, err)
			require.Equal(t, path, location)
		})
	}
}

func createTestFile(t *testing.T, filename, testLine string, count int) {
	t.Helper()
	fd, err := os.Create(filename)
	require.NoError(t, err)
	defer func() { _ = fd.Close() }()
	for i := range count {
		_, err := fmt.Fprintf(fd, "%s_%d\n", testLine, i)
		require.NoError(t, err)
	}
}

func TestGarbageCollectionManager_SaveGarbageCollectionUncommitted(t *testing.T) {
	ctx := context.Background()
	blockAdapter := mem.New(context.Background())
	refMgr := &testutil.RefsFake{}
	const prefix = "test_prefix"
	const runID = "my_test_runID"
	const repoID = "my-repo"
	ns := graveler.StorageNamespace("mem://test-namespace/" + repoID)

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			repositoryRec := graveler.RepositoryRecord{
				RepositoryID: repoID,
				Repository: &graveler.Repository{
					StorageID:        tc.StorageID,
					StorageNamespace: ns,
					CreationDate:     time.Now(),
					DefaultBranchID:  "",
					State:            graveler.RepositoryState_ACTIVE,
					InstanceUID:      uuid.New().String(),
				},
			}
			gc := retention.NewGarbageCollectionManager(blockAdapter, refMgr, prefix)
			location, err := gc.GetUncommittedLocation(runID, tc.StorageID, ns)
			require.NoError(t, err)
			filename := "uncommitted_test_file"
			testLine := "TestLine"
			lineCount := 5
			createTestFile(t, filename, testLine, lineCount)
			defer os.Remove(filename)
			err = gc.SaveGarbageCollectionUncommitted(ctx, &repositoryRec, filename, runID)
			require.NoError(t, err)
			reader, err := blockAdapter.Get(ctx, block.ObjectPointer{
				StorageID:        "",
				StorageNamespace: "",
				Identifier:       fmt.Sprintf("%s%s", location, filename),
				IdentifierType:   block.IdentifierTypeFull,
			})
			require.NoError(t, err)
			fileScanner := bufio.NewScanner(reader)
			line := 0
			for fileScanner.Scan() {
				require.Equal(t, fmt.Sprintf("%s_%d", testLine, line), fileScanner.Text())
				line += 1
			}
			require.Equal(t, lineCount, line)
		})
	}
}
