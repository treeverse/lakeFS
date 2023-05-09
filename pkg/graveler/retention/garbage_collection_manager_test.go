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
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/retention"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestGarbageCollectionManager_GetUncommittedLocation(t *testing.T) {
	blockAdapter := mem.New()
	refMgr := &testutil.RefsFake{}
	const prefix = "test_prefix"
	const runID = "my_test_runID"
	ns := graveler.StorageNamespace("mem://test-namespace/my-repo")
	path := fmt.Sprintf("%s/%s/retention/gc/uncommitted/%s/uncommitted/", ns, prefix, runID)
	gc := retention.NewGarbageCollectionManager(blockAdapter, refMgr, prefix)
	location, err := gc.GetUncommittedLocation(runID, ns)
	require.NoError(t, err)
	require.Equal(t, path, location)
}

func createTestFile(t *testing.T, filename, testLine string, count int) {
	t.Helper()
	fd, err := os.Create(filename)
	require.NoError(t, err)
	defer fd.Close()
	for i := 0; i < count; i++ {
		_, err := fd.WriteString(fmt.Sprintf("%s_%d\n", testLine, i))
		require.NoError(t, err)
	}
}

func TestGarbageCollectionManager_SaveGarbageCollectionUncommitted(t *testing.T) {
	ctx := context.Background()
	blockAdapter := mem.New()
	refMgr := &testutil.RefsFake{}
	const prefix = "test_prefix"
	const runID = "my_test_runID"
	const repoID = "my-repo"
	ns := graveler.StorageNamespace("mem://test-namespace/" + repoID)
	repositoryRec := graveler.RepositoryRecord{
		RepositoryID: repoID,
		Repository: &graveler.Repository{
			StorageNamespace: ns,
			CreationDate:     time.Now(),
			DefaultBranchID:  "",
			State:            graveler.RepositoryState_ACTIVE,
			InstanceUID:      uuid.New().String(),
		},
	}
	gc := retention.NewGarbageCollectionManager(blockAdapter, refMgr, prefix)
	location, err := gc.GetUncommittedLocation(runID, ns)
	require.NoError(t, err)
	filename := "uncommitted_test_file"
	testLine := "TestLine"
	lineCount := 5
	createTestFile(t, filename, testLine, lineCount)
	defer os.Remove(filename)
	err = gc.SaveGarbageCollectionUncommitted(ctx, &repositoryRec, filename, runID)
	require.NoError(t, err)
	reader, err := blockAdapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: "",
		Identifier:       fmt.Sprintf("%s%s", location, filename),
		IdentifierType:   block.IdentifierTypeFull,
	}, 0)
	require.NoError(t, err)
	fileScanner := bufio.NewScanner(reader)
	line := 0
	for fileScanner.Scan() {
		require.Equal(t, fmt.Sprintf("%s_%d", testLine, line), fileScanner.Text())
		line += 1
	}
	require.Equal(t, lineCount, line)
}
