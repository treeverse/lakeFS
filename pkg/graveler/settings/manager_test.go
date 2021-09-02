package settings

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestManager(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)
	repo := &graveler.Repository{
		StorageNamespace: "mem://my-storage/",
		DefaultBranchID:  "main",
	}
	blockAdapter := mem.New()
	branchLock := mock.NewMockBranchLocker(ctrl)
	m := &Manager{
		refManager:                  refManager,
		branchLock:                  branchLock,
		blockAdapter:                blockAdapter,
		committedBlockStoragePrefix: "_lakefs",
	}
	refManager.EXPECT().GetRepository(ctx, gomock.Eq(graveler.RepositoryID("example-repo"))).AnyTimes().Return(repo, nil)
	branchLock.EXPECT().MetadataUpdater(ctx, gomock.Eq(graveler.RepositoryID("example-repo")), graveler.BranchID("main"), gomock.Any()).DoAndReturn(func(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		return f()
	})
	rules := &graveler.GarbageCollectionRules{DefaultRetentionDays: 3, BranchRetentionDays: map[string]int32{"boo": 1}}
	err := m.Save(ctx, "example-repo", "gcrules", rules)
	testutil.Must(t, err)
	gotRules := &graveler.GarbageCollectionRules{}
	err = m.Get(ctx, "example-repo", "gcrules", gotRules)
	testutil.Must(t, err)
	println(gotRules.DefaultRetentionDays)
	println(gotRules.BranchRetentionDays["boo"])
	rulesToEdit := &graveler.GarbageCollectionRules{}
	err = m.UpdateWithLock(ctx, "example-repo", "gcrules", rulesToEdit, func() {
		rulesToEdit.DefaultRetentionDays--
		rulesToEdit.BranchRetentionDays["boo"]--
	})
	testutil.Must(t, err)
	gotRules = &graveler.GarbageCollectionRules{}
	err = m.Get(ctx, "example-repo", "gcrules", gotRules)
	testutil.Must(t, err)
	println(gotRules.DefaultRetentionDays)
	println(gotRules.BranchRetentionDays["boo"])
}
