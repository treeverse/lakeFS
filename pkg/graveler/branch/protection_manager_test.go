package branch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branch"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var repository = &graveler.RepositoryRecord{
	RepositoryID: "example-repo",
	Repository: &graveler.Repository{
		StorageNamespace: "mem://my-storage",
		DefaultBranchID:  "main",
	},
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		kvEnabled bool
	}{
		{
			kvEnabled: false,
		},
		{
			kvEnabled: true,
		},
	}
	for _, tt := range tests {
		kvSuffix := ""
		if tt.kvEnabled {
			kvSuffix = "_KV"
		}
		t.Run("TestGet"+kvSuffix, func(t *testing.T) {
			bpm := prepareTest(t, ctx, tt.kvEnabled)
			rule, err := bpm.Get(ctx, repository, "main*")
			testutil.Must(t, err)
			if rule != nil {
				t.Fatalf("expected nil rule, got %v", rule)
			}
			testutil.Must(t, bpm.Add(ctx, repository, "main*", []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE}))
			rule, err = bpm.Get(ctx, repository, "main*")
			testutil.Must(t, err)
			if diff := deep.Equal([]graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE}, rule); diff != nil {
				t.Fatalf("got unexpected blocked actions. diff=%s", diff)
			}
			rule, err = bpm.Get(ctx, repository, "otherpattern")
			testutil.Must(t, err)
			if rule != nil {
				t.Fatalf("expected nil rule, got %v", rule)
			}
		})
	}
}

func TestAddAlreadyExists(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		kvEnabled bool
	}{
		{
			kvEnabled: false,
		},
		{
			kvEnabled: true,
		},
	}
	for _, tt := range tests {
		kvSuffix := ""
		if tt.kvEnabled {
			kvSuffix = "_KV"
		}
		t.Run("TestAddAlreadyExists"+kvSuffix, func(t *testing.T) {
			bpm := prepareTest(t, ctx, tt.kvEnabled)
			testutil.Must(t, bpm.Add(ctx, repository, "main*", []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE}))
			err := bpm.Add(ctx, repository, "main*", []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_COMMIT})
			if !errors.Is(err, branch.ErrRuleAlreadyExists) {
				t.Fatalf("expected ErrRuleAlreadyExists, got %v", err)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		kvEnabled bool
	}{
		{
			kvEnabled: false,
		},
		{
			kvEnabled: true,
		},
	}
	for _, tt := range tests {
		kvSuffix := ""
		if tt.kvEnabled {
			kvSuffix = "_KV"
		}
		t.Run("TestDelete"+kvSuffix, func(t *testing.T) {
			bpm := prepareTest(t, ctx, tt.kvEnabled)
			err := bpm.Delete(ctx, repository, "main*")
			if !errors.Is(err, branch.ErrRuleNotExists) {
				t.Fatalf("expected ErrRuleNotExists, got %v", err)
			}
			testutil.Must(t, bpm.Add(ctx, repository, "main*", []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE}))
			rule, err := bpm.Get(ctx, repository, "main*")
			testutil.Must(t, err)
			if diff := deep.Equal([]graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_STAGING_WRITE}, rule); diff != nil {
				t.Fatalf("got unexpected blocked actions. diff=%s", diff)
			}
			testutil.Must(t, bpm.Delete(ctx, repository, "main*"))

			rule, err = bpm.Get(ctx, repository, "main*")
			testutil.Must(t, err)
			if rule != nil {
				t.Fatalf("expected nil rule after delete, got %v", rule)
			}
		})
	}
}

func TestIsBlocked(t *testing.T) {
	tests := []struct {
		kvEnabled bool
	}{
		{
			kvEnabled: false,
		},
		{
			kvEnabled: true,
		},
	}
	for _, tt := range tests {
		kvSuffix := ""
		if tt.kvEnabled {
			kvSuffix = "_KV"
		}
		t.Run("TestIsBlocked"+kvSuffix, func(t *testing.T) {
			testIsBlocked(t, tt.kvEnabled)
		})
	}
}

func testIsBlocked(t *testing.T, kvEnabled bool) {
	ctx := context.Background()
	const (
		action1 = graveler.BranchProtectionBlockedAction_STAGING_WRITE
		action2 = graveler.BranchProtectionBlockedAction_COMMIT
		action3 = 2
		action4 = 3
	)
	tests := map[string]struct {
		patternToBlockedActions map[string][]graveler.BranchProtectionBlockedAction
		expectedBlockedActions  map[string][]graveler.BranchProtectionBlockedAction
		expectedAllowedActions  map[string][]graveler.BranchProtectionBlockedAction
	}{
		"two_rules": {
			patternToBlockedActions: map[string][]graveler.BranchProtectionBlockedAction{"main*": {action1}, "dev": {action2}},
			expectedBlockedActions:  map[string][]graveler.BranchProtectionBlockedAction{"main": {action1}, "main2": {action1}, "dev": {action2}},
			expectedAllowedActions:  map[string][]graveler.BranchProtectionBlockedAction{"main": {action2}, "main2": {action2}, "dev": {action1}, "dev1": {action1, action2}},
		},
		"multiple_blocked": {
			patternToBlockedActions: map[string][]graveler.BranchProtectionBlockedAction{"main*": {action1, action2, action3}, "stable_*": {action3, action4}},
			expectedBlockedActions:  map[string][]graveler.BranchProtectionBlockedAction{"main": {action1, action2, action3}, "main2": {action1, action2, action3}, "stable_branch": {action3, action4}},
			expectedAllowedActions:  map[string][]graveler.BranchProtectionBlockedAction{"main": {action4}, "main2": {action4}, "stable_branch": {action1, action2}},
		},
		"overlapping_patterns": {
			patternToBlockedActions: map[string][]graveler.BranchProtectionBlockedAction{"main*": {action1}, "mai*": {action2}, "ma*": {action2, action3}},
			expectedBlockedActions:  map[string][]graveler.BranchProtectionBlockedAction{"main": {action1, action2, action3}},
			expectedAllowedActions:  map[string][]graveler.BranchProtectionBlockedAction{"main": {action4}},
		},
		"no_rules": {
			expectedAllowedActions: map[string][]graveler.BranchProtectionBlockedAction{"main": {action1, action2}},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			bpm := prepareTest(t, ctx, kvEnabled)
			for pattern, blockedActions := range tst.patternToBlockedActions {
				testutil.Must(t, bpm.Add(ctx, repository, pattern, blockedActions))
			}
			for branchID, expectedBlockedActions := range tst.expectedBlockedActions {
				for _, action := range expectedBlockedActions {
					res, err := bpm.IsBlocked(ctx, repository, graveler.BranchID(branchID), action)
					testutil.Must(t, err)
					if !res {
						t.Fatalf("branch %s action %s expected to be blocked, but was allowed", branchID, action)
					}
				}
			}
			for branchID, expectedAllowedActions := range tst.expectedAllowedActions {
				for _, action := range expectedAllowedActions {
					res, err := bpm.IsBlocked(ctx, repository, graveler.BranchID(branchID), action)
					testutil.Must(t, err)
					if res {
						t.Fatalf("branch %s action %s expected to be allowed, but was blocked", branchID, action)
					}
				}
			}
		})
	}

}

func prepareTest(t *testing.T, ctx context.Context, kvEnabled bool) *branch.ProtectionManager {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)
	branchLock := mock.NewMockBranchLocker(ctrl)
	cb := func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		return f()
	}
	branchLock.EXPECT().MetadataUpdater(ctx, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(cb).AnyTimes()
	refManager.EXPECT().GetRepository(ctx, gomock.Any()).AnyTimes().Return(repository, nil)
	var m settings.Manager
	if kvEnabled {
		kvStore := kvtest.GetStore(ctx, t)
		m = settings.NewManager(refManager, kv.StoreMessage{Store: kvStore})
	} else {
		blockAdapter := mem.New()
		m = settings.NewDBManager(refManager, branchLock, blockAdapter, "_lakefs")
	}
	return branch.NewProtectionManager(m)
}
