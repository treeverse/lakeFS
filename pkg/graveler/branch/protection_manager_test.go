package branch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branch"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
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
	bpm := prepareTest(t, ctx)
	rule, err := bpm.Get(ctx, repository, "main*")
	testutil.Must(t, err)
	if rule != nil {
		t.Fatalf("expected nil rule, got %v", rule)
	}
	testutil.Must(t, bpm.SetRules(ctx, repository, &graveler.BranchProtectionRules{
		BranchPatternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{
			"main*": {Value: []graveler.BranchProtectionBlockedAction{
				graveler.BranchProtectionBlockedAction_STAGING_WRITE},
			},
		},
	}, nil))

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
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	bpm := prepareTest(t, ctx)
	err := bpm.Delete(ctx, repository, "main*")
	if !errors.Is(err, branch.ErrRuleNotExists) {
		t.Fatalf("expected ErrRuleNotExists, got %v", err)
	}
	testutil.Must(t, bpm.SetRules(ctx, repository, &graveler.BranchProtectionRules{
		BranchPatternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{
			"main*": {Value: []graveler.BranchProtectionBlockedAction{
				graveler.BranchProtectionBlockedAction_STAGING_WRITE},
			},
		},
	}, nil))
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
}

func TestIsBlocked(t *testing.T) {
	ctx := context.Background()
	var (
		action1 = graveler.BranchProtectionBlockedAction_STAGING_WRITE
		action2 = graveler.BranchProtectionBlockedAction_COMMIT
		action3 = graveler.BranchProtectionBlockedAction(2)
		action4 = graveler.BranchProtectionBlockedAction(3)
	)
	tests := map[string]struct {
		patternToBlockedActions map[string]*graveler.BranchProtectionBlockedActions
		expectedBlockedActions  map[string]*graveler.BranchProtectionBlockedActions
		expectedAllowedActions  map[string]*graveler.BranchProtectionBlockedActions
	}{
		"two_rules": {
			patternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{"main*": {Value: []graveler.BranchProtectionBlockedAction{action1}}, "dev": {Value: []graveler.BranchProtectionBlockedAction{action2}}},
			expectedBlockedActions:  map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action1}}, "main2": {Value: []graveler.BranchProtectionBlockedAction{action1}}, "dev": {Value: []graveler.BranchProtectionBlockedAction{action2}}},
			expectedAllowedActions:  map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action2}}, "main2": {Value: []graveler.BranchProtectionBlockedAction{action2}}, "dev": {Value: []graveler.BranchProtectionBlockedAction{action1}}, "dev1": {Value: []graveler.BranchProtectionBlockedAction{action1, action2}}},
		},
		"multiple_blocked": {
			patternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{"main*": {Value: []graveler.BranchProtectionBlockedAction{action1, action2, action3}}, "stable_*": {Value: []graveler.BranchProtectionBlockedAction{action3, action4}}},
			expectedBlockedActions:  map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action1, action2, action3}}, "main2": {Value: []graveler.BranchProtectionBlockedAction{action1, action2, action3}}, "stable_branch": {Value: []graveler.BranchProtectionBlockedAction{action3, action4}}},
			expectedAllowedActions:  map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action4}}, "main2": {Value: []graveler.BranchProtectionBlockedAction{action4}}, "stable_branch": {Value: []graveler.BranchProtectionBlockedAction{action1, action2}}},
		},
		"overlapping_patterns": {
			patternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{"main*": {Value: []graveler.BranchProtectionBlockedAction{action1}}, "mai*": {Value: []graveler.BranchProtectionBlockedAction{action2}}, "ma*": {Value: []graveler.BranchProtectionBlockedAction{action2, action3}}},
			expectedBlockedActions:  map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action1, action2, action3}}},
			expectedAllowedActions:  map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action4}}},
		},
		"no_rules": {
			expectedAllowedActions: map[string]*graveler.BranchProtectionBlockedActions{"main": {Value: []graveler.BranchProtectionBlockedAction{action1, action2}}},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			bpm := prepareTest(t, ctx)
			testutil.Must(t, bpm.SetRules(ctx, repository, &graveler.BranchProtectionRules{
				BranchPatternToBlockedActions: tst.patternToBlockedActions,
			}, nil))

			for branchID, expectedBlockedActions := range tst.expectedBlockedActions {
				for _, action := range expectedBlockedActions.Value {
					res, err := bpm.IsBlocked(ctx, repository, graveler.BranchID(branchID), action)
					testutil.Must(t, err)
					if !res {
						t.Fatalf("branch %s action %s expected to be blocked, but was allowed", branchID, action)
					}
				}
			}
			for branchID, expectedAllowedActions := range tst.expectedAllowedActions {
				for _, action := range expectedAllowedActions.Value {
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

func prepareTest(t *testing.T, ctx context.Context) *branch.ProtectionManager {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)
	branchLock := mock.NewMockBranchLocker(ctrl)
	cb := func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		return f()
	}
	branchLock.EXPECT().MetadataUpdater(ctx, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(cb).AnyTimes()
	refManager.EXPECT().GetRepository(ctx, gomock.Any()).AnyTimes().Return(repository, nil)
	kvStore := kvtest.GetStore(ctx, t)
	m := settings.NewManager(refManager, kvStore)

	return branch.NewProtectionManager(m)
}
