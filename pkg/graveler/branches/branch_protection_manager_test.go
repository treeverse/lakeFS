package branches_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branches"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestGet(t *testing.T) {
	ctx := context.Background()
	bpm := prepareTest(t, ctx)
	rule, err := bpm.Get(ctx, "example-repo", "main*")
	testutil.Must(t, err)
	if rule != nil {
		t.Fatalf("expected nil rule, got %v", rule)
	}
	testutil.Must(t, bpm.Add(ctx, "example-repo", "main*", &graveler.BranchProtectionBlockedActions{Value: []string{"example-blocked-action"}}))
	rule, err = bpm.Get(ctx, "example-repo", "main*")
	testutil.Must(t, err)
	if diff := deep.Equal([]string{"example-blocked-action"}, rule.GetValue()); diff != nil {
		t.Fatalf("got unexpected blocked actions. diff=%s", diff)
	}
	rule, err = bpm.Get(ctx, "example-repo", "otherpattern")
	testutil.Must(t, err)
	if rule != nil {
		t.Fatalf("expected nil rule, got %v", rule)
	}
}

func TestAddAlreadyExists(t *testing.T) {
	ctx := context.Background()
	bpm := prepareTest(t, ctx)
	testutil.Must(t, bpm.Add(ctx, "example-repo", "main*", &graveler.BranchProtectionBlockedActions{Value: []string{"example-blocked-action"}}))
	err := bpm.Add(ctx, "example-repo", "main*", &graveler.BranchProtectionBlockedActions{Value: []string{"example-blocked-action2"}})
	if !errors.Is(err, branches.ErrorRuleAlreadyExists) {
		t.Fatalf("expected ErrorRuleAlreadyExists, got %v", err)
	}
}

func TestIsBlocked(t *testing.T) {
	ctx := context.Background()
	tests := map[string]struct {
		patternToBlockedActions map[string][]string
		expectedBlockedActions  map[string][]string
		expectedAllowedActions  map[string][]string
	}{
		"two_rules": {
			patternToBlockedActions: map[string][]string{"main*": {"action1"}, "dev": {"action2"}},
			expectedBlockedActions:  map[string][]string{"main": {"action1"}, "main2": {"action1"}, "dev": {"action2"}},
			expectedAllowedActions:  map[string][]string{"main": {"action2"}, "main2": {"action2"}, "dev": {"action1"}, "dev1": {"action1", "action2"}},
		},
		"multiple_blocked": {
			patternToBlockedActions: map[string][]string{"main*": {"action1", "action2", "action3"}, "stable/*": {"action3", "action4"}},
			expectedBlockedActions:  map[string][]string{"main": {"action1", "action2", "action3"}, "main2": {"action1", "action2", "action3"}, "stable/branch": {"action3", "action4"}},
			expectedAllowedActions:  map[string][]string{"main": {"action4"}, "main2": {"action4"}, "stable/branch": {"action1", "action2"}},
		},
		"overlapping_patterns": {
			patternToBlockedActions: map[string][]string{"main*": {"action1"}, "mai*": {"action2"}, "ma*": {"action2", "action3"}},
			expectedBlockedActions:  map[string][]string{"main": {"action1", "action2", "action3"}},
			expectedAllowedActions:  map[string][]string{"main": {"action4"}},
		},
		"no_rules": {
			expectedAllowedActions: map[string][]string{"main": {"action1", "action2"}},
		},
	}
	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			bpm := prepareTest(t, ctx)
			for pattern, blockedActions := range tst.patternToBlockedActions {
				testutil.Must(t, bpm.Add(ctx, "example-repo", pattern, &graveler.BranchProtectionBlockedActions{Value: blockedActions}))
			}
			for branchID, expectedBlockedActions := range tst.expectedBlockedActions {
				for _, action := range expectedBlockedActions {
					res, err := bpm.IsBlocked(ctx, "example-repo", graveler.BranchID(branchID), action)
					testutil.Must(t, err)
					if !res {
						t.Fatalf("branch %s action %s expected to be blocked, but was allowed", branchID, action)
					}
				}
			}
			for branchID, expectedAllowedActions := range tst.expectedAllowedActions {
				for _, action := range expectedAllowedActions {
					res, err := bpm.IsBlocked(ctx, "example-repo", graveler.BranchID(branchID), action)
					testutil.Must(t, err)
					if res {
						t.Fatalf("branch %s action %s expected to be allowed, but was blocked", branchID, action)
					}
				}
			}
		})
	}

}

func prepareTest(t *testing.T, ctx context.Context) *branches.BranchProtectionManager {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)
	blockAdapter := mem.New()
	branchLock := mock.NewMockBranchLocker(ctrl)
	cb := func(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		return f()
	}
	branchLock.EXPECT().MetadataUpdater(ctx, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(cb).AnyTimes()
	refManager.EXPECT().GetRepository(ctx, gomock.Any()).AnyTimes().Return(&graveler.Repository{
		StorageNamespace: "mem://my-storage",
		DefaultBranchID:  "main",
	}, nil)
	m := settings.NewManager(refManager, branchLock, blockAdapter, "_lakefs")
	return branches.NewBranchProtectionManager(m)
}
