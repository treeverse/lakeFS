package branches

import (
	"context"
	"errors"
	"time"

	"github.com/gobwas/glob"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"google.golang.org/protobuf/proto"
)

const BranchProtectionSettingKey = "protected_branches"

const (
	matcherCacheSize   = 100_000
	matcherCacheExpiry = 1 * time.Hour
	matcherCacheJitter = 1 * time.Minute
)

var ErrorRuleAlreadyExists = errors.New("branch protection rule already exists")

type BranchProtectionManager struct {
	settingManager *settings.Manager
	matchers       cache.Cache
}

func NewBranchProtectionManager(settingManager *settings.Manager) *BranchProtectionManager {
	return &BranchProtectionManager{settingManager: settingManager, matchers: cache.NewCache(matcherCacheSize, matcherCacheExpiry, cache.NewJitterFn(matcherCacheJitter))}
}

func (m *BranchProtectionManager) Add(ctx context.Context, repositoryID graveler.RepositoryID, branchNamePattern string, blockedActions []graveler.BranchProtectionBlockedAction) error {
	return m.settingManager.UpdateWithLock(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) error {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToBlockedActions == nil {
			rules.BranchPatternToBlockedActions = make(map[string]*graveler.BranchProtectionBlockedActions)
		}
		if _, ok := rules.BranchPatternToBlockedActions[branchNamePattern]; ok {
			return ErrorRuleAlreadyExists
		}
		rules.BranchPatternToBlockedActions[branchNamePattern] = &graveler.BranchProtectionBlockedActions{Value: blockedActions}
		return nil
	})
}

func (m *BranchProtectionManager) Set(ctx context.Context, repositoryID graveler.RepositoryID, branchNamePattern string, blockedActions []graveler.BranchProtectionBlockedAction) error {
	return m.settingManager.UpdateWithLock(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) error {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToBlockedActions == nil {
			rules.BranchPatternToBlockedActions = make(map[string]*graveler.BranchProtectionBlockedActions)
		}
		if len(blockedActions) == 0 {
			delete(rules.BranchPatternToBlockedActions, branchNamePattern)
		} else {
			rules.BranchPatternToBlockedActions[branchNamePattern] = &graveler.BranchProtectionBlockedActions{Value: blockedActions}
		}
		return nil
	})
}

func (m *BranchProtectionManager) Get(ctx context.Context, repositoryID graveler.RepositoryID, branchNamePattern string) ([]graveler.BranchProtectionBlockedAction, error) {
	rules, err := m.settingManager.GetLatest(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{})
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actions := rules.(*graveler.BranchProtectionRules).BranchPatternToBlockedActions[branchNamePattern]
	if actions == nil {
		return nil, nil
	}
	return actions.GetValue(), nil
}

func (m *BranchProtectionManager) GetAll(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.BranchProtectionRules, error) {
	rules, err := m.settingManager.GetLatest(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{})
	if errors.Is(err, graveler.ErrNotFound) {
		return &graveler.BranchProtectionRules{}, nil
	}
	if err != nil {
		return nil, err
	}
	return rules.(*graveler.BranchProtectionRules), nil
}

func (m *BranchProtectionManager) IsBlocked(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, action graveler.BranchProtectionBlockedAction) (bool, error) {
	rules, err := m.settingManager.Get(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{})
	if errors.Is(err, graveler.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for pattern, blockedActions := range rules.(*graveler.BranchProtectionRules).BranchPatternToBlockedActions {
		pattern := pattern
		matcher, err := m.matchers.GetOrSet(pattern, func() (v interface{}, err error) {
			return glob.Compile(pattern)
		})
		if err != nil {
			return false, err
		}
		if !matcher.(glob.Glob).Match(string(branchID)) {
			continue
		}
		for _, c := range blockedActions.GetValue() {
			if c == action {
				return true, nil
			}
		}
	}
	return false, nil
}
