package branch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gobwas/glob"
	"github.com/gobwas/glob/syntax"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"google.golang.org/protobuf/proto"
)

const ProtectionSettingKey = "protected_branches"

const (
	matcherCacheSize   = 100_000
	matcherCacheExpiry = 1 * time.Hour
	matcherCacheJitter = 1 * time.Minute
)

var (
	ErrRuleAlreadyExists = errors.New("branch protection rule already exists")
	ErrRuleNotExists     = errors.New("branch protection rule does not exist")
)

type ProtectionManager struct {
	settingManager settings.Manager
	matchers       cache.Cache
}

func NewProtectionManager(settingManager settings.Manager) *ProtectionManager {
	return &ProtectionManager{settingManager: settingManager, matchers: cache.NewCache(matcherCacheSize, matcherCacheExpiry, cache.NewJitterFn(matcherCacheJitter))}
}

func (m *ProtectionManager) Add(ctx context.Context, repository *graveler.RepositoryRecord, branchNamePattern string, blockedActions []graveler.BranchProtectionBlockedAction) error {
	_, err := syntax.Parse(branchNamePattern)
	if err != nil {
		return fmt.Errorf("invalid branch pattern syntax: %w", err)
	}
	return m.settingManager.Update(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) error {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToBlockedActions == nil {
			rules.BranchPatternToBlockedActions = make(map[string]*graveler.BranchProtectionBlockedActions)
		}
		if _, ok := rules.BranchPatternToBlockedActions[branchNamePattern]; ok {
			return ErrRuleAlreadyExists
		}
		rules.BranchPatternToBlockedActions[branchNamePattern] = &graveler.BranchProtectionBlockedActions{Value: blockedActions}
		return nil
	})
}

func (m *ProtectionManager) Delete(ctx context.Context, repository *graveler.RepositoryRecord, branchNamePattern string) error {
	return m.settingManager.Update(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) error {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToBlockedActions == nil {
			rules.BranchPatternToBlockedActions = make(map[string]*graveler.BranchProtectionBlockedActions)
		}
		if _, ok := rules.BranchPatternToBlockedActions[branchNamePattern]; !ok {
			return ErrRuleNotExists
		}
		delete(rules.BranchPatternToBlockedActions, branchNamePattern)
		return nil
	})
}

func (m *ProtectionManager) Get(ctx context.Context, repository *graveler.RepositoryRecord, branchNamePattern string) ([]graveler.BranchProtectionBlockedAction, error) {
	rules, err := m.settingManager.GetLatest(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{})
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

func (m *ProtectionManager) GetRules(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.BranchProtectionRules, error) {
	rules, err := m.settingManager.GetLatest(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{})
	if errors.Is(err, graveler.ErrNotFound) {
		return &graveler.BranchProtectionRules{}, nil
	}
	if err != nil {
		return nil, err
	}
	return rules.(*graveler.BranchProtectionRules), nil
}

func (m *ProtectionManager) IsBlocked(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, action graveler.BranchProtectionBlockedAction) (bool, error) {
	rules, err := m.settingManager.Get(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{})
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
