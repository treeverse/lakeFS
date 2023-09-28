package branch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gobwas/glob"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/proto"
)

const ProtectionSettingKey = "protected_branches"

const (
	matcherCacheSize   = 100_000
	matcherCacheExpiry = 1 * time.Hour
	matcherCacheJitter = 1 * time.Minute
)

var (
	ErrRuleNotExists = fmt.Errorf("branch protection rule does not exist: %w", graveler.ErrNotFound)
)

type ProtectionManager struct {
	settingManager *settings.Manager
	matchers       cache.Cache
}

func NewProtectionManager(settingManager *settings.Manager) *ProtectionManager {
	return &ProtectionManager{settingManager: settingManager, matchers: cache.NewCache(matcherCacheSize, matcherCacheExpiry, cache.NewJitterFn(matcherCacheJitter))}
}

func (m *ProtectionManager) Delete(ctx context.Context, repository *graveler.RepositoryRecord, branchNamePattern string) error {
	return m.settingManager.Update(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) (proto.Message, error) {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToBlockedActions == nil {
			rules.BranchPatternToBlockedActions = make(map[string]*graveler.BranchProtectionBlockedActions)
		}
		if _, ok := rules.BranchPatternToBlockedActions[branchNamePattern]; !ok {
			return nil, ErrRuleNotExists
		}
		delete(rules.BranchPatternToBlockedActions, branchNamePattern)
		return rules, nil
	})
}

func (m *ProtectionManager) GetRules(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.BranchProtectionRules, string, error) {
	rulesMsg, checksum, err := m.settingManager.GetLatest(ctx, repository, ProtectionSettingKey, &graveler.BranchProtectionRules{})
	if errors.Is(err, graveler.ErrNotFound) {
		return &graveler.BranchProtectionRules{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	if proto.Size(rulesMsg) == 0 {
		return &graveler.BranchProtectionRules{}, checksum, nil
	}
	return rulesMsg.(*graveler.BranchProtectionRules), checksum, nil
}
func (m *ProtectionManager) SetRules(ctx context.Context, repository *graveler.RepositoryRecord, rules *graveler.BranchProtectionRules) error {
	return m.settingManager.Save(ctx, repository, ProtectionSettingKey, rules)
}

func (m *ProtectionManager) SetRulesIf(ctx context.Context, repository *graveler.RepositoryRecord, rules *graveler.BranchProtectionRules, lastKnownChecksum string) error {
	err := m.settingManager.SaveIf(ctx, repository, ProtectionSettingKey, rules, lastKnownChecksum)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return graveler.ErrPreconditionFailed
	}
	return err
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
