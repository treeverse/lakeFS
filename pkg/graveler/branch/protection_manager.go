package branch

import (
	"context"
	"errors"
	"time"

	"github.com/gobwas/glob"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
)

const ProtectionSettingKey = "protected_branches"

const (
	matcherCacheSize   = 100_000
	matcherCacheExpiry = 1 * time.Hour
	matcherCacheJitter = 1 * time.Minute
)

type ProtectionManager struct {
	settingManager *settings.Manager
	matchers       cache.Cache
}

func NewProtectionManager(settingManager *settings.Manager) *ProtectionManager {
	return &ProtectionManager{settingManager: settingManager, matchers: cache.NewCache(matcherCacheSize, matcherCacheExpiry, cache.NewJitterFn(matcherCacheJitter))}
}

func (m *ProtectionManager) GetRules(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.BranchProtectionRules, *string, error) {
	rulesMsg := &graveler.BranchProtectionRules{}
	checksum, err := m.settingManager.GetLatest(ctx, repository, ProtectionSettingKey, rulesMsg)
	if err != nil {
		return nil, nil, err
	}
	return rulesMsg, checksum, nil
}

func (m *ProtectionManager) SetRules(ctx context.Context, repository *graveler.RepositoryRecord, rules *graveler.BranchProtectionRules, lastKnownChecksum *string) error {
	return m.settingManager.Save(ctx, repository, ProtectionSettingKey, rules, lastKnownChecksum)
}

func (m *ProtectionManager) IsBlocked(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, action graveler.BranchProtectionBlockedAction) (bool, error) {
	rules := &graveler.BranchProtectionRules{}
	err := m.settingManager.Get(ctx, repository, ProtectionSettingKey, rules)
	if errors.Is(err, graveler.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for pattern, blockedActions := range rules.BranchPatternToBlockedActions {
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
