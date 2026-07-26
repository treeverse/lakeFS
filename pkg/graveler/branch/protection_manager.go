package branch

import (
	"context"
	"errors"
	"slices"
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
		matched, err := m.matchBranch(pattern, branchID)
		if err != nil {
			return false, err
		}
		if !matched {
			continue
		}
		if slices.Contains(blockedActions.GetValue(), action) {
			return true, nil
		}
	}
	return false, nil
}

// RequiredApprovals returns the highest required-approvals count configured across branch protection
// rules whose pattern matches branchID (0 if none match).
func (m *ProtectionManager) RequiredApprovals(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (uint32, error) {
	rules := &graveler.BranchProtectionRules{}
	err := m.settingManager.Get(ctx, repository, ProtectionSettingKey, rules)
	if errors.Is(err, graveler.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var required uint32
	for pattern, count := range rules.BranchPatternToRequiredApprovals {
		matched, err := m.matchBranch(pattern, branchID)
		if err != nil {
			return 0, err
		}
		if matched && count > required {
			required = count
		}
	}
	return required, nil
}

func (m *ProtectionManager) matchBranch(pattern string, branchID graveler.BranchID) (bool, error) {
	matcher, err := m.matchers.GetOrSet(pattern, func() (v any, err error) {
		return glob.Compile(pattern)
	})
	if err != nil {
		return false, err
	}
	return matcher.(glob.Glob).Match(string(branchID)), nil
}
