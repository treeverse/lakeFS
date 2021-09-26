package branches

import (
	"context"
	"errors"

	"github.com/gobwas/glob"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"google.golang.org/protobuf/proto"
)

const BranchProtectionSettingKey = "protected_branches"

var ErrorRuleAlreadyExists = errors.New("branch protection rule already exists")

type BranchProtectionManager struct {
	settingManager *settings.Manager
}

func NewBranchProtectionManager(settingManager *settings.Manager) *BranchProtectionManager {
	return &BranchProtectionManager{settingManager: settingManager}
}

func (m *BranchProtectionManager) Add(ctx context.Context, repositoryID graveler.RepositoryID, branchNamePattern string, constraints *graveler.BranchProtectionConstraints) error {
	alreadyExists := false
	err := m.settingManager.UpdateWithLock(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToConstraints == nil {
			rules.BranchPatternToConstraints = make(map[string]*graveler.BranchProtectionConstraints)
		}
		if _, ok := rules.BranchPatternToConstraints[branchNamePattern]; ok {
			alreadyExists = true
			return
		}
		rules.BranchPatternToConstraints[branchNamePattern] = constraints
	})
	if err != nil {
		return err
	}
	if alreadyExists {
		return ErrorRuleAlreadyExists
	}
	return nil
}

func (m *BranchProtectionManager) Set(ctx context.Context, repositoryID graveler.RepositoryID, branchNamePattern string, constraints *graveler.BranchProtectionConstraints) error {
	return m.settingManager.UpdateWithLock(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{}, func(message proto.Message) {
		rules := message.(*graveler.BranchProtectionRules)
		if rules.BranchPatternToConstraints == nil {
			rules.BranchPatternToConstraints = make(map[string]*graveler.BranchProtectionConstraints)
		}
		if len(constraints.GetValue()) == 0 {
			delete(rules.BranchPatternToConstraints, branchNamePattern)
		} else {
			rules.BranchPatternToConstraints[branchNamePattern] = constraints
		}
	})
}

func (m *BranchProtectionManager) Get(ctx context.Context, repositoryID graveler.RepositoryID, branchNamePattern string) (*graveler.BranchProtectionConstraints, error) {
	rules, err := m.settingManager.GetLatest(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{})
	if err != nil {
		return nil, err
	}
	return rules.(*graveler.BranchProtectionRules).BranchPatternToConstraints[branchNamePattern], nil
}

func (m *BranchProtectionManager) SetAll(ctx context.Context, repositoryID graveler.RepositoryID, rules *graveler.BranchProtectionRules) error {
	return m.settingManager.Save(ctx, repositoryID, BranchProtectionSettingKey, rules)
}

func (m *BranchProtectionManager) GetAll(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.BranchProtectionRules, error) {
	rules, err := m.settingManager.GetLatest(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{})
	if err == graveler.ErrNotFound {
		return &graveler.BranchProtectionRules{}, nil
	}
	if err != nil {
		return nil, err
	}
	return rules.(*graveler.BranchProtectionRules), nil
}

func (m *BranchProtectionManager) HasConstraint(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, constraint string) (bool, error) {
	rules, err := m.settingManager.Get(ctx, repositoryID, BranchProtectionSettingKey, &graveler.BranchProtectionRules{})
	if errors.Is(err, graveler.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for pattern, constraints := range rules.(*graveler.BranchProtectionRules).BranchPatternToConstraints {
		matcher := glob.MustCompile(pattern)
		if !matcher.Match(string(branchID)) {
			continue
		}
		for _, c := range constraints.GetValue() {
			if c == constraint {
				return true, nil
			}
		}
	}
	return false, nil
}
