package retention

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type RuleManager struct {
	configurationFileSuffix string
	blockAdapter            block.Adapter
}

func NewRuleManager(blockAdapter block.Adapter, blockStoragePrefix string) *RuleManager {
	return &RuleManager{blockAdapter: blockAdapter, configurationFileSuffix: fmt.Sprintf("/%s/retention/rules/config.json", blockStoragePrefix)}
}

func (m *RuleManager) GetRules(ctx context.Context, configurationFilePrefix string) (*graveler.RetentionRules, error) {
	reader, err := m.blockAdapter.Get(ctx, block.ObjectPointer{
		Identifier:     configurationFilePrefix + m.configurationFileSuffix,
		IdentifierType: block.IdentifierTypeFull,
	}, -1)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	var rules graveler.RetentionRules
	err = json.NewDecoder(reader).Decode(&rules)
	if err != nil {
		return nil, err
	}
	return &rules, nil
}

func (m *RuleManager) SaveRules(ctx context.Context, configurationFilePrefix string, rules *graveler.RetentionRules) error {
	rulesBytes, err := json.Marshal(rules)
	if err != nil {
		return err
	}
	return m.blockAdapter.Put(ctx, block.ObjectPointer{
		Identifier:     configurationFilePrefix + m.configurationFileSuffix,
		IdentifierType: block.IdentifierTypeFull,
	}, int64(len(rulesBytes)), bytes.NewReader(rulesBytes), block.PutOpts{})
}
