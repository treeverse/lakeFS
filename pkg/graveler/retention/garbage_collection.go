package retention

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	configFileSuffixTemplate  = "/%s/retention/gc/rules/config.json"
	commitsFileSuffixTemplate = "/%s/retention/gc/commits/run_id=%s/commits.csv"
)

type GarbageCollectionManager struct {
	blockAdapter                block.Adapter
	expiredCommitsFinder        *ExpiredCommitsFinder
	committedBlockStoragePrefix string
}

func NewGarbageCollectionManager(blockAdapter block.Adapter, commitGetter graveler.CommitGetter, branchLister graveler.BranchLister, committedBlockStoragePrefix string) *GarbageCollectionManager {
	return &GarbageCollectionManager{
		blockAdapter:                blockAdapter,
		expiredCommitsFinder:        NewExpiredCommitsFinder(branchLister, commitGetter),
		committedBlockStoragePrefix: committedBlockStoragePrefix,
	}
}

func (m *GarbageCollectionManager) GetRules(ctx context.Context, storageNamespace graveler.StorageNamespace) (*graveler.GarbageCollectionRules, error) {
	reader, err := m.blockAdapter.Get(ctx, block.ObjectPointer{
		Identifier:     string(storageNamespace) + fmt.Sprintf(configFileSuffixTemplate, m.committedBlockStoragePrefix),
		IdentifierType: block.IdentifierTypeFull,
	}, -1)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	var rules graveler.GarbageCollectionRules
	err = json.NewDecoder(reader).Decode(&rules)
	if err != nil {
		return nil, err
	}
	return &rules, nil
}

func (m *GarbageCollectionManager) SaveRules(ctx context.Context, storageNamespace graveler.StorageNamespace, rules *graveler.GarbageCollectionRules) error {
	rulesBytes, err := json.Marshal(rules)
	if err != nil {
		return err
	}
	return m.blockAdapter.Put(ctx, block.ObjectPointer{
		Identifier:     string(storageNamespace) + fmt.Sprintf(configFileSuffixTemplate, m.committedBlockStoragePrefix),
		IdentifierType: block.IdentifierTypeFull,
	}, int64(len(rulesBytes)), bytes.NewReader(rulesBytes), block.PutOpts{})
}

func (m *GarbageCollectionManager) GetRunExpiredCommits(ctx context.Context, storageNamespace graveler.StorageNamespace, runID string) ([]graveler.CommitID, error) {
	if runID == "" {
		return nil, nil
	}
	previousRunReader, err := m.blockAdapter.Get(ctx, block.ObjectPointer{
		Identifier:     string(storageNamespace) + fmt.Sprintf(commitsFileSuffixTemplate, m.committedBlockStoragePrefix, runID),
		IdentifierType: block.IdentifierTypeFull,
	}, -1)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(previousRunReader)
	previousCommits, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	res := make([]graveler.CommitID, 0)
	for _, commitRow := range previousCommits {
		if commitRow[1] == "true" {
			res = append(res, graveler.CommitID(commitRow[0]))
		}
	}
	return res, nil
}

func (m *GarbageCollectionManager) SaveGarbageCollectionCommits(ctx context.Context, storageNamespace graveler.StorageNamespace, repositoryID graveler.RepositoryID, rules *graveler.GarbageCollectionRules, previouslyExpiredCommits []graveler.CommitID) (string, error) {
	active, expired, err := m.expiredCommitsFinder.GetExpiredCommits(ctx, repositoryID, rules, previouslyExpiredCommits)
	if err != nil {
		return "", fmt.Errorf("find expired commits: %w", err)
	}
	b := &strings.Builder{}
	csvWriter := csv.NewWriter(b)
	for _, commitID := range expired {
		err := csvWriter.Write([]string{string(commitID), strconv.FormatBool(true)})
		if err != nil {
			return "", err
		}
	}
	for _, commitID := range active {
		err := csvWriter.Write([]string{string(commitID), strconv.FormatBool(false)})
		if err != nil {
			return "", err
		}
	}
	csvWriter.Flush()
	commitsStr := b.String()
	runID := uuid.New().String()
	err = m.blockAdapter.Put(ctx, block.ObjectPointer{
		Identifier:     string(storageNamespace) + fmt.Sprintf(commitsFileSuffixTemplate, m.committedBlockStoragePrefix, runID),
		IdentifierType: block.IdentifierTypeFull,
	}, int64(len(commitsStr)), strings.NewReader(commitsStr), block.PutOpts{})
	if err != nil {
		return "", err
	}
	return runID, nil
}
