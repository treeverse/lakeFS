package retention

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/graveler"
	"google.golang.org/protobuf/proto"
)

const (
	configFileSuffixTemplate    = "%s/retention/gc/rules/config.json"
	addressesFilePrefixTemplate = "%s/retention/gc/addresses/"
	commitsFileSuffixTemplate   = "%s/retention/gc/commits/run_id=%s/commits.csv"
)

type GarbageCollectionManager struct {
	blockAdapter                block.Adapter
	refManager                  graveler.RefManager
	committedBlockStoragePrefix string
}

func (m *GarbageCollectionManager) GetCommitsCSVLocation(runID string, sn graveler.StorageNamespace) (string, error) {
	key := fmt.Sprintf(commitsFileSuffixTemplate, m.committedBlockStoragePrefix, runID)
	qk, err := block.ResolveNamespace(sn.String(), key, block.IdentifierTypeRelative)
	if err != nil {
		return "", err
	}
	return qk.Format(), nil
}

func (m *GarbageCollectionManager) GetAddressesLocation(sn graveler.StorageNamespace) (string, error) {
	key := fmt.Sprintf(addressesFilePrefixTemplate, m.committedBlockStoragePrefix)
	qk, err := block.ResolveNamespace(sn.String(), key, block.IdentifierTypeRelative)
	if err != nil {
		return "", err
	}
	return qk.Format(), nil
}

type RepositoryCommitGetter struct {
	refManager graveler.RefManager
	repository *graveler.RepositoryRecord
}

func (r *RepositoryCommitGetter) GetCommit(ctx context.Context, commitID graveler.CommitID) (*graveler.Commit, error) {
	return r.refManager.GetCommit(ctx, r.repository, commitID)
}

func NewGarbageCollectionManager(blockAdapter block.Adapter, refManager graveler.RefManager, committedBlockStoragePrefix string) *GarbageCollectionManager {
	return &GarbageCollectionManager{
		blockAdapter:                blockAdapter,
		refManager:                  refManager,
		committedBlockStoragePrefix: committedBlockStoragePrefix,
	}
}

func (m *GarbageCollectionManager) GetRules(ctx context.Context, storageNamespace graveler.StorageNamespace) (*graveler.GarbageCollectionRules, error) {
	objectPointer := block.ObjectPointer{
		StorageNamespace: string(storageNamespace),
		Identifier:       fmt.Sprintf(configFileSuffixTemplate, m.committedBlockStoragePrefix),
		IdentifierType:   block.IdentifierTypeRelative,
	}
	reader, err := m.blockAdapter.Get(ctx, objectPointer, -1)
	if errors.Is(err, adapter.ErrDataNotFound) {
		return nil, graveler.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	var rules graveler.GarbageCollectionRules
	rulesBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(rulesBytes, &rules)
	if err != nil {
		return nil, err
	}
	return &rules, nil
}

func (m *GarbageCollectionManager) SaveRules(ctx context.Context, storageNamespace graveler.StorageNamespace, rules *graveler.GarbageCollectionRules) error {
	rulesBytes, err := proto.Marshal(rules)
	if err != nil {
		return err
	}
	return m.blockAdapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: string(storageNamespace),
		Identifier:       fmt.Sprintf(configFileSuffixTemplate, m.committedBlockStoragePrefix),
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(rulesBytes)), bytes.NewReader(rulesBytes), block.PutOpts{})
}

func (m *GarbageCollectionManager) GetRunExpiredCommits(ctx context.Context, storageNamespace graveler.StorageNamespace, runID string) ([]graveler.CommitID, error) {
	if runID == "" {
		return nil, nil
	}
	csvLocation, err := m.GetCommitsCSVLocation(runID, storageNamespace)
	if err != nil {
		return nil, err
	}
	previousRunReader, err := m.blockAdapter.Get(ctx, block.ObjectPointer{
		Identifier:     csvLocation,
		IdentifierType: block.IdentifierTypeFull,
	}, -1)
	if err != nil {
		return nil, err
	}
	defer previousRunReader.Close()
	csvReader := csv.NewReader(previousRunReader)
	csvReader.ReuseRecord = true
	var res []graveler.CommitID
	for {
		commitRow, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if commitRow[1] == "true" {
			res = append(res, graveler.CommitID(commitRow[0]))
		}
	}
	return res, nil
}

func (m *GarbageCollectionManager) SaveGarbageCollectionCommits(ctx context.Context, repository *graveler.RepositoryRecord, rules *graveler.GarbageCollectionRules, previouslyExpiredCommits []graveler.CommitID) (string, error) {
	commitGetter := &RepositoryCommitGetter{
		refManager: m.refManager,
		repository: repository,
	}
	branchIterator, err := m.refManager.GCBranchIterator(ctx, repository)
	if err != nil {
		return "", err
	}
	defer branchIterator.Close()
	// get all commits that are not the first parent of any commit:
	commitIterator, err := m.refManager.GCCommitIterator(ctx, repository)
	if err != nil {
		return "", fmt.Errorf("create kv orderd commit iterator commits: %w", err)
	}
	defer commitIterator.Close()
	startingPointIterator := NewGCStartingPointIterator(commitIterator, branchIterator)
	defer startingPointIterator.Close()
	gcCommits, err := GetGarbageCollectionCommits(ctx, startingPointIterator, commitGetter, rules, previouslyExpiredCommits)
	if err != nil {
		return "", fmt.Errorf("find expired commits: %w", err)
	}
	b := &strings.Builder{}
	csvWriter := csv.NewWriter(b)
	err = csvWriter.Write([]string{"commit_id", "expired"}) // write headers
	if err != nil {
		return "", err
	}
	for _, commitID := range gcCommits.expired {
		err := csvWriter.Write([]string{string(commitID), "true"})
		if err != nil {
			return "", err
		}
	}
	for _, commitID := range gcCommits.active {
		err := csvWriter.Write([]string{string(commitID), "false"})
		if err != nil {
			return "", err
		}
	}
	csvWriter.Flush()
	err = csvWriter.Error()
	if err != nil {
		return "", err
	}
	commitsStr := b.String()
	runID := uuid.New().String()
	csvLocation, err := m.GetCommitsCSVLocation(runID, repository.StorageNamespace)
	if err != nil {
		return "", err
	}
	err = m.blockAdapter.Put(ctx, block.ObjectPointer{
		Identifier:     csvLocation,
		IdentifierType: block.IdentifierTypeFull,
	}, int64(len(commitsStr)), strings.NewReader(commitsStr), block.PutOpts{})
	if err != nil {
		return "", err
	}
	return runID, nil
}
