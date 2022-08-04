package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
)

// IteratorPrefetchSize is the amount of records to maybeFetch from PG
const IteratorPrefetchSize = 1000

// MaxBatchDelay - 3ms was chosen as a max delay time for critical path queries.
// It trades off amount of queries per second (and thus effectiveness of the batching mechanism) with added latency.
// Since reducing # of expensive operations is only beneficial when there are a lot of concurrent requests,
// 	the sweet spot is probably between 1-5 milliseconds (representing 200-1000 requests/second to the data store).
// 3ms of delay with ~300 requests/second per resource sounds like a reasonable tradeoff.
const MaxBatchDelay = time.Millisecond * 3

const BatchUpdateSQLSize = 10000

type KVManager struct {
	kvStore         kv.StoreMessage
	addressProvider ident.AddressProvider
	batchExecutor   batch.Batcher
}

type CommitNode struct {
	children       []graveler.CommitID
	parentsToVisit map[graveler.CommitID]struct{}
	generation     int
}

func branchFromProto(pb *graveler.BranchData) *graveler.Branch {
	var sealedTokens []graveler.StagingToken
	for _, st := range pb.SealedTokens {
		sealedTokens = append(sealedTokens, graveler.StagingToken(st))
	}
	branch := &graveler.Branch{
		CommitID:     graveler.CommitID(pb.CommitId),
		StagingToken: graveler.StagingToken(pb.StagingToken),
		SealedTokens: sealedTokens,
	}
	return branch
}

func protoFromBranch(branchID graveler.BranchID, b *graveler.Branch) *graveler.BranchData {
	var sealedTokens []string
	for _, st := range b.SealedTokens {
		sealedTokens = append(sealedTokens, st.String())
	}
	branch := &graveler.BranchData{
		Id:           branchID.String(),
		CommitId:     b.CommitID.String(),
		StagingToken: b.StagingToken.String(),
		SealedTokens: sealedTokens,
	}
	return branch
}

func NewKVRefManager(executor batch.Batcher, kvStore kv.StoreMessage, addressProvider ident.AddressProvider) *KVManager {
	return &KVManager{
		kvStore:         kvStore,
		addressProvider: addressProvider,
		batchExecutor:   executor,
	}
}

func (m *KVManager) getRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	data := graveler.RepositoryData{}
	_, err := m.kvStore.GetMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)), &data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrRepositoryNotFound
		}
		return nil, err
	}

	return graveler.RepoFromProto(&data).Repository, nil
}

// getRepositoryRec returns RepositoryRecord
func (m *KVManager) getRepositoryRec(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	repo, err := m.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return &graveler.RepositoryRecord{
		RepositoryID: repositoryID,
		Repository:   repo,
	}, nil
}

func (m *KVManager) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	repo, err := m.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	switch repo.State {
	case graveler.RepositoryState_ACTIVE:
		return repo, nil
	case graveler.RepositoryState_IN_DELETION:
		return nil, graveler.ErrRepositoryInDeletion
	default:
		return nil, fmt.Errorf("invalid repository state %v: %w", repo.State, graveler.ErrInvalid)
	}
}

func (m *KVManager) createBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	repoRecord := &graveler.RepositoryRecord{
		RepositoryID: repositoryID,
		Repository:   &repository,
	}
	repo := graveler.ProtoFromRepo(repoRecord)

	err := m.kvStore.SetMsgIf(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)), repo, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrNotUnique
		}
		return err
	}
	return nil
}

func (m *KVManager) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	firstCommit := graveler.NewCommit()
	firstCommit.Message = graveler.FirstCommitMsg
	firstCommit.Generation = 1

	repo := &graveler.RepositoryRecord{RepositoryID: repositoryID, Repository: &repository}
	// If branch creation fails - this commit will become dangling. This is a known issue that can be resolved via garbage collection
	commitID, err := m.addCommit(ctx, graveler.RepoPartition(repo), firstCommit)
	if err != nil {
		return err
	}

	branch := graveler.Branch{
		CommitID:     commitID,
		StagingToken: graveler.GenerateStagingToken(repositoryID, repository.DefaultBranchID),
		SealedTokens: nil,
	}
	err = m.createBranch(ctx, graveler.RepoPartition(repo), repository.DefaultBranchID, branch)
	if err != nil {
		return err
	}

	return m.createBareRepository(ctx, repositoryID, repository)
}

func (m *KVManager) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	return m.createBareRepository(ctx, repositoryID, repository)
}

func (m *KVManager) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return NewKVRepositoryIterator(ctx, &m.kvStore)
}

func (m *KVManager) updateRepoState(ctx context.Context, repo *graveler.RepositoryRecord, state graveler.RepositoryState) error {
	repo.State = state
	return m.kvStore.SetMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repo.RepositoryID)), graveler.ProtoFromRepo(repo))
}

func (m *KVManager) deleteRepository(ctx context.Context, repo *graveler.RepositoryRecord) {
	logger := logging.FromContext(ctx)
	// Scan through repository partition and delete entities
	partition := graveler.RepoPartition(repo)
	it, err := kv.NewPartitionIterator(ctx, m.kvStore.Store, (&kv.SecondaryIndex{}).ProtoReflect().Type(), partition)
	if err != nil {
		if err != nil {
			logger.WithField("repository", repo.RepositoryID).
				WithError(err).
				Error("Error during iteration")
		}
	}
	defer it.Close()
	for it.Next() {
		v := it.Entry()
		err = m.kvStore.DeleteMsg(ctx, partition, v.Key)
		if err != nil {
			logger.WithField("repository", repo.RepositoryID).
				WithField("key", string(v.Key)).
				Error("Failed to delete repository entity")
		}
	}
	if it.Err() != nil {
		if err != nil {
			logger.WithField("repository", repo.RepositoryID).
				WithError(it.Err()).
				Error("Error during iteration")
		}
	}

	// Finally delete the repository record itself
	err = m.kvStore.DeleteMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repo.RepositoryID)))
	if err != nil {
		logger.WithField("repository", repo.RepositoryID).Error("Failed to delete repository entry")
	}
}

func (m *KVManager) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}

	// Set repository state to deleted and then perform background delete
	if repo.State != graveler.RepositoryState_IN_DELETION {
		err = m.updateRepoState(ctx, repo, graveler.RepositoryState_IN_DELETION)
		if err != nil {
			return err
		}
	}

	// Background delete of repository, return success immediately
	go m.deleteRepository(ctx, repo)
	return nil
}

func (m *KVManager) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	return ParseRef(ref)
}

func (m *KVManager) ResolveRawRef(ctx context.Context, repositoryID graveler.RepositoryID, raw graveler.RawRef) (*graveler.ResolvedRef, error) {
	return ResolveRawRef(ctx, m, m.addressProvider, repositoryID, raw)
}

func (m *KVManager) getBranchWithPredicate(ctx context.Context, repo *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, kv.Predicate, error) {
	key := graveler.BranchPath(branchID)
	data := graveler.BranchData{}
	pred, err := m.kvStore.GetMsg(ctx, graveler.RepoPartition(repo), []byte(key), &data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrBranchNotFound
		}
		return nil, nil, err
	}
	return branchFromProto(&data), pred, nil
}

func (m *KVManager) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	branch, _, err := m.getBranchWithPredicate(ctx, repo, branchID)
	return branch, err
}

func (m *KVManager) createBranch(ctx context.Context, repositoryPartition string, branchID graveler.BranchID, branch graveler.Branch) error {
	err := m.kvStore.SetMsgIf(ctx, repositoryPartition, []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, &branch), nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		err = graveler.ErrBranchExists
	}
	return err
}

func (m *KVManager) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, branch graveler.Branch) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}

	return m.createBranch(ctx, graveler.RepoPartition(repo), branchID, branch)
}

func (m *KVManager) SetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, branch graveler.Branch) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}
	return m.kvStore.SetMsg(ctx, graveler.RepoPartition(repo), []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, &branch))
}

func (m *KVManager) BranchUpdate(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, f graveler.BranchUpdateFunc) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}
	b, pred, err := m.getBranchWithPredicate(ctx, repo, branchID)
	if err != nil {
		return err
	}
	newBranch, err := f(b)
	// return on error or nothing to update
	if err != nil || newBranch == nil {
		return err
	}
	return m.kvStore.SetMsgIf(ctx, graveler.RepoPartition(repo), []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, newBranch), pred)
}

func (m *KVManager) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}
	_, err = m.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return m.kvStore.DeleteMsg(ctx, graveler.RepoPartition(repo), []byte(graveler.BranchPath(branchID)))
}

func (m *KVManager) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewBranchSimpleIterator(ctx, &m.kvStore, repo)
}

func (m *KVManager) GCBranchIterator(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewBranchByCommitIterator(ctx, &m.kvStore, repo)
}

func (m *KVManager) GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	tagKey := graveler.TagPath(tagID)
	t := graveler.TagData{}
	_, err = m.kvStore.GetMsg(ctx, graveler.RepoPartition(repo), []byte(tagKey), &t)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrTagNotFound
		}
		return nil, err
	}
	commitID := graveler.CommitID(t.CommitId)
	return &commitID, nil
}

func (m *KVManager) CreateTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}
	t := &graveler.TagData{
		Id:       tagID.String(),
		CommitId: commitID.String(),
	}
	tagKey := graveler.TagPath(tagID)
	err = m.kvStore.SetMsgIf(ctx, graveler.RepoPartition(repo), []byte(tagKey), t, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrTagAlreadyExists
		}
		return err
	}
	return nil
}

func (m *KVManager) DeleteTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}
	tagKey := graveler.TagPath(tagID)
	// TODO (issue 3640) align with delete tag DB - return ErrNotFound when tag does not exist
	return m.kvStore.DeleteMsg(ctx, graveler.RepoPartition(repo), []byte(tagKey))
}

func (m *KVManager) ListTags(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.TagIterator, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewKVTagIterator(ctx, &m.kvStore, repo)
}

func (m *KVManager) GetCommitByPrefix(ctx context.Context, repositoryID graveler.RepositoryID, prefix graveler.CommitID) (*graveler.Commit, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	it, err := NewKVOrderedCommitIterator(ctx, &m.kvStore, repo, false)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	it.SeekGE(prefix)
	var commit *graveler.Commit
	for it.Next() {
		c := it.Value()
		if strings.HasPrefix(string(c.CommitID), string(prefix)) {
			if commit != nil {
				return nil, graveler.ErrCommitNotFound // more than 1 commit starts with the ID prefix
			}
			commit = c.Commit
		} else {
			break
		}
	}
	if commit == nil {
		return nil, graveler.ErrCommitNotFound
	}
	return commit, nil
}

func (m *KVManager) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	commitKey := graveler.CommitPath(commitID)
	c := graveler.CommitData{}
	_, err = m.kvStore.GetMsg(ctx, graveler.RepoPartition(repo), []byte(commitKey), &c)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrCommitNotFound
		}
		return nil, err
	}
	return graveler.CommitFromProto(&c), nil
}

func (m *KVManager) addCommit(ctx context.Context, repoPartition string, commit graveler.Commit) (graveler.CommitID, error) {
	commitID := m.addressProvider.ContentAddress(commit)
	c := graveler.ProtoFromCommit(graveler.CommitID(commitID), &commit)
	commitKey := graveler.CommitPath(graveler.CommitID(commitID))
	err := m.kvStore.SetMsgIf(ctx, repoPartition, []byte(commitKey), c, nil)
	// commits are written based on their content hash, if we insert the same ID again,
	// it will necessarily have the same attributes as the existing one, so if a commit already exists doesn't return an error
	if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
		return "", err
	}
	return graveler.CommitID(commitID), nil
}

func (m *KVManager) AddCommit(ctx context.Context, repositoryID graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	return m.addCommit(ctx, graveler.RepoPartition(repo), commit)
}

func (m *KVManager) RemoveCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) error {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return err
	}

	commitKey := graveler.CommitPath(commitID)
	return m.kvStore.DeleteMsg(ctx, graveler.RepoPartition(repo), []byte(commitKey))
}

func (m *KVManager) FindMergeBase(ctx context.Context, repositoryID graveler.RepositoryID, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repositoryID, commitIDs[0], commitIDs[1])
}

func (m *KVManager) Log(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.CommitID) (graveler.CommitIterator, error) {
	_, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewCommitIterator(ctx, repositoryID, from, m), nil
}

func (m *KVManager) ListCommits(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.CommitIterator, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewKVOrderedCommitIterator(ctx, &m.kvStore, repo, false)
}

func (m *KVManager) GCCommitIterator(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.CommitIterator, error) {
	repo, err := m.getRepositoryRec(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewKVOrderedCommitIterator(ctx, &m.kvStore, repo, true)
}
