package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
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

func (m *KVManager) getRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	key := fmt.Sprintf("GetRepository:%s", repositoryID)
	repository, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		data := graveler.RepositoryData{}
		_, err := m.kvStore.GetMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)), &data)
		if err != nil {
			return nil, err
		}
		return graveler.RepoFromProto(&data), nil
	}))
	if errors.Is(err, kv.ErrNotFound) {
		err = graveler.ErrRepositoryNotFound
	}
	if err != nil {
		return nil, err
	}
	return repository.(*graveler.RepositoryRecord), nil
}

func (m *KVManager) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
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

func (m *KVManager) createBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
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
		return nil, err
	}
	return repoRecord, nil
}

func (m *KVManager) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	firstCommit := graveler.NewCommit()
	firstCommit.Message = graveler.FirstCommitMsg
	firstCommit.Generation = 1

	repo := &graveler.RepositoryRecord{
		RepositoryID: repositoryID,
		Repository:   &repository,
	}
	// If branch creation fails - this commit will become dangling. This is a known issue that can be resolved via garbage collection
	commitID, err := m.addCommit(ctx, graveler.RepoPartition(repo), firstCommit)
	if err != nil {
		return nil, err
	}

	branch := graveler.Branch{
		CommitID:     commitID,
		StagingToken: graveler.GenerateStagingToken(repositoryID, repository.DefaultBranchID),
		SealedTokens: nil,
	}
	err = m.createBranch(ctx, graveler.RepoPartition(repo), repository.DefaultBranchID, branch)
	if err != nil {
		return nil, err
	}
	_, err = m.createBareRepository(ctx, repositoryID, repository)
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func (m *KVManager) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	return m.createBareRepository(ctx, repositoryID, repository)
}

func (m *KVManager) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return NewKVRepositoryIterator(ctx, &m.kvStore)
}

func (m *KVManager) updateRepoState(ctx context.Context, repo *graveler.RepositoryRecord, state graveler.RepositoryState) error {
	repo.State = state
	return m.kvStore.SetMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repo.RepositoryID)), graveler.ProtoFromRepo(repo))
}

func (m *KVManager) deleteRepositoryBranches(ctx context.Context, repository *graveler.RepositoryRecord) error {
	itr, err := m.ListBranches(ctx, repository)
	if err != nil {
		return err
	}
	defer itr.Close()
	var wg multierror.Group
	for itr.Next() {
		b := itr.Value()
		wg.Go(func() error {
			return m.DeleteBranch(ctx, repository, b.BranchID)
		})
	}
	return wg.Wait().ErrorOrNil()
}

func (m *KVManager) deleteRepositoryTags(ctx context.Context, repository *graveler.RepositoryRecord) error {
	itr, err := m.ListTags(ctx, repository)
	if err != nil {
		return err
	}
	defer itr.Close()
	var wg multierror.Group
	for itr.Next() {
		tag := itr.Value()
		wg.Go(func() error {
			return m.DeleteTag(ctx, repository, tag.TagID)
		})
	}
	return wg.Wait().ErrorOrNil()
}

func (m *KVManager) deleteRepositoryCommits(ctx context.Context, repository *graveler.RepositoryRecord) error {
	itr, err := m.ListCommits(ctx, repository)
	if err != nil {
		return err
	}
	defer itr.Close()
	var wg multierror.Group
	for itr.Next() {
		commit := itr.Value()
		wg.Go(func() error {
			return m.RemoveCommit(ctx, repository, commit.CommitID)
		})
	}
	return wg.Wait().ErrorOrNil()
}

func (m *KVManager) deleteRepository(ctx context.Context, repo *graveler.RepositoryRecord) error {
	// ctx := context.Background() TODO (niro): When running this async create a new context and remove ctx from signature
	var wg multierror.Group
	wg.Go(func() error {
		return m.deleteRepositoryBranches(ctx, repo)
	})
	wg.Go(func() error {
		return m.deleteRepositoryTags(ctx, repo)
	})
	wg.Go(func() error {
		return m.deleteRepositoryCommits(ctx, repo)
	})

	if err := wg.Wait().ErrorOrNil(); err != nil {
		return err
	}

	// Finally delete the repository record itself
	return m.kvStore.DeleteMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repo.RepositoryID)))
}

func (m *KVManager) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	repo, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}

	// Set repository state to deleted and then perform background delete.
	if repo.State != graveler.RepositoryState_IN_DELETION {
		err = m.updateRepoState(ctx, repo, graveler.RepositoryState_IN_DELETION)
		if err != nil {
			return err
		}
	}

	// TODO(niro): This should be a background delete process
	return m.deleteRepository(ctx, repo)
}

func (m *KVManager) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	return ParseRef(ref)
}

func (m *KVManager) ResolveRawRef(ctx context.Context, repository *graveler.RepositoryRecord, raw graveler.RawRef) (*graveler.ResolvedRef, error) {
	return ResolveRawRef(ctx, m, m.addressProvider, repository, raw)
}

func (m *KVManager) getBranchWithPredicate(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, kv.Predicate, error) {
	key := fmt.Sprintf("GetBranch:%s:%s", repository.RepositoryID, branchID)
	type branchPred struct {
		*graveler.Branch
		kv.Predicate
	}
	result, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		key := graveler.BranchPath(branchID)
		data := graveler.BranchData{}
		pred, err := m.kvStore.GetMsg(ctx, graveler.RepoPartition(repository), []byte(key), &data)
		if err != nil {
			return nil, err
		}
		return &branchPred{Branch: branchFromProto(&data), Predicate: pred}, nil
	}))
	if errors.Is(err, kv.ErrNotFound) {
		err = graveler.ErrBranchNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	branchWithPred := result.(*branchPred)
	return branchWithPred.Branch, branchWithPred.Predicate, nil
}

func (m *KVManager) GetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, error) {
	branch, _, err := m.getBranchWithPredicate(ctx, repository, branchID)
	return branch, err
}

func (m *KVManager) createBranch(ctx context.Context, repositoryPartition string, branchID graveler.BranchID, branch graveler.Branch) error {
	err := m.kvStore.SetMsgIf(ctx, repositoryPartition, []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, &branch), nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		err = graveler.ErrBranchExists
	}
	return err
}

func (m *KVManager) CreateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, branch graveler.Branch) error {
	return m.createBranch(ctx, graveler.RepoPartition(repository), branchID, branch)
}

func (m *KVManager) SetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, branch graveler.Branch) error {
	return m.kvStore.SetMsg(ctx, graveler.RepoPartition(repository), []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, &branch))
}

func (m *KVManager) BranchUpdate(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, f graveler.BranchUpdateFunc) error {
	b, pred, err := m.getBranchWithPredicate(ctx, repository, branchID)
	if err != nil {
		return err
	}
	newBranch, err := f(b)
	// return on error or nothing to update
	if err != nil || newBranch == nil {
		return err
	}
	return m.kvStore.SetMsgIf(ctx, graveler.RepoPartition(repository), []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, newBranch), pred)
}

func (m *KVManager) DeleteBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) error {
	_, err := m.GetBranch(ctx, repository, branchID)
	if err != nil {
		return err
	}
	return m.kvStore.DeleteMsg(ctx, graveler.RepoPartition(repository), []byte(graveler.BranchPath(branchID)))
}

func (m *KVManager) ListBranches(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return NewBranchSimpleIterator(ctx, &m.kvStore, repository)
}

func (m *KVManager) GCBranchIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return NewBranchByCommitIterator(ctx, &m.kvStore, repository)
}

func (m *KVManager) GetTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) (*graveler.CommitID, error) {
	key := fmt.Sprintf("GetTag:%s:%s", repository.RepositoryID, tagID)
	commitID, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		tagKey := graveler.TagPath(tagID)
		t := graveler.TagData{}
		_, err := m.kvStore.GetMsg(ctx, graveler.RepoPartition(repository), []byte(tagKey), &t)
		if err != nil {
			return nil, err
		}
		commitID := graveler.CommitID(t.CommitId)
		return &commitID, nil
	}))
	if errors.Is(err, kv.ErrNotFound) {
		err = graveler.ErrTagNotFound
	}
	if err != nil {
		return nil, err
	}
	return commitID.(*graveler.CommitID), nil
}

func (m *KVManager) CreateTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID, commitID graveler.CommitID) error {
	t := &graveler.TagData{
		Id:       tagID.String(),
		CommitId: commitID.String(),
	}
	tagKey := graveler.TagPath(tagID)
	err := m.kvStore.SetMsgIf(ctx, graveler.RepoPartition(repository), []byte(tagKey), t, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrTagAlreadyExists
		}
		return err
	}
	return nil
}

func (m *KVManager) DeleteTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) error {
	tagKey := graveler.TagPath(tagID)
	// TODO (issue 3640) align with delete tag DB - return ErrNotFound when tag does not exist
	return m.kvStore.DeleteMsg(ctx, graveler.RepoPartition(repository), []byte(tagKey))
}

func (m *KVManager) ListTags(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.TagIterator, error) {
	return NewKVTagIterator(ctx, &m.kvStore, repository)
}

func (m *KVManager) GetCommitByPrefix(ctx context.Context, repository *graveler.RepositoryRecord, prefix graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommitByPrefix:%s:%s", repository.RepositoryID, prefix)
	commit, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		it, err := NewKVOrderedCommitIterator(ctx, &m.kvStore, repository, false)
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
		if err := it.Err(); err != nil {
			return nil, err
		}
		if commit == nil {
			return nil, graveler.ErrCommitNotFound
		}
		return commit, nil
	}))
	if err != nil {
		return nil, err
	}
	return commit.(*graveler.Commit), nil
}

func (m *KVManager) GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommit:%s:%s", repository.RepositoryID, commitID)
	commit, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		commitKey := graveler.CommitPath(commitID)
		c := graveler.CommitData{}
		_, err := m.kvStore.GetMsg(ctx, graveler.RepoPartition(repository), []byte(commitKey), &c)
		if err != nil {
			return nil, err
		}
		return graveler.CommitFromProto(&c), nil
	}))
	if errors.Is(err, kv.ErrNotFound) {
		err = graveler.ErrCommitNotFound
	}
	if err != nil {
		return nil, err
	}
	return commit.(*graveler.Commit), nil
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

func (m *KVManager) AddCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
	return m.addCommit(ctx, graveler.RepoPartition(repository), commit)
}

func (m *KVManager) RemoveCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) error {
	commitKey := graveler.CommitPath(commitID)
	return m.kvStore.DeleteMsg(ctx, graveler.RepoPartition(repository), []byte(commitKey))
}

func (m *KVManager) FindMergeBase(ctx context.Context, repository *graveler.RepositoryRecord, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repository, commitIDs[0], commitIDs[1])
}

func (m *KVManager) Log(ctx context.Context, repository *graveler.RepositoryRecord, from graveler.CommitID) (graveler.CommitIterator, error) {
	return NewCommitIterator(ctx, repository, from, m), nil
}

func (m *KVManager) ListCommits(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return NewKVOrderedCommitIterator(ctx, &m.kvStore, repository, false)
}

func (m *KVManager) GCCommitIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return NewKVOrderedCommitIterator(ctx, &m.kvStore, repository, true)
}
