package ref

import (
	"context"
	"errors"
	"time"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/db"
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
	db              *DBManager
	kvStore         kv.StoreMessage
	addressProvider ident.AddressProvider
	batchExecutor   batch.Batcher
}

type CommitNode struct {
	children       []graveler.CommitID
	parentsToVisit map[graveler.CommitID]struct{}
	generation     int
}

func NewKVRefManager(executor batch.Batcher, kvStore kv.StoreMessage, db db.Database, addressProvider ident.AddressProvider) *KVManager {
	return &KVManager{
		db:              NewPGRefManager(executor, db, addressProvider),
		kvStore:         kvStore,
		addressProvider: addressProvider,
		batchExecutor:   executor,
	}
}

func (m *KVManager) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	repo := graveler.RepositoryData{}
	_, err := m.kvStore.GetMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)), &repo)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrRepositoryNotFound
		}
		return nil, err
	}
	return graveler.RepoFromProto(&repo).Repository, nil
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

func (m *KVManager) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository, token graveler.StagingToken) error {
	firstCommit := graveler.NewCommit()
	firstCommit.Message = graveler.FirstCommitMsg
	firstCommit.Generation = 1

	err := m.createBareRepository(ctx, repositoryID, repository)
	if err != nil {
		return err
	}

	commitID, err := m.AddCommit(ctx, repositoryID, firstCommit)
	if err != nil {
		return err
	}

	return m.CreateBranch(ctx, repositoryID, repository.DefaultBranchID, graveler.Branch{CommitID: commitID, StagingToken: token})
}

func (m *KVManager) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	return m.createBareRepository(ctx, repositoryID, repository)
}

func (m *KVManager) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return NewKVRepositoryIterator(ctx, &m.kvStore)
}

func (m *KVManager) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	// TODO: delete me
	// temp code to align with DB manager. Delete once https://github.com/treeverse/lakeFS/issues/3640 is done
	_, err := m.GetRepository(ctx, repositoryID)
	if errors.Is(err, kv.ErrNotFound) {
		return graveler.ErrRepositoryNotFound
	}
	if err != nil {
		return err
	}
	// END TODO: delete me

	return m.kvStore.DeleteMsg(ctx, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)))
}

func (m *KVManager) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	return ParseRef(ref)
}

func (m *KVManager) ResolveRawRef(ctx context.Context, repositoryID graveler.RepositoryID, raw graveler.RawRef) (*graveler.ResolvedRef, error) {
	return ResolveRawRef(ctx, m, m.addressProvider, repositoryID, raw)
}

func (m *KVManager) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	return m.db.GetBranch(ctx, repositoryID, branchID)
}

func (m *KVManager) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, branch graveler.Branch) error {
	return m.db.CreateBranch(ctx, repositoryID, branchID, branch)
}

func (m *KVManager) SetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, branch graveler.Branch) error {
	return m.db.SetBranch(ctx, repositoryID, branchID, branch)
}

func (m *KVManager) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	return m.db.DeleteBranch(ctx, repositoryID, branchID)
}

func (m *KVManager) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	// TODO: temporary "implementation" due to dependency in GetRepository
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewBranchIterator(ctx, m.db.db, repositoryID, IteratorPrefetchSize), nil
}

func (m *KVManager) GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error) {
	tagKey := graveler.TagPath(tagID)
	t := graveler.TagData{}
	_, err := m.kvStore.GetMsg(ctx, graveler.TagPartition(repositoryID), []byte(tagKey), &t)
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
	t := &graveler.TagData{
		Id:       tagID.String(),
		CommitId: commitID.String(),
	}
	tagKey := graveler.TagPath(tagID)
	err := m.kvStore.SetMsgIf(ctx, graveler.TagPartition(repositoryID), []byte(tagKey), t, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrTagAlreadyExists
		}
		return err
	}
	return nil
}

func (m *KVManager) DeleteTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) error {
	tagKey := graveler.TagPath(tagID)
	// TODO (issue 3640) align with delete tag DB - return ErrNotFound when tag does not exist
	return m.kvStore.DeleteMsg(ctx, graveler.TagPartition(repositoryID), []byte(tagKey))
}

func (m *KVManager) ListTags(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.TagIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewKVTagIterator(ctx, &m.kvStore, repositoryID)
}

func (m *KVManager) GetCommitByPrefix(ctx context.Context, repositoryID graveler.RepositoryID, prefix graveler.CommitID) (*graveler.Commit, error) {
	return m.db.GetCommitByPrefix(ctx, repositoryID, prefix)
}

func (m *KVManager) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	return m.db.GetCommit(ctx, repositoryID, commitID)
}

func (m *KVManager) AddCommit(ctx context.Context, repositoryID graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	return m.db.AddCommit(ctx, repositoryID, commit)
}

func (m *KVManager) FindMergeBase(ctx context.Context, repositoryID graveler.RepositoryID, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repositoryID, commitIDs[0], commitIDs[1])
}

func (m *KVManager) Log(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.CommitID) (graveler.CommitIterator, error) {
	// TODO: temporary "implementation" due to dependency in GetRepository
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewCommitIterator(ctx, m.db.db, repositoryID, from), nil
}

func (m *KVManager) ListCommits(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.CommitIterator, error) {
	// TODO: temporary "implementation" due to dependency in GetRepository
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewOrderedCommitIterator(ctx, m.db.db, repositoryID, IteratorPrefetchSize), nil
}

func (m *KVManager) FillGenerations(ctx context.Context, repositoryID graveler.RepositoryID) error {
	return m.db.FillGenerations(ctx, repositoryID)
}
