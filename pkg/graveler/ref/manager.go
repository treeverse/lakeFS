package ref

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// MaxBatchDelay - 3ms was chosen as a max delay time for critical path queries.
	// It trades off amount of queries per second (and thus effectiveness of the batching mechanism) with added latency.
	// Since reducing # of expensive operations is only beneficial when there are a lot of concurrent requests,
	//
	//	the sweet spot is probably between 1-5 milliseconds (representing 200-1000 requests/second to the data store).
	//
	// 3ms of delay with ~300 requests/second per resource sounds like a reasonable tradeoff.
	MaxBatchDelay = 3 * time.Millisecond
	// commitIDStringLength string representation length of commit ID - based on hex representation of sha256
	commitIDStringLength = 64
	// LinkAddressTime the time address is valid from get to link
	LinkAddressTime = 6 * time.Hour
	// ImportExpiryTime Expiry time to remove imports from ref-store
	ImportExpiryTime = 24 * time.Hour
)

type CacheConfig struct {
	Size   int
	Expiry time.Duration
	Jitter time.Duration
}

type Manager struct {
	kvStore         kv.Store
	kvStoreLimited  kv.Store
	addressProvider ident.AddressProvider
	batchExecutor   batch.Batcher
	repoCache       cache.Cache
	commitCache     cache.Cache
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

type ManagerConfig struct {
	Executor              batch.Batcher
	KVStore               kv.Store
	KVStoreLimited        kv.Store
	AddressProvider       ident.AddressProvider
	RepositoryCacheConfig CacheConfig
	CommitCacheConfig     CacheConfig
}

func NewRefManager(cfg ManagerConfig) *Manager {
	return &Manager{
		kvStore:         cfg.KVStore,
		kvStoreLimited:  cfg.KVStoreLimited,
		addressProvider: cfg.AddressProvider,
		batchExecutor:   cfg.Executor,
		repoCache:       newCache(cfg.RepositoryCacheConfig),
		commitCache:     newCache(cfg.CommitCacheConfig),
	}
}

func (m *Manager) getRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	data := graveler.RepositoryData{}
	_, err := kv.GetMsg(ctx, m.kvStore, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)), &data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrRepositoryNotFound
		}
		return nil, err
	}
	return graveler.RepoFromProto(&data), nil
}

func (m *Manager) getRepositoryBatch(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	key := fmt.Sprintf("GetRepository:%s", repositoryID)
	repository, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.ExecuterFunc(func() (interface{}, error) {
		return m.getRepository(context.Background(), repositoryID)
	}))
	if err != nil {
		return nil, err
	}
	return repository.(*graveler.RepositoryRecord), nil
}

func (m *Manager) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	rec, err := m.repoCache.GetOrSet(repositoryID, func() (interface{}, error) {
		repo, err := m.getRepositoryBatch(ctx, repositoryID)
		if err != nil {
			return nil, err
		}

		switch repo.State {
		case graveler.RepositoryState_ACTIVE:
			return repo, nil
		case graveler.RepositoryState_IN_DELETION:
			return nil, graveler.ErrRepositoryInDeletion
		default:
			return nil, fmt.Errorf("invalid repository state (%d) rec: %w", repo.State, graveler.ErrInvalid)
		}
	})
	if err != nil {
		return nil, err
	}
	return rec.(*graveler.RepositoryRecord), nil
}

func (m *Manager) createBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	repoRecord := &graveler.RepositoryRecord{
		RepositoryID: repositoryID,
		Repository:   &repository,
	}
	repo := graveler.ProtoFromRepo(repoRecord)

	err := kv.SetMsgIf(ctx, m.kvStore, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repositoryID)), repo, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrNotUnique
		}
		return nil, err
	}

	return repoRecord, nil
}

func (m *Manager) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
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

func (m *Manager) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	return m.createBareRepository(ctx, repositoryID, repository)
}

func (m *Manager) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return NewRepositoryIterator(ctx, m.kvStore)
}

func (m *Manager) updateRepoState(ctx context.Context, repo *graveler.RepositoryRecord, state graveler.RepositoryState) error {
	repo.State = state
	return kv.SetMsg(ctx, m.kvStore, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repo.RepositoryID)), graveler.ProtoFromRepo(repo))
}

func (m *Manager) deleteRepositoryBranches(ctx context.Context, repository *graveler.RepositoryRecord) error {
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

func (m *Manager) deleteRepositoryTags(ctx context.Context, repository *graveler.RepositoryRecord) error {
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

func (m *Manager) deleteRepositoryCommits(ctx context.Context, repository *graveler.RepositoryRecord) error {
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

func (m *Manager) deleteRepositoryMetadata(ctx context.Context, repository *graveler.RepositoryRecord) error {
	return m.kvStore.Delete(ctx, []byte(graveler.RepoPartition(repository)), []byte(graveler.RepoMetadataPath()))
}

func (m *Manager) deleteRepository(ctx context.Context, repo *graveler.RepositoryRecord) error {
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
	wg.Go(func() error {
		return m.deleteRepositoryMetadata(ctx, repo)
	})

	if err := wg.Wait().ErrorOrNil(); err != nil {
		return err
	}

	// Finally delete the repository record itself
	return m.kvStore.Delete(ctx, []byte(graveler.RepositoriesPartition()), []byte(graveler.RepoPath(repo.RepositoryID)))
}

func (m *Manager) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	repo, err := m.getRepository(ctx, repositoryID)
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

func (m *Manager) getRepositoryMetadata(ctx context.Context, repo *graveler.RepositoryRecord) (graveler.RepositoryMetadata, kv.Predicate, error) {
	data := graveler.RepoMetadata{}
	pred, err := kv.GetMsg(ctx, m.kvStore, graveler.RepoPartition(repo), []byte(graveler.RepoMetadataPath()), &data)
	if err != nil {
		return nil, nil, err
	}
	return graveler.RepoMetadataFromProto(&data), pred, nil
}

func (m *Manager) GetRepositoryMetadata(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.RepositoryMetadata, error) {
	repo, err := m.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	metadata, _, err := m.getRepositoryMetadata(ctx, repo)
	if errors.Is(err, kv.ErrNotFound) { // Return nil map if not exists
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (m *Manager) SetRepositoryMetadata(ctx context.Context, repo *graveler.RepositoryRecord, updateFunc graveler.RepoMetadataUpdateFunc) error {
	metadata, pred, err := m.getRepositoryMetadata(ctx, repo)
	if errors.Is(err, kv.ErrNotFound) { // Create new metadata map and set predicate to nil for setIf not exists
		metadata = graveler.RepositoryMetadata{}
		pred = nil
	} else if err != nil {
		return err
	}

	newMetadata, err := updateFunc(metadata)
	// return on error or nothing to update
	if err != nil || newMetadata == nil {
		return err
	}
	return kv.SetMsgIf(ctx, m.kvStore, graveler.RepoPartition(repo), []byte(graveler.RepoMetadataPath()), graveler.ProtoFromRepositoryMetadata(newMetadata), pred)
}

func (m *Manager) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	return ParseRef(ref)
}

func (m *Manager) ResolveRawRef(ctx context.Context, repository *graveler.RepositoryRecord, raw graveler.RawRef) (*graveler.ResolvedRef, error) {
	return ResolveRawRef(ctx, m, m.addressProvider, repository, raw)
}

func (m *Manager) getBranchWithPredicate(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, kv.Predicate, error) {
	key := fmt.Sprintf("GetBranch:%s:%s", repository.RepositoryID, branchID)
	type branchPred struct {
		*graveler.Branch
		kv.Predicate
	}
	result, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.ExecuterFunc(func() (interface{}, error) {
		key := graveler.BranchPath(branchID)
		data := graveler.BranchData{}
		pred, err := kv.GetMsg(context.Background(), m.kvStore, graveler.RepoPartition(repository), []byte(key), &data)
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

func (m *Manager) GetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, error) {
	branch, _, err := m.getBranchWithPredicate(ctx, repository, branchID)
	return branch, err
}

func (m *Manager) createBranch(ctx context.Context, repositoryPartition string, branchID graveler.BranchID, branch graveler.Branch) error {
	err := kv.SetMsgIf(ctx, m.kvStore, repositoryPartition, []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, &branch), nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		err = graveler.ErrBranchExists
	}
	return err
}

func (m *Manager) CreateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, branch graveler.Branch) error {
	return m.createBranch(ctx, graveler.RepoPartition(repository), branchID, branch)
}

func (m *Manager) SetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, branch graveler.Branch) error {
	return kv.SetMsg(ctx, m.kvStore, graveler.RepoPartition(repository), []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, &branch))
}

func (m *Manager) BranchUpdate(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, f graveler.BranchUpdateFunc) error {
	b, pred, err := m.getBranchWithPredicate(ctx, repository, branchID)
	if err != nil {
		return err
	}
	newBranch, err := f(b)
	// return on error or nothing to update
	if err != nil || newBranch == nil {
		return err
	}
	return kv.SetMsgIf(ctx, m.kvStore, graveler.RepoPartition(repository), []byte(graveler.BranchPath(branchID)), protoFromBranch(branchID, newBranch), pred)
}

func (m *Manager) DeleteBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) error {
	_, err := m.GetBranch(ctx, repository, branchID)
	if err != nil {
		return err
	}
	return m.kvStore.Delete(ctx, []byte(graveler.RepoPartition(repository)), []byte(graveler.BranchPath(branchID)))
}

func (m *Manager) ListBranches(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return NewBranchSimpleIterator(ctx, m.kvStore, repository)
}

func (m *Manager) GCBranchIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return NewBranchByCommitIterator(ctx, m.kvStore, repository)
}

func (m *Manager) GetTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) (*graveler.CommitID, error) {
	key := fmt.Sprintf("GetTag:%s:%s", repository.RepositoryID, tagID)
	commitID, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.ExecuterFunc(func() (interface{}, error) {
		tagKey := graveler.TagPath(tagID)
		t := graveler.TagData{}
		_, err := kv.GetMsg(context.Background(), m.kvStore, graveler.RepoPartition(repository), []byte(tagKey), &t)
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

func (m *Manager) CreateTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID, commitID graveler.CommitID) error {
	t := &graveler.TagData{
		Id:       tagID.String(),
		CommitId: commitID.String(),
	}
	tagKey := graveler.TagPath(tagID)
	err := kv.SetMsgIf(ctx, m.kvStore, graveler.RepoPartition(repository), []byte(tagKey), t, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrTagAlreadyExists
		}
		return err
	}
	return nil
}

func (m *Manager) DeleteTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) error {
	tagKey := graveler.TagPath(tagID)
	// TODO (issue 3640) align with delete tag DB - return ErrNotFound when tag does not exist
	return m.kvStore.Delete(ctx, []byte(graveler.RepoPartition(repository)), []byte(tagKey))
}

func (m *Manager) ListTags(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.TagIterator, error) {
	return NewTagIterator(ctx, m.kvStore, repository)
}

func (m *Manager) GetCommitByPrefix(ctx context.Context, repository *graveler.RepositoryRecord, prefix graveler.CommitID) (*graveler.Commit, error) {
	// optimize by get if prefix is not a prefix, but a full length commit id
	if len(prefix) == commitIDStringLength {
		return m.GetCommit(ctx, repository, prefix)
	}
	key := fmt.Sprintf("GetCommitByPrefix:%s:%s", repository.RepositoryID, prefix)
	commit, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.ExecuterFunc(func() (interface{}, error) {
		it, err := NewOrderedCommitIterator(context.Background(), m.kvStore, repository, false)
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

func (m *Manager) GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("%s:%s", repository.RepositoryID, commitID)
	v, err := m.commitCache.GetOrSet(key, func() (v interface{}, err error) {
		return m.getCommitBatch(ctx, repository, commitID)
	})
	if err != nil {
		return nil, err
	}
	return v.(*graveler.Commit), nil
}

func (m *Manager) getCommitBatch(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommit:%s:%s", repository.RepositoryID, commitID)
	commit, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.ExecuterFunc(func() (interface{}, error) {
		return m.getCommit(context.Background(), commitID, repository)
	}))
	if err != nil {
		return nil, err
	}
	return commit.(*graveler.Commit), nil
}

func (m *Manager) getCommit(ctx context.Context, commitID graveler.CommitID, repository *graveler.RepositoryRecord) (interface{}, error) {
	commitKey := graveler.CommitPath(commitID)
	c := graveler.CommitData{}
	_, err := kv.GetMsg(ctx, m.kvStore, graveler.RepoPartition(repository), []byte(commitKey), &c)
	if errors.Is(err, kv.ErrNotFound) {
		err = graveler.ErrCommitNotFound
	}
	if err != nil {
		return nil, err
	}
	return graveler.CommitFromProto(&c), nil
}

func (m *Manager) addCommit(ctx context.Context, repoPartition string, commit graveler.Commit) (graveler.CommitID, error) {
	commitID := m.addressProvider.ContentAddress(commit)
	c := graveler.ProtoFromCommit(graveler.CommitID(commitID), &commit)
	commitKey := graveler.CommitPath(graveler.CommitID(commitID))
	err := kv.SetMsgIf(ctx, m.kvStore, repoPartition, []byte(commitKey), c, nil)
	// commits are written based on their content hash, if we insert the same ID again,
	// it will necessarily have the same attributes as the existing one, so if a commit already exists doesn't return an error
	if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
		return "", err
	}
	return graveler.CommitID(commitID), nil
}

func (m *Manager) AddCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
	return m.addCommit(ctx, graveler.RepoPartition(repository), commit)
}

func (m *Manager) RemoveCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) error {
	commitKey := graveler.CommitPath(commitID)
	return m.kvStore.Delete(ctx, []byte(graveler.RepoPartition(repository)), []byte(commitKey))
}

func (m *Manager) FindMergeBase(ctx context.Context, repository *graveler.RepositoryRecord, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repository, commitIDs[0], commitIDs[1])
}

func (m *Manager) Log(ctx context.Context, repository *graveler.RepositoryRecord, from graveler.CommitID, firstParent bool) (graveler.CommitIterator, error) {
	return NewCommitIterator(ctx, &CommitIteratorConfig{
		repository:  repository,
		start:       from,
		firstParent: firstParent,
		manager:     m,
	}), nil
}

func (m *Manager) ListCommits(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return NewOrderedCommitIterator(ctx, m.kvStore, repository, false)
}

func (m *Manager) GCCommitIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return NewOrderedCommitIterator(ctx, m.kvStore, repository, true)
}

func newCache(cfg CacheConfig) cache.Cache {
	if cfg.Size == 0 {
		return cache.NoCache
	}
	return cache.NewCache(cfg.Size, cfg.Expiry, cache.NewJitterFn(cfg.Jitter))
}

func (m *Manager) SetLinkAddress(ctx context.Context, repository *graveler.RepositoryRecord, physicalAddress string) error {
	a := &graveler.LinkAddressData{
		Address: physicalAddress,
	}
	err := kv.SetMsgIf(ctx, m.kvStore, graveler.RepoPartition(repository), []byte(graveler.LinkedAddressPath(physicalAddress)), a, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = graveler.ErrLinkAddressAlreadyExists
		}
		return err
	}
	return nil
}

func (m *Manager) VerifyLinkAddress(ctx context.Context, repository *graveler.RepositoryRecord, physicalAddress string) error {
	data := graveler.LinkAddressData{}
	addrPath := []byte(graveler.LinkedAddressPath(physicalAddress))
	_, err := kv.GetMsg(ctx, m.kvStore, graveler.RepoPartition(repository), addrPath, &data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrLinkAddressNotFound
		}
		return err
	}
	expired, err := m.IsLinkAddressExpired(&data)
	if err != nil {
		return err
	}
	if expired {
		err = graveler.ErrLinkAddressExpired
	}
	_ = deleteLinkAddress(ctx, m.kvStore, repository, physicalAddress)
	return err
}

func deleteLinkAddress(ctx context.Context, kvStore kv.Store, repository *graveler.RepositoryRecord, physicalAddress string) error {
	addrPath := []byte(graveler.LinkedAddressPath(physicalAddress))
	return kvStore.Delete(ctx, []byte(graveler.RepoPartition(repository)), addrPath)
}

func (m *Manager) ListLinkAddresses(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.LinkAddressIterator, error) {
	return NewLinkAddressIterator(ctx, m.kvStore, repository)
}

func (m *Manager) IsLinkAddressExpired(linkAddress *graveler.LinkAddressData) (bool, error) {
	creationTime, err := m.resolveLinkAddressTime(linkAddress.Address)
	if err != nil {
		return false, err
	}
	return time.Since(creationTime) > LinkAddressTime, nil
}

func (m *Manager) resolveLinkAddressTime(address string) (time.Time, error) {
	_, name := path.Split(address)
	id, err := xid.FromString(name)
	if err != nil {
		return time.Time{}, err
	}
	return id.Time(), nil
}

// DeleteExpiredLinkAddresses delete expired link addresses from kv store. This call uses limiter to access
// kv, assuming the call does in the background.
func (m *Manager) DeleteExpiredLinkAddresses(ctx context.Context, repository *graveler.RepositoryRecord) error {
	itr, err := NewLinkAddressIterator(ctx, m.kvStoreLimited, repository)
	if err != nil {
		return err
	}
	defer itr.Close()
	for itr.Next() {
		linkAddress := itr.Value()
		expired, err := m.IsLinkAddressExpired(linkAddress)
		if err != nil {
			return err
		}
		if expired {
			err := deleteLinkAddress(ctx, m.kvStoreLimited, repository, linkAddress.Address)
			if err != nil {
				return err
			}
		}
	}
	return itr.Err()
}

func (m *Manager) DeleteExpiredImports(ctx context.Context, repository *graveler.RepositoryRecord) error {
	expiry := time.Now().Add(-ImportExpiryTime)
	repoPartition := graveler.RepoPartition(repository)
	key := []byte(graveler.ImportsPath(""))
	options := kv.IteratorOptionsFrom([]byte(""))
	itr, err := kv.NewPrimaryIterator(ctx, m.kvStoreLimited, (&graveler.ImportStatusData{}).ProtoReflect().Type(), repoPartition, key, options)
	if err != nil {
		return fmt.Errorf("failed to get imports iterator from store: %w", err)
	}
	defer itr.Close()

	var errs multierror.Error
	for itr.Next() {
		entry := itr.Entry()
		status, ok := entry.Value.(*graveler.ImportStatusData)
		if !ok {
			return fmt.Errorf("invalid protobuf type %s: %w", entry.Value.ProtoReflect().Type().Descriptor().FullName(), graveler.ErrReadingFromStore)
		}
		if status.UpdatedAt.AsTime().Before(expiry) {
			if !status.Completed && status.Error == "" {
				logging.FromContext(ctx).WithFields(logging.Fields{"import_id": status.Id}).Warning("removing stale import")
			}
			err = m.kvStoreLimited.Delete(ctx, []byte(repoPartition), entry.Key)
			if err != nil {
				errs.Errors = append(errs.Errors, fmt.Errorf("delete failed for import ID %s: %w", status.Id, err))
			}
		}
	}
	return errs.ErrorOrNil()
}
