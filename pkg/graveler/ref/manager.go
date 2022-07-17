package ref

import (
	"context"
	"errors"
	"strings"
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
	it, err := NewKVOrderedCommitIterator(ctx, m.Store(), repositoryID, false)
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
				return nil, graveler.ErrCommitNotFound // more than 1 commit starts with the ID prefixבםצצ
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
	commitKey := graveler.CommitPath(commitID)
	c := graveler.CommitData{}
	_, err := m.kvStore.GetMsg(ctx, graveler.CommitPartition(repositoryID), []byte(commitKey), &c)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrCommitNotFound
		}
		return nil, err
	}
	commit := graveler.CommitFromProto(&c)
	return commit, nil
}

func (m *KVManager) AddCommit(ctx context.Context, repositoryID graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	commitID := m.addressProvider.ContentAddress(commit)
	c := graveler.ProtoFromCommit(graveler.CommitID(commitID), &commit)
	commitKey := graveler.CommitPath(graveler.CommitID(commitID))
	err := m.kvStore.SetMsgIf(ctx, graveler.CommitPartition(repositoryID), []byte(commitKey), c, nil)
	if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
		return "", err
	}
	return graveler.CommitID(commitID), nil
}

func (m *KVManager) updateCommitGeneration(ctx context.Context, repositoryID graveler.RepositoryID, nodes map[graveler.CommitID]*CommitNode) error {
	for len(nodes) != 0 {
		for commitID, node := range nodes {
			c, err := m.GetCommit(ctx, repositoryID, commitID)
			if err != nil {
				return nil
			}
			c.Generation = node.generation
			commit := graveler.ProtoFromCommit(commitID, c)
			err = m.kvStore.SetMsg(ctx, graveler.CommitPartition(repositoryID), []byte(graveler.CommitPath(commitID)), commit)
			if err != nil {
				return err
			}
			delete(nodes, commitID)
		}
	}
	return nil
}

func (m *KVManager) FindMergeBase(ctx context.Context, repositoryID graveler.RepositoryID, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repositoryID, commitIDs[0], commitIDs[1])
}

func (m *KVManager) Log(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.CommitID) (graveler.CommitIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewKVCommitIterator(ctx, m.kvStore, repositoryID, from), nil
}

func (m *KVManager) ListCommits(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.CommitIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewKVOrderedCommitIterator(ctx, &m.kvStore, repositoryID, false)
}

func (m *KVManager) FillGenerations(ctx context.Context, repositoryID graveler.RepositoryID) error {
	// update commitNodes' generation in nodes "tree" using BFS algorithm.
	// using a queue implementation
	// adding a node to the queue only after all of its parents were visited in order to avoid redundant visits of nodesCommitIDs
	nodes, err := m.createCommitIDsMap(ctx, repositoryID)
	if err != nil {
		return err
	}
	rootsCommitIDs := m.getRootNodes(nodes)
	m.mapCommitNodesToChildren(nodes)
	m.addGenerationToNodes(nodes, rootsCommitIDs)
	return m.updateCommitGeneration(ctx, repositoryID, nodes)
}

func (m *KVManager) createCommitIDsMap(ctx context.Context, repositoryID graveler.RepositoryID) (map[graveler.CommitID]*CommitNode, error) {
	iter, err := m.ListCommits(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	nodes := make(map[graveler.CommitID]*CommitNode)
	for iter.Next() {
		commit := iter.Value()
		parentsToVisit := map[graveler.CommitID]struct{}{}
		for _, parentID := range commit.Parents {
			parentsToVisit[parentID] = struct{}{}
		}

		nodes[commit.CommitID] = &CommitNode{parentsToVisit: parentsToVisit}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (m *KVManager) getRootNodes(nodes map[graveler.CommitID]*CommitNode) []graveler.CommitID {
	var rootsCommitIDs []graveler.CommitID
	for commitID, node := range nodes {
		if len(node.parentsToVisit) == 0 {
			rootsCommitIDs = append(rootsCommitIDs, commitID)
		}
	}
	return rootsCommitIDs
}

func (m *KVManager) mapCommitNodesToChildren(nodes map[graveler.CommitID]*CommitNode) {
	for commitID, commitNode := range nodes {
		// adding current node as a child to all parents in commitNode.parentsToVisit
		for parentID := range commitNode.parentsToVisit {
			nodes[parentID].children = append(nodes[parentID].children, commitID)
		}
	}
}

func (m *KVManager) addGenerationToNodes(nodes map[graveler.CommitID]*CommitNode, rootsCommitIDs []graveler.CommitID) {
	nodesCommitIDs := rootsCommitIDs
	for currentGeneration := 1; len(nodesCommitIDs) > 0; currentGeneration++ {
		var nextIterationNodes []graveler.CommitID
		for _, nodeCommitID := range nodesCommitIDs {
			currentNode := nodes[nodeCommitID]
			nodes[nodeCommitID].generation = currentGeneration
			for _, childNodeID := range currentNode.children {
				delete(nodes[childNodeID].parentsToVisit, nodeCommitID)
				if len(nodes[childNodeID].parentsToVisit) == 0 {
					nextIterationNodes = append(nextIterationNodes, childNodeID)
				}
			}
		}
		nodesCommitIDs = nextIterationNodes
	}
}

func (m *KVManager) Store() *kv.StoreMessage {
	return &m.kvStore
}
