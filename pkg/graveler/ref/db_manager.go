package ref

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/adapter"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branch"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/version"
	"google.golang.org/protobuf/proto"
)

const (
	packageName      = "graveler"
	jobWorkers       = 10
	migrateQueueSize = 100
)

//nolint:gochecknoinits
func init() {
	kvpg.RegisterMigrate(packageName, Migrate, []string{
		"graveler_repositories",
		"graveler_commits",
		"graveler_tags",
		"graveler_branches"})
}

var encoder kv.SafeEncoder

type DBManager struct {
	db              db.Database
	addressProvider ident.AddressProvider
	batchExecutor   batch.Batcher
}

func NewPGRefManager(executor batch.Batcher, db db.Database, addressProvider ident.AddressProvider) *DBManager {
	return &DBManager{
		db:              db,
		addressProvider: addressProvider,
		batchExecutor:   executor,
	}
}

func (m *DBManager) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	key := fmt.Sprintf("GetRepository:%s", repositoryID)
	repository, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		repo := &graveler.Repository{}
		err := m.db.Get(ctx, repo, `SELECT storage_namespace, creation_date, default_branch FROM graveler_repositories WHERE id = $1`, repositoryID)
		if err != nil {
			return nil, err
		}
		return repo, nil
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrRepositoryNotFound
	}
	if err != nil {
		return nil, err
	}
	return &graveler.RepositoryRecord{
		RepositoryID: repositoryID,
		Repository:   repository.(*graveler.Repository),
	}, nil
}

func (m *DBManager) createBareRepository(tx db.Tx, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	_, err := tx.Exec(
		`INSERT INTO graveler_repositories (id, storage_namespace, creation_date, default_branch) VALUES ($1, $2, $3, $4)`,
		repositoryID, repository.StorageNamespace, repository.CreationDate, repository.DefaultBranchID)
	if errors.Is(err, db.ErrAlreadyExists) {
		return nil, graveler.ErrNotUnique
	}
	if err != nil {
		return nil, err
	}

	return &graveler.RepositoryRecord{
		RepositoryID: repositoryID,
		Repository:   &repository,
	}, nil
}

func (m *DBManager) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	firstCommit := graveler.NewCommit()
	firstCommit.Message = graveler.FirstCommitMsg
	firstCommit.Generation = 1
	commitID := m.addressProvider.ContentAddress(firstCommit)

	result, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// create a bare repository first
		repo, err := m.createBareRepository(tx, repositoryID, repository)
		if err != nil {
			return nil, err
		}

		// Create the default branch with its staging token
		_, err = tx.Exec(`INSERT INTO graveler_branches (repository_id, id, staging_token, commit_id) VALUES ($1, $2, $3, $4)`,
			repositoryID, repository.DefaultBranchID, graveler.GenerateStagingToken(repositoryID, repository.DefaultBranchID), commitID)

		if err != nil {
			if errors.Is(err, db.ErrAlreadyExists) {
				return nil, graveler.ErrNotUnique
			}
			return nil, err
		}

		// Add a first empty commit to allow branching off the default branch immediately after repository creation
		err = m.addCommit(tx, repo, commitID, firstCommit)
		if err != nil {
			return nil, err
		}
		return repo, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*graveler.RepositoryRecord), nil
}

func (m *DBManager) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) (*graveler.RepositoryRecord, error) {
	result, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return m.createBareRepository(tx, repositoryID, repository)
	})
	if err != nil {
		return nil, err
	}
	return result.(*graveler.RepositoryRecord), nil
}

func (m *DBManager) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return NewDBRepositoryIterator(ctx, m.db, IteratorPrefetchSize), nil
}

func (m *DBManager) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		var err error
		_, err = tx.Exec(`DELETE FROM graveler_branches WHERE repository_id = $1`, repositoryID)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`DELETE FROM graveler_tags WHERE repository_id = $1`, repositoryID)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`DELETE FROM graveler_commits WHERE repository_id = $1`, repositoryID)
		if err != nil {
			return nil, err
		}
		r, err := tx.Exec(`DELETE FROM graveler_repositories WHERE id = $1`, repositoryID)
		if err != nil {
			return nil, err
		}
		if r.RowsAffected() == 0 {
			return nil, db.ErrNotFound
		}
		return nil, nil
	})
	if errors.Is(err, db.ErrNotFound) {
		return graveler.ErrRepositoryNotFound
	}
	return err
}

func (m *DBManager) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	return ParseRef(ref)
}

func (m *DBManager) ResolveRawRef(ctx context.Context, repository *graveler.RepositoryRecord, raw graveler.RawRef) (*graveler.ResolvedRef, error) {
	return ResolveRawRef(ctx, m, m.addressProvider, repository, raw)
}

func (m *DBManager) GetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, error) {
	key := fmt.Sprintf("GetBranch:%s:%s", repository.RepositoryID, branchID)
	b, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		var rec branchRecord
		err := m.db.Get(ctx, &rec, `SELECT commit_id, staging_token FROM graveler_branches WHERE repository_id = $1 AND id = $2`,
			repository.RepositoryID, branchID)
		if err != nil {
			return nil, err
		}
		return &graveler.Branch{
			CommitID:     rec.CommitID,
			StagingToken: rec.StagingToken,
		}, nil
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrBranchNotFound
	}
	if err != nil {
		return nil, err
	}
	return b.(*graveler.Branch), nil
}

func (m *DBManager) CreateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, branch graveler.Branch) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO graveler_branches (repository_id, id, staging_token, commit_id)
			VALUES ($1, $2, $3, $4)`,
			repository.RepositoryID, branchID, branch.StagingToken, branch.CommitID)
		return nil, err
	})
	if errors.Is(err, db.ErrAlreadyExists) {
		return graveler.ErrBranchExists
	}
	return err
}

func (m *DBManager) SetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, branch graveler.Branch) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO graveler_branches (repository_id, id, staging_token, commit_id)
			VALUES ($1, $2, $3, $4)
				ON CONFLICT (repository_id, id)
				DO UPDATE SET staging_token = $3, commit_id = $4`,
			repository.RepositoryID, branchID, branch.StagingToken, branch.CommitID)
		return nil, err
	})
	return err
}

// BranchUpdate Implement refManager interface - in DB implementation simply panics
func (m *DBManager) BranchUpdate(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, _ graveler.BranchUpdateFunc) error {
	panic("not implemented")
}

func (m *DBManager) DeleteBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		r, err := tx.Exec(
			`DELETE FROM graveler_branches WHERE repository_id = $1 AND id = $2`,
			repository.RepositoryID, branchID)
		if err != nil {
			return nil, err
		}
		if r.RowsAffected() == 0 {
			return nil, graveler.ErrNotFound
		}
		return nil, nil
	})
	if errors.Is(err, db.ErrNotFound) {
		return graveler.ErrBranchNotFound
	}
	return err
}

func (m *DBManager) ListBranches(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return NewDBBranchIterator(ctx, m.db, repository.RepositoryID, IteratorPrefetchSize), nil
}

func (m *DBManager) GCBranchIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	return NewDBBranchIterator(ctx, m.db, repository.RepositoryID, IteratorPrefetchSize, WithOrderByCommitID()), nil
}

func (m *DBManager) GetTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) (*graveler.CommitID, error) {
	key := fmt.Sprintf("GetTag:%s:%s", repository.RepositoryID, tagID)
	commitID, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		var commitID graveler.CommitID
		err := m.db.Get(ctx, &commitID, `SELECT commit_id FROM graveler_tags WHERE repository_id = $1 AND id = $2`,
			repository.RepositoryID, tagID)
		if err != nil {
			return nil, err
		}
		return &commitID, nil
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrTagNotFound
	}
	if err != nil {
		return nil, err
	}
	return commitID.(*graveler.CommitID), nil
}

func (m *DBManager) CreateTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID, commitID graveler.CommitID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		res, err := tx.Exec(`INSERT INTO graveler_tags (repository_id, id, commit_id) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`,
			repository.RepositoryID, tagID, commitID)
		if err != nil {
			return nil, err
		}
		if res.RowsAffected() == 0 {
			return nil, graveler.ErrTagAlreadyExists
		}
		return nil, nil
	})
	return err
}

func (m *DBManager) DeleteTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		r, err := tx.Exec(
			`DELETE FROM graveler_tags WHERE repository_id = $1 AND id = $2`,
			repository.RepositoryID, tagID)
		if err != nil {
			return nil, err
		}
		if r.RowsAffected() == 0 {
			return nil, graveler.ErrNotFound
		}
		return nil, nil
	})
	if errors.Is(err, db.ErrNotFound) {
		return graveler.ErrTagNotFound
	}
	return err
}

func (m *DBManager) ListTags(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.TagIterator, error) {
	return NewDBTagIterator(ctx, m.db, repository.RepositoryID, IteratorPrefetchSize)
}

func (m *DBManager) GetCommitByPrefix(ctx context.Context, repository *graveler.RepositoryRecord, prefix graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommitByPrefix:%s:%s", repository.RepositoryID, prefix)
	commit, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		return m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			records := make([]*commitRecord, 0)
			// LIMIT 2 is used to test if a truncated commit ID resolves to *one* commit.
			// if we get 2 results that start with the truncated ID, that's enough to determine this prefix is not unique
			err := tx.Select(&records, `
					SELECT id, committer, message, creation_date, parents, meta_range_id, metadata, version, generation
					FROM graveler_commits
					WHERE repository_id = $1 AND id LIKE $2 || '%'
					LIMIT 2`,
				repository.RepositoryID, prefix)
			if errors.Is(err, db.ErrNotFound) {
				return nil, graveler.ErrNotFound
			}
			if err != nil {
				return nil, err
			}
			if len(records) == 0 {
				return "", graveler.ErrNotFound
			}
			if len(records) > 1 {
				return "", graveler.ErrRefAmbiguous // more than 1 commit starts with the ID prefix
			}
			return records[0].toGravelerCommit(), nil
		}, db.ReadOnly())
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrCommitNotFound
	}
	if err != nil {
		return nil, err
	}
	return commit.(*graveler.Commit), nil
}

func (m *DBManager) GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommit:%s:%s", repository.RepositoryID, commitID)
	commit, err := m.batchExecutor.BatchFor(ctx, key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		var rec commitRecord
		err := m.db.Get(ctx, &rec, `SELECT committer, message, creation_date, parents, meta_range_id, metadata, version, generation
					FROM graveler_commits WHERE repository_id = $1 AND id = $2`,
			repository.RepositoryID, commitID)
		if err != nil {
			return nil, err
		}
		return rec.toGravelerCommit(), nil
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrCommitNotFound
	}
	if err != nil {
		return nil, err
	}
	return commit.(*graveler.Commit), nil
}

func (m *DBManager) AddCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
	commitID := m.addressProvider.ContentAddress(commit)
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, m.addCommit(tx, repository, commitID, commit)
	})
	if err != nil {
		return "", err
	}
	return graveler.CommitID(commitID), err
}

func (m *DBManager) addCommit(tx db.Tx, repository *graveler.RepositoryRecord, commitID string, commit graveler.Commit) error {
	// convert parents to slice of strings
	var parents []string
	for _, parent := range commit.Parents {
		parents = append(parents, string(parent))
	}

	// commits are written based on their content hash, if we insert the same ID again,
	// it will necessarily have the same attributes as the existing one, so no need to overwrite it
	_, err := tx.Exec(`
				INSERT INTO graveler_commits 
				(repository_id, id, committer, message, creation_date, parents, meta_range_id, metadata, version, generation)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT DO NOTHING`,
		repository.RepositoryID, commitID, commit.Committer, commit.Message,
		commit.CreationDate.UTC(), parents, commit.MetaRangeID, commit.Metadata, commit.Version, commit.Generation)

	return err
}

func (m *DBManager) RemoveCommit(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.CommitID) error {
	panic("Not implemented")
}

func (m *DBManager) updateCommitGeneration(tx db.Tx, repository *graveler.RepositoryRecord, nodes map[graveler.CommitID]*CommitNode) error {
	for len(nodes) != 0 {
		command := `WITH updated(id, generation) AS (VALUES `
		var updatingRows int
		for commitID, node := range nodes {
			if updatingRows != 0 {
				command += ","
			}
			command += fmt.Sprintf(`(%s, %d)`, pq.QuoteLiteral(string(commitID)), node.generation)

			delete(nodes, commitID)
			updatingRows += 1
			if updatingRows == BatchUpdateSQLSize {
				break
			}
		}
		command += `) UPDATE graveler_commits SET generation = updated.generation FROM updated WHERE (graveler_commits.id=updated.id AND graveler_commits.repository_id=$1)`
		_, err := tx.Exec(command, repository.RepositoryID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *DBManager) FindMergeBase(ctx context.Context, repository *graveler.RepositoryRecord, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repository, commitIDs[0], commitIDs[1])
}

func (m *DBManager) Log(ctx context.Context, repository *graveler.RepositoryRecord, from graveler.CommitID) (graveler.CommitIterator, error) {
	return NewCommitIterator(ctx, repository, from, m), nil
}

func (m *DBManager) ListCommits(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return NewDBOrderedCommitIterator(ctx, m.db, repository, IteratorPrefetchSize)
}

func (m *DBManager) GCCommitIterator(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.CommitIterator, error) {
	return NewDBOrderedCommitIterator(ctx, m.db, repository, IteratorPrefetchSize, WithOnlyAncestryLeaves())
}

func (m *DBManager) FillGenerations(ctx context.Context, repository *graveler.RepositoryRecord) error {
	// update commitNodes' generation in nodes "tree" using BFS algorithm.
	// using a queue implementation
	// adding a node to the queue only after all of its parents were visited in order to avoid redundant visits of nodesCommitIDs
	nodes, err := m.createCommitIDsMap(ctx, repository)
	if err != nil {
		return err
	}
	rootsCommitIDs := m.getRootNodes(nodes)
	m.mapCommitNodesToChildren(nodes)
	m.addGenerationToNodes(nodes, rootsCommitIDs)
	_, err = m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, m.updateCommitGeneration(tx, repository, nodes)
	})
	return err
}

func (m *DBManager) createCommitIDsMap(ctx context.Context, repository *graveler.RepositoryRecord) (map[graveler.CommitID]*CommitNode, error) {
	iter, err := m.ListCommits(ctx, repository)

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

func (m *DBManager) getRootNodes(nodes map[graveler.CommitID]*CommitNode) []graveler.CommitID {
	var rootsCommitIDs []graveler.CommitID
	for commitID, node := range nodes {
		if len(node.parentsToVisit) == 0 {
			rootsCommitIDs = append(rootsCommitIDs, commitID)
		}
	}
	return rootsCommitIDs
}

func (m *DBManager) mapCommitNodesToChildren(nodes map[graveler.CommitID]*CommitNode) {
	for commitID, commitNode := range nodes {
		// adding current node as a child to all parents in commitNode.parentsToVisit
		for parentID := range commitNode.parentsToVisit {
			nodes[parentID].children = append(nodes[parentID].children, commitID)
		}
	}
}

func (m *DBManager) addGenerationToNodes(nodes map[graveler.CommitID]*CommitNode, rootsCommitIDs []graveler.CommitID) {
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

// Migration Code

var (
	blockstore       block.Adapter
	blockstorePrefix string
)

type stagedEntry struct {
	Token graveler.StagingToken
	Entry *graveler.ValueRecord
}

func cWorker(ctx context.Context, d *pgxpool.Pool, repository *graveler.RepositoryRecord) error {
	commits, err := d.Query(ctx, "SELECT id,committer,message,creation_date,meta_range_id,metadata,parents,version,generation FROM graveler_commits WHERE repository_id=$1", repository.RepositoryID)
	if err != nil {
		return err
	}
	defer commits.Close()
	commitScanner := pgxscan.NewRowScanner(commits)
	for commits.Next() {
		c := new(commitRecord)
		err = commitScanner.Scan(c)
		if err != nil {
			return err
		}
		pb := CommitRecordToCommitData(c)
		data, err := proto.Marshal(pb)
		if err != nil {
			return err
		}
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(graveler.RepoPartition(repository)),
			Key:          []byte(graveler.CommitPath(graveler.CommitID(c.CommitID))),
			Value:        data,
		}); err != nil {
			return err
		}
	}
	return nil
}

// sWorker writes staging token data of a branch
func sWorker(sChan <-chan *stagedEntry) error {
	for s := range sChan {
		pb := graveler.ProtoFromStagedEntry(s.Entry.Key, s.Entry.Value)
		data, err := proto.Marshal(pb)
		if err != nil {
			return err
		}
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(s.Token),
			Key:          s.Entry.Key,
			Value:        data,
		}); err != nil {
			return err
		}
	}
	return nil
}

func bWorker(ctx context.Context, d *pgxpool.Pool, repository *graveler.RepositoryRecord) error {
	branches, err := d.Query(ctx, "SELECT id,staging_token,commit_id FROM graveler_branches WHERE repository_id=$1", repository.RepositoryID)
	if err != nil {
		return err
	}
	defer branches.Close()
	sChan := make(chan *stagedEntry, migrateQueueSize)
	var wg multierror.Group
	for i := 0; i < jobWorkers; i++ {
		wg.Go(func() error {
			return sWorker(sChan)
		})
	}
	branchesScanner := pgxscan.NewRowScanner(branches)
	for branches.Next() {
		b := new(graveler.BranchRecord)
		err = branchesScanner.Scan(b)
		if err != nil {
			break
		}
		rows, err := d.Query(ctx, "SELECT key, identity, data FROM graveler_staging_kv where staging_token=$1", b.StagingToken)
		if err != nil {
			return err
		}
		scanner := pgxscan.NewRowScanner(rows)
		for rows.Next() {
			record := &graveler.ValueRecord{}
			err = scanner.Scan(record)
			if err != nil {
				break
			}
			sChan <- &stagedEntry{
				Token: b.StagingToken,
				Entry: record,
			}
		}
		rows.Close()

		pb := protoFromBranch(b.BranchID, b.Branch)
		data, err := proto.Marshal(pb)
		if err != nil {
			return err
		}
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(graveler.RepoPartition(repository)),
			Key:          []byte(graveler.BranchPath(b.BranchID)),
			Value:        data,
		}); err != nil {
			break
		}
	}
	close(sChan)
	workersErr := wg.Wait().ErrorOrNil()
	if err != nil {
		return err
	}
	if workersErr != nil {
		return workersErr
	}
	return nil
}

func tWorker(ctx context.Context, d *pgxpool.Pool, repository *graveler.RepositoryRecord) error {
	tags, err := d.Query(ctx, "SELECT id,commit_id FROM graveler_tags WHERE repository_id=$1", repository.RepositoryID)
	if err != nil {
		return err
	}
	defer tags.Close()
	tagScanner := pgxscan.NewRowScanner(tags)
	for tags.Next() {
		t := new(graveler.TagRecord)
		err = tagScanner.Scan(t)
		if err != nil {
			return err
		}
		pb := graveler.ProtoFromTag(&graveler.TagRecord{TagID: t.TagID, CommitID: t.CommitID})
		data, err := proto.Marshal(pb)
		if err != nil {
			return err
		}
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(graveler.RepoPartition(repository)),
			Key:          []byte(graveler.TagPath(t.TagID)),
			Value:        data,
		}); err != nil {
			return err
		}
	}
	return nil
}

// rWorker writes a repository, and all repository related entities, to fd
func rWorker(ctx context.Context, d *pgxpool.Pool, rChan <-chan *graveler.RepositoryRecord) error {
	for r := range rChan {
		pb := graveler.ProtoFromRepo(r)
		data, err := proto.Marshal(pb)
		if err != nil {
			return err
		}
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(graveler.RepositoriesPartition()),
			Key:          []byte(graveler.RepoPath(r.RepositoryID)),
			Value:        data,
		}); err != nil {
			return err
		}

		// migrate repository entities
		var wg multierror.Group
		wg.Go(func() error {
			return cWorker(ctx, d, r)
		})
		wg.Go(func() error {
			return bWorker(ctx, d, r)
		})
		wg.Go(func() error {
			return tWorker(ctx, d, r)
		})

		wg.Go(func() error {
			// Migrate Settings
			objectPointer := block.ObjectPointer{
				StorageNamespace: string(r.StorageNamespace),
				Identifier:       fmt.Sprintf(graveler.SettingsRelativeKey, blockstorePrefix, branch.ProtectionSettingKey),
				IdentifierType:   block.IdentifierTypeRelative,
			}
			reader, err := blockstore.Get(ctx, objectPointer, -1)
			if err != nil && !errors.Is(err, adapter.ErrDataNotFound) { // skip settings migration if not found
				return fmt.Errorf("failed to get from blockstore: %w", err)
			} else if err == nil {
				buff, err := io.ReadAll(reader)
				reader.Close()
				if err != nil {
					return fmt.Errorf("failed to read object: %w", err)
				}

				if err = encoder.Encode(kv.Entry{
					PartitionKey: []byte(graveler.RepoPartition(r)),
					Key:          []byte(graveler.SettingsPath(branch.ProtectionSettingKey)),
					Value:        buff,
				}); err != nil {
					return err
				}
			}
			return nil
		})

		workersErr := wg.Wait().ErrorOrNil()
		if err != nil {
			return err
		}
		if workersErr != nil {
			return workersErr
		}
	}
	return nil
}

func Migrate(ctx context.Context, d *pgxpool.Pool, writer io.Writer) error {
	cfg, err := config.NewConfig()
	if err != nil {
		return err
	}

	blockstorePrefix = cfg.GetCommittedBlockStoragePrefix()
	bs, err := factory.BuildBlockAdapter(ctx, nil, cfg)
	if err != nil {
		return err
	}

	return MigrateWithBlockstore(ctx, d, writer, bs, blockstorePrefix)
}

func MigrateWithBlockstore(ctx context.Context, d *pgxpool.Pool, writer io.Writer, bs block.Adapter, bsPrefix string) error {
	blockstore = bs
	blockstorePrefix = bsPrefix
	encoder = kv.SafeEncoder{
		Je: json.NewEncoder(writer),
		Mu: sync.Mutex{},
	}

	// Create header
	if err := encoder.Encode(kv.Header{
		LakeFSVersion:   version.Version,
		PackageName:     packageName,
		DBSchemaVersion: kv.InitialMigrateVersion,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		return err
	}

	rChan := make(chan *graveler.RepositoryRecord, migrateQueueSize)
	var g multierror.Group
	for i := 0; i < jobWorkers; i++ {
		g.Go(func() error {
			return rWorker(ctx, d, rChan)
		})
	}

	rows, err := d.Query(ctx, "SELECT * FROM graveler_repositories")
	if err == nil {
		rowScanner := pgxscan.NewRowScanner(rows)
		for rows.Next() {
			r := new(graveler.RepositoryRecord)
			err = rowScanner.Scan(r)
			if err != nil {
				break
			}

			// Create unique identifier and set repo state to active
			r.InstanceUID = graveler.NewRepoInstanceID()
			r.State = graveler.RepositoryState_ACTIVE
			rChan <- r
		}
	}
	rows.Close() // We don't want to deffer - free connections in the DB pool
	close(rChan)
	workersErr := g.Wait().ErrorOrNil()
	if err != nil {
		return err
	}
	if workersErr != nil {
		return workersErr
	}
	return nil
}
