package ref

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
)

// IteratorPrefetchSize is the amount of records to maybeFetch from PG
const IteratorPrefetchSize = 1000

// MaxBatchDelay - 3ms was chosen as a max delay time for critical path queries.
// It trades off amount of queries per second (and thus effectiveness of the batching mechanism) with added latency.
// Since reducing # of expensive operations is only beneficial when there are a lot of concurrent requests,
// 	the sweet spot is probably between 1-5 milliseconds (representing 200-1000 requests/second to the data store).
// 3ms of delay with ~300 requests/second per resource sounds like a reasonable tradeoff.
const MaxBatchDelay = time.Millisecond * 3

type Manager struct {
	db              db.Database
	addressProvider ident.AddressProvider
	batchExecutor   batch.Batcher
}

func NewPGRefManager(executor batch.Batcher, db db.Database, addressProvider ident.AddressProvider) *Manager {
	return &Manager{
		db:              db,
		addressProvider: addressProvider,
		batchExecutor:   executor,
	}
}

func (m *Manager) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	key := fmt.Sprintf("GetRepository:%s", repositoryID)
	repository, err := m.batchExecutor.BatchFor(key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		return m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			repository := &graveler.Repository{}
			err := tx.Get(repository,
				`SELECT storage_namespace, creation_date, default_branch FROM graveler_repositories WHERE id = $1`,
				repositoryID)
			if err != nil {
				return nil, err
			}
			return repository, nil
		}, db.ReadOnly())
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrRepositoryNotFound
	}
	if err != nil {
		return nil, err
	}
	return repository.(*graveler.Repository), nil
}

func createBareRepository(tx db.Tx, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	_, err := tx.Exec(
		`INSERT INTO graveler_repositories (id, storage_namespace, creation_date, default_branch) VALUES ($1, $2, $3, $4)`,
		repositoryID, repository.StorageNamespace, repository.CreationDate, repository.DefaultBranchID)
	if errors.Is(err, db.ErrAlreadyExists) {
		return graveler.ErrNotUnique
	}
	return nil
}

func (m *Manager) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository, token graveler.StagingToken) error {
	firstCommit := graveler.NewCommit()
	firstCommit.Message = graveler.FirstCommitMsg
	firstCommit.Generation = 1
	commitID := m.addressProvider.ContentAddress(firstCommit)

	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// create an bare repository first
		err := createBareRepository(tx, repositoryID, repository)
		if err != nil {
			return nil, err
		}

		// Create the default branch with its staging token
		_, err = tx.Exec(`
				INSERT INTO graveler_branches (repository_id, id, staging_token, commit_id)
				VALUES ($1, $2, $3, $4)`,
			repositoryID, repository.DefaultBranchID, token, commitID)

		if err != nil {
			if errors.Is(err, db.ErrAlreadyExists) {
				return nil, graveler.ErrNotUnique
			}
			return nil, err
		}

		// Add a first empty commit to allow branching off the default branch immediately after repository creation
		return nil, m.addCommit(tx, repositoryID, commitID, firstCommit)
	})
	return err
}

func (m *Manager) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, createBareRepository(tx, repositoryID, repository)
	})
	return err
}

func (m *Manager) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return NewRepositoryIterator(ctx, m.db, IteratorPrefetchSize), nil
}

func (m *Manager) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
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

func (m *Manager) RevParse(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.Reference, error) {
	return ResolveRef(ctx, m, m.addressProvider, repositoryID, ref)
}

func (m *Manager) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	key := fmt.Sprintf("GetBranch:%s:%s", repositoryID, branchID)
	branch, err := m.batchExecutor.BatchFor(key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		return m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var rec branchRecord
			err := tx.Get(&rec, `SELECT commit_id, staging_token FROM graveler_branches WHERE repository_id = $1 AND id = $2`,
				repositoryID, branchID)
			if err != nil {
				return nil, err
			}
			return &graveler.Branch{
				CommitID:     rec.CommitID,
				StagingToken: rec.StagingToken,
			}, nil
		}, db.ReadOnly())
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrBranchNotFound
	}
	if err != nil {
		return nil, err
	}
	return branch.(*graveler.Branch), nil
}

func (m *Manager) SetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, branch graveler.Branch) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO graveler_branches (repository_id, id, staging_token, commit_id)
			VALUES ($1, $2, $3, $4)
				ON CONFLICT (repository_id, id)
				DO UPDATE SET staging_token = $3, commit_id = $4`,
			repositoryID, branchID, branch.StagingToken, branch.CommitID)
		return nil, err
	})
	return err
}

func (m *Manager) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		r, err := tx.Exec(
			`DELETE FROM graveler_branches WHERE repository_id = $1 AND id = $2`,
			repositoryID, branchID)
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

func (m *Manager) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewBranchIterator(ctx, m.db, repositoryID, IteratorPrefetchSize), nil
}

func (m *Manager) GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error) {
	key := fmt.Sprintf("GetTag:%s:%s", repositoryID, tagID)
	commitID, err := m.batchExecutor.BatchFor(key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		return m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var commitID graveler.CommitID
			err := tx.Get(&commitID, `SELECT commit_id FROM graveler_tags WHERE repository_id = $1 AND id = $2`,
				repositoryID, tagID)
			if err != nil {
				return nil, err
			}
			return &commitID, nil
		}, db.ReadOnly())
	}))
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrTagNotFound
	}
	if err != nil {
		return nil, err
	}
	return commitID.(*graveler.CommitID), nil
}

func (m *Manager) CreateTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		res, err := tx.Exec(`INSERT INTO graveler_tags (repository_id, id, commit_id) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`,
			repositoryID, tagID, commitID)
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

func (m *Manager) DeleteTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		r, err := tx.Exec(
			`DELETE FROM graveler_tags WHERE repository_id = $1 AND id = $2`,
			repositoryID, tagID)
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

func (m *Manager) ListTags(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.TagIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewTagIterator(ctx, m.db, repositoryID, IteratorPrefetchSize), nil
}

func (m *Manager) GetCommitByPrefix(ctx context.Context, repositoryID graveler.RepositoryID, prefix graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommitByPrefix:%s:%s", repositoryID, prefix)

	commit, err := m.batchExecutor.BatchFor(key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		return m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			records := make([]*commitRecord, 0)
			// LIMIT 2 is used to test if a truncated commit ID resolves to *one* commit.
			// if we get 2 results that start with the truncated ID, that's enough to determine this prefix is not unique
			err := tx.Select(&records, `
					SELECT id, committer, message, creation_date, parents, meta_range_id, metadata, version, generation
					FROM graveler_commits
					WHERE repository_id = $1 AND id LIKE $2 || '%'
					LIMIT 2`,
				repositoryID, prefix)
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

func (m *Manager) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	key := fmt.Sprintf("GetCommit:%s:%s", repositoryID, commitID)
	commit, err := m.batchExecutor.BatchFor(key, MaxBatchDelay, batch.BatchFn(func() (interface{}, error) {
		return m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			var rec commitRecord
			err := tx.Get(&rec, `
					SELECT committer, message, creation_date, parents, meta_range_id, metadata, version, generation
					FROM graveler_commits WHERE repository_id = $1 AND id = $2`,
				repositoryID, commitID)
			if err != nil {
				return nil, err
			}
			return rec.toGravelerCommit(), nil
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

func (m *Manager) AddCommit(ctx context.Context, repositoryID graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	commitID := m.addressProvider.ContentAddress(commit)
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, m.addCommit(tx, repositoryID, commitID, commit)
	})
	if err != nil {
		return "", err
	}
	return graveler.CommitID(commitID), err
}

func (m *Manager) addCommit(tx db.Tx, repositoryID graveler.RepositoryID, commitID string, commit graveler.Commit) error {
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
		repositoryID, commitID, commit.Committer, commit.Message,
		commit.CreationDate.UTC(), parents, commit.MetaRangeID, commit.Metadata, commit.Version, commit.Generation)

	return err
}

func (m *Manager) FindMergeBase(ctx context.Context, repositoryID graveler.RepositoryID, commitIDs ...graveler.CommitID) (*graveler.Commit, error) {
	const allowedCommitsToCompare = 2
	if len(commitIDs) != allowedCommitsToCompare {
		return nil, graveler.ErrInvalidMergeBase
	}
	return FindMergeBase(ctx, m, repositoryID, commitIDs[0], commitIDs[1])
}

func (m *Manager) Log(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.CommitID) (graveler.CommitIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewCommitIterator(ctx, m.db, repositoryID, from), nil
}

func (m *Manager) ListCommits(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.CommitIterator, error) {
	_, err := m.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return NewOrderedCommitIterator(ctx, m.db, repositoryID, IteratorPrefetchSize)
}

func (m *Manager) FillGenerations(ctx context.Context, repositoryID graveler.RepositoryID) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`WITH RECURSIVE cte AS
								(
									SELECT id, 1 AS generation,creation_date
									FROM graveler_commits
									WHERE repository_id = $1 AND parents IS NULL OR array_length(parents,1) = 0
									UNION ALL
									SELECT DISTINCT(c.id), cte.generation+1 AS generation,c.creation_date
									FROM graveler_commits c INNER JOIN cte
									ON cte.id = ANY(c.parents) WHERE repository_id = $1
								)
								UPDATE graveler_commits u SET generation = cte.generation FROM cte WHERE u.id = cte.id;`,
			repositoryID)
	})
	return err
}
