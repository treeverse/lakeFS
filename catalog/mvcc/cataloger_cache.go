package mvcc

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) getRepositoryCache(tx db.Tx, repository string) (*catalog.Repository, error) {
	repo, err := c.cache.Repository(repository, func(repository string) (*catalog.Repository, error) {
		repo, err := getRepository(tx, repository)
		if err != nil {
			return nil, fmt.Errorf("get repository: %w", err)
		}
		return repo, nil
	})
	if errors.Is(err, cache.ErrCacheItemNotFound) {
		return repo, ErrRepositoryNotFound
	}
	return repo, err
}

func (c *cataloger) getRepositoryIDCache(tx db.Tx, repository string) (int, error) {
	repoID, err := c.cache.RepositoryID(repository, func(repository string) (int, error) {
		repoID, err := getRepositoryID(tx, repository)
		if err != nil {
			return 0, fmt.Errorf("get repository id: %w", err)
		}
		return repoID, nil
	})
	if errors.Is(err, cache.ErrCacheItemNotFound) {
		return repoID, ErrRepositoryNotFound
	}
	return repoID, err
}

func (c *cataloger) getBranchIDCache(tx db.Tx, repository string, branch string) (int64, error) {
	branchID, err := c.cache.BranchID(repository, branch, func(repository string, branch string) (int64, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeNone)
		if err != nil {
			return 0, fmt.Errorf("get branch id: %w", err)
		}
		return branchID, nil
	})

	if !(errors.Is(err, cache.ErrCacheItemNotFound) || errors.Is(err, db.ErrNotFound)) {
		return branchID, err
	}

	if _, err := c.getRepositoryIDCache(tx, repository); err != nil {
		return 0, ErrRepositoryNotFound
	}

	return 0, ErrBranchNotFound
}
