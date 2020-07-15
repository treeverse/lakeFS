package catalog

import (
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) getRepositoryCache(tx db.Tx, repository string) (*Repository, error) {
	return c.cache.Repository(repository, func(repository string) (*Repository, error) {
		return getRepository(tx, repository)
	})
}

func (c *cataloger) getRepositoryIDCache(tx db.Tx, repository string) (int, error) {
	return c.cache.RepositoryID(repository, func(repository string) (int, error) {
		return getRepositoryID(tx, repository)
	})
}

func (c *cataloger) getBranchIDCache(tx db.Tx, repository string, branch string) (int64, error) {
	return c.cache.BranchID(repository, branch, func(repository string, branch string) (int64, error) {
		return getBranchID(tx, repository, branch, LockTypeNone)
	})
}
