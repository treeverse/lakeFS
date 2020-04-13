package store

import (
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/model"
)

type ClientOperations interface {
	ListRepos(amount int, after string) ([]*model.Repo, bool, error)
	ReadRepo(repoId string) (*model.Repo, error)
	DeleteRepo(repoId string) error
	WriteRepo(repo *model.Repo) error
}

type DBClientOperations struct {
	tx db.Tx
}

func (c *DBClientOperations) ReadRepo(repoId string) (*model.Repo, error) {
	repo := &model.Repo{}
	err := c.tx.Get(repo, `SELECT * FROM repositories WHERE id = $1`, repoId)
	return repo, err
}

func (c *DBClientOperations) ListRepos(amount int, after string) ([]*model.Repo, bool, error) {
	var repos []*model.Repo
	var hasMore bool
	var err error
	if amount < 0 {
		err = c.tx.Select(&repos, `SELECT * FROM repositories WHERE id > $1 ORDER BY id ASC`, after)
	} else {
		err = c.tx.Select(&repos, `SELECT * FROM repositories WHERE id > $1 ORDER BY id ASC LIMIT $2`, after, amount+1)
	}
	if err != nil {
		return nil, false, err
	}
	if amount >= 0 && len(repos) > amount {
		repos = repos[0:amount]
		hasMore = true
	}
	return repos, hasMore, err
}

func (c *DBClientOperations) DeleteRepo(repoId string) error {
	// clear all workspaces, branches, entries, etc.
	queries := []string{
		`DELETE FROM multipart_upload_parts WHERE repository_id = $1`,
		`DELETE FROM multipart_uploads WHERE repository_id = $1`,
		`DELETE FROM workspace_entries WHERE repository_id = $1`,
		`DELETE FROM branches WHERE repository_id = $1`,
		`DELETE FROM commits WHERE repository_id = $1`,
		`DELETE FROM entries WHERE repository_id = $1`,
		`DELETE FROM roots WHERE repository_id = $1`,
		`DELETE FROM objects WHERE repository_id = $1`,
		`DELETE FROM repositories WHERE id = $1`,
	}

	for _, q := range queries {
		_, err := c.tx.Exec(q, repoId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *DBClientOperations) WriteRepo(repo *model.Repo) error {
	_, err := c.tx.Exec(
		`INSERT INTO repositories (id, creation_date, default_branch, storage_namespace) VALUES ($1, $2, $3, $4)`,
		repo.Id,
		repo.CreationDate,
		repo.DefaultBranch,
		repo.StorageNamespace)
	return err
}
