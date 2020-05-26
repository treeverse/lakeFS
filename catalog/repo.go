package catalog

import (
	"errors"
	"time"

	"github.com/treeverse/lakefs/db"
)

func repoByID(tx db.Tx, id int) (*Repo, error) {
	var repo Repo
	err := tx.Get(&repo, `SELECT * FROM repositories WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	return &repo, nil
}

func repoByName(tx db.Tx, name string) (*Repo, error) {
	var repo Repo
	err := tx.Get(&repo, `SELECT * FROM repositories WHERE name = $1`, name)
	if err != nil {
		return nil, err
	}
	return &repo, nil
}

func repoInsert(tx db.Tx, name string, bucket string, creationDate time.Time, defaultBranchID int) (int, error) {
	res, err := tx.Exec(`
		INSERT INTO repositories (name, storage_namespace, creation_date, default_branch)
		VALUES ($1, $2, $3, $4)`,
		name, bucket, creationDate, defaultBranchID)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(id), nil
}

func branchByName(tx db.Tx, repoID int, name string) (*Branch, error) {
	var branch Branch
	err := tx.Get(branch, `SELECT * FROM branches WHERE repository_id = $1 AND id = $2`, repoID, branch)
	if errors.Is(err, db.ErrNotFound) {
		return nil, ErrBranchNotFound
	}
	if err != nil {
		return nil, err
	}
	return &branch, nil
}
