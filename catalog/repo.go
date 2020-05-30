package catalog

import "github.com/treeverse/lakefs/db"

func repoGetByName(tx db.Tx, repo string) (*RepoDB, error) {
	var repoDB RepoDB
	sql, args := db.Builder.
		NewSelectBuilder().
		From("repositories").
		Select("id", "name", "storage_namespace", "creation_date", "default_branch").
		Build()
	if err := tx.Get(&repoDB, sql, args...); err != nil {
		return nil, err
	}
	return &repoDB, nil
}

func repoGetIDByName(tx db.Tx, repo string) (int, error) {
	var repoID int
	if err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repo); err != nil {
		return 0, err
	}
	return repoID, nil
}
