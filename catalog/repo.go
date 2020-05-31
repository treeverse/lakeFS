package catalog

import "github.com/treeverse/lakefs/db"

func repoGetIDByName(tx db.Tx, repo string) (int, error) {
	var repoID int
	if err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repo); err != nil {
		return 0, err
	}
	return repoID, nil
}
