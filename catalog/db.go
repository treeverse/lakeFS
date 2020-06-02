package catalog

import "github.com/treeverse/lakefs/db"

func getRepoIDByName(tx db.Tx, repo string) (int, error) {
	var repoID int
	err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repo)
	return repoID, err
}

func getBranchIDByName(tx db.Tx, repoID int, branch string) (int, error) {
	var branchID int
	err := tx.Get(&branchID, `SELECT id FROM branches WHERE repository_id = $1 AND name = $2`, repoID, branch)
	return branchID, err
}

func getBranchIDByRepoBranch(tx db.Tx, repo, branch string) (int, error) {
	repoID, err := getRepoIDByName(tx, repo)
	if err != nil {
		return 0, err
	}
	branchID, err := getBranchIDByName(tx, repoID, branch)
	return branchID, err
}
