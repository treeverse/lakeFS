package mvcc

import (
	"fmt"
	"reflect"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

type LockType int

const (
	LockTypeNone LockType = iota
	LockTypeShare
	LockTypeUpdate
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func getBranchID(tx db.Tx, repository, branch string, lockType LockType) (int64, error) {
	const b = `SELECT b.id FROM catalog_branches b join catalog_repositories r 
					ON r.id = b.repository_id
					WHERE r.name = $1 AND b.name = $2`
	q, err := formatSQLWithLockType(b, lockType)
	if err != nil {
		return 0, err
	}
	var branchID int64
	err = tx.GetPrimitive(&branchID, q, repository, branch)
	return branchID, err
}

func formatSQLWithLockType(sql string, lockType LockType) (string, error) {
	var q string
	switch lockType {
	case LockTypeNone:
		q = sql
	case LockTypeShare:
		q = sql + " FOR SHARE"
	case LockTypeUpdate:
		q = sql + " FOR UPDATE"
	default:
		return "", catalog.ErrInvalidLockValue
	}
	return q, nil
}

func getRepositoryID(tx db.Tx, repository string) (int, error) {
	var repoID int
	err := tx.GetPrimitive(&repoID, `SELECT id FROM catalog_repositories WHERE name=$1`, repository)
	return repoID, err
}

func getLastCommitIDByBranchID(tx db.Tx, branchID int64) (CommitID, error) {
	var commitID CommitID
	err := tx.GetPrimitive(&commitID, `SELECT max(commit_id) FROM catalog_commits where branch_id=$1`, branchID)
	return commitID, err
}

func getNextCommitID(tx db.Tx) (CommitID, error) {
	var commitID CommitID
	err := tx.GetPrimitive(&commitID, `SELECT nextval('catalog_commit_id_seq');`)
	return commitID, err
}

func getRepository(tx db.Tx, repository string) (*catalog.Repository, error) {
	var r catalog.Repository
	err := tx.Get(&r, `SELECT r.name, r.storage_namespace, b.name as default_branch, r.creation_date
			FROM catalog_repositories r, catalog_branches b
			WHERE r.id = b.repository_id AND r.default_branch = b.id AND r.name = $1`,
		repository)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// paginateSlice take slice address, resize and return 'has more' when needed
func paginateSlice(s interface{}, limit int) bool {
	if limit < 0 {
		return false
	}
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Ptr {
		return false
	}
	el := v.Elem()
	if el.Kind() != reflect.Slice {
		return false
	}
	if el.Len() > limit {
		el.Set(el.Slice(0, limit))
		return true
	}
	return false
}

func getLineage(tx db.Tx, branchID int64, commitID CommitID) ([]lineageCommit, error) {
	effectiveCommit := commitID
	if commitID <= 0 {
		effectiveCommit = MaxCommitID
	}
	sql := `SELECT * FROM UNNEST(
				(SELECT lineage from catalog_branches WHERE id=$1),
				(SELECT lineage_commits FROM catalog_commits WHERE branch_id=$1 AND merge_type='from_parent' AND commit_id <= $2 ORDER BY commit_id DESC LIMIT 1)
			) as t(branch_id, commit_id)`
	var requestedLineage []lineageCommit
	err := tx.Select(&requestedLineage, sql, branchID, effectiveCommit)
	if err != nil {
		return nil, err
	}
	return requestedLineage, nil
}

func getLineageAsValues(lineage []lineageCommit, branchID int64, commitID CommitID) string {
	valArray := make([]string, 1, len(lineage)+1)
	valArray[0] = fmt.Sprintf("(0,%d::bigint,%d::bigint)", branchID, commitID)
	for precedence, lineageBranch := range lineage {
		valArray = append(valArray, fmt.Sprintf("(%d,%d,%d)", precedence+1, lineageBranch.BranchID, lineageBranch.CommitID))
	}
	valTable := "(VALUES " + strings.Join(valArray, " ,\n ") + ") as l(precedence,branch_id,commit_id) "
	return valTable
}
