package catalog

import (
	"fmt"
	"reflect"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

type LockType int

const (
	LockTypeNone LockType = iota
	LockTypeShare
	LockTypeUpdate
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func getBranchIDByRef(tx db.Tx, repository string, ref Ref, lockType LockType) (int64, error) {
	if ref.Branch != "" {
		return getBranchID(tx, repository, ref.Branch, lockType)
	}
	const b = `SELECT branch_id FROM commits c JOIN branches b 
		ON c.branch_id=b.id
		JOIN repositories r ON b.repository_id=r.id
		WHERE c.commit_id=$1 AND r.name=$2`

	q, err := formatSqlWithLockType(b, lockType)
	if err != nil {
		return 0, err
	}
	var branchID int64
	err = tx.Get(&branchID, q, ref.CommitID, repository)
	return branchID, err
}

func getBranchID(tx db.Tx, repository, branch string, lockType LockType) (int64, error) {
	const b = `SELECT b.id FROM branches b join repositories r 
					ON r.id = b.repository_id
					WHERE r.name = $1 AND b.name = $2`
	q, err := formatSqlWithLockType(b, lockType)
	if err != nil {
		return 0, err
	}
	var branchID int64
	err = tx.Get(&branchID, q, repository, branch)
	return branchID, err
}

func formatSqlWithLockType(sql string, lockType LockType) (string, error) {
	var q string
	switch lockType {
	case LockTypeNone:
		q = sql
	case LockTypeShare:
		q = sql + " FOR SHARE"
	case LockTypeUpdate:
		q = sql + " FOR UPDATE"
	default:
		return "", ErrInvalidLockValue
	}
	return q, nil
}

func getRepositoryID(tx db.Tx, repository string) (int, error) {
	var repoID int
	err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repository)
	return repoID, err
}

func getLastCommitIDByBranchID(tx db.Tx, branchID int64) (CommitID, error) {
	var commitID CommitID
	err := tx.Get(&commitID, `SELECT max(commit_id) FROM commits where branch_id=$1`, branchID)
	return commitID, err
}

func getNextCommitID(tx db.Tx) (CommitID, error) {
	var commitID CommitID
	err := tx.Get(&commitID, `SELECT nextval('commit_id_seq');`)
	return commitID, err
}

func getBranchesRelationType(tx db.Tx, sourceBranchID, destinationBranchID int64) (RelationType, error) {
	if sourceBranchID == destinationBranchID {
		return RelationTypeNone, nil
	}
	const directLinkQuery = `SELECT COUNT(*) FROM lineage WHERE branch_id=$2 AND ancestor_branch=$1 AND precedence=1`
	var directLink int
	if err := tx.Get(&directLink, directLinkQuery, sourceBranchID, destinationBranchID); err != nil {
		return RelationTypeNone, err
	}
	if directLink > 0 {
		return RelationTypeFromFather, nil
	}
	if err := tx.Get(&directLink, directLinkQuery, destinationBranchID, sourceBranchID); err != nil {
		return RelationTypeNone, err
	}
	if directLink > 0 {
		return RelationTypeFromSon, nil
	}
	return RelationTypeNotDirect, nil
}

// paginateSlice take slice address, resize and return 'has more' when needed
func paginateSlice(s interface{}, limit int) bool {
	if limit <= 0 {
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
				(SELECT lineage from branches WHERE id=$1),
				(SELECT lineage_commits FROM commits WHERE branch_id=$1 AND merge_type='from_father' AND commit_id <= $2 ORDER BY commit_id DESC LIMIT 1)
			) as t(branch_id, commit_id)`
	var requestedLineage []lineageCommit
	err := tx.Select(&requestedLineage, sql, branchID, effectiveCommit)
	if err != nil {
		return nil, err
	}
	return requestedLineage, nil
}

func getLineageAsValues(tx db.Tx, branchID int64, commitID CommitID) (string, error) {
	val, err := getLineage(tx, branchID, commitID)
	if err != nil {
		return "", err
	}
	valArray := make([]string, 1)
	valArray[0] = fmt.Sprintf("(0,%d,%d)", branchID, MaxCommitID)
	for precedence, lineageBranch := range val {
		valArray = append(valArray, fmt.Sprintf("(%d, %d, %d)", precedence+1, lineageBranch.BranchID, lineageBranch.CommitID))
	}
	valTable := "(VALUES " + strings.Join(valArray, " , ") + ") as l(precedence,branch_id,commit_id) "
	return valTable, nil

}
