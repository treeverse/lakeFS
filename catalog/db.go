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

func getBranchID(tx db.Tx, repository, branch string, lockType LockType) (int64, error) {
	const b = `SELECT b.id FROM branches b join repositories r 
					ON r.id = b.repository_id
					WHERE r.name = $1 AND b.name = $2`
	q, err := formatSQLWithLockType(b, lockType)
	if err != nil {
		return 0, err
	}
	var branchID int64
	err = tx.Get(&branchID, q, repository, branch)
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

func getRepository(tx db.Tx, repository string) (*Repository, error) {
	var r Repository
	err := tx.Get(&r, `SELECT r.name, r.storage_namespace, b.name as default_branch, r.creation_date
			FROM repositories r, branches b
			WHERE r.id = b.repository_id AND r.default_branch = b.id AND r.name = $1`,
		repository)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func getBranchesRelationType(tx db.Tx, sourceBranchID, destinationBranchID int64) (RelationType, error) {
	var youngerBranch, olderBranch int64
	var possibleRelation RelationType
	if sourceBranchID == destinationBranchID {
		return RelationTypeNone, nil
	}
	if sourceBranchID > destinationBranchID {
		possibleRelation = RelationTypeFromSon
		youngerBranch = sourceBranchID
		olderBranch = destinationBranchID
	} else {
		possibleRelation = RelationTypeFromFather
		youngerBranch = destinationBranchID
		olderBranch = sourceBranchID
	}
	var isDirectRelation bool
	err := tx.Get(&isDirectRelation,
		`select lineage[1]=$1 from branches where id=$2`, olderBranch, youngerBranch)
	if err != nil {
		return RelationTypeNone, err
	}
	if isDirectRelation {
		return possibleRelation, nil
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

func GetLineage(tx db.Tx, branchID int64, commitID CommitID) ([]lineageCommit, error) {
	return getLineage(tx, branchID, commitID)
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

func getLineageAsValues(lineage []lineageCommit, branchID int64) string {
	valArray := make([]string, 1, len(lineage)+1)
	valArray[0] = fmt.Sprintf("(0,%d,%d)", branchID, MaxCommitID)
	for precedence, lineageBranch := range lineage {
		valArray = append(valArray, fmt.Sprintf("(%d,%d,%d)", precedence+1, lineageBranch.BranchID, lineageBranch.CommitID))
	}
	valTable := "(VALUES " + strings.Join(valArray, " ,\n ") + ") as l(precedence,branch_id,commit_id) "
	return valTable
}
