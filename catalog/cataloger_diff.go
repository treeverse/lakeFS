package catalog

import (
	"context"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Diff(ctx context.Context, repo, leftBranch, rightBranch string) (Differences, error) {
	if err := Validate(ValidateFields{
		"repo":        ValidateRepoName(repo),
		"leftBranch":  ValidateBranchName(leftBranch),
		"rightBranch": ValidateBranchName(rightBranch),
	}); err != nil {
		return nil, err
	}
	differences, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		log := c.log.WithContext(ctx)
		leftId, err := getBranchID(tx, repo, leftBranch, NoLock)
		if err != nil {
			log.WithError(err).
				WithFields(logging.Fields{
					"branch": leftBranch,
					"repo":   repo,
				}).Warn("Branch not found")
			return nil, err
		}
		rightId, err := getBranchID(tx, repo, rightBranch, NoLock)
		if err != nil {
			log.WithError(err).
				WithFields(logging.Fields{
					"branch": rightBranch,
					"repo":   repo,
				}).Warn("Branch not found")
			return nil, err
		}
		return doDiff(tx, leftId, rightId, log)
	})
	return differences.(Differences), err
}

func doDiff(tx db.Tx, leftId, rightId int, log logging.Logger) (Differences, error) {
	// check diff type
	var directLink int
	directLinkQuery := `select count(*) from lineage where branch_id = $1 and ancestor_branch = $2 and precedence = 1`
	err := tx.Get(&directLink, directLinkQuery, leftId, rightId)
	if err != nil {
		log.WithError(err).Error("error reading lineage table")
		return nil, err
	}
	if directLink > 0 {
		return FromFatherDiff(tx, leftId, rightId, log)
	}
	err = tx.Get(&directLink, directLinkQuery, rightId, leftId)
	if err != nil {
		log.WithError(err).Error("error reading lineage table")
		return nil, err
	}
	if directLink > 0 {
		return FromSonDiff(tx, leftId, rightId, log)
	}

	return nonDirectDiff(tx, leftId, rightId, log)

}

func FromFatherDiff(tx db.Tx, leftId, rightId int, log logging.Logger) (Differences, error) {
	// just need to check there are no conflicts

	// check if father lineage was modified since last diff. if not we can skip the view
	_ = `select * from lineage l join lineage r on
	     l.branch_id=$1 and r.branch_id =$2 and
		  l.ancestor_branch=r.ancestor_branch
	     where
	     l.effective_commit > r.effective_commit`
	// from father - select objects that were modified after the lineage was created

	// from son - select objects that were modified since last time lineage was created

	return nil, nil
}

func FromSonDiff(tx db.Tx, leftId, rightId int, log logging.Logger) (Differences, error) {

	return nil, nil
}

func nonDirectDiff(tx db.Tx, leftId, rightId int, log logging.Logger) (Differences, error) {

	return nil, nil
}
