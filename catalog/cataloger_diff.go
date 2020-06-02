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
		leftId, err := getBranchId(tx, repo, leftBranch, NoLock)
		if err != nil {
			log.WithError(err).
				WithFields(logging.Fields{
					"branch": leftBranch,
					"repo":   repo,
				}).Warn("Branch not found")
			return nil, err
		}
		rightId, err := getBranchId(tx, repo, rightBranch, NoLock)
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
	// get the last son commit number of the last father merge
	// if there is none - then it is  the first merge
	// the condition on merge_source_branch is redundent. a given branch may have only one father.
	// it is there in the hope it makes the idea less confusing
	x := `select coalesc(max(commit_number),0) from commits 
			where branch_id = $1 and merge_type = 'fromFather' and merge_source_branch = $2`
	x = `select * from entries where branch_id = $1`
	// check if father lineage was modified since last diff. if not we can skip the view
	x = `select s.effective_commit as min_effective_commit,e.effective_commit as min_effective_commit from lineage s join lineage e on
	     s.branch_id=$1 and e.branch_id =$2 and
		  s.ancestor_branch=e.ancestor_branch
	     `
	x = `select * from entries_lineage_v e join lineage_dif l 
		 on e.source_branch = l.ancestore_branch
		where
		e.branch_id = $1 and
		e.min_branch > l.min_effective_commit`
	x = `select * from father f  left join  son s on 
			s.path  = f.path 
			where
			s.path is null 
			or not  -- if they have the same object, than it is not deleted in one and exist in the other
			(f.physical_address = s.physical_address and --  the same object 
              ( f.max_commit != 2147483647 or f.max_commit != s.max_commit ))
			or`
	// from father - select comitted objects that were modified after the lineage was created
	x := ` with father as ( select * from entries_lineage_v e join lineage l on
			e.displayed_branch = $1 and l.branch_id = $2 and
			e.source_branch=l.ancestor_branch
			where
			e.min_commit > l.effective_commit)`
	// from son - select objects that were modified since last time lineage was created
	x = `select max(min_commit) from lineage where branch_id = $1` // get highest commit number from son that was synchronized

	x = `son as (select * from entries where branch_id = $1 and min_commit > $2)`
	x = ` select * from father f full outer join son s on f.path=s.path and f.physical_address != s.physical_address
		order by coalesc(s.path,f.path)`
	return nil, nil
}

func FromSonDiff(tx db.Tx, leftId, rightId int, log logging.Logger) (Differences, error) {
	x := `select coalesc(max(merge_source_commit),0) from commits where branch_id = $1 and merge_type = 'fromSon' `
	// relevant entries from son
	x = ` select * from entries where min_commit > merge_commit and branch_id = $1 `
	// relevant entries from father
	x := `select l.ancestor_branch, l.effective_commit as start_commit,r.effective_commit as end_commit 
			from lineage l join lineage_v r 
		 on l.ancestor_branch = r.ancestor_branch 
		where
		l.branch_id = $1 and r.branch_id = $2
		order by ancestor_branch desc`
	x = ` select * from entries_lineage_v e join ancestor_relevant a
			on e.displayed_branch=a.branch_id
			where displayed_branch = $1 and 
			e.min_commit between a.start_commit and a.end_commit`

	return nil, nil
}

func nonDirectDiff(tx db.Tx, leftId, rightId int, log logging.Logger) (Differences, error) {

	return nil, nil
}
