package ref

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type ExpirationDateGetter interface {
	Get(c *graveler.CommitRecord) time.Time
}

type ExpiredCommitsFinder struct {
	branchLister graveler.BranchLister
	commitGetter graveler.CommitGetter
	rules        *graveler.RetentionRules
}

func NewExpiredCommitsFinder(branchLister graveler.BranchLister, commitGetter graveler.CommitGetter, rules *graveler.RetentionRules) *ExpiredCommitsFinder {
	return &ExpiredCommitsFinder{branchLister: branchLister, commitGetter: commitGetter, rules: rules}
}

func (e *ExpiredCommitsFinder) GetExpiredCommits(ctx context.Context, repositoryID graveler.RepositoryID, previouslyExpiredCommits []graveler.CommitID) (expired []graveler.CommitID, active []graveler.CommitID, err error) {
	now := time.Now()
	processed := make(map[graveler.CommitID]time.Time)

	branchIterator, err := e.branchLister.ListBranches(ctx, repositoryID)
	if err != nil {
		return nil, nil, err
	}
	previouslyExpiredMap := make(map[graveler.CommitID]bool)
	for _, commitID := range previouslyExpiredCommits {
		previouslyExpiredMap[commitID] = true
	}
	activeMap := make(map[graveler.CommitID]bool)
	expiredMap := make(map[graveler.CommitID]bool)
	for branchIterator.Next() {
		branchRecord := branchIterator.Value()
		branchExpirationThreshold := now.AddDate(0, 0, -e.rules.DefaultRetentionDays)
		if branchExpirationPeriod, ok := e.rules.BranchRetentionDays[branchRecord.BranchID]; ok {
			branchExpirationThreshold = now.AddDate(0, 0, -branchExpirationPeriod)
		}
		commitID := branchRecord.CommitID
		previousCommit, err := e.commitGetter.GetCommit(ctx, repositoryID, commitID)
		if err != nil {
			return nil, nil, err
		}
		if previousThreshold, ok := processed[commitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
			// was already here with earlier expiration date
			continue
		}
		processed[commitID] = branchExpirationThreshold
		activeMap[commitID] = true
		for len(previousCommit.Parents) > 0 {
			commitID = previousCommit.Parents[0]
			if _, ok := previouslyExpiredMap[commitID]; ok {
				// commit was already expired in a previous run
				break
			}
			if previousThreshold, ok := processed[commitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// was already here with earlier expiration date
				break
			}
			if previousCommit.CreationDate.After(branchExpirationThreshold) {
				activeMap[commitID] = true
				delete(expiredMap, commitID)
			} else if active, ok := activeMap[commitID]; !ok || !active {
				expiredMap[commitID] = true
			}
			previousCommit, err = e.commitGetter.GetCommit(ctx, repositoryID, commitID)
			if err != nil {
				return nil, nil, err
			}
			processed[commitID] = branchExpirationThreshold
		}
	}
	if branchIterator.Err() != nil {
		return nil, nil, branchIterator.Err()
	}
	return toArray(activeMap), toArray(expiredMap), nil
}

func toArray(commitMap map[graveler.CommitID]bool) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commitMap))
	for commitID := range commitMap {
		res = append(res, commitID)
	}
	return res
}
