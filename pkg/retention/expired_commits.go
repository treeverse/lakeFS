package retention

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type ExpirationDateGetter interface {
	Get(c *graveler.CommitRecord) time.Time
}

type ExpiredCommitFinder struct {
	refManager           graveler.RefManager
	expirationDateGetter ExpirationDateGetter
}

type CommitSet map[graveler.CommitID]bool

type Commits struct {
	Expired CommitSet
	Active  CommitSet
}

func (a *ExpiredCommitFinder) Find(ctx context.Context, repositoryId graveler.RepositoryID, previouslyExpiredCommits CommitSet) (*Commits, error) {
	processed := make(map[graveler.CommitID]time.Time)
	res := &Commits{
		Active:  make(map[graveler.CommitID]bool, 0),
		Expired: make(map[graveler.CommitID]bool, 0),
	}
	branchIterator, err := a.refManager.ListBranches(ctx, repositoryId)
	if err != nil {
		return nil, err
	}
	for branchIterator.Next() {
		branchRecord := branchIterator.Value()
		commitID := branchRecord.CommitID
		previousCommit, err := a.refManager.GetCommit(ctx, repositoryId, commitID)
		if err != nil {
			return nil, err
		}
		var branchExpirationThreshold time.Time
		if a.expirationDateGetter == nil {
			branchExpirationThreshold = getExpirationThresholdForCommit(previousCommit)
		} else {
			branchExpirationThreshold = a.expirationDateGetter.Get(&graveler.CommitRecord{CommitID: commitID, Commit: previousCommit})
		}
		if previousThreshold, ok := processed[commitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
			// was already here with earlier expiration date
			continue
		}
		processed[commitID] = branchExpirationThreshold
		res.Active[commitID] = true
		for len(previousCommit.Parents) > 0 {
			commitID = previousCommit.Parents[0]
			if _, ok := previouslyExpiredCommits[commitID]; ok {
				// commit was already expired in a previous run
				break
			}
			if previousThreshold, ok := processed[commitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// was already here with earlier expiration date
				break
			}
			if previousCommit.CreationDate.After(branchExpirationThreshold) {
				res.Active[commitID] = true
				delete(res.Expired, commitID)
			} else if active, ok := res.Active[commitID]; !ok || !active {
				res.Expired[commitID] = true
			}
			previousCommit, err = a.refManager.GetCommit(ctx, repositoryId, commitID)
			if err != nil {
				return nil, err
			}
			processed[commitID] = branchExpirationThreshold
		}
	}
	if branchIterator.Err() != nil {
		return nil, branchIterator.Err()
	}
	return res, nil
}

func getExpirationThresholdForCommit(_ *graveler.Commit) time.Time {
	return time.Now().AddDate(0, 0, -28)
}
