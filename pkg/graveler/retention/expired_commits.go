package retention

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

var empty struct{}

type GarbageCollectionCommitsFinder struct {
	branchLister graveler.BranchLister
	commitGetter graveler.CommitGetter
}

type GarbageCollectionCommits struct {
	expired []graveler.CommitID
	active  []graveler.CommitID
}

func NewGarbageCollectionCommitsFinder(branchLister graveler.BranchLister, commitGetter graveler.CommitGetter) *GarbageCollectionCommitsFinder {
	return &GarbageCollectionCommitsFinder{branchLister: branchLister, commitGetter: commitGetter}
}

func (e *GarbageCollectionCommitsFinder) GetGarbageCollectionCommits(ctx context.Context, repositoryID graveler.RepositoryID, rules *graveler.GarbageCollectionRules, previouslyExpired []graveler.CommitID) (*GarbageCollectionCommits, error) {
	now := time.Now()
	processed := make(map[graveler.CommitID]time.Time)

	branchIterator, err := e.branchLister.ListBranches(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	previouslyExpiredMap := make(map[graveler.CommitID]bool)
	for _, commitID := range previouslyExpired {
		previouslyExpiredMap[commitID] = true
	}
	activeMap := make(map[graveler.CommitID]struct{})
	expiredMap := make(map[graveler.CommitID]struct{})
	for branchIterator.Next() {
		branchRecord := branchIterator.Value()
		branchExpirationThreshold := now.AddDate(0, 0, int(-rules.DefaultRetentionDays))
		if branchExpirationPeriod, ok := rules.BranchRetentionDays[string(branchRecord.BranchID)]; ok {
			branchExpirationThreshold = now.AddDate(0, 0, int(-branchExpirationPeriod))
		}
		commitID := branchRecord.CommitID
		previousCommit, err := e.commitGetter.GetCommit(ctx, repositoryID, commitID)
		if err != nil {
			return nil, err
		}
		if previousThreshold, ok := processed[commitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
			// was already here with earlier expiration date
			continue
		}
		processed[commitID] = branchExpirationThreshold
		activeMap[commitID] = empty
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
				activeMap[commitID] = empty
				delete(expiredMap, commitID)
			} else if _, ok := activeMap[commitID]; !ok {
				expiredMap[commitID] = empty
			}
			previousCommit, err = e.commitGetter.GetCommit(ctx, repositoryID, commitID)
			if err != nil {
				return nil, err
			}
			processed[commitID] = branchExpirationThreshold
		}
	}
	if branchIterator.Err() != nil {
		return nil, branchIterator.Err()
	}
	return &GarbageCollectionCommits{active: commitSetToArray(activeMap), expired: commitSetToArray(expiredMap)}, nil
}

func commitSetToArray(commitMap map[graveler.CommitID]struct{}) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commitMap))
	for commitID := range commitMap {
		res = append(res, commitID)
	}
	return res
}
