package retention

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type ActiveCommitFinder struct {
	refManager graveler.RefManager
}

func (a *ActiveCommitFinder) FindActiveCommits(ctx context.Context, repositoryId graveler.RepositoryID) ([]graveler.CommitID, error) {
	activeCommitsToThreshold := make(map[graveler.CommitID]time.Time)
	branchIterator, err := a.refManager.ListBranches(ctx, repositoryId)
	if err != nil {
		return nil, err
	}
	for branchIterator.Next() {
		branchRecord := branchIterator.Value()
		commit, err := a.refManager.GetCommit(ctx, repositoryId, branchRecord.CommitID)
		if err != nil {
			return nil, err
		}
		branchExpirationThreshold := getExpirationThresholdForCommit(commit)
		if !activeCommitsToThreshold[branchRecord.CommitID].After(branchExpirationThreshold) {
			// was already here with earlier expiration date
			continue
		}
		activeCommitsToThreshold[branchRecord.CommitID] = branchExpirationThreshold
		for len(commit.Parents) > 0 && commit.CreationDate.After(branchExpirationThreshold) {
			commitID := commit.Parents[0]
			if !activeCommitsToThreshold[commitID].After(branchExpirationThreshold) {
				// was already here with earlier expiration date
				break
			}
			commit, err = a.refManager.GetCommit(ctx, repositoryId, commitID)
			if err != nil {
				return nil, err
			}
			activeCommitsToThreshold[commitID] = branchExpirationThreshold
		}
	}
	if branchIterator.Err() != nil {
		return nil, branchIterator.Err()
	}
	res := make([]graveler.CommitID, 0, len(activeCommitsToThreshold))
	for commitID := range activeCommitsToThreshold {
		res = append(res, commitID)
	}
	return res, nil
}

func getExpirationThresholdForCommit(c *graveler.Commit) time.Time {
	return time.Now().AddDate(0, 0, -28)
}
