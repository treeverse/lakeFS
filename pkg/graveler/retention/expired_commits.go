package retention

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

var empty struct{}

type GarbageCollectionCommits struct {
	expired []graveler.CommitID
	active  []graveler.CommitID
}

// GetGarbageCollectionCommits returns the sets of expired and active commits, according to the repository's garbage collection rules.
func GetGarbageCollectionCommits(ctx context.Context, branchIterator graveler.BranchIterator, commitGetter *RepositoryCommitGetter, rules *graveler.GarbageCollectionRules, previouslyExpired []graveler.CommitID) (*GarbageCollectionCommits, error) {
	now := time.Now()
	processed := make(map[graveler.CommitID]time.Time)
	previouslyExpiredMap := make(map[graveler.CommitID]bool)
	for _, commitID := range previouslyExpired {
		previouslyExpiredMap[commitID] = true
	}
	activeMap := make(map[graveler.CommitID]struct{})
	expiredMap := make(map[graveler.CommitID]struct{})
	for branchIterator.Next() {
		branchRecord := branchIterator.Value()
		retentionDays := int(rules.DefaultRetentionDays)
		if branchRetentionDays, ok := rules.BranchRetentionDays[string(branchRecord.BranchID)]; ok {
			retentionDays = int(branchRetentionDays)
		}
		branchExpirationThreshold := now.AddDate(0, 0, -retentionDays)
		commitID := branchRecord.CommitID
		commit, err := commitGetter.GetCommit(ctx, commitID)
		if err != nil {
			return nil, err
		}
		if previousThreshold, ok := processed[commitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
			// was already here with earlier expiration date
			continue
		}
		processed[commitID] = branchExpirationThreshold
		activeMap[commitID] = empty
		for len(commit.Parents) > 0 {
			// every branch retains only its main ancestry, acquired by recursively taking the first parent:
			nextCommitID := commit.Parents[0]
			if _, ok := previouslyExpiredMap[nextCommitID]; ok {
				// commit was already expired in a previous run
				break
			}
			if previousThreshold, ok := processed[nextCommitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// was already here with earlier expiration date
				break
			}
			if commit.CreationDate.After(branchExpirationThreshold) {
				activeMap[nextCommitID] = empty
				delete(expiredMap, nextCommitID)
			} else if _, ok := activeMap[nextCommitID]; !ok {
				expiredMap[nextCommitID] = empty
			}
			commit, err = commitGetter.GetCommit(ctx, nextCommitID)
			if err != nil {
				return nil, err
			}
			processed[nextCommitID] = branchExpirationThreshold
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
