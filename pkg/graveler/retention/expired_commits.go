package retention

import (
	"context"
	"github.com/treeverse/lakefs/pkg/logging"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type GarbageCollectionCommits struct {
	expired []graveler.CommitID
	active  []graveler.CommitID
}

// GetGarbageCollectionCommits returns the sets of expired and active commits, according to the repository's garbage collection rules.
// See https://github.com/treeverse/lakeFS/issues/1932 for more details.
// Upon completion, the given startingPointIterator is closed.
func GetGarbageCollectionCommits(ctx context.Context, startingPointIterator *GCStartingPointIterator, commitGetter *RepositoryCommitGetter, rules *graveler.GarbageCollectionRules, previouslyExpired []graveler.CommitID) (*GarbageCollectionCommits, error) {
	// From each starting point in the given startingPointIterator, it iterates through its main ancestry.
	// All commits reached are added to the active set, until and including the first commit performed before the start of the retention period.
	// All further commits in the ancestry are added to the expired set. The iteration stops upon reaching a commit which exists in the previouslyExpired set, or the DAG root.
	now := time.Now()
	processed := make(map[graveler.CommitID]time.Time)
	previouslyExpiredMap := make(map[graveler.CommitID]bool)
	for _, commitID := range previouslyExpired {
		previouslyExpiredMap[commitID] = true
	}
	activeMap := make(map[graveler.CommitID]struct{})
	expiredMap := make(map[graveler.CommitID]struct{})
	defer startingPointIterator.Close()
	for startingPointIterator.Next() {
		startingPoint := startingPointIterator.Value()
		retentionDays := int(rules.DefaultRetentionDays)
		commit, err := commitGetter.GetCommit(ctx, startingPoint.CommitID)
		if err != nil {
			return nil, err
		}
		if startingPoint.BranchID == "" {
			// not a branch HEAD - add a hypothetical HEAD as its parent
			commit = &graveler.Commit{
				CreationDate: commit.CreationDate,
				Parents:      []graveler.CommitID{startingPoint.CommitID},
			}
		} else {
			if branchRetentionDays, ok := rules.BranchRetentionDays[string(startingPoint.BranchID)]; ok {
				retentionDays = int(branchRetentionDays)
			}
			activeMap[startingPoint.CommitID] = struct{}{}
			delete(expiredMap, startingPoint.CommitID)
		}
		branchExpirationThreshold := now.AddDate(0, 0, -retentionDays)
		if startingPoint.BranchID != "" {
			processed[startingPoint.CommitID] = now.AddDate(0, 0, -retentionDays)
		}
		for len(commit.Parents) > 0 {
			// every branch retains only its main ancestry, acquired by recursively taking the first parent:
			nextCommitID := commit.Parents[0]
			if commit.Version < graveler.CommitVersionParentSwitch {
				nextCommitID = commit.Parents[len(commit.Parents)-1]
			}
			if _, ok := previouslyExpiredMap[nextCommitID]; ok {
				// commit was already expired in a previous run
				break
			}
			if previousThreshold, ok := processed[nextCommitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// was already here with earlier expiration date
				break
			}
			if commit.CreationDate.After(branchExpirationThreshold) {
				activeMap[nextCommitID] = struct{}{}
				delete(expiredMap, nextCommitID)
			} else if _, ok := activeMap[nextCommitID]; !ok {
				expiredMap[nextCommitID] = struct{}{}
			}
			commit, err = commitGetter.GetCommit(ctx, nextCommitID)
			if err != nil {
				return nil, err
			}
			processed[nextCommitID] = branchExpirationThreshold
		}
	}
	if startingPointIterator.Err() != nil {
		return nil, startingPointIterator.Err()
	}
	logging.Default().Info("done after ", time.Since(now))
	logging.Default().Info("active commits- ", len(activeMap))
	logging.Default().Info("non-active commits- ", len(expiredMap))
	return &GarbageCollectionCommits{active: commitSetToSlice(activeMap), expired: commitSetToSlice(expiredMap)}, nil
}

func commitSetToSlice(commitMap map[graveler.CommitID]struct{}) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commitMap))
	for commitID := range commitMap {
		res = append(res, commitID)
	}
	return res
}
