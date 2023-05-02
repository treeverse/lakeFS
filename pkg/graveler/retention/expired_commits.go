package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type GarbageCollectionCommits struct {
	expired []graveler.CommitID
	active  []graveler.CommitID
}

type CommitNode struct {
	CreationDate time.Time
	MainParent   graveler.CommitID
}

func NewCommitNode(creationDate time.Time, mainParent graveler.CommitID) CommitNode {
	return CommitNode{
		CreationDate: creationDate,
		MainParent:   mainParent,
	}
}

var ErrCommitNotFound = errors.New("commit not found")

// GetGarbageCollectionCommits returns the sets of expired and active commits, according to the repository's garbage collection rules.
// See https://github.com/treeverse/lakeFS/issues/1932 for more details.
// Upon completion, the given startingPointIterator is closed.
func GetGarbageCollectionCommits(ctx context.Context, startingPointIterator *GCStartingPointIterator, commitGetter *RepositoryCommitGetter, rules *graveler.GarbageCollectionRules, previouslyExpired []graveler.CommitID) (*GarbageCollectionCommits, error) {
	// From each starting point in the given startingPointIterator, it iterates through its main ancestry.
	// All commits reached are added to the active set, until and including the first commit performed before the start of the retention period.
	// All further commits in the ancestry are added to the expired set. The iteration stops upon reaching a commit which exists in the previouslyExpired set, or the DAG root.
	processed := make(map[graveler.CommitID]time.Time)
	previouslyExpiredMap := make(map[graveler.CommitID]bool)
	for _, commitID := range previouslyExpired {
		previouslyExpiredMap[commitID] = true
	}
	activeMap := make(map[graveler.CommitID]struct{})
	expiredMap := make(map[graveler.CommitID]struct{})

	commitsIterator, err := commitGetter.ListCommits(ctx)
	if err != nil {
		return nil, err
	}
	commitsMap := make(map[graveler.CommitID]CommitNode)
	defer commitsIterator.Close()
	for commitsIterator.Next() {
		commitRecord := commitsIterator.Value()
		var mainParent graveler.CommitID
		if len(commitRecord.Commit.Parents) > 0 {
			// every branch retains only its main ancestry, acquired by recursively taking the first parent:
			mainParent = commitRecord.Commit.Parents[0]
			if commitRecord.Commit.Version < graveler.CommitVersionParentSwitch {
				mainParent = commitRecord.Commit.Parents[len(commitRecord.Commit.Parents)-1]
			}
		}
		commitsMap[commitRecord.CommitID] = NewCommitNode(commitRecord.Commit.CreationDate, mainParent)
	}

	now := time.Now()
	defer startingPointIterator.Close()
	for startingPointIterator.Next() {
		startingPoint := startingPointIterator.Value()
		retentionDays := int(rules.DefaultRetentionDays)
		commitNode, ok := commitsMap[startingPoint.CommitID]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrCommitNotFound, startingPoint.CommitID)
		}
		if startingPoint.BranchID == "" {
			// If the current commit is NOT a branch HEAD (a dangling commit) - add a hypothetical HEAD as its parent
			commitNode = CommitNode{
				CreationDate: commitNode.CreationDate,
				MainParent:   startingPoint.CommitID,
			}
		} else {
			// If the current commit IS a branch HEAD - fetch and retention rules for this branch and...
			var branchRetentionDays int32
			if branchRetentionDays, ok = rules.BranchRetentionDays[string(startingPoint.BranchID)]; ok {
				retentionDays = int(branchRetentionDays)
			}
			// set it as active (we don't delete branch HEADs), and remove it from the expired list if it was put there
			// by some other commit path traversal.
			activeMap[startingPoint.CommitID] = struct{}{}
			delete(expiredMap, startingPoint.CommitID)
		}
		// Calculate the expiration time for the current commit
		branchExpirationThreshold := now.AddDate(0, 0, -retentionDays)
		if startingPoint.BranchID != "" {
			// If the current commit IS a branch's HEAD, add it to the `processed` with the calculated expiration threshold.
			// (it will be optionally examined later on by different commit paths to get the longest expiration threshold for a given commit)
			processed[startingPoint.CommitID] = branchExpirationThreshold
		}
		// Start traversing the commit's ancestors (path):
		for commitNode.MainParent != "" {
			nextCommitID := commitNode.MainParent
			var previousThreshold time.Time
			if previousThreshold, ok = processed[nextCommitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// If the parent commit was already processed and its threshold was longer than the current threshold,
				// i.e. the current threshold doesn't hold for it, stop processing it because the other path decision
				// wins
				break
			}
			if commitNode.CreationDate.After(branchExpirationThreshold) {
				// If the current commit creation time is after the threshold, then its parent is active because the
				// definition for 'active' is either creation time is after the threshold, or the first beyond
				// the threshold. In either way, the PARENT is active.
				activeMap[nextCommitID] = struct{}{}
				delete(expiredMap, nextCommitID)
			} else if _, ok = activeMap[nextCommitID]; !ok {
				// If the parent commit was expired in a previous GC run (incremental GC case) we can stop because it's
				// both expired in this run and the previous (in this path)
				if _, ok = previouslyExpiredMap[nextCommitID]; ok {
					break
				}
				// Else, if the parent commit isn't in the active map, and the current commit's creation time isn't after the
				// threshold, then the parent has expired.
				expiredMap[nextCommitID] = struct{}{}
			}
			// Continue down the rabbit hole.
			commitNode, ok = commitsMap[nextCommitID]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrCommitNotFound, nextCommitID)
			}
			// Set the parent commit ID's expiration threshold as the current (this is true because this one is the
			// longest, because we wouldn't have gotten here otherwise)
			processed[nextCommitID] = branchExpirationThreshold
		}
	}
	if startingPointIterator.Err() != nil {
		return nil, startingPointIterator.Err()
	}
	return &GarbageCollectionCommits{active: commitSetToSlice(activeMap), expired: commitSetToSlice(expiredMap)}, nil
}

func commitSetToSlice(commitMap map[graveler.CommitID]struct{}) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commitMap))
	for commitID := range commitMap {
		res = append(res, commitID)
	}
	return res
}
