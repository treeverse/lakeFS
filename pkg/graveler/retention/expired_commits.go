package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
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
	logger := logging.FromContext(ctx)
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
		logger.WithFields(logging.Fields{
			"starting_point_branch": startingPoint.BranchID,
			"starting_point_commit": startingPoint.CommitID,
			"retention_days":        retentionDays,
			"commit_node_parent":    commitNode.MainParent,
		}).Trace("start here")
		if startingPoint.BranchID == "" {
			// not a branch HEAD - add a hypothetical HEAD as its parent
			commitNode = CommitNode{
				CreationDate: commitNode.CreationDate,
				MainParent:   startingPoint.CommitID,
			}
		} else {
			var branchRetentionDays int32
			if branchRetentionDays, ok = rules.BranchRetentionDays[string(startingPoint.BranchID)]; ok {
				retentionDays = int(branchRetentionDays)
			}
			activeMap[startingPoint.CommitID] = struct{}{}
			delete(expiredMap, startingPoint.CommitID)
		}
		branchExpirationThreshold := now.AddDate(0, 0, -retentionDays)
		if startingPoint.BranchID != "" {
			processed[startingPoint.CommitID] = now.AddDate(0, 0, -retentionDays)
		}
		for commitNode.MainParent != "" {
			nextCommitID := commitNode.MainParent
			if _, ok = previouslyExpiredMap[nextCommitID]; ok {
				// commit was already expired in a previous run
				break
			}
			var previousThreshold time.Time
			if previousThreshold, ok = processed[nextCommitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				logger.WithFields(logging.Fields{
					"next_commit_id": nextCommitID,
					"prev_thresh":    previousThreshold,
				}).Debug("Was already here with earlier date")
				break
			}
			if commitNode.CreationDate.After(branchExpirationThreshold) {
				activeMap[nextCommitID] = struct{}{}
				delete(expiredMap, nextCommitID)
			} else if _, ok = activeMap[nextCommitID]; !ok {
				expiredMap[nextCommitID] = struct{}{}
			}
			commitNode, ok = commitsMap[nextCommitID]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrCommitNotFound, nextCommitID)
			}
			processed[nextCommitID] = branchExpirationThreshold
		}
	}
	if startingPointIterator.Err() != nil {
		return nil, startingPointIterator.Err()
	}
	logger.WithFields(logging.Fields{"num_active": len(activeMap), "num_expired": len(expiredMap)}).Info("Return")
	return &GarbageCollectionCommits{active: commitSetToSlice(activeMap), expired: commitSetToSlice(expiredMap)}, nil
}

func commitSetToSlice(commitMap map[graveler.CommitID]struct{}) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commitMap))
	for commitID := range commitMap {
		res = append(res, commitID)
	}
	return res
}
