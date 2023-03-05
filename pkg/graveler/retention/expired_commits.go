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
	CommitID     graveler.CommitID
	CreationDate time.Time
	ParentsIDs   []graveler.CommitID
	Version      graveler.CommitVersion
}

func NewCommitNode(commitID graveler.CommitID, creationDate time.Time, parentsIDs []graveler.CommitID, version graveler.CommitVersion) CommitNode {
	return CommitNode{
		CommitID:     commitID,
		CreationDate: creationDate,
		ParentsIDs:   parentsIDs,
		Version:      version,
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
	var (
		commitNode CommitNode
		ok         bool
	)
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
	isIncremental := len(previouslyExpired) > 0
	commitsMap := make(map[graveler.CommitID]CommitNode)
	if !isIncremental {
		defer commitsIterator.Close()
		for commitsIterator.Next() {
			commitRecord := commitsIterator.Value()
			commitsMap[commitRecord.CommitID] = NewCommitNode(commitRecord.CommitID, commitRecord.Commit.CreationDate, commitRecord.Commit.Parents, commitRecord.Commit.Version)
		}
	}

	now := time.Now()
	defer startingPointIterator.Close()
	for startingPointIterator.Next() {
		startingPoint := startingPointIterator.Value()
		retentionDays := int(rules.DefaultRetentionDays)
		if isIncremental {
			var commit *graveler.Commit
			commit, err = commitGetter.GetCommit(ctx, startingPoint.CommitID)
			if err != nil {
				return nil, err
			}
			commitNode = NewCommitNode(startingPoint.CommitID, commit.CreationDate, commit.Parents, commit.Version)
		} else {
			commitNode, ok = commitsMap[startingPoint.CommitID]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrCommitNotFound, startingPoint.CommitID)
			}
		}
		if startingPoint.BranchID == "" {
			// not a branch HEAD - add a hypothetical HEAD as its parent
			commitNode = CommitNode{
				CreationDate: time.Now(),
				ParentsIDs:   []graveler.CommitID{startingPoint.CommitID},
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
		for len(commitNode.ParentsIDs) > 0 {
			// every branch retains only its main ancestry, acquired by recursively taking the first parent:
			nextCommitID := commitNode.ParentsIDs[0]
			if commitNode.Version < graveler.CommitVersionParentSwitch {
				nextCommitID = commitNode.ParentsIDs[len(commitNode.ParentsIDs)-1]
			}
			if _, ok = previouslyExpiredMap[nextCommitID]; ok {
				// commit was already expired in a previous run
				break
			}
			var previousThreshold time.Time
			if previousThreshold, ok = processed[nextCommitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// was already here with earlier expiration date
				break
			}
			if commitNode.CreationDate.After(branchExpirationThreshold) {
				activeMap[nextCommitID] = struct{}{}
				delete(expiredMap, nextCommitID)
			} else if _, ok = activeMap[nextCommitID]; !ok {
				expiredMap[nextCommitID] = struct{}{}
			}
			if isIncremental {
				var commit *graveler.Commit
				commit, err = commitGetter.GetCommit(ctx, nextCommitID)
				if err != nil {
					return nil, err
				}
				commitNode = NewCommitNode(startingPoint.CommitID, commit.CreationDate, commit.Parents, commit.Version)
			} else {
				commitNode, ok = commitsMap[startingPoint.CommitID]
				if !ok {
					return nil, fmt.Errorf("%w: %s", ErrCommitNotFound, startingPoint.CommitID)
				}
			}
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
