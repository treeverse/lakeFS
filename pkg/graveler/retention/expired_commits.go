package retention

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type GarbageCollectionCommits struct {
	expired []graveler.CommitID
	active  []graveler.CommitID
}

type GCStartingPoint struct {
	BranchID graveler.BranchID
	CommitID graveler.CommitID
}

type GCStartingPointIterator struct {
	commitIterator graveler.CommitIterator
	branchIterator graveler.BranchIterator

	commitValue *graveler.CommitRecord
	branchValue *graveler.BranchRecord
	value       *GCStartingPoint
}

func NewGCStartingPointIterator(commitIterator graveler.CommitIterator, branchIterator graveler.BranchIterator) *GCStartingPointIterator {
	return &GCStartingPointIterator{commitIterator: commitIterator, branchIterator: branchIterator}
}

func (sp *GCStartingPointIterator) Next() bool {
	prepareBranchValue := func() bool {
		sp.value = &GCStartingPoint{
			BranchID: sp.branchValue.BranchID,
			CommitID: sp.branchValue.CommitID,
		}
		sp.branchValue = nil
		return true
	}
	prepareCommitValue := func() bool {
		sp.value = &GCStartingPoint{
			CommitID: sp.commitValue.CommitID,
		}
		sp.commitValue = nil
		return true
	}

	if sp.branchValue == nil && sp.branchIterator.Next() {
		sp.branchValue = sp.branchIterator.Value()
	}
	if sp.commitValue == nil && sp.commitIterator.Next() {
		sp.commitValue = sp.commitIterator.Value()
	}
	if sp.commitValue == nil {
		if sp.branchValue == nil {
			return false
		}
		// has only branch
		return prepareBranchValue()
	}
	if sp.branchValue == nil || sp.commitValue.CommitID < sp.branchValue.CommitID {
		// has only commit, or commit is before branch
		return prepareCommitValue()
	}
	if sp.branchValue.CommitID == sp.commitValue.CommitID {
		// commit is same as branch head - skip the commit
		sp.commitValue = nil
	}
	// branch is before or equal to commit
	return prepareBranchValue()
}

func (sp *GCStartingPointIterator) Value() *GCStartingPoint {
	return sp.value
}

func (sp *GCStartingPointIterator) Err() error {
	if sp.branchIterator != nil {
		return sp.branchIterator.Err()
	}
	return sp.commitIterator.Err()
}

func (sp *GCStartingPointIterator) Close() {
	sp.commitIterator.Close()
	sp.branchIterator.Close()
}

// GetGarbageCollectionCommits returns the sets of expired and active commits, according to the repository's garbage collection rules.
func GetGarbageCollectionCommits(ctx context.Context, startingPointIterator *GCStartingPointIterator, commitGetter *RepositoryCommitGetter, rules *graveler.GarbageCollectionRules, previouslyExpired []graveler.CommitID) (*GarbageCollectionCommits, error) {
	now := time.Now()
	processed := make(map[graveler.CommitID]time.Time)
	previouslyExpiredMap := make(map[graveler.CommitID]bool)
	for _, commitID := range previouslyExpired {
		previouslyExpiredMap[commitID] = true
	}
	activeMap := make(map[graveler.CommitID]struct{})
	expiredMap := make(map[graveler.CommitID]struct{})
	for startingPointIterator.Next() {
		var err error
		startingPoint := startingPointIterator.Value()
		retentionDays := int(rules.DefaultRetentionDays)
		var commit *graveler.Commit
		commit, err = commitGetter.GetCommit(ctx, startingPoint.CommitID)
		if err != nil {
			return nil, err
		}
		if startingPoint.BranchID == "" {
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
	startingPointIterator.Close()
	return &GarbageCollectionCommits{active: commitSetToArray(activeMap), expired: commitSetToArray(expiredMap)}, nil
}

func commitSetToArray(commitMap map[graveler.CommitID]struct{}) []graveler.CommitID {
	res := make([]graveler.CommitID, 0, len(commitMap))
	for commitID := range commitMap {
		res = append(res, commitID)
	}
	return res
}
