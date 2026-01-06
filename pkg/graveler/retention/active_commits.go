package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/logging"
)

// CommitsMap is an immutable cache of commits.  Each commit can be set once, and will be read
// if needed.  It is *not* thread-safe.
type CommitsMap struct {
	ctx          context.Context
	Log          logging.Logger
	NumMisses    int64
	CommitGetter RepositoryCommitGetter
	Store        kv.Store
}

const (
	commitsPartition = "gc:commits"
)

var (
	ErrBadCommitID = errors.New("bad format commit ID")
)

func NewCommitsMap(ctx context.Context, commitGetter RepositoryCommitGetter, store kv.Store) (CommitsMap, error) {
	c := CommitsMap{
		ctx:          ctx,
		Log:          logging.FromContext(ctx),
		NumMisses:    int64(0),
		CommitGetter: commitGetter,
		Store:        store,
	}

	it, err := commitGetter.List(ctx)
	if err != nil {
		return CommitsMap{}, fmt.Errorf("list existing commits into map: %w", err)
	}
	defer it.Close()
	for it.Next() {
		commit := it.Value()
		err = c.Set(ctx, commit.CommitID, nodeFromCommit(commit.Commit))
		if err != nil {
			return CommitsMap{}, fmt.Errorf("set commit %s in local store: %w", commit.CommitID, err)
		}
	}
	return c, nil
}

// Set sets a commit.  It will not be looked up again in CommitGetter.
func (c *CommitsMap) Set(ctx context.Context, id graveler.CommitID, node *CommitNode) error {
	return kv.SetMsg(ctx, c.Store, commitsPartition, []byte(id), node)
}

// Get gets a commit.  If the commit has not been Set it uses CommitGetter
// to read it.
func (c *CommitsMap) Get(ctx context.Context, id graveler.CommitID) (*CommitNode, error) {
	{
		var fastRet CommitNode
		_, err := kv.GetMsg(ctx, c.Store, commitsPartition, []byte(id), &fastRet)
		if err == nil {
			return &fastRet, nil
		}
	}
	// Unlikely: commit ID was created during a race with the initial bunch of Sets.
	commit, err := c.CommitGetter.Get(c.ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get missing commit ID %s: %w", id, err)
	}
	ret := nodeFromCommit(commit)
	err = c.Set(ctx, id, ret)
	if err != nil {
		// Doubly unlikely, so fail the entire operation.
		return nil, fmt.Errorf("fill missing commit ID %s into local cache: %w", id, err)
	}
	c.NumMisses++
	createdAt := time.UnixMicro(ret.CreationUsecs)
	c.Log.WithFields(logging.Fields{
		"commit_id": id,
		"created":   createdAt,
		"age":       time.Since(createdAt),
	}).Warn("Loaded single commit, probably new")
	return ret, nil
}

func (c *CommitsMap) Close() {
	c.Store.Close()
}

// nodeFromCommit returns a new CommitNode for a Commit.
func nodeFromCommit(commit *graveler.Commit) *CommitNode {
	var mainParent graveler.CommitID
	if len(commit.Parents) > 0 {
		// every branch retains only its main ancestry, acquired by recursively taking the first parent:
		mainParent = commit.Parents[0]
		if commit.Version < graveler.CommitVersionParentSwitch {
			mainParent = commit.Parents[len(commit.Parents)-1]
		}
	}
	return &CommitNode{
		CreationUsecs: commit.CreationDate.UnixMicro(),
		MainParent:    string(mainParent),
		MetaRangeID:   string(commit.MetaRangeID),
	}
}

// GetGarbageCollectionCommits returns the sets of active commits, according to the repository's garbage collection rules.
// See https://github.com/treeverse/lakeFS/issues/1932 for more details.
// Upon completion, the given startingPointIterator is closed.
func GetGarbageCollectionCommits(ctx context.Context, startingPointIterator *GCStartingPointIterator, commitGetter RepositoryCommitGetter, rules *graveler.GarbageCollectionRules) (map[graveler.CommitID]graveler.MetaRangeID, error) {
	// From each starting point in the given startingPointIterator, it iterates through its main ancestry.
	// All commits reached are added to the active set, until and including the first commit performed before the start of the retention period.
	processed := make(map[graveler.CommitID]time.Time)
	activeMap := make(map[graveler.CommitID]struct{})

	// TODO(ariels): Re-use configurable path.
	store, err := kv.Open(ctx, kvparams.Config{
		Type:  local.DriverName,
		Local: &kvparams.Local{Path: "gc.db"},
	})
	if err != nil {
		return nil, fmt.Errorf("open commits map temp KV: %w", err)
	}

	commitsMap, err := NewCommitsMap(ctx, commitGetter, store)
	if err != nil {
		return nil, fmt.Errorf("initial read commits: %w", err)
	}
	defer commitsMap.Close()
	// Observe NumMisses.  This should not be a metric unless we see it happen a _lot_.
	defer func() {
		logging.FromContext(ctx).
			WithField("num_misses", commitsMap.NumMisses).
			Info("Commits map - misses are due to concurrent commits")
	}()

	now := time.Now()
	defer startingPointIterator.Close()
	for startingPointIterator.Next() {
		startingPoint := startingPointIterator.Value()
		retentionDays := int(rules.DefaultRetentionDays)
		commitNode, err := commitsMap.Get(ctx, startingPoint.CommitID)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, startingPoint.CommitID)
		}
		if startingPoint.BranchID == "" {
			// If the current commit is NOT a branch HEAD (a dangling commit) - add a hypothetical HEAD as its child
			commitNode = &CommitNode{
				CreationUsecs: commitNode.CreationUsecs,
				MainParent:    string(startingPoint.CommitID),
			}
		} else {
			// If the current commit IS a branch HEAD - fetch and retention rules for this branch and...
			if branchRetentionDays, ok := rules.BranchRetentionDays[string(startingPoint.BranchID)]; ok {
				retentionDays = int(branchRetentionDays)
			}
			activeMap[startingPoint.CommitID] = struct{}{}
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
			nextCommitID := graveler.CommitID(commitNode.MainParent)
			if previousThreshold, ok := processed[nextCommitID]; ok && !previousThreshold.After(branchExpirationThreshold) {
				// If the parent commit was already processed and its threshold was longer than the current threshold,
				// i.e. the current threshold doesn't hold for it, stop processing it because the other path decision
				// wins
				break
			}
			createdAt := time.UnixMicro(commitNode.CreationUsecs)
			if createdAt.After(branchExpirationThreshold) {
				// If the current commit creation time is after the threshold, then its parent is active because the
				// definition for 'active' is either creation time is after the threshold, or the first beyond
				// the threshold. In either way, the PARENT is active.
				activeMap[nextCommitID] = struct{}{}
			}
			// Continue down the rabbit hole.
			commitNode, err = commitsMap.Get(ctx, nextCommitID)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", err, nextCommitID)
			}
			// Set the parent commit ID's expiration threshold as the current (this is true because this one is the
			// longest, because we wouldn't have gotten here otherwise)
			processed[nextCommitID] = branchExpirationThreshold
		}
	}
	if startingPointIterator.Err() != nil {
		return nil, startingPointIterator.Err()
	}
	return makeCleanupMap(ctx, &commitsMap, activeMap)
}

func makeCleanupMap(ctx context.Context, commitsMap *CommitsMap, commitSet map[graveler.CommitID]struct{}) (map[graveler.CommitID]graveler.MetaRangeID, error) {
	res := make(map[graveler.CommitID]graveler.MetaRangeID)
	for commitID := range commitSet {
		commit, err := commitsMap.Get(ctx, commitID)
		if err != nil {
			return nil, err
		}
		res[commitID] = graveler.MetaRangeID(commit.MetaRangeID)
	}
	return res, nil
}
