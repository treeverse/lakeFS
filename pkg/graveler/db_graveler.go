package graveler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

type DBGraveler struct {
	CommittedManager         CommittedManager
	StagingManager           StagingManager
	RefManager               RefManager
	branchLocker             BranchLocker
	hooks                    HooksHandler
	garbageCollectionManager GarbageCollectionManager
	protectedBranchesManager ProtectedBranchesManager
	log                      logging.Logger
}

func NewDBGraveler(branchLocker BranchLocker, committedManager CommittedManager, stagingManager StagingManager, refManager RefManager, gcManager GarbageCollectionManager, protectedBranchesManager ProtectedBranchesManager) *DBGraveler {
	return &DBGraveler{
		CommittedManager:         committedManager,
		StagingManager:           stagingManager,
		RefManager:               refManager,
		branchLocker:             branchLocker,
		hooks:                    &HooksNoOp{},
		garbageCollectionManager: gcManager,
		protectedBranchesManager: protectedBranchesManager,
		log:                      logging.Default().WithField("service_name", "DBGraveler_DBGraveler"),
	}
}

func (g *DBGraveler) GetRepository(ctx context.Context, repositoryID RepositoryID) (*RepositoryRecord, error) {
	return g.RefManager.GetRepository(ctx, repositoryID)
}

func (g *DBGraveler) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*RepositoryRecord, error) {
	repo := Repository{
		CreationDate:     time.Now(),
		StorageNamespace: storageNamespace,
		DefaultBranchID:  branchID,
	}
	repository, err := g.RefManager.CreateRepository(ctx, repositoryID, repo)
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func (g *DBGraveler) CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*RepositoryRecord, error) {
	repo := Repository{
		StorageNamespace: storageNamespace,
		CreationDate:     time.Now(),
		DefaultBranchID:  defaultBranchID,
	}
	repository, err := g.RefManager.CreateBareRepository(ctx, repositoryID, repo)
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func (g *DBGraveler) ListRepositories(ctx context.Context) (RepositoryIterator, error) {
	return g.RefManager.ListRepositories(ctx)
}

func (g *DBGraveler) WriteRange(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*RangeInfo, error) {
	return g.CommittedManager.WriteRange(ctx, repository.StorageNamespace, it)
}

func (g *DBGraveler) WriteMetaRange(ctx context.Context, repository *RepositoryRecord, ranges []*RangeInfo) (*MetaRangeInfo, error) {
	return g.CommittedManager.WriteMetaRange(ctx, repository.StorageNamespace, ranges)
}

func (g *DBGraveler) WriteMetaRangeByIterator(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*MetaRangeID, error) {
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace, it, nil)
}

func (g *DBGraveler) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return g.RefManager.DeleteRepository(ctx, repositoryID)
}

func (g *DBGraveler) GetCommit(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (*Commit, error) {
	return g.RefManager.GetCommit(ctx, repository, commitID)
}

func (g *DBGraveler) CreateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
	storageNamespace := repository.StorageNamespace

	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, fmt.Errorf("source reference '%s': %w", ref, err)
	}

	_, err = g.RefManager.GetBranch(ctx, repository, branchID)
	if err == nil {
		return nil, ErrBranchExists
	}
	if !errors.Is(err, ErrBranchNotFound) {
		return nil, err
	}

	if reference.ResolvedBranchModifier == ResolvedBranchModifierStaging {
		return nil, fmt.Errorf("source reference '%s': %w", ref, ErrCreateBranchNoCommit)
	}
	newBranch := Branch{
		CommitID:     reference.CommitID,
		StagingToken: GenerateStagingToken(repository.RepositoryID, branchID),
	}

	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreCreateBranchHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreCreateBranch,
		SourceRef:        ref,
		RepositoryID:     repository.RepositoryID,
		BranchID:         branchID,
		CommitID:         reference.CommitID,
	})
	if err != nil {
		return nil, &HookAbortError{
			EventType: EventTypePreCreateBranch,
			RunID:     preRunID,
			Err:       err,
		}
	}

	err = g.RefManager.CreateBranch(ctx, repository, branchID, newBranch)
	if err != nil {
		return nil, fmt.Errorf("set branch '%s' to '%v': %w", branchID, newBranch, err)
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostCreateBranchHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostCreateBranch,
		SourceRef:        ref,
		RepositoryID:     repository.RepositoryID,
		BranchID:         branchID,
		CommitID:         reference.CommitID,
		PreRunID:         preRunID,
	})

	return &newBranch, nil
}

func (g *DBGraveler) UpdateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repository, branchID, func() (interface{}, error) {
		return g.updateBranchNoLock(ctx, repository, branchID, ref)
	})
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
}

func (g *DBGraveler) updateBranchNoLock(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	if reference.ResolvedBranchModifier == ResolvedBranchModifierStaging {
		return nil, fmt.Errorf("reference '%s': %w", ref, ErrDereferenceCommitWithStaging)
	}

	curBranch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	// validate no conflict
	// TODO(Guys) return error only on conflicts, currently returns error for any changes on staging
	iter, err := g.StagingManager.List(ctx, curBranch.StagingToken, ListingDefaultBatchSize)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	if iter.Next() {
		return nil, ErrConflictFound
	}

	newBranch := Branch{
		CommitID:     reference.CommitID,
		StagingToken: curBranch.StagingToken,
	}
	err = g.RefManager.SetBranch(ctx, repository, branchID, newBranch)
	if err != nil {
		return nil, err
	}
	return &newBranch, nil
}

func (g *DBGraveler) GetBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*Branch, error) {
	return g.RefManager.GetBranch(ctx, repository, branchID)
}

func (g *DBGraveler) GetTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) (*CommitID, error) {
	return g.RefManager.GetTag(ctx, repository, tagID)
}

func (g *DBGraveler) CreateTag(ctx context.Context, repository *RepositoryRecord, tagID TagID, commitID CommitID) error {
	storageNamespace := repository.StorageNamespace

	// Check that Tag doesn't exist before running hook
	_, err := g.RefManager.GetTag(ctx, repository, tagID)
	if err == nil {
		return ErrTagAlreadyExists
	}
	if !errors.Is(err, ErrTagNotFound) {
		return err
	}

	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreCreateTagHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreCreateTag,
		RepositoryID:     repository.RepositoryID,
		CommitID:         commitID,
		SourceRef:        commitID.Ref(),
		TagID:            tagID,
	})
	if err != nil {
		return &HookAbortError{
			EventType: EventTypePreCreateTag,
			RunID:     preRunID,
			Err:       err,
		}
	}

	err = g.RefManager.CreateTag(ctx, repository, tagID, commitID)
	if err != nil {
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostCreateTagHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostCreateTag,
		RepositoryID:     repository.RepositoryID,
		CommitID:         commitID,
		SourceRef:        commitID.Ref(),
		TagID:            tagID,
		PreRunID:         preRunID,
	})

	return nil
}

func (g *DBGraveler) DeleteTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) error {
	storageNamespace := repository.StorageNamespace

	// Sanity check that Tag exists before running hook.
	commitID, err := g.RefManager.GetTag(ctx, repository, tagID)
	if err != nil {
		return err
	}

	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreDeleteTagHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreDeleteTag,
		RepositoryID:     repository.RepositoryID,
		SourceRef:        commitID.Ref(),
		CommitID:         *commitID,
		TagID:            tagID,
	})
	if err != nil {
		return &HookAbortError{
			EventType: EventTypePreDeleteTag,
			RunID:     preRunID,
			Err:       err,
		}
	}

	err = g.RefManager.DeleteTag(ctx, repository, tagID)
	if err != nil {
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostDeleteTagHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostDeleteTag,
		RepositoryID:     repository.RepositoryID,
		SourceRef:        commitID.Ref(),
		CommitID:         *commitID,
		TagID:            tagID,
		PreRunID:         preRunID,
	})
	return nil
}

func (g *DBGraveler) ListTags(ctx context.Context, repository *RepositoryRecord) (TagIterator, error) {
	return g.RefManager.ListTags(ctx, repository)
}

func (g *DBGraveler) Dereference(ctx context.Context, repository *RepositoryRecord, ref Ref) (*ResolvedRef, error) {
	rawRef, err := g.ParseRef(ref)
	if err != nil {
		return nil, err
	}
	return g.ResolveRawRef(ctx, repository, rawRef)
}

func (g *DBGraveler) ParseRef(ref Ref) (RawRef, error) {
	return g.RefManager.ParseRef(ref)
}

func (g *DBGraveler) ResolveRawRef(ctx context.Context, repository *RepositoryRecord, rawRef RawRef) (*ResolvedRef, error) {
	return g.RefManager.ResolveRawRef(ctx, repository, rawRef)
}

func (g *DBGraveler) Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (CommitIterator, error) {
	return g.RefManager.Log(ctx, repository, commitID)
}

func (g *DBGraveler) ListBranches(ctx context.Context, repository *RepositoryRecord) (BranchIterator, error) {
	return g.RefManager.ListBranches(ctx, repository)
}

func (g *DBGraveler) DeleteBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
	var (
		preRunID         string
		storageNamespace StorageNamespace
		commitID         CommitID
	)
	_, err := g.branchLocker.MetadataUpdater(ctx, repository, branchID, func() (interface{}, error) {
		if repository.DefaultBranchID == branchID {
			return nil, ErrDeleteDefaultBranch
		}
		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}
		commitID = branch.CommitID
		err = g.StagingManager.Drop(ctx, branch.StagingToken)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}

		storageNamespace = repository.StorageNamespace
		preRunID = g.hooks.NewRunID()
		preHookRecord := HookRecord{
			RunID:            preRunID,
			StorageNamespace: storageNamespace,
			EventType:        EventTypePreDeleteBranch,
			RepositoryID:     repository.RepositoryID,
			SourceRef:        commitID.Ref(),
			BranchID:         branchID,
		}
		err = g.hooks.PreDeleteBranchHook(ctx, preHookRecord)
		if err != nil {
			return nil, &HookAbortError{
				EventType: EventTypePreDeleteBranch,
				RunID:     preRunID,
				Err:       err,
			}
		}

		return nil, g.RefManager.DeleteBranch(ctx, repository, branchID)
	})

	if err != nil { // Don't perform post action hook if operation finished with error
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostDeleteBranchHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostDeleteBranch,
		RepositoryID:     repository.RepositoryID,
		SourceRef:        commitID.Ref(),
		BranchID:         branchID,
		PreRunID:         preRunID,
	})

	return nil
}

func (g *DBGraveler) GetStagingToken(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*StagingToken, error) {
	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	return &branch.StagingToken, nil
}

func (g *DBGraveler) GetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord) (*GarbageCollectionRules, error) {
	return g.garbageCollectionManager.GetRules(ctx, repository.StorageNamespace)
}

func (g *DBGraveler) SetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord, rules *GarbageCollectionRules) error {
	return g.garbageCollectionManager.SaveRules(ctx, repository.StorageNamespace, rules)
}

func (g *DBGraveler) SaveGarbageCollectionCommits(ctx context.Context, repository *RepositoryRecord, previousRunID string) (*GarbageCollectionRunMetadata, error) {
	rules, err := g.GetGarbageCollectionRules(ctx, repository)
	if err != nil {
		return nil, fmt.Errorf("get gc rules: %w", err)
	}
	previouslyExpiredCommits, err := g.garbageCollectionManager.GetRunExpiredCommits(ctx, repository.StorageNamespace, previousRunID)
	if err != nil {
		return nil, fmt.Errorf("get expired commits from previous run: %w", err)
	}

	runID, err := g.garbageCollectionManager.SaveGarbageCollectionCommits(ctx, repository, rules, previouslyExpiredCommits)
	if err != nil {
		return nil, fmt.Errorf("save garbage collection commits: %w", err)
	}
	commitsLocation, err := g.garbageCollectionManager.GetCommitsCSVLocation(runID, repository.StorageNamespace)
	if err != nil {
		return nil, err
	}
	addressLocation, err := g.garbageCollectionManager.GetAddressesLocation(repository.StorageNamespace)
	if err != nil {
		return nil, err
	}

	return &GarbageCollectionRunMetadata{
		RunId:              runID,
		CommitsCsvLocation: commitsLocation,
		AddressLocation:    addressLocation,
	}, err
}

func (g *DBGraveler) GetBranchProtectionRules(ctx context.Context, repository *RepositoryRecord) (*BranchProtectionRules, error) {
	return g.protectedBranchesManager.GetRules(ctx, repository)
}

func (g *DBGraveler) DeleteBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string) error {
	return g.protectedBranchesManager.Delete(ctx, repository, pattern)
}

func (g *DBGraveler) CreateBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string, blockedActions []BranchProtectionBlockedAction) error {
	return g.protectedBranchesManager.Add(ctx, repository, pattern, blockedActions)
}

func (g *DBGraveler) Get(ctx context.Context, repository *RepositoryRecord, ref Ref, key Key) (*Value, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	if reference.StagingToken != "" {
		// try to get from staging, if not found proceed to committed
		value, err := g.StagingManager.Get(ctx, reference.StagingToken, key)
		if !errors.Is(err, ErrNotFound) {
			if err != nil {
				return nil, err
			}
			if value == nil {
				// tombstone
				return nil, ErrNotFound
			}
			return value, nil
		}
	}
	return g.GetByCommitID(ctx, repository, reference.CommitID, key)
}

func (g *DBGraveler) GetByCommitID(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (*Value, error) {
	commit, err := g.RefManager.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
}

func (g *DBGraveler) Set(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key, value Value, writeConditions ...WriteConditionOption) error {
	_, err := g.branchLocker.Writer(ctx, repository, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrWriteToProtectedBranch
		}
		branch, err := g.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}
		writeCondition := &WriteCondition{}
		for _, cond := range writeConditions {
			cond(writeCondition)
		}

		if writeCondition.IfAbsent {
			// Ensure the given key doesn't exist in the underlying commit first
			// Since we're being protected by the branch locker, we're guaranteed the commit
			// won't change before we finish the operation
			_, err := g.Get(ctx, repository, Ref(branch.CommitID), key)
			if err == nil {
				// we got a key here already!
				return nil, ErrPreconditionFailed
			}
			if !errors.Is(err, ErrNotFound) {
				// another error occurred!
				return nil, err
			}
		}
		err = g.StagingManager.Set(ctx, branch.StagingToken, key, &value, !writeCondition.IfAbsent)
		return nil, err
	})
	return err
}

// isStagedTombstone returns true if key is staged as tombstone on manager at token.  It treats staging manager
// errors by returning "not a tombstone", and is unsafe to use if that matters!
func (g *DBGraveler) isStagedTombstone(ctx context.Context, token StagingToken, key Key) bool {
	e, err := g.StagingManager.Get(ctx, token, key)
	if err != nil {
		return false
	}
	return e == nil
}

func (g *DBGraveler) Delete(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	_, err := g.branchLocker.Writer(ctx, repository, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrWriteToProtectedBranch
		}
		branch, err := g.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}

		// mark err as not found and lookup the branch's commit
		err = ErrNotFound
		if branch.CommitID != "" {
			var commit *Commit
			commit, err = g.RefManager.GetCommit(ctx, repository, branch.CommitID)
			if err != nil {
				return nil, err
			}
			// check key in committed - do we need tombstone?
			_, err = g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
		}

		if errors.Is(err, ErrNotFound) {
			// no need for tombstone - drop key from stage
			return nil, g.StagingManager.DropKey(ctx, branch.StagingToken, key)
		}
		if err != nil {
			return nil, err
		}

		// key is in committed, stage its tombstone -- regardless of whether it
		// is also in staging.  But... if it already has a tombstone staged, return
		// ErrNotFound.

		// Safe to ignore errors when checking staging (if all delete actions worked):
		// we only give a possible incorrect error message if a tombstone was already
		// staged.
		if g.isStagedTombstone(ctx, branch.StagingToken, key) {
			return nil, ErrNotFound
		}

		return nil, g.StagingManager.Set(ctx, branch.StagingToken, key, nil, true)
	})
	return err
}

func (g *DBGraveler) List(ctx context.Context, repository *RepositoryRecord, ref Ref) (ValueIterator, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	var metaRangeID MetaRangeID
	if reference.CommitID != "" {
		commit, err := g.RefManager.GetCommit(ctx, repository, reference.CommitID)
		if err != nil {
			return nil, err
		}
		metaRangeID = commit.MetaRangeID
	}

	listing, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
	if err != nil {
		return nil, err
	}
	if reference.StagingToken != "" {
		stagingList, err := g.StagingManager.List(ctx, reference.StagingToken, ListingDefaultBatchSize)
		if err != nil {
			return nil, err
		}
		listing = NewFilterTombstoneIterator(NewCombinedIterator(stagingList, listing))
	}
	return listing, nil
}

func (g *DBGraveler) Commit(ctx context.Context, repository *RepositoryRecord, branchID BranchID, params CommitParams) (CommitID, error) {
	var preRunID string
	var commit Commit
	var storageNamespace StorageNamespace
	res, err := g.branchLocker.MetadataUpdater(ctx, repository, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_COMMIT)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrCommitToProtectedBranch
		}
		storageNamespace = repository.StorageNamespace

		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return "", fmt.Errorf("get branch: %w", err)
		}

		// fill commit information - use for pre-commit and after adding the commit information used by commit
		commit = NewCommit()

		if params.Date != nil {
			commit.CreationDate = time.Unix(*params.Date, 0)
		}
		commit.Committer = params.Committer
		commit.Message = params.Message
		commit.Metadata = params.Metadata
		if branch.CommitID != "" {
			commit.Parents = CommitParents{branch.CommitID}
		}

		if params.SourceMetaRange != nil {
			empty, err := g.stagingEmpty(ctx, branch)
			if err != nil {
				return nil, fmt.Errorf("checking empty branch: %w", err)
			}
			if !empty {
				return nil, ErrCommitMetaRangeDirtyBranch
			}
		}

		preRunID = g.hooks.NewRunID()
		err = g.hooks.PreCommitHook(ctx, HookRecord{
			RunID:            preRunID,
			EventType:        EventTypePreCommit,
			SourceRef:        branchID.Ref(),
			RepositoryID:     repository.RepositoryID,
			StorageNamespace: storageNamespace,
			BranchID:         branchID,
			Commit:           commit,
		})
		if err != nil {
			return "", &HookAbortError{
				EventType: EventTypePreCommit,
				RunID:     preRunID,
				Err:       err,
			}
		}

		var branchMetaRangeID MetaRangeID
		var parentGeneration int
		if branch.CommitID != "" {
			branchCommit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
			if err != nil {
				return "", fmt.Errorf("get commit: %w", err)
			}
			branchMetaRangeID = branchCommit.MetaRangeID
			parentGeneration = branchCommit.Generation
		}
		commit.Generation = parentGeneration + 1
		if params.SourceMetaRange != nil {
			commit.MetaRangeID = *params.SourceMetaRange
		} else {
			changes, err := g.StagingManager.List(ctx, branch.StagingToken, ListingMaxBatchSize)
			if err != nil {
				return "", fmt.Errorf("staging list: %w", err)
			}
			defer changes.Close()

			commit.MetaRangeID, _, err = g.CommittedManager.Commit(ctx, storageNamespace, branchMetaRangeID, changes)
			if err != nil {
				return "", fmt.Errorf("commit: %w", err)
			}
		}

		// add commit
		newCommit, err := g.RefManager.AddCommit(ctx, repository, commit)
		if err != nil {
			return "", fmt.Errorf("add commit: %w", err)
		}
		err = g.RefManager.SetBranch(ctx, repository, branchID, Branch{
			CommitID:     newCommit,
			StagingToken: GenerateStagingToken(repository.RepositoryID, branchID),
		})
		if err != nil {
			return "", fmt.Errorf("set branch commit %s: %w", newCommit, err)
		}
		err = g.StagingManager.Drop(ctx, branch.StagingToken)
		if err != nil {
			g.log.WithContext(ctx).WithFields(logging.Fields{
				"repository_id": repository.RepositoryID,
				"branch_id":     branchID,
				"commit_id":     branch.CommitID,
				"message":       params.Message,
				"staging_token": branch.StagingToken,
			}).Error("Failed to drop staging data")
		}
		return newCommit, nil
	})
	if err != nil {
		return "", err
	}
	newCommitID := res.(CommitID)
	postRunID := g.hooks.NewRunID()
	err = g.hooks.PostCommitHook(ctx, HookRecord{
		EventType:        EventTypePostCommit,
		RunID:            postRunID,
		RepositoryID:     repository.RepositoryID,
		StorageNamespace: storageNamespace,
		SourceRef:        res.(CommitID).Ref(),
		BranchID:         branchID,
		Commit:           commit,
		CommitID:         newCommitID,
		PreRunID:         preRunID,
	})
	if err != nil {
		g.log.WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-commit hook failed")
	}
	return newCommitID, nil
}

func (g *DBGraveler) AddCommitToBranchHead(ctx context.Context, repository *RepositoryRecord, branchID BranchID, commit Commit) (CommitID, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repository, branchID, func() (interface{}, error) {
		// parentCommitID should always match the HEAD of the branch.
		// Empty parentCommitID matches first commit of the branch.
		parentCommitID, err := validateCommitParent(ctx, repository, commit, g.RefManager)
		if err != nil {
			return nil, err
		}

		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}
		if branch.CommitID != parentCommitID {
			return nil, ErrCommitNotHeadBranch
		}

		// check if commit already exists.
		commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
		if exists, err := CommitExists(ctx, repository, commitID, g.RefManager); err != nil {
			return nil, err
		} else if exists {
			return commitID, nil
		}

		commitID, err = g.addCommitNoLock(ctx, repository, commit)
		if err != nil {
			return nil, fmt.Errorf("adding commit: %w", err)
		}
		_, err = g.updateBranchNoLock(ctx, repository, branchID, Ref(commitID))
		if err != nil {
			return nil, err
		}
		return commitID, nil
	})
	if err != nil {
		return "", err
	}
	return res.(CommitID), nil
}

func (g *DBGraveler) AddCommit(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error) {
	// at least a single parent must exists
	if len(commit.Parents) == 0 {
		return "", ErrAddCommitNoParent
	}
	_, err := validateCommitParent(ctx, repository, commit, g.RefManager)
	if err != nil {
		return "", err
	}

	// check if commit already exists.
	commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
	if exists, err := CommitExists(ctx, repository, commitID, g.RefManager); err != nil {
		return "", err
	} else if exists {
		return commitID, nil
	}

	commitID, err = g.addCommitNoLock(ctx, repository, commit)
	if err != nil {
		return "", fmt.Errorf("adding commit: %w", err)
	}

	return commitID, nil
}

// addCommitNoLock lower API used to add commit into a repository. It will verify that the commit meta-range is accessible but will not lock any metadata update.
func (g *DBGraveler) addCommitNoLock(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error) {
	// verify access to meta range
	ok, err := g.CommittedManager.Exists(ctx, repository.StorageNamespace, commit.MetaRangeID)
	if err != nil {
		return "", fmt.Errorf("checking for meta range %s: %w", commit.MetaRangeID, err)
	}
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrMetaRangeNotFound, commit.MetaRangeID)
	}

	// add commit
	commitID, err := g.RefManager.AddCommit(ctx, repository, commit)
	if err != nil {
		return "", fmt.Errorf("add commit: %w", err)
	}
	return commitID, nil
}

func (g *DBGraveler) stagingEmpty(ctx context.Context, branch *Branch) (bool, error) {
	stIt, err := g.StagingManager.List(ctx, branch.StagingToken, ListingDefaultBatchSize)
	if err != nil {
		return false, fmt.Errorf("staging list (token %s): %w", branch.StagingToken, err)
	}

	defer stIt.Close()

	if stIt.Next() {
		return false, nil
	}

	return true, nil
}

func (g *DBGraveler) Reset(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
	_, err := g.branchLocker.Writer(ctx, repository, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrWriteToProtectedBranch
		}
		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}
		return nil, g.StagingManager.Drop(ctx, branch.StagingToken)
	})
	return err
}

func (g *DBGraveler) ResetKey(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	_, err := g.branchLocker.Writer(ctx, repository, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrWriteToProtectedBranch
		}
		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}
		return nil, g.StagingManager.DropKey(ctx, branch.StagingToken, key)
	})
	return err
}

func (g *DBGraveler) ResetPrefix(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	_, err := g.branchLocker.Writer(ctx, repository, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrWriteToProtectedBranch
		}
		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return nil, err
		}
		return nil, g.StagingManager.DropByPrefix(ctx, branch.StagingToken, key)
	})
	return err
}

// Revert creates a reverse patch to the commit given as 'ref', and applies it as a new commit on the given branch.
// This is implemented by merging the parent of 'ref' into the branch, with 'ref' as the merge base.
// Example: consider the following tree: C1 -> C2 -> C3, with the branch pointing at C3.
// To revert C2, we merge C1 into the branch, with C2 as the merge base.
// That is, try to apply the diff from C2 to C1 on the tip of the branch.
// If the commit is a merge commit, 'parentNumber' is the parent number (1-based) relative to which the revert is done.
func (g *DBGraveler) Revert(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, error) {
	commitRecord, err := g.dereferenceCommit(ctx, repository, ref)
	if err != nil {
		return "", fmt.Errorf("get commit from ref %s: %w", ref, err)
	}
	if len(commitRecord.Parents) > 1 && parentNumber <= 0 {
		// if commit has more than one parent, must explicitly specify parent number
		return "", ErrRevertMergeNoParent
	}
	if parentNumber > 0 {
		// validate parent is in range:
		if parentNumber > len(commitRecord.Parents) { // parent number is 1-based
			return "", fmt.Errorf("%w: parent %d", ErrRevertParentOutOfRange, parentNumber)
		}
		parentNumber--
	}
	res, err := g.branchLocker.MetadataUpdater(ctx, repository, branchID, func() (interface{}, error) {
		branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
		if err != nil {
			return "", fmt.Errorf("get branch %s: %w", branchID, err)
		}
		if empty, err := g.stagingEmpty(ctx, branch); err != nil {
			return "", err
		} else if !empty {
			return "", fmt.Errorf("%s: %w", branchID, ErrDirtyBranch)
		}
		var parentMetaRangeID MetaRangeID
		if len(commitRecord.Parents) > 0 {
			parentCommit, err := g.dereferenceCommit(ctx, repository, commitRecord.Parents[parentNumber].Ref())
			if err != nil {
				return "", fmt.Errorf("get commit from ref %s: %w", commitRecord.Parents[parentNumber], err)
			}
			parentMetaRangeID = parentCommit.MetaRangeID
		}
		branchCommit, err := g.dereferenceCommit(ctx, repository, branch.CommitID.Ref())
		if err != nil {
			return "", fmt.Errorf("get commit from ref %s: %w", branch.CommitID, err)
		}
		// merge from the parent to the top of the branch, with the given ref as the merge base:
		metaRangeID, err := g.CommittedManager.Merge(ctx, repository.StorageNamespace, branchCommit.MetaRangeID, parentMetaRangeID, commitRecord.MetaRangeID, MergeStrategyNone)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge: %w", err)
			}
			return "", err
		}
		commit := NewCommit()
		commit.Committer = commitParams.Committer
		commit.Message = commitParams.Message
		commit.MetaRangeID = metaRangeID
		commit.Parents = []CommitID{branch.CommitID}
		commit.Metadata = commitParams.Metadata
		commit.Generation = branchCommit.Generation + 1
		commitID, err := g.RefManager.AddCommit(ctx, repository, commit)
		if err != nil {
			return "", fmt.Errorf("add commit: %w", err)
		}
		err = g.RefManager.SetBranch(ctx, repository, branchID, Branch{
			CommitID:     commitID,
			StagingToken: branch.StagingToken,
		})
		if err != nil {
			return "", fmt.Errorf("set branch: %w", err)
		}
		return commitID, nil
	})
	if err != nil {
		return "", err
	}
	return res.(CommitID), nil
}

func (g *DBGraveler) Merge(ctx context.Context, repository *RepositoryRecord, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error) {
	var preRunID string
	var storageNamespace StorageNamespace
	var commit Commit
	res, err := g.branchLocker.MetadataUpdater(ctx, repository, destination, func() (interface{}, error) {
		storageNamespace = repository.StorageNamespace

		branch, err := g.GetBranch(ctx, repository, destination)
		if err != nil {
			return nil, fmt.Errorf("get branch: %w", err)
		}
		empty, err := g.stagingEmpty(ctx, branch)
		if err != nil {
			return nil, fmt.Errorf("check if staging empty: %w", err)
		}
		if !empty {
			return nil, fmt.Errorf("%s: %w", destination, ErrDirtyBranch)
		}
		fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repository, source, Ref(destination))
		if err != nil {
			return nil, err
		}
		g.log.WithFields(logging.Fields{
			"repository":             source,
			"source":                 source,
			"destination":            destination,
			"source_meta_range":      fromCommit.MetaRangeID,
			"destination_meta_range": toCommit.MetaRangeID,
			"base_meta_range":        baseCommit.MetaRangeID,
		}).Trace("Merge")
		mergeStrategy := MergeStrategyNone
		if strategy == MergeStrategyDestWins {
			mergeStrategy = MergeStrategyDest
		}
		if strategy == MergeStrategySrcWins {
			mergeStrategy = MergeStrategySource
		}
		metaRangeID, err := g.CommittedManager.Merge(ctx, storageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID, mergeStrategy)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge in CommitManager: %w", err)
			}
			return nil, err
		}
		commit = NewCommit()
		commit.Committer = commitParams.Committer
		commit.Message = commitParams.Message
		commit.MetaRangeID = metaRangeID
		commit.Parents = []CommitID{toCommit.CommitID, fromCommit.CommitID}
		if toCommit.Generation > fromCommit.Generation {
			commit.Generation = toCommit.Generation + 1
		} else {
			commit.Generation = fromCommit.Generation + 1
		}
		commit.Metadata = commitParams.Metadata
		preRunID = g.hooks.NewRunID()
		err = g.hooks.PreMergeHook(ctx, HookRecord{
			EventType:        EventTypePreMerge,
			RunID:            preRunID,
			RepositoryID:     repository.RepositoryID,
			StorageNamespace: storageNamespace,
			BranchID:         destination,
			SourceRef:        fromCommit.CommitID.Ref(),
			Commit:           commit,
		})
		if err != nil {
			return nil, &HookAbortError{
				EventType: EventTypePreMerge,
				RunID:     preRunID,
				Err:       err,
			}
		}
		commitID, err := g.RefManager.AddCommit(ctx, repository, commit)
		if err != nil {
			return nil, fmt.Errorf("add commit: %w", err)
		}
		branch.CommitID = commitID
		err = g.RefManager.SetBranch(ctx, repository, destination, *branch)
		if err != nil {
			return commitID, fmt.Errorf("update branch %s: %w", destination, err)
		}
		return commitID, nil
	})
	if err != nil {
		return "", err
	}
	postRunID := g.hooks.NewRunID()
	err = g.hooks.PostMergeHook(ctx, HookRecord{
		EventType:        EventTypePostMerge,
		RunID:            postRunID,
		RepositoryID:     repository.RepositoryID,
		StorageNamespace: storageNamespace,
		BranchID:         destination,
		SourceRef:        res.(CommitID).Ref(),
		Commit:           commit,
		CommitID:         res.(CommitID),
		PreRunID:         preRunID,
	})
	if err != nil {
		g.log.
			WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-merge hook failed")
	}
	return res.(CommitID), nil
}

func (g *DBGraveler) DiffUncommitted(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (DiffIterator, error) {
	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	var metaRangeID MetaRangeID
	if branch.CommitID != "" {
		commit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
		if err != nil {
			return nil, err
		}
		metaRangeID = commit.MetaRangeID
	}

	valueIterator, err := g.StagingManager.List(ctx, branch.StagingToken, ListingDefaultBatchSize)
	if err != nil {
		return nil, err
	}
	var committedValueIterator ValueIterator
	if metaRangeID != "" {
		committedValueIterator, err = g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
		if err != nil {
			return nil, err
		}
	}
	return NewUncommittedDiffIterator(ctx, committedValueIterator, valueIterator), nil
}

// dereferenceCommit will dereference and load the commit record based on 'ref'.
//   will return an error if 'ref' points to an explicit staging area
func (g *DBGraveler) dereferenceCommit(ctx context.Context, repository *RepositoryRecord, ref Ref) (*CommitRecord, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	if reference.ResolvedBranchModifier == ResolvedBranchModifierStaging {
		return nil, fmt.Errorf("reference '%s': %w", ref, ErrDereferenceCommitWithStaging)
	}
	commit, err := g.RefManager.GetCommit(ctx, repository, reference.CommitID)
	if err != nil {
		return nil, err
	}
	return &CommitRecord{
		CommitID: reference.CommitID,
		Commit:   commit,
	}, nil
}

func (g *DBGraveler) Diff(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error) {
	leftCommit, err := g.dereferenceCommit(ctx, repository, left)
	if err != nil {
		return nil, err
	}
	rightRawRef, err := g.Dereference(ctx, repository, right)
	if err != nil {
		return nil, err
	}
	rightCommit, err := g.RefManager.GetCommit(ctx, repository, rightRawRef.CommitID)
	if err != nil {
		return nil, err
	}
	diff, err := g.CommittedManager.Diff(ctx, repository.StorageNamespace, leftCommit.MetaRangeID, rightCommit.MetaRangeID)
	if err != nil {
		return nil, err
	}
	if rightRawRef.ResolvedBranchModifier != ResolvedBranchModifierStaging {
		return diff, nil
	}
	leftValueIterator, err := g.CommittedManager.List(ctx, repository.StorageNamespace, leftCommit.MetaRangeID)
	if err != nil {
		return nil, err
	}
	rightBranch, err := g.RefManager.GetBranch(ctx, repository, rightRawRef.BranchID)
	if err != nil {
		return nil, err
	}
	stagingIterator, err := g.StagingManager.List(ctx, rightBranch.StagingToken, ListingDefaultBatchSize)
	if err != nil {
		return nil, err
	}
	return NewCombinedDiffIterator(diff, leftValueIterator, stagingIterator), nil
}

func (g *DBGraveler) Compare(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error) {
	fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repository, right, left)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Compare(ctx, repository.StorageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID)
}

func (g *DBGraveler) SetHooksHandler(handler HooksHandler) {
	if handler == nil {
		g.hooks = &HooksNoOp{}
	} else {
		g.hooks = handler
	}
}

func (g *DBGraveler) getCommitsForMerge(ctx context.Context, repository *RepositoryRecord, from Ref, to Ref) (*CommitRecord, *CommitRecord, *Commit, error) {
	fromCommit, err := g.dereferenceCommit(ctx, repository, from)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get commit by ref %s: %w", from, err)
	}
	toCommit, err := g.dereferenceCommit(ctx, repository, to)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get commit by branch %s: %w", to, err)
	}
	baseCommit, err := g.RefManager.FindMergeBase(ctx, repository, fromCommit.CommitID, toCommit.CommitID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("find merge base: %w", err)
	}
	if baseCommit == nil {
		return nil, nil, nil, ErrNoMergeBase
	}
	return fromCommit, toCommit, baseCommit, nil
}

func (g *DBGraveler) LoadCommits(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
	iter, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		commit := &CommitData{}
		err := proto.Unmarshal(rawValue.Data, commit)
		if err != nil {
			return err
		}
		parents := make(CommitParents, len(commit.GetParents()))
		for i, p := range commit.GetParents() {
			parents[i] = CommitID(p)
		}
		if commit.GetGeneration() == 0 {
			return fmt.Errorf("dumps created by lakeFS versions before v0.61.0 are no longer supported: %w", ErrNoCommitGeneration)
		}
		commitID, err := g.RefManager.AddCommit(ctx, repository, Commit{
			Version:      CommitVersion(commit.Version),
			Committer:    commit.GetCommitter(),
			Message:      commit.GetMessage(),
			MetaRangeID:  MetaRangeID(commit.GetMetaRangeId()),
			CreationDate: commit.GetCreationDate().AsTime(),
			Parents:      parents,
			Metadata:     commit.GetMetadata(),
			Generation:   int(commit.GetGeneration()),
		})
		if err != nil {
			return err
		}
		// integrity check that we get for free!
		if commitID != CommitID(commit.Id) {
			return fmt.Errorf("commit ID does not match for %s: %w", commitID, ErrInvalidCommitID)
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil
}

func (g *DBGraveler) LoadBranches(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
	iter, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		branch := &BranchData{}
		err := proto.Unmarshal(rawValue.Data, branch)
		if err != nil {
			return err
		}
		branchID := BranchID(branch.Id)
		err = g.RefManager.SetBranch(ctx, repository, branchID, Branch{
			CommitID:     CommitID(branch.CommitId),
			StagingToken: GenerateStagingToken(repository.RepositoryID, branchID),
		})
		if err != nil {
			return err
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil
}

func (g *DBGraveler) LoadTags(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
	iter, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		tag := &TagData{}
		err := proto.Unmarshal(rawValue.Data, tag)
		if err != nil {
			return err
		}
		tagID := TagID(tag.Id)
		err = g.RefManager.CreateTag(ctx, repository, tagID, CommitID(tag.CommitId))
		if err != nil {
			return err
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil
}

func (g *DBGraveler) GetMetaRange(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) (MetaRangeAddress, error) {
	return g.CommittedManager.GetMetaRange(ctx, repository.StorageNamespace, metaRangeID)
}

func (g *DBGraveler) GetRange(ctx context.Context, repository *RepositoryRecord, rangeID RangeID) (RangeAddress, error) {
	return g.CommittedManager.GetRange(ctx, repository.StorageNamespace, rangeID)
}

func (g *DBGraveler) DumpCommits(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
	iter, err := g.RefManager.ListCommits(ctx, repository)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&CommitData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace,
		commitsToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeCommit,
			EntitySchemaKey:           EntitySchemaCommit,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func (g *DBGraveler) DumpBranches(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
	iter, err := g.RefManager.ListBranches(ctx, repository)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&BranchData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace,
		branchesToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeBranch,
			EntitySchemaKey:           EntitySchemaBranch,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func (g *DBGraveler) DumpTags(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
	iter, err := g.RefManager.ListTags(ctx, repository)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&TagData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace,
		tagsToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeTag,
			EntitySchemaKey:           EntitySchemaTag,
			EntitySchemaDefinitionKey: schema,
		},
	)
}
