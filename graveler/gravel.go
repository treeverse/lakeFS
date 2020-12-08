package graveler

import (
	"context"
	"errors"
	"fmt"
	"time"

	uuid2 "github.com/google/uuid"
)

type Gravel struct {
	CommittedManager CommittedManager
	StagingManager   StagingManager
	RefManager       RefManager
}

func (r *Gravel) GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error) {
	return r.RefManager.GetRepository(ctx, repositoryID)
}

const (
	initialCommitID = "" //Todo(Guys) decide what should we do with initial commit ID
)

func (r *Gravel) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*Repository, error) {
	repo := Repository{
		StorageNamespace: storageNamespace,
		CreationDate:     time.Now(),
		DefaultBranchID:  branchID,
	}
	branch := Branch{
		CommitID:     initialCommitID,
		stagingToken: generateStagingToken(repositoryID, branchID),
	}
	err := r.RefManager.CreateRepository(ctx, repositoryID, repo, branch)
	if err != nil {
		return nil, err
	}
	return &repo, nil
}

func (r *Gravel) ListRepositories(ctx context.Context, from RepositoryID) (RepositoryIterator, error) {
	return r.RefManager.ListRepositories(ctx, from)
}

func (r *Gravel) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return r.RefManager.DeleteRepository(ctx, repositoryID)
}

func (r *Gravel) GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error) {
	return r.RefManager.GetCommit(ctx, repositoryID, commitID)
}
func generateStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	// Todo(Guys): initial implementation, change this
	uuid := uuid2.New().String()
	return StagingToken(fmt.Sprintf("%s-%s:%s", repositoryID, branchID, uuid))
}

func (r *Gravel) CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	// check if branch exists
	_, err := r.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, ErrBranchExists
	}

	reference, err := r.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}

	newBranch := Branch{
		CommitID:     reference.CommitID(),
		stagingToken: generateStagingToken(repositoryID, branchID),
	}
	err = r.RefManager.SetBranch(ctx, repositoryID, branchID, newBranch)
	if err != nil {
		return nil, err
	}
	return &newBranch, nil
}

func (r *Gravel) UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	reference, err := r.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}

	curBranch, err := r.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	// validate no conflict
	// Todo(Guys) return error only on conflicts, currently returns error for any changes on staging
	list, err := r.StagingManager.List(ctx, curBranch.stagingToken)
	if err != nil {
		return nil, err
	}
	if list.Next() {
		return nil, ErrConflictFound
	}

	newBranch := Branch{
		CommitID:     reference.CommitID(),
		stagingToken: curBranch.stagingToken,
	}
	err = r.RefManager.SetBranch(ctx, repositoryID, branchID, newBranch)
	if err != nil {
		return nil, err
	}
	return &newBranch, nil
}

func (r *Gravel) GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error) {
	return r.RefManager.GetBranch(ctx, repositoryID, branchID)
}

func (r *Gravel) Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (CommitID, error) {
	reference, err := r.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return "", err
	}
	return reference.CommitID(), nil
}

func (r *Gravel) Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error) {
	return r.RefManager.Log(ctx, repositoryID, commitID)
}

func (r *Gravel) ListBranches(ctx context.Context, repositoryID RepositoryID, from BranchID) (BranchIterator, error) {
	return r.RefManager.ListBranches(ctx, repositoryID, from)
}

func (r *Gravel) DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	branch, err := r.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	err = r.StagingManager.Drop(ctx, branch.stagingToken)
	if err != nil {
		return err
	}
	return r.RefManager.DeleteBranch(ctx, repositoryID, branchID)
}

func (r *Gravel) Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error) {
	repo, err := r.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	reference, err := r.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	if reference.Type() == ReferenceTypeBranch {
		// try to get from staging, if not found proceed to committed
		branch := reference.Branch()
		value, err := r.StagingManager.Get(ctx, branch.stagingToken, key)
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
	commitID := reference.CommitID()
	commit, err := r.RefManager.GetCommit(ctx, repositoryID, commitID)
	if err != nil {
		return nil, err
	}
	return r.CommittedManager.Get(ctx, repo.StorageNamespace, commit.TreeID, key)
}

func (r *Gravel) Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value) error {
	branch, err := r.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return r.StagingManager.Set(ctx, branch.stagingToken, key, value)
}

func (r *Gravel) Delete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	branch, err := r.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return r.StagingManager.Delete(ctx, branch.stagingToken, key)
}

func (r *Gravel) List(ctx context.Context, repositoryID RepositoryID, ref Ref, prefix, from, delimiter Key) (ListingIterator, error) {
	repo, err := r.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	reference, err := r.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	commit, err := r.RefManager.GetCommit(ctx, repositoryID, reference.CommitID())
	if err != nil {
		return nil, err
	}

	var listing ListingIterator
	committedValues, err := r.CommittedManager.List(ctx, repo.StorageNamespace, commit.TreeID, from)
	if err != nil {
		return nil, err
	}
	committedListing := NewListingIter(NewPrefixIterator(committedValues, prefix), delimiter, prefix)
	if reference.Type() == ReferenceTypeBranch {
		stagingList, err := r.StagingManager.List(ctx, reference.Branch().stagingToken)
		if err != nil {
			return nil, err
		}
		listing = NewCombinedIterator(NewListingIter(NewPrefixIterator(stagingList, prefix), delimiter, prefix), committedListing)
	} else {
		listing = committedListing
	}
	return listing, nil
}

func (r *Gravel) Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, committer string, message string, metadata Metadata) (CommitID, error) {
	panic("implement me")
}

func (r *Gravel) Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	panic("implement me") // waiting for staging reset
}

func (r *Gravel) Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (CommitID, error) {
	panic("implement me")
}

func (r *Gravel) Merge(ctx context.Context, repositoryID RepositoryID, from Ref, to BranchID) (CommitID, error) {
	repo, err := r.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	fromCommit, err := r.getCommitRecordFromRef(ctx, repositoryID, from)
	if err != nil {
		return "", err
	}
	toCommit, err := r.getCommitRecordFromBranchID(ctx, repositoryID, to)
	if err != nil {
		return "", err
	}
	baseCommit, err := r.RefManager.FindMergeBase(ctx, repositoryID, fromCommit.CommitID, toCommit.CommitID)
	if err != nil {
		return "", err
	}

	treeID, err := r.CommittedManager.Merge(ctx, repo.StorageNamespace, fromCommit.TreeID, toCommit.TreeID, baseCommit.TreeID)
	if err != nil {
		return "", err
	}
	commit := Commit{
		Committer:    "unknown",       // Todo(Guys): pass committer or enter default value
		Message:      "merge message", // Todo(Guys): get merge message
		TreeID:       treeID,
		CreationDate: time.Time{},
		Parents:      []CommitID{fromCommit.CommitID, toCommit.CommitID},
		Metadata:     nil,
	}
	return r.RefManager.AddCommit(ctx, repositoryID, commit)
}

func (r *Gravel) DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID, from Key) (DiffIterator, error) {
	repo, err := r.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	branch, err := r.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	commit, err := r.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
	if err != nil {
		return nil, err
	}
	valueIterator, err := r.StagingManager.List(ctx, branch.stagingToken)

	return newUncommittedDiffIterator(r.CommittedManager, valueIterator, repo.StorageNamespace, commit.TreeID), nil
}

func (r *Gravel) getCommitRecordFromRef(ctx context.Context, repositoryID RepositoryID, ref Ref) (*CommitRecord, error) {
	reference, err := r.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	commit, err := r.RefManager.GetCommit(ctx, repositoryID, reference.CommitID())
	if err != nil {
		return nil, err
	}
	return &CommitRecord{
		CommitID: reference.CommitID(),
		Commit:   commit,
	}, nil
}

func (r *Gravel) getCommitRecordFromBranchID(ctx context.Context, repositoryID RepositoryID, branch BranchID) (*CommitRecord, error) {
	return r.getCommitRecordFromRef(ctx, repositoryID, Ref(branch))
}

func (r *Gravel) Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref, from Key) (DiffIterator, error) {
	repo, err := r.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	leftCommit, err := r.getCommitRecordFromRef(ctx, repositoryID, left)
	if err != nil {
		return nil, err
	}
	rightCommit, err := r.getCommitRecordFromRef(ctx, repositoryID, right)
	if err != nil {
		return nil, err
	}
	baseCommit, err := r.RefManager.FindMergeBase(ctx, repositoryID, leftCommit.CommitID, rightCommit.CommitID)
	if err != nil {
		return nil, err
	}

	return r.CommittedManager.Diff(ctx, repo.StorageNamespace, leftCommit.TreeID, rightCommit.TreeID, baseCommit.TreeID, from)
}
