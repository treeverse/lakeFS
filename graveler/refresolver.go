package graveler

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/ident"
)

type RefStore interface {
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error)
	GetTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) (*CommitID, error)
	GetCommitByPrefix(ctx context.Context, repositoryID RepositoryID, prefix CommitID) (*Commit, error)
	Log(ctx context.Context, repositoryID RepositoryID, from CommitID) (CommitIterator, error)
}

type reference struct {
	typ      ReferenceType
	branch   *Branch
	commitID *CommitID
}

func (r reference) Type() ReferenceType {
	return r.typ
}

func (r reference) Branch() Branch {
	return *r.branch
}

func (r reference) CommitID() CommitID {
	return *r.commitID
}

type resolveHelperFunc func(context.Context, RefStore, RepositoryID, string) (Reference, error)

var resolveHelpers = []resolveHelperFunc{
	resolveRevAHash,
	resolveRevBranch,
	resolveRevTag,
}

func ResolveRef(ctx context.Context, store RefStore, repositoryID RepositoryID, ref Ref) (Reference, error) {
	// first we need to parse-rev to get a list references
	// valid revs: branch, tag, commit ID, commit ID prefix (as long as unambiguous)
	// valid modifiers: ~N
	parsed, err := RevParse(ref)
	if err != nil {
		return nil, err
	}

	var baseCommit CommitID
	for _, resolveHelper := range resolveHelpers {
		r, err := resolveHelper(ctx, store, repositoryID, parsed.BaseRev)
		if err != nil {
			return nil, err
		}
		if r != nil && len(parsed.Modifiers) == 0 {
			return r, nil
		}
		if r != nil {
			baseCommit = r.CommitID()
			break
		}
	}
	if baseCommit == "" {
		return nil, ErrNotFound
	}

	for _, mod := range parsed.Modifiers {
		// lastly, apply modifier
		switch mod.Type {
		case RevModTypeTilde:
			// skip mod.ValueNumeric iterations
			iter, err := store.Log(ctx, repositoryID, baseCommit)
			if err != nil {
				return nil, err
			}
			i := 0
			found := false
			for iter.Next() {
				i++ // adding 1 because we start at base commit
				if i == mod.Value+1 {
					baseCommit = iter.Value().CommitID
					found = true
					break
				}
			}
			if iter.Err() != nil {
				return nil, iter.Err()
			}
			iter.Close()
			// went too far!
			if !found {
				return nil, ErrNotFound
			}
		case RevModTypeCaret:
			switch mod.Value {
			case 0:
				continue // ^0 = the commit itself
			default:
				// get the commit and extract parents
				c, err := store.GetCommitByPrefix(ctx, repositoryID, baseCommit)
				if err != nil {
					return nil, err
				}
				if mod.Value > len(c.Parents) {
					return nil, ErrInvalidRef
				}
				baseCommit = c.Parents[mod.Value-1]
			}

		default:
			return nil, ErrInvalidRef
		}
	}

	return reference{
		typ:      ReferenceTypeCommit,
		commitID: &baseCommit,
	}, nil
}

func resolveRevAHash(ctx context.Context, store RefStore, repositoryID RepositoryID, rev string) (Reference, error) {
	if !isAHash(rev) {
		return nil, nil
	}
	commit, err := store.GetCommitByPrefix(ctx, repositoryID, CommitID(rev))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	commitID := CommitID(ident.ContentAddress(commit))
	return &reference{
		typ:      ReferenceTypeCommit,
		commitID: &commitID,
	}, nil
}

func resolveRevBranch(ctx context.Context, store RefStore, repositoryID RepositoryID, rev string) (Reference, error) {
	branch, err := store.GetBranch(ctx, repositoryID, BranchID(rev))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &reference{
		typ:      ReferenceTypeBranch,
		branch:   branch,
		commitID: &branch.CommitID,
	}, nil
}

func resolveRevTag(ctx context.Context, store RefStore, repositoryID RepositoryID, rev string) (Reference, error) {
	commitID, err := store.GetTag(ctx, repositoryID, TagID(rev))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &reference{
		typ:      ReferenceTypeTag,
		commitID: commitID,
	}, nil
}
