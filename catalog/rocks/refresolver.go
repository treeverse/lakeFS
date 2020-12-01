package rocks

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/ident"
)

type RefStore interface {
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error)
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)
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

func ResolveRef(ctx context.Context, store RefStore, repositoryID RepositoryID, ref Ref) (Reference, error) {
	// first we need to parse-rev to get a list references
	// valid revs: branch, tag, commit ID, commit ID prefix (as long as unambiguous)
	// valid modifiers: ~N
	parsed, err := RevParse(ref)
	if err != nil {
		return nil, err
	}

	var baseCommit CommitID
	if isAHash(parsed.BaseRev) {
		commit, err := store.GetCommit(ctx, repositoryID, CommitID(parsed.BaseRev))
		if err != nil && !errors.Is(err, ErrNotFound) {
			// couldn't check if it's a commit
			return nil, err
		}
		if err == nil {
			baseCommit = CommitID(ident.ContentAddress(commit))
		}
		// otherwise, simply not a commit. Moving on.
	}

	if baseCommit == "" {
		// check if it's a branch
		branch, err := store.GetBranch(ctx, repositoryID, BranchID(parsed.BaseRev))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err == nil {
			baseCommit = branch.CommitID
		}

		if err == nil && len(parsed.Modifiers) == 0 {
			return &reference{
				typ:      ReferenceTypeBranch,
				branch:   branch,
				commitID: &branch.CommitID,
			}, nil
		}
	}

	// TODO(ozkatz): once we have tags, they should also be resolved
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
				c, err := store.GetCommit(ctx, repositoryID, baseCommit)
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
