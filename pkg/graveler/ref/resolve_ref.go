package ref

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
)

type Store interface {
	GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error)
	GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error)
	GetCommitByPrefix(ctx context.Context, repositoryID graveler.RepositoryID, prefix graveler.CommitID) (*graveler.Commit, error)
	GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, prefix graveler.CommitID) (*graveler.Commit, error)
}

type reference struct {
	typ      graveler.ReferenceType
	branch   *graveler.Branch
	commitID *graveler.CommitID
}

type revResolverFunc func(context.Context, Store, ident.AddressProvider, graveler.RepositoryID, string) (graveler.Reference, error)

func (r reference) Type() graveler.ReferenceType {
	return r.typ
}

func (r reference) Branch() graveler.Branch {
	return *r.branch
}

func (r reference) CommitID() graveler.CommitID {
	return *r.commitID
}

// revResolve return the first resolve of 'rev' - by hash, branch or tag
func revResolve(ctx context.Context, store Store, addressProvider ident.AddressProvider, repositoryID graveler.RepositoryID, rev string) (graveler.Reference, error) {
	resolvers := []revResolverFunc{revResolveAHash, revResolveBranch, revResolveTag}
	for _, resolveHelper := range resolvers {
		r, err := resolveHelper(ctx, store, addressProvider, repositoryID, rev)
		if err != nil {
			return nil, err
		}
		if r != nil {
			return r, nil
		}
	}
	return nil, graveler.ErrNotFound
}

func ResolveRef(ctx context.Context, store Store, addressProvider ident.AddressProvider, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.Reference, error) {
	// first we need to parse-rev to get a list references
	// valid revs: branch, tag, commit ID, commit ID prefix (as long as unambiguous)
	// valid modifiers: ~N
	parsed, err := RevParse(ref)
	if err != nil {
		return nil, err
	}

	rr, err := revResolve(ctx, store, addressProvider, repositoryID, parsed.BaseRev)
	if err != nil {
		return nil, err
	}
	// return the matched reference, when no modifiers on ref or use the commit id as base
	if len(parsed.Modifiers) == 0 {
		return rr, nil
	}
	baseCommit := rr.CommitID()

	for _, mod := range parsed.Modifiers {
		// lastly, apply modifier
		switch mod.Type {
		case RevModTypeTilde:
			// skip mod.ValueNumeric iterations
			for i := 0; i < mod.Value; i++ {
				commit, err := store.GetCommit(ctx, repositoryID, baseCommit)
				if err != nil {
					return nil, err
				}
				if len(commit.Parents) == 0 {
					return nil, graveler.ErrNotFound
				}
				baseCommit = commit.Parents[0]
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
					return nil, graveler.ErrInvalidRef
				}
				baseCommit = c.Parents[mod.Value-1]
			}

		default:
			return nil, graveler.ErrInvalidRef
		}
	}

	return reference{
		typ:      graveler.ReferenceTypeCommit,
		commitID: &baseCommit,
	}, nil
}

func revResolveAHash(ctx context.Context, store Store, addressProvider ident.AddressProvider, repositoryID graveler.RepositoryID, rev string) (graveler.Reference, error) {
	if !isAHash(rev) {
		return nil, nil
	}
	commit, err := store.GetCommitByPrefix(ctx, repositoryID, graveler.CommitID(rev))
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	commitID := graveler.CommitID(addressProvider.ContentAddress(commit))
	return &reference{
		typ:      graveler.ReferenceTypeCommit,
		commitID: &commitID,
	}, nil
}

func revResolveBranch(ctx context.Context, store Store, _ ident.AddressProvider, repositoryID graveler.RepositoryID, rev string) (graveler.Reference, error) {
	branch, err := store.GetBranch(ctx, repositoryID, graveler.BranchID(rev))
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &reference{
		typ:      graveler.ReferenceTypeBranch,
		branch:   branch,
		commitID: &branch.CommitID,
	}, nil
}

func revResolveTag(ctx context.Context, store Store, _ ident.AddressProvider, repositoryID graveler.RepositoryID, rev string) (graveler.Reference, error) {
	commitID, err := store.GetTag(ctx, repositoryID, graveler.TagID(rev))
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &reference{
		typ:      graveler.ReferenceTypeTag,
		commitID: commitID,
	}, nil
}
