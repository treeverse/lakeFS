package ref

import (
	"context"
	"errors"
	"regexp"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
)

type Store interface {
	GetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, error)
	GetTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) (*graveler.CommitID, error)
	GetCommitByPrefix(ctx context.Context, repository *graveler.RepositoryRecord, prefix graveler.CommitID) (*graveler.Commit, error)
	GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, prefix graveler.CommitID) (*graveler.Commit, error)
}

type revResolverFunc func(context.Context, Store, ident.AddressProvider, *graveler.RepositoryRecord, string) (*graveler.ResolvedRef, error)

var hashRegexp = regexp.MustCompile("^[a-fA-F0-9]{1,64}$")

func isAHash(part string) bool {
	return hashRegexp.MatchString(part)
}

// revResolve return the first resolve of 'rev' - by hash, branch or tag
func revResolve(ctx context.Context, store Store, addressProvider ident.AddressProvider, repository *graveler.RepositoryRecord, rev string) (*graveler.ResolvedRef, error) {
	resolvers := []revResolverFunc{revResolveAHash, revResolveBranch, revResolveTag}
	for _, resolveHelper := range resolvers {
		r, err := resolveHelper(ctx, store, addressProvider, repository, rev)
		if err != nil {
			return nil, err
		}
		if r != nil {
			return r, nil
		}
	}
	return nil, graveler.ErrNotFound
}

func ResolveRawRef(ctx context.Context, store Store, addressProvider ident.AddressProvider, repository *graveler.RepositoryRecord, rawRef graveler.RawRef) (*graveler.ResolvedRef, error) {
	rr, err := revResolve(ctx, store, addressProvider, repository, rawRef.BaseRef)
	if err != nil {
		return nil, err
	}
	// return the matched reference, when no modifiers on ref or use the commit id as base
	if len(rawRef.Modifiers) == 0 {
		return rr, nil
	}
	baseCommit := rr.CommitID
	for _, mod := range rawRef.Modifiers {
		// lastly, apply modifier
		switch mod.Type {
		case graveler.RefModTypeAt:
			if rr.Type != graveler.ReferenceTypeBranch || len(rawRef.Modifiers) != 1 {
				return nil, graveler.ErrInvalidRef
			}
			return &graveler.ResolvedRef{
				Type:                   graveler.ReferenceTypeBranch,
				ResolvedBranchModifier: graveler.ResolvedBranchModifierCommitted,
				BranchRecord: graveler.BranchRecord{
					Branch: &graveler.Branch{
						CommitID: rr.CommitID,
					}},
			}, nil

		case graveler.RefModTypeDollar:
			if rr.Type != graveler.ReferenceTypeBranch || len(rawRef.Modifiers) != 1 {
				return nil, graveler.ErrInvalidRef
			}
			return &graveler.ResolvedRef{
				Type:                   graveler.ReferenceTypeBranch,
				ResolvedBranchModifier: graveler.ResolvedBranchModifierStaging,
				BranchRecord: graveler.BranchRecord{
					BranchID: rr.BranchID,
					Branch: &graveler.Branch{
						CommitID:     rr.CommitID,
						StagingToken: rr.StagingToken,
						SealedTokens: rr.SealedTokens,
					}},
			}, nil

		case graveler.RefModTypeTilde:
			// skip mod.ValueNumeric iterations
			for i := 0; i < mod.Value; i++ {
				commit, err := store.GetCommit(ctx, repository, baseCommit)
				if err != nil {
					return nil, err
				}
				if len(commit.Parents) == 0 {
					return nil, graveler.ErrNotFound
				}
				baseCommit = commit.Parents[0]
			}
		case graveler.RefModTypeCaret:
			if mod.Value == 0 {
				// ^0 = the commit itself
				continue
			}
			// get the commit and extract parents
			c, err := store.GetCommitByPrefix(ctx, repository, baseCommit)
			if err != nil {
				return nil, err
			}
			if mod.Value > len(c.Parents) {
				return nil, graveler.ErrInvalidRef
			}
			baseCommit = c.Parents[mod.Value-1]

		default:
			return nil, graveler.ErrInvalidRef
		}
	}

	return &graveler.ResolvedRef{
		Type: graveler.ReferenceTypeCommit,
		BranchRecord: graveler.BranchRecord{
			BranchID: rr.BranchID,
			Branch: &graveler.Branch{
				CommitID: baseCommit,
			}},
	}, nil
}

func revResolveAHash(ctx context.Context, store Store, addressProvider ident.AddressProvider, repository *graveler.RepositoryRecord, rev string) (*graveler.ResolvedRef, error) {
	if !isAHash(rev) {
		return nil, nil
	}
	commit, err := store.GetCommitByPrefix(ctx, repository, graveler.CommitID(rev))
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	commitID := graveler.CommitID(addressProvider.ContentAddress(commit))
	return &graveler.ResolvedRef{
		Type: graveler.ReferenceTypeCommit,
		BranchRecord: graveler.BranchRecord{
			Branch: &graveler.Branch{
				CommitID: commitID,
			}},
	}, nil
}

func revResolveBranch(ctx context.Context, store Store, _ ident.AddressProvider, repository *graveler.RepositoryRecord, rev string) (*graveler.ResolvedRef, error) {
	branchID := graveler.BranchID(rev)
	branch, err := store.GetBranch(ctx, repository, branchID)
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &graveler.ResolvedRef{
		Type: graveler.ReferenceTypeBranch,
		BranchRecord: graveler.BranchRecord{
			BranchID: branchID,
			Branch: &graveler.Branch{
				CommitID:     branch.CommitID,
				StagingToken: branch.StagingToken,
				SealedTokens: branch.SealedTokens,
			}},
	}, nil
}

func revResolveTag(ctx context.Context, store Store, _ ident.AddressProvider, repository *graveler.RepositoryRecord, rev string) (*graveler.ResolvedRef, error) {
	commitID, err := store.GetTag(ctx, repository, graveler.TagID(rev))
	if errors.Is(err, graveler.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &graveler.ResolvedRef{
		Type: graveler.ReferenceTypeTag,
		BranchRecord: graveler.BranchRecord{
			Branch: &graveler.Branch{
				CommitID: *commitID,
			}},
	}, nil
}
