package ref_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestResolveRawRef(t *testing.T) {
	r := testRefManager(t)
	for _, tt := range r {
		ctx := context.Background()
		repository, err := tt.refManager.CreateRepository(ctx, "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		})
		testutil.Must(t, err)

		mainBranch, err := tt.refManager.GetBranch(ctx, repository, "main")
		if err != nil {
			t.Fatalf("Failed to get main branch information: %s", err)
		}

		ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
		var previous graveler.CommitID
		for i := 0; i < 20; i++ {
			c := graveler.Commit{
				Committer:    "user1",
				Message:      "message1",
				MetaRangeID:  "deadbeef123",
				CreationDate: ts,
				Parents:      graveler.CommitParents{},
				Metadata:     graveler.Metadata{"foo": "bar"},
			}
			if previous != "" {
				c.Parents = append(c.Parents, previous)
			}
			cid, err := tt.refManager.AddCommit(ctx, repository, c)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			previous = cid
			ts.Add(time.Minute)
		}

		iter, err := tt.refManager.Log(ctx, repository, previous)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var commitIDs []graveler.CommitID
		for iter.Next() {
			commit := iter.Value()
			if commit == nil {
				t.Fatal("Log iterator returned nil value after Next")
			}
			commitIDs = append(commitIDs, commit.CommitID)
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		//  commit log:
		commitLog := []graveler.CommitID{
			"6ededfa12763c5eeb6ce48e5d5d505c33383e4a7e599f55764c0753cb196e244",
			"a8fcc52c74a09642e0f3ec5997f49ce4f7c73d6c74c03c13143b7fe19a3f156b",
			"1e471f2170d8f99e093186f7e9195ec340d5f4560df234dd1201dd749fd71f8f",
			"7217e786dab7c42985c3268b680ff1ac5b45c3ee83d8578b5e63b3f1079ce753",
			"d0a251a277ac1bc977a0e35efad1284ab69eeb3635165661d4cf62ed9b9029f5",
			"59f72f2fbc8291857d5b6788ad2a698121d0f2735df41c3bf582c3193f44f148",
			"a5ee924bba14751f3573caf52a50a44bb5909e658d5017f323c8d826b74cfc6a",
			"b99c85a0dfbd5caf950d1874c8968ceeb4773c53d088ef9358bd7d3b175f5c4b",
			"d02cdab99549019edf83ebc2c33a6fd65052a113845c11158fdedae3f5855bcb",
			"3bf45fcce06016398758d69046b7492a99a1c60f281b0695761c8d1b18c68106",
			"474cc3b19124b04a230ff743aee56d91f4c275c3b6ae18b8d2abb90a236c7fa2",
			"41f8200d4edd82befe47514dc79c3eb11f79e3b8e6e31e7b73de459a88b20975",
			"d706a40d808c819a4e15a8f9c733c243c3a159f701355952fd92375c575fc1f2",
			"d6f64d7d9d379ba8878d5f867a0dd0cd01f93c50152e7c2e03d783bb3801d31a",
			"085d9c8a83aedc8c2e6c160fe98a00b2470a0432fc75c7372324e2c72bded7df",
			"8ac7ccae8718d49bc38a7a50ef05e0ac698aa5f05daac00dff8f7c94373013e6",
			"967889b77f6ada86683cdbbbba6e5542f760974f9dbfeb7813337e9455aafa50",
			"d58c3e63162be1cc8f902277cd3515bc2faf226a1a2048e9f0f2c7fc544c94c7",
			"8ad89e3856f90f3af080747f487363eeaff42cafa4ab0536d2a985d0ae362089",
			"fac53a04432b2e6e7185f3ac8314a874c556b2557adf3aa8d5b1df985cf96566",
		}

		if diff := deep.Equal(commitIDs, commitLog); diff != nil {
			t.Error("Difference found on commit log", diff)
		}

		branch1CommitID := commitLog[3]
		testutil.Must(t, tt.refManager.SetBranch(ctx, repository, "branch1", graveler.Branch{
			CommitID:     branch1CommitID,
			StagingToken: "token1",
		}))

		branch2CommitID := commitLog[16]
		testutil.Must(t, tt.refManager.SetBranch(ctx, repository, "branch2", graveler.Branch{
			CommitID:     branch2CommitID,
			StagingToken: "token2",
		}))

		tagCommitID := commitLog[9]
		testutil.Must(t, tt.refManager.CreateTag(ctx, repository, "v1.0", tagCommitID))

		commitCommitID := commitLog[11]

		table := []struct {
			Name                   string
			Ref                    graveler.Ref
			ExpectedCommitID       graveler.CommitID
			ExpectedBranchModifier graveler.ResolvedBranchModifier
			ExpectedToken          graveler.StagingToken
			ExpectedErr            error
		}{
			{
				Name:                   "branch_exist",
				Ref:                    graveler.Ref("branch1"),
				ExpectedBranchModifier: graveler.ResolvedBranchModifierNone,
				ExpectedToken:          "token1",
				ExpectedCommitID:       branch1CommitID,
			},
			{
				Name:        "branch_doesnt_exist",
				Ref:         graveler.Ref("branch3"),
				ExpectedErr: graveler.ErrNotFound,
			},
			{
				Name:                   "branch_head",
				Ref:                    graveler.Ref("branch1@"),
				ExpectedBranchModifier: graveler.ResolvedBranchModifierCommitted,
				ExpectedCommitID:       branch1CommitID,
			},
			{
				Name:        "branch_invalid_head",
				Ref:         graveler.Ref("branch1@1"),
				ExpectedErr: graveler.ErrInvalidRef,
			},
			{
				Name:        "branch_invalid_head_caret1",
				Ref:         graveler.Ref("branch1@^"),
				ExpectedErr: graveler.ErrInvalidRef,
			},
			{
				Name:        "branch_invalid_head_caret2",
				Ref:         graveler.Ref("branch1^@"),
				ExpectedErr: graveler.ErrInvalidRef,
			},
			{
				Name:                   "main_staging",
				Ref:                    graveler.Ref("main$"),
				ExpectedBranchModifier: graveler.ResolvedBranchModifierStaging,
				ExpectedCommitID:       mainBranch.CommitID,
				ExpectedToken:          mainBranch.StagingToken,
			},
			{
				Name:                   "main_committed",
				Ref:                    graveler.Ref("main@"),
				ExpectedBranchModifier: graveler.ResolvedBranchModifierCommitted,
				ExpectedCommitID:       mainBranch.CommitID,
			},
			{
				Name:             "tag_exist",
				Ref:              graveler.Ref("v1.0"),
				ExpectedCommitID: tagCommitID,
			},
			{
				Name:        "tag_doesnt_exist",
				Ref:         graveler.Ref("v1.bad"),
				ExpectedErr: graveler.ErrNotFound,
			},
			{
				Name:             "commit",
				Ref:              graveler.Ref(commitCommitID),
				ExpectedCommitID: commitCommitID,
			},
			{
				Name:             "commit_prefix_good",
				Ref:              graveler.Ref(commitCommitID[:5]),
				ExpectedCommitID: commitCommitID,
			},
			{
				Name:        "commit_prefix_ambiguous",
				Ref:         graveler.Ref("a"),
				ExpectedErr: graveler.ErrNotFound,
			},
			{
				Name:        "commit_prefix_missing",
				Ref:         graveler.Ref("66666"),
				ExpectedErr: graveler.ErrNotFound,
			},
			{
				Name:             "branch_with_modifier",
				Ref:              graveler.Ref(branch1CommitID + "~2"),
				ExpectedCommitID: commitLog[5],
			},
			{
				Name:             "commit_with_modifier",
				Ref:              graveler.Ref(commitCommitID + "~2"),
				ExpectedCommitID: commitLog[13],
			},
			{
				Name:             "commit_prefix_with_modifier",
				Ref:              graveler.Ref(commitCommitID[:5] + "~2"),
				ExpectedCommitID: commitLog[13],
			},
			{
				Name:        "commit_prefix_with_modifier_too_big",
				Ref:         graveler.Ref(commitCommitID + "~200"),
				ExpectedErr: graveler.ErrNotFound,
			},
		}

		for _, cas := range table {
			t.Run(cas.Name, func(t *testing.T) {
				rawRef, err := tt.refManager.ParseRef(cas.Ref)
				if err != nil {
					if cas.ExpectedErr == nil || !errors.Is(err, cas.ExpectedErr) {
						t.Fatalf("unexpected error while parse '%s': %v, expected: %s", cas.Ref, err, cas.ExpectedErr)
					}
					return
				}
				resolvedRef, err := tt.refManager.ResolveRawRef(ctx, repository, rawRef)
				if err != nil {
					if cas.ExpectedErr == nil || !errors.Is(err, cas.ExpectedErr) {
						t.Fatalf("unexpected error while resolve '%s': %v, expected: %s", cas.Ref, err, cas.ExpectedErr)
					}
					return
				}
				if cas.ExpectedCommitID != resolvedRef.CommitID {
					t.Fatalf("got unexpected commit ID: '%s', expected: '%s'",
						resolvedRef.CommitID, cas.ExpectedCommitID)
				}
				if cas.ExpectedToken != resolvedRef.StagingToken {
					t.Fatalf("got unexpected staging token: '%s', expected: '%s'",
						resolvedRef.StagingToken, cas.ExpectedToken)
				}
				if cas.ExpectedBranchModifier != resolvedRef.ResolvedBranchModifier {
					t.Fatalf("got unexpected branch modifier: %d, expected: %d",
						resolvedRef.ResolvedBranchModifier, cas.ExpectedBranchModifier)
				}
			})
		}
	}
}

func TestResolveRef_SameDate(t *testing.T) {
	r := testRefManager(t)
	for _, tt := range r {
		ctx := context.Background()
		repository, err := tt.refManager.CreateRepository(ctx, "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		})
		testutil.Must(t, err)

		ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
		addCommit := func(message string, parents ...graveler.CommitID) graveler.CommitID {
			c := graveler.Commit{
				Message:      message,
				Committer:    "tester",
				MetaRangeID:  "deadbeef1",
				CreationDate: ts,
				Parents:      graveler.CommitParents{},
			}
			for _, p := range parents {
				c.Parents = append(c.Parents, p)
			}
			cid, err := tt.refManager.AddCommit(ctx, repository, c)
			testutil.MustDo(t, "add commit", err)
			return cid
		}
		c1 := addCommit("c1")
		c2 := addCommit("c2", c1)
		c3 := addCommit("c3", c1)
		c4 := addCommit("c4", c3)
		c5 := addCommit("c5", c4, c2)

		it, err := tt.refManager.Log(ctx, repository, c5)
		testutil.MustDo(t, "Log request", err)
		var commitIDs []graveler.CommitID
		for it.Next() {
			commit := it.Value()
			commitIDs = append(commitIDs, commit.CommitID)
		}
		testutil.MustDo(t, "Log complete iteration", it.Err())

		expected := []graveler.CommitID{c5, c4, c3, c2, c1}
		if diff := deep.Equal(commitIDs, expected); diff != nil {
			t.Fatal("Iterator over commits found diff:", diff)
		}
	}
}

func TestResolveRef_DereferenceWithGraph(t *testing.T) {
	/*
		This is taken from `git help rev-parse` - let's run these tests

		           G   H   I   J
		            \ /     \ /
		             D   E   F
		              \  |  / \
		               \ | /   |
		                \|/    |
		                 B     C
		                  \   /
		                   \ /
		                    A

		           A =      = A^0
		           B = A^   = A^1     = A~1
		           C =      = A^2
		           D = A^^  = A^1^1   = A~2
		           E = B^2  = A^^2
		           F = B^3  = A^^3
		           G = A^^^ = A^1^1^1 = A~3
		           H = D^2  = B^^2    = A^^^2  = A~2^2
		           I = F^   = B^3^    = A^^3^
		           J = F^2  = B^3^2   = A^^3^2
	*/
	r := testRefManager(t)
	for _, tt := range r {
		repository, err := tt.refManager.CreateRepository(context.Background(), "repo1", graveler.Repository{
			StorageNamespace: "s3://",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		})
		testutil.Must(t, err)

		ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
		addCommit := func(parents ...graveler.CommitID) graveler.CommitID {
			commit := graveler.Commit{
				CreationDate: ts,
			}
			for _, p := range parents {
				commit.Parents = append(commit.Parents, p)
			}
			cid, err := tt.refManager.AddCommit(context.Background(), repository, commit)
			testutil.MustDo(t, "add commit", err)
			ts = ts.Add(time.Second)
			return cid
		}

		G := addCommit()
		H := addCommit()
		I := addCommit()
		J := addCommit()
		D := addCommit(G, H)
		E := addCommit()
		F := addCommit(I, J)
		B := addCommit(D, E, F)
		C := addCommit(F)
		A := addCommit(B, C)

		resolve := func(base graveler.CommitID, mod string, expected graveler.CommitID) {
			t.Helper()
			reference := string(base) + mod
			resolved, err := resolveRef(context.Background(), tt.refManager, ident.NewHexAddressProvider(), repository, graveler.Ref(reference))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resolved.CommitID != expected {
				t.Fatalf("Ref %s got %s, expected %s", reference, resolved.CommitID, expected)
			}
		}

		// now the tests:
		resolve(A, "^0", A)
		resolve(A, "^", B)
		resolve(A, "^1", B)
		resolve(A, "~1", B)
		resolve(A, "^2", C)
		resolve(A, "^^", D)
		resolve(A, "^1^1", D)
		resolve(A, "~2", D)
		resolve(B, "^2", E)
		resolve(A, "^^2", E)
		resolve(B, "^2", E)
		resolve(A, "^^2", E)
		resolve(B, "^3", F)
		resolve(A, "^^3", F)
		resolve(A, "^^^", G)
		resolve(A, "^1^1^1", G)
		resolve(A, "~3", G)
		resolve(D, "^2", H)
		resolve(B, "^^2", H)
		resolve(A, "^^^2", H)
		resolve(A, "~2^2", H)
		resolve(F, "^", I)
		resolve(B, "^3^", I)
		resolve(A, "^^3^", I)
		resolve(F, "^2", J)
		resolve(B, "^3^2", J)
		resolve(A, "^^3^2", J)
	}
}

func resolveRef(ctx context.Context, store ref.Store, addressProvider ident.AddressProvider, repository *graveler.RepositoryRecord, reference graveler.Ref) (*graveler.ResolvedRef, error) {
	rawRef, err := ref.ParseRef(reference)
	if err != nil {
		return nil, err
	}
	return ref.ResolveRawRef(ctx, store, addressProvider, repository, rawRef)
}
