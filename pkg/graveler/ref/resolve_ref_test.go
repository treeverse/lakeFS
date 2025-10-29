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
	r, _ := testRefManager(t)

	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	mainBranch, err := r.GetBranch(ctx, repository, "main")
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
		cid, err := r.AddCommit(ctx, repository, c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		previous = cid
		ts = ts.Add(time.Minute)
	}

	iter, err := r.Log(ctx, repository, previous, false, nil)
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
		"de85cde9a3871433ed0afad67d6505aef3b3a779223916be237b44bbdc1b65ed",
		"abd533122a68be2fe2fea4a6a5c48229560acb98331754f87d69d93fade7a497",
		"f5d6eb69609e31016f8c89c983a0b59ee4daca07434c89ecabf927a265feb497",
		"5376fcefd52e7a49ea24d88af06fd3f600210abd3c9fc52fadbcc2ba36b05588",
		"0cb1b3655d48b0b068951fac1cfc276ca01623000b7aabc53f16076a8c91725d",
		"74f7a8402e808b8f621a4f46c2138fbf51f242273f24982d235fe3ab67db4e3b",
		"c2b8083b2ec9d30a9e6d09d4d0938ff48a67ad7bedd532ebf17808f5f199f0b0",
		"6564419ee5290a114a353dc1bc0c4828f46eaeddfde2d6e1b9612fbf1098203d",
		"2f9ac33733f0a0b284d97194711b3b46de9b4ff723545d0a95951668a34cff04",
		"1c6977726cabb5549f1ff9c8ab7ee7672033c8b5b47427b4099f26103e7fb4bc",
		"155a755e8956eebdb31d20dace8b8fd7e8eaa4f84a58f1af701757f9a8bddc4c",
		"504956bd68f17e1a2fec4fc9e2ada276a885236dcc730d9dc99187a4bbe73900",
		"5ed05bd6c8ee3e1fd4d1009b19c5d74b0ef918d5e2c597a4d7d88ba36f4075d2",
		"eb0751d2da465e3f78e90b328630c360085fa8f129351379fa3b8a0ad3fc4424",
		"daf38e9d2a32b43a07a0768e8223d06d2f3122fcf600b2eabde613c077508c73",
		"4f4cc1feb893ed337608cff0962bc1bcde08d3bde9752b1309eea692d87c4470",
		"b12888687d6d3956fd40767f2697f4a3cdf7230e3b86ce63711200e420041b59",
		"2c910787d475900509fef33de2c20700baf33dae8c0abdd6ad416e73b651ab24",
		"c634a9bdf2d2e4d819cadaeb62a0b43ced0b0398aa513ac3cf218b4dca05dcd7",
		"fac53a04432b2e6e7185f3ac8314a874c556b2557adf3aa8d5b1df985cf96566",
	}

	if diff := deep.Equal(commitIDs, commitLog); diff != nil {
		t.Error("Difference found on commit log", diff)
	}

	branch1CommitID := commitLog[3]
	testutil.Must(t, r.SetBranch(ctx, repository, "branch1", graveler.Branch{
		CommitID:     branch1CommitID,
		StagingToken: "token1",
	}))

	branch2CommitID := commitLog[16]
	testutil.Must(t, r.SetBranch(ctx, repository, "branch2", graveler.Branch{
		CommitID:     branch2CommitID,
		StagingToken: "token2",
	}))

	tagCommitID := commitLog[9]
	testutil.Must(t, r.CreateTag(ctx, repository, "v1.0", tagCommitID))

	commitCommitID := commitLog[11]

	branch3Name := string(commitLog[10])[:6]
	branch3CommitID := commitLog[14]

	testutil.Must(t, r.SetBranch(ctx, repository, graveler.BranchID(branch3Name), graveler.Branch{
		CommitID:     branch3CommitID,
		StagingToken: "token3",
	}))

	tag2Name := string(commitLog[6])[:10]
	tag2CommitID := commitLog[8]

	testutil.Must(t, r.CreateTag(ctx, repository, graveler.TagID(tag2Name), tag2CommitID))

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
			Name:                   "branch_precedes_commit_prefix",
			Ref:                    graveler.Ref(branch3Name),
			ExpectedBranchModifier: graveler.ResolvedBranchModifierNone,
			ExpectedToken:          "token3",
			ExpectedCommitID:       branch3CommitID,
		},
		{
			Name:             "tag_precedes_commit_prefix",
			Ref:              graveler.Ref(tag2Name),
			ExpectedCommitID: tag2CommitID,
		},
		{
			Name:        "commit_prefix_ambiguous",
			Ref:         graveler.Ref("5"),
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
			rawRef, err := r.ParseRef(cas.Ref)
			if err != nil {
				if cas.ExpectedErr == nil || !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("unexpected error while parse '%s': %v, expected: %v", cas.Ref, err, cas.ExpectedErr)
				}
				return
			}
			resolvedRef, err := r.ResolveRawRef(ctx, repository, rawRef)
			if err != nil {
				if cas.ExpectedErr == nil || !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("unexpected error while resolve '%s': %v, expected: %v", cas.Ref, err, cas.ExpectedErr)
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

func TestResolveRef_SameDate(t *testing.T) {
	r, _ := testRefManager(t)
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageID:        "sid",
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
		cid, err := r.AddCommit(ctx, repository, c)
		testutil.MustDo(t, "add commit", err)
		return cid
	}
	c1 := addCommit("c1")
	c2 := addCommit("c2", c1)
	c3 := addCommit("c3", c1)
	c4 := addCommit("c4", c3)
	c5 := addCommit("c5", c4, c2)

	it, err := r.Log(ctx, repository, c5, false, nil)
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
	r, _ := testRefManager(t)
	repository, err := r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageID:        "sid",
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
		cid, err := r.AddCommit(context.Background(), repository, commit)
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
		resolved, err := resolveRef(context.Background(), r, ident.NewHexAddressProvider(), repository, graveler.Ref(reference))
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

func resolveRef(ctx context.Context, store ref.Store, addressProvider ident.AddressProvider, repository *graveler.RepositoryRecord, reference graveler.Ref) (*graveler.ResolvedRef, error) {
	rawRef, err := ref.ParseRef(reference)
	if err != nil {
		return nil, err
	}
	return ref.ResolveRawRef(ctx, store, addressProvider, repository, rawRef)
}
