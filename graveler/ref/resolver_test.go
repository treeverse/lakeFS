package ref_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
)

func TestRefManager_Dereference(t *testing.T) {
	r := testRefManager(t)
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{}))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
	var previous graveler.CommitID
	for i := 0; i < 20; i++ {
		c := graveler.Commit{
			Committer:    "user1",
			Message:      "message1",
			MetaRangeID:  "deadbeef123",
			CreationDate: ts,
			Parents:      graveler.CommitParents{previous},
			Metadata:     graveler.Metadata{"foo": "bar"},
		}
		cid, err := r.AddCommit(ctx, "repo1", c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		previous = cid
	}

	iter, err := r.Log(ctx, "repo1", previous)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for iter.Next() {
		c := iter.Value()
		if c == nil {
			t.Fatal("Log iterator returned nil value after Next")
		}
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}

	//  commit log:
	//  "c3f815d633789cd7c1325352277d4de528844c758a9beedfa8a3cfcfb5c75627",
	//	"8549d7544244ba1b63b5967b6b328b331658f627369cb89bd442684719c318ae",
	//	"13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774",
	//	"7de38592b9e6046ffb55915a40848f05749f168531f0cd6a2aa61fe6e8d92d02",
	//	"94c7773c89650e99671c33d46e31226230cdaeed79a77cdbd7c7419ea68b91ca",
	//	"0efcb6e81db6bdd2cfeb77664b6573a7d69f555bbe561fd1fd018a4e4cac7603",
	//	"d85e4ae46b63f641b439afde9ebab794a3c39c203a42190c0b9d7773ab71a60e",
	//	"a766cfdb311fe5f18f489d90d283f65ed522e719fe1ad5397277339eee0d1964",
	//	"67ea954d570e20172775f41ac9763905d16d73490d9b72731d353db33f85d437",
	//	"d3b16c2cf7f5b9adc2770976bcabe463a5bdd3b5dbf740034f09a9c663620aed",
	//	"d420fbf793716d6d53798218d7a247f38a5bbed095d57df71ee79e05446e46ec",
	//	"cc72bda1adade1a72b3de617472c16af187063c79e7edc7921c04e883b44de4c",
	//	"752581ac60bd8e38a2e65a754591a93a1703dc6c658f91380b8836013188c566",
	//	"3cf70857454c71fd0bbf69af8a5360671ba98f6ac9371b047144208c58c672a2",
	//	"bfa1e0382ff3c51905dc62ced0a67588b5219c1bba71a517ae7e7857f0c26afe",
	//	"d2248dcc1a4de004e10e3bc6b820655e649b8d986d983b60ec98a357a0df194b",
	//	"a2d98d820f6ff3f221223dbe6a22548f78549830d3b19286b101f13a0ee34085",
	//	"4f13621ec00d4e44e8a0f0ad340224f9d51db9b6518ee7bef17f598aea9e0431",
	//	"df87d5329f4438662d6ecb9b90ee17c0bdc9a78a884acc93c0c4fe9f0f79d059",
	//	"29706d36de7219e0796c31b278f87201ef835e8cdafbcc3c907d292cd31f77d5",

	testutil.Must(t, r.SetBranch(ctx, "repo1", "branch1", graveler.Branch{
		CommitID: "13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774",
	}))

	testutil.Must(t, r.SetBranch(ctx, "repo1", "branch2", graveler.Branch{
		CommitID: "d420fbf793716d6d53798218d7a247f38a5bbed095d57df71ee79e05446e46ec",
	}))

	testutil.Must(t, r.CreateTag(ctx, "repo1", "v1.0", "d85e4ae46b63f641b439afde9ebab794a3c39c203a42190c0b9d7773ab71a60e"))

	table := []struct {
		Name        string
		Ref         graveler.Ref
		Expected    graveler.CommitID
		ExpectedErr error
	}{
		{
			Name:     "branch_exist",
			Ref:      graveler.Ref("branch1"),
			Expected: graveler.CommitID("13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774"),
		},
		{
			Name:        "branch_doesnt_exist",
			Ref:         graveler.Ref("branch3"),
			ExpectedErr: graveler.ErrNotFound,
		},
		{
			Name:     "tag_exist",
			Ref:      graveler.Ref("v1.0"),
			Expected: graveler.CommitID("d85e4ae46b63f641b439afde9ebab794a3c39c203a42190c0b9d7773ab71a60e"),
		},
		{
			Name:        "tag_doesnt_exist",
			Ref:         graveler.Ref("v1.bad"),
			ExpectedErr: graveler.ErrNotFound,
		},
		{
			Name:     "commit",
			Ref:      graveler.Ref("13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774"),
			Expected: graveler.CommitID("13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774"),
		},
		{
			Name:     "commit_prefix_good",
			Ref:      graveler.Ref("13daf"),
			Expected: graveler.CommitID("13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774"),
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
			Name:     "branch_with_modifier",
			Ref:      graveler.Ref("branch1~2"),
			Expected: graveler.CommitID("94c7773c89650e99671c33d46e31226230cdaeed79a77cdbd7c7419ea68b91ca"),
		},
		{
			Name:     "commit_with_modifier",
			Ref:      graveler.Ref("13dafa9c45bcf67e6997776039cbf8ab571ace560ce9e13665f383434a495774~2"),
			Expected: graveler.CommitID("94c7773c89650e99671c33d46e31226230cdaeed79a77cdbd7c7419ea68b91ca"),
		},
		{
			Name:     "commit_prefix_with_modifier",
			Ref:      graveler.Ref("13dafa~2"),
			Expected: graveler.CommitID("94c7773c89650e99671c33d46e31226230cdaeed79a77cdbd7c7419ea68b91ca"),
		},
		{
			Name:        "commit_prefix_with_modifier_too_big",
			Ref:         graveler.Ref("2c14ddd9b097a8f96db3f27a454877c9513378635d313ba0f0277d793a183e72~200"),
			ExpectedErr: graveler.ErrNotFound,
		},
	}

	for _, cas := range table {
		t.Run(cas.Name, func(t *testing.T) {
			ref, err := r.RevParse(ctx, "repo1", cas.Ref)
			if err != nil {
				if cas.ExpectedErr == nil || !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if cas.Expected != ref.CommitID() {
				t.Fatalf("got unexpected commit ID: %s - expected %s", ref.CommitID(), cas.Expected)
			}
		})
	}
}

func TestRefManager_DereferenceWithGraph(t *testing.T) {
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
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{}))

	G, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	H, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	I, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	J, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	D, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{G, H},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	E, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	F, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{I, J},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	B, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{D, E, F},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	C, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{F},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	A, err := r.AddCommit(context.Background(), "repo1", graveler.Commit{
		Parents: graveler.CommitParents{B, C},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resolve := func(base graveler.CommitID, mod string, expected graveler.CommitID) {
		ref := fmt.Sprintf("%s%s", base, mod)
		resolved, err := r.RevParse(context.Background(), "repo1", graveler.Ref(ref))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resolved.CommitID() != expected {
			t.Fatalf("expected %s == %s", ref, expected)
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
