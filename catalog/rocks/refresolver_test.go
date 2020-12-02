package rocks_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/testutil"
)

func TestPGRefManager_Dereference(t *testing.T) {
	r := testRefManager(t)
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{}))

	ts, _ := time.Parse(time.RFC3339, "2020-12-01T15:00:00Z")
	var previous rocks.CommitID
	for i := 0; i < 20; i++ {
		c := rocks.Commit{
			Committer:    "user1",
			Message:      "message1",
			TreeID:       "deadbeef123",
			CreationDate: ts,
			Parents:      rocks.CommitParents{previous},
			Metadata:     catalog.Metadata{"foo": "bar"},
		}
		cid, err := r.AddCommit(context.Background(), "repo1", c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		previous = cid
	}

	iter, err := r.Log(context.Background(), "repo1", previous)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ids := make([]rocks.CommitID, 0)
	for iter.Next() {
		c := iter.Value()
		ids = append(ids, c.CommitID)
	}
	if iter.Err() != nil {
		t.Fatalf("unexpected error: %v", iter.Err())
	}

	//  commit log:
	//  "c607be9a1103b8bac51399a386648729e2bc9b7503dde5cd3de6318c58b79fd6",
	//	"c3c52217e0b35f3abdffdbe2d304e4114a6232b3d3063622eff0212f4e384321",
	//	"19d18225090199f0054588771a85df04c5dc13dda396b6d2b096e2ce0e95c654",
	//	"fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb",
	//	"d0891d8aef006e532e4816ada3babe8dfb0c382f0f941458abc0a608af48c96e",
	//	"958a88c4c7506772b15fde2b4095ce984a0cfa101cc6fc1a50573f68dc289872",
	//	"25898c1fcb316d782f9e522be6e4a0c7c316ec5a4b9bfecdd6f3577de70cfd34",
	//	"b3794a3cdd7fad101ed64e07b7d42010362adb0228b64b7db92b29699c06ffa7",
	//	"9385029ea6e0171ae75557fda1ea55774c5e57745a56bff6630b050a80d0370a",
	//	"c6b747a1caafef68942a03af99f274174cbbe593d6d1c9cbc836b3a127c79581",
	//	"f42efb7a556bc638c94ea46e121bb388bff2dbf39be01226274ced570417d3c8",
	//	"0c6efef612a3d6a6a6b3985208d27d456c76c7338839d51bb49ce33d06fbaae8",
	//	"84d304a943f4ecac7e3ad5f7489bef55ec0034c659d8c2b16680d1a9d2fe4fcc",
	//	"48714265f7e6d219e32e5ac7ac011056e2837638711b61fb0a3b5baf2ec0c78d",
	//	"689d81325fecd7bea9ef3acc1f0975476f40996ad0ca57703556b512aff2a1f7",
	//	"24f9a7d15802c298430185dafb35b1c3c625385525d973c855152e23eef47ca2",
	//	"95a5b23c5cc6a890de6868b07dc787b13fca078429cf69c41bcce04cd4094f1e",
	//	"a4085647a9868ae5b1c13abee15918377cdba065e26c37b1fc9e4d868851ea98",
	//	"a404202e948cad8dfbeed76a6686079fc63960484120017b7f9b387bab996b38",
	//	"5acae1bce3f1ebbc67371dab00b1474323e776bf09fd4a1bdee3de1549566938",

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch1", rocks.Branch{
		CommitID: "fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", rocks.Branch{
		CommitID: "0c6efef612a3d6a6a6b3985208d27d456c76c7338839d51bb49ce33d06fbaae8",
	}))

	table := []struct {
		Name        string
		Ref         rocks.Ref
		Expected    rocks.CommitID
		ExpectedErr error
	}{
		{
			Name:     "branch_exist",
			Ref:      rocks.Ref("branch1"),
			Expected: rocks.CommitID("fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb"),
		},
		{
			Name:        "branch_doesnt_exist",
			Ref:         rocks.Ref("branch3"),
			ExpectedErr: rocks.ErrNotFound,
		},
		{
			Name:     "commit",
			Ref:      rocks.Ref("fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb"),
			Expected: rocks.CommitID("fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb"),
		},
		{
			Name:     "commit_prefix_good",
			Ref:      rocks.Ref("fc51d"),
			Expected: rocks.CommitID("fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb"),
		},
		{
			Name:        "commit_prefix_ambiguous",
			Ref:         rocks.Ref("a40"),
			ExpectedErr: rocks.ErrNotFound,
		},
		{
			Name:        "commit_prefix_missing",
			Ref:         rocks.Ref("66666"),
			ExpectedErr: rocks.ErrNotFound,
		},
		{
			Name:     "branch_with_modifier",
			Ref:      rocks.Ref("branch1~2"),
			Expected: rocks.CommitID("958a88c4c7506772b15fde2b4095ce984a0cfa101cc6fc1a50573f68dc289872"),
		},
		{
			Name:     "commit_with_modifier",
			Ref:      rocks.Ref("fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb~2"),
			Expected: rocks.CommitID("958a88c4c7506772b15fde2b4095ce984a0cfa101cc6fc1a50573f68dc289872"),
		},
		{
			Name:     "commit_prefix_with_modifier",
			Ref:      rocks.Ref("fc51d~2"),
			Expected: rocks.CommitID("958a88c4c7506772b15fde2b4095ce984a0cfa101cc6fc1a50573f68dc289872"),
		},
		{
			Name:        "commit_prefix_with_modifier_too_big",
			Ref:         rocks.Ref("fc51d43fb19e5b28410663ff1df1a34665b36d86c684ddbcdef520ec170081cb~200"),
			ExpectedErr: rocks.ErrNotFound,
		},
	}

	for _, cas := range table {
		t.Run(cas.Name, func(t *testing.T) {
			ref, err := r.RevParse(context.Background(), "repo1", cas.Ref)
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

func TestPGRefManager_DereferenceWithGraph(t *testing.T) {
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
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{}))

	G, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	H, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	I, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	J, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	D, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{G, H},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	E, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	F, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{I, J},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	B, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{D, E, F},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	C, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{F},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	A, err := r.AddCommit(context.Background(), "repo1", rocks.Commit{
		Parents: rocks.CommitParents{B, C},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resolve := func(base rocks.CommitID, mod string, expected rocks.CommitID) {
		ref := fmt.Sprintf("%s%s", base, mod)
		resolved, err := r.RevParse(context.Background(), "repo1", rocks.Ref(ref))
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
