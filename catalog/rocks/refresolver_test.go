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
	//  "c5262f504dd81a34e6e7578ad66a8395bee18ea3d198c5f5d74830eb43098279",
	//  "f2712b1cace8d1d6aa2638a1fe12d2428a1a001943f890c086702eb6eb0a5606",
	//  "1f3a725fe62d9e5bfe5abc74ad75162d27416935d16d842afa808c6f2f880084",
	//  "992ac1278647b8a7f374b1bfed8d8bed82e6cb16f8d50f272fc48d5f998182f0",
	//  "d5e1ff6470911b0a5d3f587a3db09504f8d3fef9a34ab9025a003b3bc1636023",
	//  "5ce8db7eeacafa343783adcb885aa6239f5df70c416826a5d78519a3ca2a6313",
	//  "a42282eab74cfd036ab0fa6ddc070fd3541632ee008f3a3f7de611ad4f395653",
	//  "798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c",
	//  "a4f672a9a1ceb49729dfd159fc0d9786765a09c8b178abd731953cf4c72e1542",
	//  "2fee5fd15a7bc6802ba4d4f8febc46519a8334012cba5b942fb03ed1ebd2b2b9",
	//  "cbebc01677dd5f39736f48e59620258aef5603de91fc713c5002330c0ed0103d",
	//  "680d546cf6cb876a6def445f8b15dc6ceab61a4193af9b623dcdfa1abc0a6d01",
	//  "36669c265362c2e07f8ffd413c125eac2ac90eacf65cf51b7f79c56f815813b2",
	//  "2077138a1aa3fb43c1ab7e2ba616c56d46bd069a1197ba00de8744c79f493c5c",
	//  "fb63f72ba0994dcbcbab8d63ce166dbff114ad2f390a175902134bc85d96fbb2",
	//  "7fc0e540820668488479f86c41828a85144993b196a5bf2405d37847438a78f4",
	//  "128233c82d80f91d62cb9bb49fd589ad8249fb4567a915f2a7dafd88d82c7527",
	//  "0dcaed021298fa2c4a4d99bbecb5630646363b6a0acc64e9113251d1511260c7",
	//  "f17975abb9442d7cd838c3e5ef63449f4ace261bd6975a51ddd343a2e654e2ad",
	//  "a66bb92bd9b389f942adb2d5e9e50e63d04d510dd2945da104b7f35c1302378c",

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch1", rocks.Branch{
		CommitID: "798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", rocks.Branch{
		CommitID: "2077138a1aa3fb43c1ab7e2ba616c56d46bd069a1197ba00de8744c79f493c5c",
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
			Expected: rocks.CommitID("798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c"),
		},
		{
			Name:        "branch_doesnt_exist",
			Ref:         rocks.Ref("branch3"),
			ExpectedErr: rocks.ErrNotFound,
		},
		{
			Name:     "commit",
			Ref:      rocks.Ref("798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c"),
			Expected: rocks.CommitID("798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c"),
		},
		{
			Name:     "commit_prefix_good",
			Ref:      rocks.Ref("798300f7"),
			Expected: rocks.CommitID("798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c"),
		},
		{
			Name:        "commit_prefix_ambiguous",
			Ref:         rocks.Ref("a4"),
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
			Expected: rocks.CommitID("2fee5fd15a7bc6802ba4d4f8febc46519a8334012cba5b942fb03ed1ebd2b2b9"),
		},
		{
			Name:     "commit_with_modifier",
			Ref:      rocks.Ref("798300f78bf9c1c6f4df8b87976fa9efa1b317070d49b7c54cd282b25c4db39c~2"),
			Expected: rocks.CommitID("2fee5fd15a7bc6802ba4d4f8febc46519a8334012cba5b942fb03ed1ebd2b2b9"),
		},
		{
			Name:     "commit_prefix_with_modifier",
			Ref:      rocks.Ref("798300f~2"),
			Expected: rocks.CommitID("2fee5fd15a7bc6802ba4d4f8febc46519a8334012cba5b942fb03ed1ebd2b2b9"),
		},
		{
			Name:        "commit_prefix_with_modifier_too_big",
			Ref:         rocks.Ref("2fee5fd15a7bc6802ba4d4f8febc46519a8334012cba5b942fb03ed1ebd2b2b9~200"),
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
