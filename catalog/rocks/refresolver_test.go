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
	//  "a5899fab11823646608324bb98549df39b335c515a5b6d19372a01e707400943",
	//	"7abd46b1ad3ad508091840a51fc6dd932501d676d16006f8129b215c51405db9",
	//	"ee222be6e5398fbc6b0a955142a819e4af4537da59f65b06cee002607ba75085",
	//	"95137b38639b2902354e7cb7d1aa3df7bf826ebce3fb69a109e1c01086f62fc8",
	//	"80398db2757f5ea0cb6c7dce906fa1e3a03d4a4c220a88a852102d2f371d61ec",
	//	"cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116",
	//	"c6915a2386812c43094ed3fb56845b76d2a4d88ed5074a7ba67196551c8e008c",
	//	"f0dcb06adb3fd79e827e641ffbda68d9712853c3dce02b0bfc0e266f44f5ab9d",
	//	"d05d6fd07d095190cac6e3f3f3e11e842ea6c0e694c2cd369f1026fb767110e8",
	//	"ef48f58cb5dba2754c8b1271f18e85fb58fc5cdccec3e9459c41f21c41de471b",
	//	"459aba33665c6eba4e9fa537a6b48347ad022d2a5af196a3f4f8341e6a105f5b",
	//	"903cdfe6bbb36fd60c790e52d808015ab5530df0bea23c07aef80cf87f8fb958",
	//	"95bb67123ad61e68688bd47b51a9a61b001b7c0aa378bbcde1435d164df8a896",
	//	"38fb704cf1b40daf8dc83672a5bfd879339d209786baf97c12866756252b4470",
	//	"22dcbfb5814efb714930ddb24946c4a465f7a666d82cda30c02c82d3fdd5a0ea",
	//	"e4ded0a935313e5f26d787e6f9cb2af4cc9d812c29d3ea5d990b6fd8e9f9660d",
	//	"706e04df978b0e8fa636fd8e13a775ae77303ee36fc69f2d5649751228b7f42e",
	//	"230e258e0bb8c337b92e1073eff3d92dc4c4331b5ccaccd4244c468c03725973",
	//	"1eecf99f174ee0f3abc0e04a73b534392dbff0b7645cfa9ef1a75963a93d5aeb",
	//	"5ee7f7268d93d97f51fde483f3d26869c42c2e18e2d690b49a82df7442c14836",

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch1", rocks.Branch{
		CommitID: "cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116",
	}))

	testutil.Must(t, r.SetBranch(context.Background(), "repo1", "branch2", rocks.Branch{
		CommitID: "c6915a2386812c43094ed3fb56845b76d2a4d88ed5074a7ba67196551c8e008c",
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
			Expected: rocks.CommitID("cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116"),
		},
		{
			Name:        "branch_doesnt_exist",
			Ref:         rocks.Ref("branch3"),
			ExpectedErr: rocks.ErrNotFound,
		},
		{
			Name:     "commit",
			Ref:      rocks.Ref("cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116"),
			Expected: rocks.CommitID("cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116"),
		},
		{
			Name:     "commit_prefix_good",
			Ref:      rocks.Ref("cb0fc65fe0"),
			Expected: rocks.CommitID("cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116"),
		},
		{
			Name:        "commit_prefix_ambiguous",
			Ref:         rocks.Ref("95"),
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
			Expected: rocks.CommitID("f0dcb06adb3fd79e827e641ffbda68d9712853c3dce02b0bfc0e266f44f5ab9d"),
		},
		{
			Name:     "commit_with_modifier",
			Ref:      rocks.Ref("cb0fc65fe0e4b08af9dae54b1749050969dd49cf2f070c82c65fde5cd0b98116~2"),
			Expected: rocks.CommitID("f0dcb06adb3fd79e827e641ffbda68d9712853c3dce02b0bfc0e266f44f5ab9d"),
		},
		{
			Name:     "commit_prefix_with_modifier",
			Ref:      rocks.Ref("cb0fc65fe~2"),
			Expected: rocks.CommitID("f0dcb06adb3fd79e827e641ffbda68d9712853c3dce02b0bfc0e266f44f5ab9d"),
		},
		{
			Name:        "commit_prefix_with_modifier_too_big",
			Ref:         rocks.Ref("f0dcb06adb3fd79e827e641ffbda68d9712853c3dce02b0bfc0e266f44f5ab9d~200"),
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
