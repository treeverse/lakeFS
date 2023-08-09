package local_test

import (
	"sort"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/local"
)

func TestDiffLocal(t *testing.T) {
	cases := []struct {
		Name       string
		LocalPath  string
		RemoteList []api.ObjectStats
		Expected   []*local.Change
	}{
		{
			Name:      "t1_no_diff",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []api.ObjectStats{
				{
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     1691420202,
				},
				{
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1691420202,
				},
			},
			Expected: []*local.Change{},
		},
		{
			Name:      "t1_modified",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []api.ObjectStats{
				{
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     169095766,
				},
				{
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(12),
					Mtime:     1690957665,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeModified,
				},
				{
					Path: "sub/folder/f.txt",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:      "t1_local_before",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []api.ObjectStats{
				{
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1691420202,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeAdded,
				},
			},
		},
		{
			Name:      "t1_local_after",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []api.ObjectStats{
				{
					Path:      "tub/r.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1690957665,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeAdded,
				},
				{
					Path: "sub/folder/f.txt",
					Type: local.ChangeTypeAdded,
				},
				{
					Path: "tub/r.txt",
					Type: local.ChangeTypeRemoved,
				},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			left := tt.RemoteList
			sort.SliceStable(left, func(i, j int) bool {
				return left[i].Path < left[j].Path
			})
			lc := make(chan api.ObjectStats, len(left))
			makeChan(lc, left)
			changes, err := local.DiffLocalWithHead(lc, tt.LocalPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != len(tt.Expected) {
				t.Fatalf("expected %d changes, got %d\n\n%v", len(tt.Expected), len(changes), changes)
			}
			for i, c := range changes {
				require.Equal(t, c.Path, tt.Expected[i].Path, "wrong path")
				require.Equal(t, c.Type, tt.Expected[i].Type, "wrong type")
			}
		})
	}
}

func makeChan[T any](c chan<- T, l []T) {
	for _, o := range l {
		c <- o
	}
	close(c)
}
