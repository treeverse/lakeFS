package local_test

import (
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"github.com/treeverse/lakefs/pkg/api"
	"sort"
	"testing"
)

func TestDiffLocal(t *testing.T) {
	cases := []struct {
		Name     string
		Right    string
		Left     []api.ObjectStats
		Expected []*local.Change
	}{
		{
			Name:  "t1_no_diff",
			Right: "testdata/localdiff/t1",
			Left: []api.ObjectStats{
				{
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     1685099702,
				},
				{
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1685099715,
				},
			},
			Expected: []*local.Change{},
		},
		{
			Name:  "t1_local_before",
			Right: "testdata/localdiff/t1",
			Left: []api.ObjectStats{
				{
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1685099715,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: "added",
				},
			},
		},
		{
			Name:  "t1_local_after",
			Right: "testdata/localdiff/t1",
			Left: []api.ObjectStats{
				{
					Path:      "tub/r.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     1685099715,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: "added",
				},
				{
					Path: "sub/folder/f.txt",
					Type: "added",
				},
				{
					Path: "tub/r.txt",
					Type: "removed",
				},
			},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			left := cas.Left
			sort.SliceStable(left, func(i, j int) bool {
				return left[i].Path < left[j].Path
			})
			changes, err := local.DiffLocal(left, cas.Right)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != len(cas.Expected) {
				t.Fatalf("expected %d changes, got %d\n\n%v", len(cas.Expected), len(changes), changes)
			}
			for i, c := range changes {
				if c.Path != cas.Expected[i].Path {
					t.Errorf("expected path = '%s',  got =  '%s'", cas.Expected[i].Path, c.Path)
				}
				if c.Type != cas.Expected[i].Type {
					t.Errorf("expected type = '%s',  got =  '%s'", cas.Expected[i].Type, c.Type)
				}
			}
		})
	}
}
