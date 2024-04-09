package local_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/local"
)

const (
	diffTestCorrectTime = 1691570412
)

func TestDiffLocal(t *testing.T) {
	cases := []struct {
		Name       string
		LocalPath  string
		RemoteList []apigen.ObjectStats
		Expected   []*local.Change
	}{
		{
			Name:      "t1_no_diff",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
				},
			},
			Expected: []*local.Change{},
		},
		{
			Name:      "t1_modified",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     169095766,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(12),
					Mtime:     1690957665,
				},
			},
			Expected: []*local.Change{
				{
					Path: "sub/f.txt",
					Type: local.ChangeTypeModified,
				}, {
					Path: "sub/folder/f.txt",
					Type: local.ChangeTypeModified,
				},
			},
		},
		{
			Name:      "t1_local_before",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(64),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
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
			RemoteList: []apigen.ObjectStats{{
				Path:      ".hidden-file",
				SizeBytes: swag.Int64(64),
				Mtime:     diffTestCorrectTime,
			}, {
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
		{
			Name:      "t1_hidden_changed",
			LocalPath: "testdata/localdiff/t1",
			RemoteList: []apigen.ObjectStats{
				{
					Path:      ".hidden-file",
					SizeBytes: swag.Int64(17),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/f.txt",
					SizeBytes: swag.Int64(3),
					Mtime:     diffTestCorrectTime,
				}, {
					Path:      "sub/folder/f.txt",
					SizeBytes: swag.Int64(6),
					Mtime:     diffTestCorrectTime,
				},
			},
			Expected: []*local.Change{
				{
					Path: ".hidden-file",
					Type: local.ChangeTypeModified,
				},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			fixTime(t, tt.LocalPath)
			left := tt.RemoteList
			sort.SliceStable(left, func(i, j int) bool {
				return left[i].Path < left[j].Path
			})
			lc := make(chan apigen.ObjectStats, len(left))
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

func fixTime(t *testing.T, localPath string) {
	err := filepath.WalkDir(localPath, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			return os.Chtimes(path, time.Now(), time.Unix(diffTestCorrectTime, 0))
		}
		return nil
	})
	require.NoError(t, err)
}
