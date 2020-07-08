package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListEntriesByLevel(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	testutil.MustDo(t, "create test repo",
		c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"))
	for i := 0; i < 7; i++ {
		n := i + 1
		filePath := fmt.Sprintf("/file%d", n)
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := int64(n) * 10
		if n >= 6 {
			filePath += "/xx" + strconv.Itoa(n)
		}
		testutil.MustDo(t, "create test entry",
			c.CreateEntry(ctx, "repo1", "master", Entry{
				Path:            filePath,
				Checksum:        fileChecksum,
				PhysicalAddress: fileAddress,
				Size:            fileSize,
				Metadata:        nil,
			}))
		if i == 2 {
			_, err := c.Commit(ctx, "repo1", "master", "commit test files", "tester", nil)
			testutil.MustDo(t, "commit test files", err)
		}
	}

	type args struct {
		repository string
		reference  string
		path       string
		after      string
		limit      int
	}
	tests := []struct {
		name        string
		args        args
		wantEntries []Entry
		wantMore    bool
		wantErr     bool
	}{
		{
			name: "all uncommitted",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "",
				limit:      -1,
			},
			wantEntries: []Entry{
				{Path: "/file1"},
				{Path: "/file2"},
				{Path: "/file2/"},
				{Path: "/file3"},
				{Path: "/file4"},
				{Path: "/file5"},
				{Path: "/file6/"},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "first 2 uncommitted",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "",
				limit:      2,
			},
			wantEntries: []Entry{
				{Path: "/file1"},
				{Path: "/file2"},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last 3",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "/file3",
				limit:      2,
			},
			wantEntries: []Entry{
				{Path: "/file4"},
				{Path: "/file5"},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "committed",
			args: args{
				repository: "repo1",
				reference:  "master:HEAD",
				path:       "",
				after:      "/file1",
				limit:      -1,
			},
			wantEntries: []Entry{
				{Path: "/file2"},
				{Path: "/file3"},
			},
			wantMore: false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntriesByLevel(ctx, tt.args.repository, tt.args.reference, tt.args.path, "/", tt.args.after, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
			// copy the Entry fields we like to compare
			var gotEntries []Entry
			for _, ent := range got {
				gotEntries = append(gotEntries, Entry{
					Path:            ent.Entry.Path,
					PhysicalAddress: ent.Entry.PhysicalAddress,
					Size:            ent.Entry.Size,
					Checksum:        ent.Entry.Checksum,
				})
			}

			if !reflect.DeepEqual(gotEntries, tt.wantEntries) {
				t.Errorf("ListEntries() got = %+v, want = %+v", gotEntries, tt.wantEntries)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListEntries() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}
