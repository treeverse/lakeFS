package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListEntriesByLevel(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	testutil.MustDo(t, "create test repo",
		c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"))
	suffixList := []string{"file1", "file2", "file2/xxx", "file3/", "file4", "file5", "file6/yyy", "file6/zzz/zzz", "file6/ccc", "file7", "file8", "file9", "filea",
		"/fileb", "//filec", "///filed"}
	for i, suffix := range suffixList {
		n := i + 1
		filePath := suffix
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := int64(n) * 10
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
		wantEntries []string
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
				limit:      100,
			},
			wantEntries: []string{"/", "file1", "file2", "file2/", "file3/", "file4", "file5", "file6/", "file7", "file8", "file9", "filea"},
			wantMore:    false,
			wantErr:     false,
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
			wantEntries: []string{"/", "file1"},
			wantMore:    true,
			wantErr:     false,
		},
		{
			name: "2 after file3",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "file3",
				limit:      2,
			},
			wantEntries: []string{"file4", "file5"},
			wantMore:    true,
			wantErr:     false,
		}, {
			name: "2 after file2/",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "file2/",
				limit:      2,
			},
			wantEntries: []string{"file3/", "file4"},
			wantMore:    true,
			wantErr:     false,
		},
		{
			name: "committed",
			args: args{
				repository: "repo1",
				reference:  "master:HEAD",
				path:       "",
				after:      "file1",
				limit:      100,
			},
			wantEntries: []string{"file2", "file2/"},
			wantMore:    false,
			wantErr:     false,
		}, {
			name: "slash",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "/",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"/", "fileb"},
			wantMore:    false,
			wantErr:     false,
		}, {
			name: "double slash",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "//",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"/", "filec"},
			wantMore:    false,
			wantErr:     false,
		}, {
			name: "under file6",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "file6/",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"ccc", "yyy", "zzz/"},
			wantMore:    false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntriesByLevel(ctx, tt.args.repository, tt.args.reference, tt.args.path, tt.args.after, "/", tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf(" error = %v, wantErr %v", err, tt.wantErr)
			}
			// test that directories have null entries, and vice versa
			for _, res := range got {
				resEnd := res.Path[len(res.Path)-1:]
				if resEnd == "/" && res.Entry != nil {
					t.Errorf("%s is a directory, pointer to entry is not nil", res.Path)
				} else if resEnd != "/" && res.Entry == nil {
					t.Errorf("%s is an entry,but pointer to entry is nil", res.Path)
				}
			}
			// copy the Entry fields we like to compare
			var gotNames []string
			for _, ent := range got {
				gotNames = append(gotNames, ent.Path)
			}

			if !reflect.DeepEqual(gotNames, tt.wantEntries) {
				t.Errorf("got = %+v, want = %+v", gotNames, tt.wantEntries)
			}
			if gotMore != tt.wantMore {
				t.Errorf(" gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}
