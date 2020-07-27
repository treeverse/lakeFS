package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	testutil.MustDo(t, "create test repo",
		c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"))
	for i := 0; i < 5; i++ {
		n := i + 1
		filePath := fmt.Sprintf("/file%d", n)
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
			}, CreateEntryParams{}))
		if i == 2 {
			_, err := c.Commit(ctx, "repo1", "master", "commit test files", "tester", nil)
			testutil.MustDo(t, "commit test files", err)
		}
	}
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, "repo1", "master", "/file1"))

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
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
				{Path: "/file4", PhysicalAddress: "/addr4", Size: 40, Checksum: "49f014abae232570cc48072bac6b70531bba7e883ea04b448c6cbeed1446e6ff"},
				{Path: "/file5", PhysicalAddress: "/addr5", Size: 50, Checksum: "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"},
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
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last 2",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "/file3",
				limit:      2,
			},
			wantEntries: []Entry{
				{Path: "/file4", PhysicalAddress: "/addr4", Size: 40, Checksum: "49f014abae232570cc48072bac6b70531bba7e883ea04b448c6cbeed1446e6ff"},
				{Path: "/file5", PhysicalAddress: "/addr5", Size: 50, Checksum: "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"},
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
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
			},
			wantMore: false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, tt.args.repository, tt.args.reference, tt.args.path, tt.args.after, "", tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
			// copy the Entry's fields we like to compare
			var gotEntries []Entry
			for i, ent := range got {
				if ent == nil {
					t.Fatalf("Expected entry at index %d, found nil", i)
				}
				gotEntries = append(gotEntries, Entry{
					CommonLevel:     ent.CommonLevel,
					Path:            ent.Path,
					PhysicalAddress: ent.PhysicalAddress,
					Size:            ent.Size,
					Checksum:        ent.Checksum,
				})
			}

			if !reflect.DeepEqual(gotEntries, tt.wantEntries) {
				t.Errorf("ListEntries() got = %s, want = %s", spew.Sdump(gotEntries), spew.Sdump(tt.wantEntries))
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListEntries() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}

func TestCataloger_ListEntries_ByLevel(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	suffixList := []string{"rip0", "file1", "file2", "file2/xxx", "file3/", "file4", "file5", "file6/yyy", "file6/zzz/zzz", "file6/ccc", "file7", "file8", "file9", "filea",
		"/fileb", "//filec", "///filed", "rip"}
	for i, suffix := range suffixList {
		n := i + 1
		filePath := suffix
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := int64(n) * 10
		testutil.MustDo(t, "create test entry",
			c.CreateEntry(ctx, repo, "master", Entry{
				Path:            filePath,
				Checksum:        fileChecksum,
				PhysicalAddress: fileAddress,
				Size:            fileSize,
				Metadata:        nil,
			}, CreateEntryParams{}))

		if i == 3 {
			_, err := c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
			testutil.MustDo(t, "commit test files", err)
		}
	}
	// delete one file
	testutil.MustDo(t, "delete rip0 file",
		c.DeleteEntry(ctx, repo, "master", "rip0"))
	testutil.MustDo(t, "delete rip file",
		c.DeleteEntry(ctx, repo, "master", "rip"))

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
				repository: repo,
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
				repository: repo,
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
				repository: repo,
				reference:  "master",
				path:       "",
				after:      "file3",
				limit:      2,
			},
			wantEntries: []string{"file3/", "file4"},
			wantMore:    true,
			wantErr:     false,
		}, {
			name: "2 after file2/",
			args: args{
				repository: repo,
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
				repository: repo,
				reference:  "master:HEAD",
				path:       "",
				after:      "file1",
				limit:      100,
			},
			wantEntries: []string{"file2", "file2/", "rip0"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "slash",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "/",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"//", "/fileb"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "double slash",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "//",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"///", "//filec"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "under file6",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "file6/",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"file6/ccc", "file6/yyy", "file6/zzz/"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "under file6 after ccc",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "file6/",
				after:      "file6/ccc",
				limit:      100,
			},
			wantEntries: []string{"file6/yyy", "file6/zzz/"},
			wantMore:    false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, tt.args.repository, tt.args.reference, tt.args.path, tt.args.after, DefaultPathDelimiter, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListEntriesByLevel() err = %s, expected error %t", err, tt.wantErr)
			}
			// test that directories have null entries, and vice versa
			var gotNames []string
			for _, res := range got {
				if strings.HasSuffix(res.Path, DefaultPathDelimiter) != res.CommonLevel {
					t.Errorf("%s suffix doesn't match the CommonLevel = %t", res.Path, res.CommonLevel)
				}
				if !strings.HasSuffix(res.Path, res.Path) {
					t.Errorf("Name '%s' expected to be path '%s' suffix", res.Path, res.Path)
				}
				gotNames = append(gotNames, res.Path)
			}

			if !reflect.DeepEqual(gotNames, tt.wantEntries) {
				t.Errorf("ListEntriesByLevel got = %s, want = %s", spew.Sdump(gotNames), spew.Sdump(tt.wantEntries))
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListEntriesByLevel gotMore = %t, want = %t", gotMore, tt.wantMore)
			}
		})
	}
}

func TestCataloger_ListEntries_ByLevelDeleted(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.1", nil, "")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.2", nil, "")
	_, err := c.Commit(ctx, repo, "master", "commit two files", "tester", nil)

	testutil.MustDo(t, "commit 2 files", err)
	testutil.MustDo(t, "delete file 1",
		c.DeleteEntry(ctx, repo, "master", "place/to/go.1"))
	testutil.MustDo(t, "delete file 2",
		c.DeleteEntry(ctx, repo, "master", "place/to/go.2"))
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.0", nil, "")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.3", nil, "")
	_, err = c.Commit(ctx, repo, "master", "commit two more files", "tester", nil)
	testutil.MustDo(t, "commit 2 files", err)

	entries, hasMore, err := c.ListEntries(ctx, repo, "master", "place/to/", "", DefaultPathDelimiter, -1)
	testutil.MustDo(t, "list entries under place/to/", err)
	if len(entries) != 2 {
		t.Errorf("Expected two entries, got = %s", spew.Sdump(entries))
	}
	if hasMore {
		t.Errorf("ListEntriesByLevel() hasMore = %t, expected fasle", hasMore)
	}
}
