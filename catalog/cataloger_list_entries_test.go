package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-test/deep"
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
			wantEntries: []string{"file2/", "file3/"},
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
			wantEntries: []string{"file1", "file2", "file2/", "rip0"},
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
			wantEntries: []string{"file6/ccc", "file6/yyy", "file6/zzz/"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "specific file",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "file6/zzz/zzz",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"file6/zzz/zzz"},
			wantMore:    false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, tt.args.repository, tt.args.reference, tt.args.path, tt.args.after, DefaultPathDelimiter, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListEntries() err = %s, expected error %t", err, tt.wantErr)
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
				t.Errorf("ListEntries got = %s, want = %s", spew.Sdump(gotNames), spew.Sdump(tt.wantEntries))
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListEntries gotMore = %t, want = %t", gotMore, tt.wantMore)
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
		t.Errorf("ListEntries() hasMore = %t, expected fasle", hasMore)
	}
}

func TestCataloger_ListByLevel_Delete(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 150; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "master", "xxx/entry"+pathExt(i), nil, strconv.Itoa(i*10000))
	}
	_, err := c.Commit(ctx, repo, "master", "message", "committer1", nil)
	testutil.MustDo(t, "commit to master", err)
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")

	testCatalogerBranch(t, ctx, c, repo, "br_2", "br_1")
	for i := 0; i < 100; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "br_2", "xxx/entry"+pathExt(i), nil, strconv.Itoa(i*10000))
	}
	for i := 0; i < 100; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "br_1", "xxx/entry"+pathExt(i), nil, strconv.Itoa(i*10000))
	}

	_, err = c.Commit(ctx, repo, "br_1", "message", "committer1", nil)
	testutil.MustDo(t, "commit to br_1", err)

	for i := 0; i < 100; i++ {
		testutil.MustDo(t, "delete entry "+"xxx/entry"+pathExt(i),
			c.DeleteEntry(ctx, repo, "br_2", "xxx/entry"+pathExt(i)))
	}

	got, gotMore, err := c.ListEntries(ctx, repo, "br_2", "", "", "/", 20)
	testCatalogerListEntriesVerifyResponse(t, got, gotMore, err, []string{"xxx/"})
}

func TestCataloger_ListByLevel_DirectoriesAndTombstones(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			testCatalogerCreateEntry(t, ctx, c, repo, "master", "xxx"+pathExt(i)+"/entry"+pathExt(j), nil, strconv.Itoa(i*10000))
		}
	}
	_, err := c.Commit(ctx, repo, "master", "message", "committer1", nil)
	testutil.MustDo(t, "commit to master", err)
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")
	for i := 0; i < 10; i += 2 {
		for j := 0; j < 10; j += 2 {
			testCatalogerCreateEntry(t, ctx, c, repo, "br_1", "xxx"+pathExt(i)+"/entry"+pathExt(j), nil, strconv.Itoa(i*10000))
		}
	}
	_, err = c.Commit(ctx, repo, "br_1", "message", "committer1", nil)
	testutil.MustDo(t, "commit to br_1", err)
	testCatalogerBranch(t, ctx, c, repo, "br_2", "br_1")
	for i := 0; i < 10; i += 3 {
		for j := 0; j < 10; j++ {
			testCatalogerCreateEntry(t, ctx, c, repo, "br_2", "xxx"+pathExt(i)+"/entry"+pathExt(j), nil, strconv.Itoa(i*10000))
		}
	}
	_, err = c.Commit(ctx, repo, "br_2", "message", "committer1", nil)
	testutil.MustDo(t, "commit to br_2", err)
	for i := 0; i < 10; i += 3 {
		for j := 0; j < 10; j++ {
			testutil.MustDo(t, "delete entry "+"xxx"+pathExt(i)+"/entry"+pathExt(j),
				c.DeleteEntry(ctx, repo, "br_2", "xxx"+pathExt(i)+"/entry"+pathExt(j)))
		}
	}
	wantEntries := []string{"xxx001/", "xxx002/", "xxx004/", "xxx005/", "xxx007/", "xxx008/"}
	got, gotMore, err := c.ListEntries(ctx, repo, "br_2", "", "", "/", 20)
	testCatalogerListEntriesVerifyResponse(t, got, gotMore, err, wantEntries)

	wantEntries = []string{"xxx002/entry000", "xxx002/entry001", "xxx002/entry002", "xxx002/entry003",
		"xxx002/entry004", "xxx002/entry005", "xxx002/entry006", "xxx002/entry007", "xxx002/entry008",
		"xxx002/entry009"}
	got, gotMore, err = c.ListEntries(ctx, repo, "br_2", "xxx002/", "", "/", 20)
	testCatalogerListEntriesVerifyResponse(t, got, gotMore, err, wantEntries)
}

func testCatalogerListEntriesVerifyResponse(t *testing.T, got []*Entry, gotMore bool, gotErr error, entries []string) {
	t.Helper()
	if gotErr != nil {
		t.Fatalf("Got expected error: %s", gotErr)
	}
	if len(entries) != len(got) {
		t.Fatalf("Got %d entries, expected %d", len(got), len(entries))
	}
	for i, p := range entries {
		if p != got[i].Path {
			t.Fatalf("Entry path '%s', expected '%s'", p, got[i].Path)
		}
	}
	if gotMore {
		t.Fatalf("not expected more")
	}
}

func pathExt(i int) string {
	return fmt.Sprintf("%03d", i)
}

func TestCataloger_ListEntries_Prefix(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	prefixes := []string{"aaa", "bbb", "ccc"}
	for _, prefix := range prefixes {
		testCatalogerCreateEntry(t, ctx, c, repo, "master", prefix+"/index.txt", nil, "")
		for i := 0; i < 3; i++ {
			testCatalogerCreateEntry(t, ctx, c, repo, "master", prefix+"/file."+strconv.Itoa(i), nil, "")
		}
	}

	tests := []struct {
		name        string
		path        string
		want        []string
		wantByLevel []string
	}{
		{
			name:        "specific file",
			path:        "bbb/file.2",
			want:        []string{"bbb/file.2"},
			wantByLevel: []string{"bbb/file.2"},
		},
	}
	for _, tt := range tests {
		// without delimiter
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, repo, "master", tt.path, "", "", -1)
			if err != nil {
				t.Fatalf("ListEntries err = %s, expected no error", err)
			}
			gotPaths := extractEntriesPaths(got)
			if diff := deep.Equal(gotPaths, tt.wantByLevel); diff != nil {
				t.Fatal("ListEntries", diff)
			}
			if gotMore != false {
				t.Fatal("ListEntries got more should be false")
			}
		})
		// by level
		t.Run(tt.name+"-by level", func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, repo, "master", tt.path, "", DefaultPathDelimiter, -1)
			if err != nil {
				t.Fatalf("ListEntries by level - err = %s, expected no error", err)
			}
			gotPaths := extractEntriesPaths(got)
			if diff := deep.Equal(gotPaths, tt.wantByLevel); diff != nil {
				t.Fatal("ListEntries by level", diff)
			}
			if gotMore != false {
				t.Fatal("ListEntries got more should be false")
			}
		})
	}
}

func extractEntriesPaths(entries []*Entry) []string {
	result := make([]string, len(entries))
	for i, ent := range entries {
		result[i] = ent.Path
	}
	return result
}

func TestCataloger_ListEntries_Uncommitted(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "my_entry", nil, "abcd")
	_, err := c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
	testutil.MustDo(t, "commit my_entry", err)
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, repo, "master", "my_entry"))
	// an uncommitted tombstone "hides" a committed entry
	got, _, err := c.ListEntries(ctx, repo, "master", "", "", "/", -1)
	if err != nil {
		t.Fatalf("ListEntries err = %s, expected no error", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListEntries listLen = %d, expected no error", len(got))
	}
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "my_entry", nil, "abcd")
	// an uncommitted entry is detected
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", "/", -1)
	if err != nil {
		t.Fatalf("ListEntries err = %s, expected 0", err)
	}
	if len(got) != 1 {
		t.Fatalf("ListEntries listLen = %d, expected 1", len(got))
	}
	if got[0].Path != "my_entry" {
		t.Fatalf("ListEntries got entry = %s, expected my_entry", got[0].Path)
	}
	_, err = c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
	testutil.MustDo(t, "commit my_entry", err)
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, repo, "master", "my_entry"))
	_, err = c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
	testutil.MustDo(t, "commit my_entry deletion", err)
	// deleted entry will not be displayed
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", "/", -1)
	if err != nil {
		t.Fatalf("ListEntries err = %s, expected no error", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListEntries listLen = %d, expected no error", len(got))
	}

	testutil.MustDo(t, "commit my_entry", err)
	// an uncommitted entry is detected even if the entry is deleted
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "my_entry", nil, "abcd")
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", "/", -1)
	if err != nil {
		t.Fatalf("ListEntries err = %s, expected 0", err)
	}
	if len(got) != 1 {
		t.Fatalf("ListEntries listLen = %d, expected 1", len(got))
	}
	if got[0].Path != "my_entry" {
		t.Fatalf("ListEntries got entry = %s, expected my_entry", got[0].Path)
	}
}
